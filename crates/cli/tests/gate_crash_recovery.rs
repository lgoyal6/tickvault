//! **Phase 3 gate.**
//!
//! > Kill the process at randomized offsets during sustained write, restart,
//! > assert every file reads back and the truncation point is recorded.
//!
//! A real `SIGKILL` to a real child process, not a simulated one. Simulating a
//! crash by dropping a writer would test the code paths I remembered to write;
//! killing the process tests the ones I did not.
//!
//! The soak writes rows whose `seq` counts up from a value this test chooses,
//! one distinct range per round. That turns three vaguer questions into
//! checkable ones:
//!
//! - **Nothing is lost in the middle.** Each round's surviving rows must form a
//!   contiguous run from where that round started. A kill may cost the tail,
//!   which is the rotation bound; a hole in the middle would mean a file was
//!   silently skipped.
//! - **Nothing is counted twice.** No `seq` may appear more than once. A file
//!   adopted twice, or a compaction half-applied, would show up here.
//! - **Every loss is named.** If any round lost its tail, the manifest must
//!   carry a truncation record for it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use arrow::array::{Array, UInt64Array};
use tickvault::store::reader::{ArchiveReader, read_batches};
use tickvault::store::recovery;

/// Distinct `seq` range per round, wide enough that no round can run into the
/// next even at full speed.
const STRIDE: u64 = 100_000_000;
const ROUNDS: u64 = 8;

/// Deterministic offsets, so a failure can be reproduced exactly.
fn kill_after(round: u64) -> Duration {
    let mut x = round.wrapping_mul(0x9E3779B97F4A7C15) | 1;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    // Between 120ms and 620ms: long enough to have opened a file and started
    // writing, short enough that the test stays quick.
    Duration::from_millis(120 + (x.wrapping_mul(0x2545F4914F6CDD1D) % 500))
}

/// Every file under `root`, as paths relative to it.
fn walk(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(
                    path.strip_prefix(root)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    out.sort();
    out
}

/// Block until the soak has demonstrably started writing.
///
/// Waiting a fixed number of milliseconds and hoping made the test depend on
/// how long a cold binary takes to start, which is not what it is measuring.
/// Killing a process that had not written anything yet proves nothing about
/// crash recovery.
fn wait_until_writing(root: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if walk(root).iter().any(|p| p.contains(".parquet")) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

fn wall_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Every `seq` in the archive, and how many times each appears.
fn read_seqs(root: &Path) -> BTreeMap<u64, usize> {
    let reader = ArchiveReader::open(root).expect("open archive");
    let mut counts = BTreeMap::new();
    for record in reader.files() {
        for batch in read_batches(reader.path_of(record)).expect("read file") {
            let seq = batch
                .column_by_name("seq")
                .expect("seq column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("seq is u64");
            for i in 0..seq.len() {
                if !seq.is_null(i) {
                    *counts.entry(seq.value(i)).or_insert(0) += 1;
                }
            }
        }
    }
    counts
}

#[test]
#[ignore = "spawns and SIGKILLs a real process; run with --ignored"]
fn killing_the_writer_never_leaves_an_unreadable_archive() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let binary = env!("CARGO_BIN_EXE_tickvault");

    let mut killed_rounds = 0;
    for round in 0..ROUNDS {
        let mut child = Command::new(binary)
            .args([
                "soak",
                "--archive",
                root.to_str().unwrap(),
                "--start-seq",
                &(round * STRIDE).to_string(),
                // Rotate often, so a kill lands mid-file most of the time and
                // the surviving data is bounded tightly.
                "--rotate-ms",
                "120",
                "--batch-rows",
                "100",
                "--pace-us",
                "200",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the soak process");

        assert!(
            wait_until_writing(root, Duration::from_secs(10)),
            "round {round}: the soak never started writing"
        );
        // Randomised from here, so the kill lands at an arbitrary point *in the
        // writing* rather than at an arbitrary point in process startup.
        std::thread::sleep(kill_after(round));
        // SIGKILL: no unwinding, no destructors, no flush. Exactly what a
        // power loss or an OOM kill looks like to the archive.
        child.kill().expect("kill the soak process");
        let status = child.wait().expect("reap the soak process");
        assert!(
            !status.success(),
            "round {round} exited cleanly, so it was not killed"
        );
        killed_rounds += 1;

        // Every file the archive currently vouches for must read, at every
        // point between kills, not only at the end.
        let recovered = recovery::recover(root, wall_now()).expect("recover");
        let report = ArchiveReader::open(root).expect("open").verify();
        assert!(
            report.is_clean(),
            "round {round}: archive unreadable after a kill\n{report}\nrecovery: {recovered}"
        );
    }

    assert_eq!(killed_rounds, ROUNDS);

    // ---- every file reads back ----
    let reader = ArchiveReader::open(root).expect("open archive");
    let report = reader.verify();
    assert!(report.is_clean(), "{report}");
    assert!(
        report.files_checked > 0 && report.rows_read > 0,
        "the soak wrote nothing, so this proved nothing: {report}"
    );

    // ---- nothing counted twice ----
    let counts = read_seqs(root);
    let duplicated: Vec<u64> = counts
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(seq, _)| *seq)
        .take(5)
        .collect();
    assert!(
        duplicated.is_empty(),
        "these sequence numbers appear more than once: {duplicated:?}"
    );
    assert_eq!(
        report.rows_read as usize,
        counts.len(),
        "row count and distinct sequence numbers disagree"
    );

    // ---- loss is confined to the tail of each round ----
    let present: BTreeSet<u64> = counts.keys().copied().collect();
    let mut rounds_that_lost_data = 0;
    for round in 0..ROUNDS {
        let start = round * STRIDE;
        let mine: Vec<u64> = present
            .range(start..start + STRIDE)
            .copied()
            .map(|s| s - start)
            .collect();
        if mine.is_empty() {
            // Killed before anything was ever closed. Nothing to check, but it
            // is still a round that lost data.
            rounds_that_lost_data += 1;
            continue;
        }
        assert_eq!(mine[0], 0, "round {round} is missing its own first rows");
        let expected: Vec<u64> = (0..mine.len() as u64).collect();
        assert_eq!(
            mine, expected,
            "round {round} has a hole in the middle, not just a truncated tail"
        );
        rounds_that_lost_data += 1;
    }

    // ---- every loss is named ----
    let truncations = reader.manifest().truncations();
    assert!(
        !truncations.is_empty(),
        "{rounds_that_lost_data} rounds were killed mid-write and not one \
         truncation was recorded"
    );
    for t in truncations {
        assert!(!t.reason.is_empty());
        assert!(
            t.quarantined.is_some(),
            "a truncation must say where the wreckage went"
        );
        assert!(t.lost_to_wall > 0);
        assert_eq!(
            t.unrecoverable_rows, None,
            "a footerless file cannot be counted; None says so, zero would lie"
        );
    }

    println!(
        "{ROUNDS} kills: {} files, {} rows, {} truncation(s) recorded, no duplicates, no interior holes",
        report.files_checked,
        report.rows_read,
        truncations.len()
    );
}

#[test]
#[ignore = "spawns and SIGKILLs a real process; run with --ignored"]
fn a_killed_run_leaves_wreckage_that_the_next_run_sets_aside() {
    // The narrower claim, isolated: one kill, and the interrupted file must be
    // quarantined rather than read, with the loss recorded.
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();

    let mut child = Command::new(env!("CARGO_BIN_EXE_tickvault"))
        .args([
            "soak",
            "--archive",
            root.to_str().unwrap(),
            "--rotate-ms",
            "150",
            "--batch-rows",
            "100",
            "--pace-us",
            "200",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    assert!(
        wait_until_writing(root, Duration::from_secs(10)),
        "the soak never started writing"
    );
    // Far enough past the 150ms rotation that a file is certainly open.
    std::thread::sleep(Duration::from_millis(400));
    child.kill().expect("kill");
    let output = child.wait_with_output().expect("reap");

    // Before recovery there is a file on disk that no reader can open.
    let on_disk = walk(root);
    assert!(
        on_disk.iter().any(|p| p.ends_with(".parquet.partial")),
        "the soak left no interrupted file, so there is nothing to recover.\n\
         on disk: {on_disk:?}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let outcome = recovery::recover(root, wall_now()).expect("recover");
    assert!(
        !outcome.was_clean(),
        "a SIGKILL mid-write must leave something to recover"
    );
    assert_eq!(outcome.quarantined.len(), 1, "{outcome}");
    assert_eq!(outcome.truncations.len(), 1);
    assert!(outcome.truncations[0].reason.contains("footer"));

    // The wreckage is still on disk for a human, just not in the archive.
    let quarantined = root.join(&outcome.quarantined[0]);
    assert!(quarantined.exists());
    assert!(ArchiveReader::open(root).expect("open").verify().is_clean());

    // And running again is a no-op.
    let second = recovery::recover(root, wall_now()).expect("recover");
    assert!(second.was_clean(), "{second}");
}
