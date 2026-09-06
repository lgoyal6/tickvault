//! **Restore gate.**
//!
//! A backup is worth exactly what a restore of it can be shown to be worth. So
//! this restores a recorded archive into an empty directory nothing else has
//! touched, and then asks the restored copy the questions a consumer would:
//!
//! - **Counts match.** Every file the source manifest vouched for is present,
//!   with the same rows, and verification reads the pages to say so rather than
//!   trusting a footer.
//! - **No gaps inside the covered window.** The manifest's own truncation
//!   records are the only holes, and the restored copy declares the same window
//!   as the source rather than a wider one.
//! - **Sequence is monotonic within an epoch, and only within one.** A
//!   reconnect restarts the venue's numbering, so a restore that checked
//!   monotonicity across the whole partition would either fail on healthy data
//!   or, worse, be written to pass by sorting -- which would reorder the tape.
//! - **The rebuild is identical.** Same instant, same book, same digest.
//!
//! A restore that is silently partial is the failure mode worth most of this
//! file: it verifies, it rebuilds, and the book it produces is a real book from
//! a real venue that is simply missing an hour. The third case here is that one.
//!
//! # What the timer covers, and why that took a correction
//!
//! An earlier version of this file timed only the byte copy. It then printed
//! that duration next to a row count taken from the manifest, which produced
//! "38.8 ms for 300,090 rows" -- a copy rate wearing a throughput figure's
//! clothes, because not one of those rows was decoded inside the timed region.
//!
//! A restore is not over when the bytes land. It is over when the pages read
//! back, because a Parquet file that copied cleanly and does not decode is not
//! a recovered archive, it is a recovered file. So the phases are timed
//! separately and the recovery time is their sum, and every row count reported
//! beside a duration is a count of rows this test actually decoded.

mod common;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use arrow::array::{Array, TimestampNanosecondArray, UInt8Array, UInt64Array};

use tickvault::book::{BookDelta, BookSnapshot, LevelChange};
use tickvault::clock::{Stamp, Timestamps};
use tickvault::fixed::Fixed;
use tickvault::reconstruct::{Reconstructor, Request};
use tickvault::store::manifest::{FileRecord, Manifest};
use tickvault::store::reader::{self, ArchiveReader};
use tickvault::store::schema::RowBuilder;
use tickvault::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};
use tickvault::types::{Side, Symbol, VenueId};

const EPOCH: i64 = 1_787_000_000_000_000_000;
const TICK: i64 = 1_000_000;
const VENUE: VenueId = VenueId::Coinbase;

/// Messages per connection, and how many connections the recording spans.
const PER_CONNECTION: usize = 400;
const CONNECTIONS: usize = 3;

fn f(s: &str) -> Fixed {
    Fixed::from_decimal_str(s).expect("decimal literal")
}

fn symbol() -> Symbol {
    Symbol::new("BTC", "USD")
}

fn stamps(wall: i64) -> Timestamps {
    Timestamps::new(
        Stamp {
            mono_nanos: (wall - EPOCH) as u64,
            wall_nanos: wall,
        },
        Some(wall - 250_000),
    )
}

/// Record an archive spanning `CONNECTIONS` connections.
///
/// Each connection opens with a snapshot and then numbers its deltas from one,
/// which is what the recorder writes: every reconnect re-snapshots, and the
/// venue's counter is scoped to the socket. The snapshot rows are therefore the
/// epoch boundaries in the archive, and the checks below rely on that being
/// true rather than on a column that does not exist.
fn record(root: &Path, rows_per_file: usize) -> ArchiveWriter {
    let mut writer = ArchiveWriter::open(WriterConfig {
        max_rows_per_file: rows_per_file,
        max_file_age: std::time::Duration::from_secs(86_400),
        ..WriterConfig::new(root)
    })
    .expect("open archive");

    let mut wall = EPOCH;
    let mut builder = RowBuilder::new();
    let mut key = PartitionKey::new(VENUE, &symbol(), wall);

    for connection in 0..CONNECTIONS {
        // A fresh connection re-snapshots before it sends a delta.
        builder.push_snapshot(
            VENUE,
            &BookSnapshot {
                symbol: symbol(),
                bids: vec![(f("60000"), f("1"))],
                asks: vec![(f("60001"), f("1"))],
                seq: Some(0),
                checksum: None,
                stamps: stamps(wall),
            },
            (connection * (PER_CONNECTION + 1)) as u64,
            false,
        );
        wall += TICK;

        for i in 0..PER_CONNECTION {
            builder.push_delta(
                VENUE,
                &BookDelta {
                    symbol: symbol(),
                    changes: vec![LevelChange::new(
                        Side::Bid,
                        f("60000")
                            .checked_sub(Fixed::from_mantissa((i % 16) as i64 * 1_000_000))
                            .unwrap(),
                        Fixed::from_mantissa(1_000_000 + i as i64),
                    )],
                    // Restarts at one on every connection. This is the point.
                    seq: Some(i as u64 + 1),
                    checksum: None,
                    stamps: stamps(wall),
                    prev_seq: None,
                    first_seq: None,
                },
                (connection * (PER_CONNECTION + 1) + i + 1) as u64,
                false,
            );
            wall += TICK;

            if builder.len() >= rows_per_file {
                writer.write_builder(&key, &mut builder).expect("write");
                writer.close_partition(&key).expect("close");
                key = PartitionKey::new(VENUE, &symbol(), wall);
            }
        }
    }
    if !builder.is_empty() {
        writer.write_builder(&key, &mut builder).expect("write");
        writer.close_partition(&key).expect("close");
    }
    writer
}

/// One restore, with its two phases timed apart.
struct Restored {
    files: usize,
    bytes: u64,
    /// Manifest read, file copy, manifest append, directory fsync.
    copy: Duration,
    /// Every page in every restored file decoded and counted.
    verify: Duration,
    /// Rows this restore actually decoded. Not a manifest figure.
    decoded_rows: u64,
}

impl Restored {
    fn recovery(&self) -> Duration {
        self.copy + self.verify
    }
}

/// Copy into `target` exactly the files the source manifest vouches for,
/// rebuild the manifest there, and then read every page back.
///
/// The manifest is the list, not the directory. Copying whatever `.parquet`
/// happens to be on disk would restore an interrupted write as if it were
/// data, which is the mistake the manifest exists to prevent.
///
/// The decode is inside the measurement rather than after it. A file that
/// copied and does not read is not recovered data, so the instant the archive
/// becomes usable is the instant the last page decodes, not the instant the
/// last byte lands.
fn restore(source: &Path, target: &Path) -> Restored {
    let copy_started = Instant::now();
    let archive = ArchiveReader::open(source).expect("open source");
    let mut manifest = Manifest::open(target).expect("open target manifest");
    let mut files = 0usize;
    let mut bytes = 0u64;
    for record in archive.files() {
        let from = archive.path_of(record);
        let to = target.join(&record.path);
        std::fs::create_dir_all(to.parent().expect("parent")).expect("mkdir");
        bytes += std::fs::copy(&from, &to).expect("copy");
        manifest.record_file(record.clone()).expect("append");
        files += 1;
    }
    manifest.sync_dir().expect("fsync");
    let copy = copy_started.elapsed();

    let verify_started = Instant::now();
    let report = ArchiveReader::open(target).expect("open target").verify();
    let verify = verify_started.elapsed();
    assert!(
        report.is_clean(),
        "restored archive does not read: {report}"
    );

    Restored {
        files,
        bytes,
        copy,
        verify,
        decoded_rows: report.rows_read,
    }
}

/// What the rows themselves say, read out of the pages.
///
/// Every field here comes from a decoded column. The manifest carries its own
/// row count and its own first and last instant, and comparing the two is how
/// this test can say the restored data agrees with the restored bookkeeping
/// rather than assuming it.
#[derive(Debug, PartialEq, Eq)]
struct Decoded {
    rows: u64,
    first_recv_wall: i64,
    last_recv_wall: i64,
    first_venue_ts: Option<i64>,
    last_venue_ts: Option<i64>,
    /// Order-sensitive digest of (recv_wall, venue_ts, event, seq) per row.
    digest: u64,
}

fn decoded_extent(root: &Path) -> Decoded {
    let archive = ArchiveReader::open(root).expect("open");
    let mut records: Vec<&FileRecord> = archive.files().iter().collect();
    records.sort_by_key(|f| (f.first_recv_wall, f.path.clone()));

    let mut rows = 0u64;
    let mut first = i64::MAX;
    let mut last = i64::MIN;
    let mut first_venue: Option<i64> = None;
    let mut last_venue: Option<i64> = None;
    let mut hasher = DefaultHasher::new();

    for record in records {
        for batch in reader::read_batches(archive.path_of(record)).expect("read") {
            let recv = batch
                .column_by_name("recv_wall")
                .expect("recv_wall")
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("recv_wall is a nanosecond timestamp")
                .clone();
            let venue_ts = batch.column_by_name("venue_ts").and_then(|c| {
                c.as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .cloned()
            });
            let event = batch
                .column_by_name("event")
                .expect("event")
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("event is a small integer")
                .clone();
            let seq = batch
                .column_by_name("seq")
                .expect("seq")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("seq is an unsigned integer")
                .clone();

            for i in 0..batch.num_rows() {
                rows += 1;
                let wall = recv.value(i);
                first = first.min(wall);
                last = last.max(wall);
                let venue = venue_ts
                    .as_ref()
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
                if let Some(v) = venue {
                    first_venue = Some(first_venue.map_or(v, |f: i64| f.min(v)));
                    last_venue = Some(last_venue.map_or(v, |l: i64| l.max(v)));
                }
                wall.hash(&mut hasher);
                venue.hash(&mut hasher);
                event.value(i).hash(&mut hasher);
                (!seq.is_null(i)).then(|| seq.value(i)).hash(&mut hasher);
            }
        }
    }

    Decoded {
        rows,
        first_recv_wall: first,
        last_recv_wall: last,
        first_venue_ts: first_venue,
        last_venue_ts: last_venue,
        digest: hasher.finish(),
    }
}

/// `(seq, is_snapshot)` for every row in the archive, in arrival order.
fn sequence_of(root: &Path) -> Vec<(Option<u64>, bool)> {
    let archive = ArchiveReader::open(root).expect("open");
    let mut records: Vec<&FileRecord> = archive.files().iter().collect();
    records.sort_by_key(|f| f.first_recv_wall);
    let mut out = Vec::new();
    for record in records {
        for batch in reader::read_batches(archive.path_of(record)).expect("read") {
            let seq = batch
                .column_by_name("seq")
                .expect("seq")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("seq is an unsigned integer")
                .clone();
            let event = batch
                .column_by_name("event")
                .expect("event")
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("event is a small integer")
                .clone();
            for i in 0..batch.num_rows() {
                out.push(((!seq.is_null(i)).then(|| seq.value(i)), event.value(i) == 0));
            }
        }
    }
    out
}

/// Wall-clock instants the manifest claims, and the rows it claims.
fn window(root: &Path) -> (i64, i64, u64) {
    let archive = ArchiveReader::open(root).expect("open");
    let first = archive
        .files()
        .iter()
        .map(|f| f.first_recv_wall)
        .min()
        .expect("files");
    let last = archive
        .files()
        .iter()
        .map(|f| f.last_recv_wall)
        .max()
        .expect("files");
    (first, last, archive.files().iter().map(|f| f.rows).sum())
}

#[test]
fn a_restored_archive_is_the_archive_that_was_recorded() {
    let src = tempfile::tempdir().expect("tempdir");
    let dst = tempfile::tempdir().expect("tempdir");
    let source = src.path();
    let target = dst.path();

    let writer = record(source, 250);
    let source_files = writer.manifest().files().len();
    assert!(
        source_files > 1,
        "the recording must span several files or the restore proves nothing"
    );

    // Isolated: a different directory, and nothing but the restore has ever
    // written to it.
    assert_ne!(
        source.canonicalize().expect("source path"),
        target.canonicalize().expect("target path"),
        "the restore target must not be the source"
    );
    assert!(
        std::fs::read_dir(target)
            .expect("read target")
            .next()
            .is_none(),
        "the restore target must start empty"
    );

    let restored = restore(source, target);
    assert_eq!(
        restored.files, source_files,
        "every vouched file must be restored"
    );

    // --- the rows, decoded on both sides ---------------------------------
    let src_rows = decoded_extent(source);
    let dst_rows = decoded_extent(target);
    assert_eq!(
        src_rows, dst_rows,
        "the restored rows must decode to what the source rows decode to"
    );
    assert_eq!(
        restored.decoded_rows, dst_rows.rows,
        "the verify pass and the extent pass must agree on how many rows exist"
    );
    assert_eq!(
        dst_rows.rows as usize,
        CONNECTIONS * (PER_CONNECTION + 2),
        "every message written must have survived: a snapshot is two rows"
    );

    // --- the bookkeeping agrees with the rows ----------------------------
    let (src_from, src_to, src_claimed) = window(source);
    let (dst_from, dst_to, dst_claimed) = window(target);
    assert_eq!(
        (src_from, src_to, src_claimed),
        (dst_from, dst_to, dst_claimed)
    );
    assert_eq!(
        (dst_from, dst_to, dst_claimed),
        (
            dst_rows.first_recv_wall,
            dst_rows.last_recv_wall,
            dst_rows.rows
        ),
        "the manifest's window and row count must be what the pages actually hold"
    );

    // --- timestamps, compared rather than assumed ------------------------
    // Both clocks, not just the receive clock: an archive that kept recv_wall
    // and lost the venue's own timestamps would still line up on the first
    // comparison and be useless for anything asking when the venue said a
    // thing happened.
    assert_eq!(src_rows.first_recv_wall, dst_rows.first_recv_wall);
    assert_eq!(src_rows.last_recv_wall, dst_rows.last_recv_wall);
    assert_eq!(src_rows.first_venue_ts, dst_rows.first_venue_ts);
    assert_eq!(src_rows.last_venue_ts, dst_rows.last_venue_ts);
    assert!(
        src_rows.first_venue_ts.is_some(),
        "this recording carries venue timestamps, so the comparison must be looking at some"
    );

    // --- no unaccounted-for gaps -----------------------------------------
    assert!(
        ArchiveReader::open(target)
            .expect("open")
            .manifest()
            .truncations()
            .is_empty(),
        "a clean recording restored cleanly declares no lost window"
    );

    // --- the rebuild is identical ----------------------------------------
    let at = src_to;
    let from_source = Reconstructor::open(source)
        .expect("open")
        .at(&Request::new(VENUE, &symbol(), at))
        .expect("rebuild source");
    let from_target = Reconstructor::open(target)
        .expect("open")
        .at(&Request::new(VENUE, &symbol(), at))
        .expect("rebuild target");
    assert_eq!(from_source.digest(), from_target.digest());
    assert_eq!(from_source.rows_applied, from_target.rows_applied);
    assert!(from_source.rows_applied > 0);

    // Measured, not estimated. The row count printed here is the count this
    // test decoded, and the time printed beside it includes the decode.
    println!(
        "restored {} file(s), {} bytes, {} rows decoded | copy {:.1} ms + decode {:.1} ms = recovery {:.1} ms | \
         covered window {:.3} s ({} to {})",
        restored.files,
        restored.bytes,
        restored.decoded_rows,
        restored.copy.as_secs_f64() * 1e3,
        restored.verify.as_secs_f64() * 1e3,
        restored.recovery().as_secs_f64() * 1e3,
        (dst_rows.last_recv_wall - dst_rows.first_recv_wall) as f64 / 1e9,
        dst_rows.first_recv_wall,
        dst_rows.last_recv_wall
    );
}

#[test]
fn sequence_is_monotonic_within_an_epoch_and_restarts_between_them() {
    let src = tempfile::tempdir().expect("tempdir");
    let dst = tempfile::tempdir().expect("tempdir");
    record(src.path(), 250);
    restore(src.path(), dst.path());

    let rows = sequence_of(dst.path());
    assert_eq!(rows.len(), CONNECTIONS * (PER_CONNECTION + 2));

    // Inside one epoch the venue's counter steps by exactly one and never
    // repeats. Between epochs it restarts, and a restore that quietly repaired
    // that would be reordering a tape it does not understand.
    let mut epochs = 0usize;
    let mut previous: Option<u64> = None;
    let mut restarts = 0usize;
    for (seq, is_snapshot) in &rows {
        if *is_snapshot {
            // A snapshot row opens an epoch. Both of its rows carry seq 0.
            if previous.is_some() {
                restarts += 1;
            }
            previous = None;
            continue;
        }
        let seq = seq.expect("a delta on this feed carries a sequence number");
        match previous {
            None => {
                assert_eq!(seq, 1, "an epoch starts its numbering at one");
                epochs += 1;
            }
            Some(last) => assert_eq!(
                seq,
                last + 1,
                "no hole is allowed inside an epoch: {last} then {seq}"
            ),
        }
        previous = Some(seq);
    }
    assert_eq!(epochs, CONNECTIONS);
    assert_eq!(restarts, CONNECTIONS - 1);

    // And the check that would have been wrong: across the whole recording the
    // sequence is *not* monotonic, so anything asserting that globally is
    // asserting something the data does not promise.
    let all: Vec<u64> = rows
        .iter()
        .filter(|(_, snap)| !snap)
        .map(|(s, _)| s.expect("seq"))
        .collect();
    assert!(
        !all.windows(2).all(|w| w[0] < w[1]),
        "a reconnect restarts the counter; a global monotonicity check is a lie"
    );
}

#[test]
fn a_restore_that_dropped_a_file_does_not_pass_as_complete() {
    let src = tempfile::tempdir().expect("tempdir");
    let dst = tempfile::tempdir().expect("tempdir");
    record(src.path(), 250);
    restore(src.path(), dst.path());
    assert!(
        ArchiveReader::open(dst.path())
            .expect("open")
            .verify()
            .is_clean()
    );

    // One file lost in transit: the copy failed, the disk filled, the object
    // store 404ed. The manifest still vouches for it.
    let victim = {
        let archive = ArchiveReader::open(dst.path()).expect("open");
        archive.path_of(&archive.files()[1])
    };
    std::fs::remove_file(&victim).expect("remove");

    let report = ArchiveReader::open(dst.path()).expect("open").verify();
    assert!(
        !report.is_clean(),
        "a partial restore must not verify: {report}"
    );
    assert_eq!(report.failures.len(), 1, "{report}");

    // And the rebuild that would have been quietly wrong: without the check
    // above it still produces a book, from real rows, missing a stretch.
    let rebuilt = Reconstructor::open(dst.path())
        .expect("open")
        .at(&Request::new(
            VENUE,
            &symbol(),
            EPOCH + (CONNECTIONS * (PER_CONNECTION + 1)) as i64 * TICK,
        ));
    match rebuilt {
        Ok(book) => assert!(
            !book.is_empty(),
            "the point is that it looks fine, so it must not be empty"
        ),
        Err(e) => {
            // Equally acceptable, and better: it refused outright.
            assert!(e.to_string().contains("No such file") || e.to_string().contains("os error"));
        }
    }
}

/// A file whose bytes copied and whose pages do not read.
///
/// The negative control for the decode being inside the timer. If the restore
/// were measured on the copy alone, this archive would look restored: every
/// file the manifest names is present, at the right path, with a plausible
/// size. It is the decode that refuses it.
#[test]
fn a_file_that_copied_but_does_not_decode_is_not_a_restore() {
    let src = tempfile::tempdir().expect("tempdir");
    let dst = tempfile::tempdir().expect("tempdir");
    record(src.path(), 250);
    restore(src.path(), dst.path());

    let victim = {
        let archive = ArchiveReader::open(dst.path()).expect("open");
        archive.path_of(&archive.files()[1])
    };
    let before = std::fs::metadata(&victim).expect("stat").len();
    // Corrupt the pages and leave the length alone, which is what a bad sector
    // or a half-flushed page cache looks like from the outside.
    let mut bytes = std::fs::read(&victim).expect("read");
    let start = bytes.len() / 4;
    for byte in &mut bytes[start..start + 512] {
        *byte ^= 0xFF;
    }
    std::fs::write(&victim, &bytes).expect("write");
    assert_eq!(
        std::fs::metadata(&victim).expect("stat").len(),
        before,
        "the control has to leave the file the same size, or size alone would catch it"
    );

    let report = ArchiveReader::open(dst.path()).expect("open").verify();
    assert!(
        !report.is_clean(),
        "a file that does not decode must not verify: {report}"
    );
    println!("corruption control: {report}");
}

/// The same restore, measured against the archive this repository publishes.
///
/// Ignored by default because it reads `docs/data`, which is excluded from the
/// packaged crate: a `cargo test` run from a downloaded `.crate` has no such
/// directory. Run with `cargo test --release --test gate_restore -- --ignored
/// --nocapture` from a checkout. The synthetic case above is what CI asserts;
/// this is what produces a number worth quoting.
#[test]
#[ignore = "reads the repository's published demo archive; run with --ignored"]
fn measure_restoring_the_published_archive() {
    let mut total_files = 0usize;
    let mut total_bytes = 0u64;
    let mut total_rows = 0u64;
    let mut total_copy = Duration::ZERO;
    let mut total_verify = Duration::ZERO;

    println!(
        "{:<11} {:>5} {:>9} {:>9} {:>10} {:>11} {:>12} {:>10}",
        "venue", "files", "MB", "rows", "copy ms", "decode ms", "recovery ms", "window s"
    );
    for venue in [
        "binance-us",
        "bitstamp",
        "bybit",
        "coinbase",
        "kraken",
        "okx",
    ] {
        let source = Path::new("docs/data").join(venue);
        if !source.join("_manifest.jsonl").exists() {
            eprintln!("{venue}: no published archive here, skipping");
            continue;
        }
        let dst = tempfile::tempdir().expect("tempdir");
        let restored = restore(&source, dst.path());

        // Decoded on both sides and compared, so the row count in this table
        // is a count of rows that were read, and the window is the window the
        // rows describe rather than the one the manifest asserts.
        let src_rows = decoded_extent(&source);
        let dst_rows = decoded_extent(dst.path());
        assert_eq!(src_rows, dst_rows, "{venue}: restored rows differ");
        assert_eq!(restored.decoded_rows, dst_rows.rows);
        let (m_from, m_to, m_rows) = window(dst.path());
        assert_eq!(
            (m_from, m_to, m_rows),
            (
                dst_rows.first_recv_wall,
                dst_rows.last_recv_wall,
                dst_rows.rows
            ),
            "{venue}: the manifest and the pages disagree"
        );

        println!(
            "{venue:<11} {:>5} {:>9.2} {:>9} {:>10.1} {:>11.1} {:>12.1} {:>10.1}",
            restored.files,
            restored.bytes as f64 / 1e6,
            dst_rows.rows,
            restored.copy.as_secs_f64() * 1e3,
            restored.verify.as_secs_f64() * 1e3,
            restored.recovery().as_secs_f64() * 1e3,
            (dst_rows.last_recv_wall - dst_rows.first_recv_wall) as f64 / 1e9,
        );
        total_files += restored.files;
        total_bytes += restored.bytes;
        total_rows += dst_rows.rows;
        total_copy += restored.copy;
        total_verify += restored.verify;
    }
    println!(
        "{:<11} {:>5} {:>9.2} {:>9} {:>10.1} {:>11.1} {:>12.1}",
        "TOTAL",
        total_files,
        total_bytes as f64 / 1e6,
        total_rows,
        total_copy.as_secs_f64() * 1e3,
        total_verify.as_secs_f64() * 1e3,
        (total_copy + total_verify).as_secs_f64() * 1e3,
    );
    assert!(total_rows > 0, "the published archive must not be empty");
}

/// How much of a live recording a crash can cost, measured rather than quoted.
///
/// A row is recoverable once its file is closed and the manifest has a record
/// for it. Everything written since the last rotation is not, so the
/// recoverable data window is the wall-clock gap between the last instant the
/// manifest vouches for and the instant the process died. That is what this
/// measures: a real `SIGKILL` to a real writer, then the gap, over several
/// rounds, against a rotation interval this test chose.
///
/// Ignored by default because it spawns and kills a child process. Run with
/// `cargo test --release --test gate_restore -- --ignored --nocapture`.
#[test]
#[ignore = "spawns and SIGKILLs a real writer; run with --ignored"]
fn the_recoverable_window_is_bounded_by_the_rotation_interval() {
    // Overridable so the same measurement can be taken at more than one
    // rotation interval, which is what shows the window tracks the interval
    // rather than happening to be small.
    let rotate_ms: u64 = std::env::var("TICKVAULT_ROTATE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(250);
    const ROUNDS: usize = 6;
    let binary = env!("CARGO_BIN_EXE_tickvault");

    let mut gaps_ms = Vec::new();
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let mut child = Command::new(binary)
            .args([
                "soak",
                "--archive",
                root.to_str().unwrap(),
                "--start-seq",
                &(round as u64 * 1_000_000).to_string(),
                "--rotate-ms",
                &rotate_ms.to_string(),
                "--batch-rows",
                "100",
                "--pace-us",
                "200",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the soak");

        // Let it get several rotations in, so the manifest is not empty and
        // the kill lands mid-file rather than mid-startup.
        std::thread::sleep(Duration::from_millis(
            rotate_ms.saturating_mul(5).max(1_200) + round as u64 * 130,
        ));
        child.kill().expect("kill");
        let killed_at = wall_now();
        child.wait().expect("reap");

        // Everything the archive can still speak for.
        let archive = ArchiveReader::open(root).expect("open");
        if archive.files().is_empty() {
            continue;
        }
        let report = archive.verify();
        assert!(report.is_clean(), "round {round}: {report}");
        let decoded = decoded_extent(root);
        let gap_ms = (killed_at - decoded.last_recv_wall) as f64 / 1e6;
        println!(
            "round {round}: {} rows decoded, last durable row {:.1} ms before the kill",
            decoded.rows, gap_ms
        );
        gaps_ms.push(gap_ms);
    }

    assert!(
        gaps_ms.len() >= 4,
        "not enough rounds produced a manifest to measure"
    );
    let worst = gaps_ms.iter().cloned().fold(f64::MIN, f64::max);
    let mean = gaps_ms.iter().sum::<f64>() / gaps_ms.len() as f64;
    println!(
        "recoverable data window over {} kills at rotate-ms {rotate_ms}: worst {:.1} ms, mean {:.1} ms",
        gaps_ms.len(),
        worst,
        mean
    );
    // The bound the rotation interval buys, with room for the kill and the
    // reap. What matters is that the window is on the order of the rotation
    // interval rather than of the whole recording.
    assert!(
        worst < rotate_ms as f64 * 8.0,
        "the window should be within a small multiple of the rotation interval, was {worst:.1} ms"
    );
}

fn wall_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i64
}

#[allow(dead_code)]
fn _uses_common() {
    let _ = common::btc_usd();
}
