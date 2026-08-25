//! Restarting after a crash.
//!
//! The archive's promise is that everything it publishes reads back, and that
//! anything it could not capture is *named*. Recovery is what makes both true
//! after a process dies mid-write.
//!
//! Three kinds of file can be on disk that the manifest does not vouch for, and
//! they are not the same thing:
//!
//! - **A `.partial`.** The footer was never written, so it can never be read.
//!   Wreckage. Quarantine it and record what was lost.
//! - **A `.parquet` that reads back.** The rename landed but the manifest
//!   append did not. The data is complete and perfectly good, so it is
//!   *adopted* rather than thrown away. Treating anything unlisted as broken
//!   would discard real data for a bookkeeping gap.
//! - **A `.parquet` that does not read back.** Torn some other way.
//!   Quarantine and record.
//!
//! Nothing is deleted. A quarantined file is moved, not removed, because the
//! bytes may still be worth something to a human even when no reader will
//! accept them.

use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::store::compact::{DAILY_PREFIX, sweep_retired};
use crate::store::manifest::{FileRecord, Manifest, QUARANTINE_DIR, TruncationRecord};
use crate::store::reader::inspect;
use crate::store::writer::PARTIAL_SUFFIX;
use crate::types::{Symbol, VenueId};

/// What a recovery pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryOutcome {
    /// Complete files the manifest had not recorded, now vouched for.
    pub adopted: Vec<String>,
    /// Unreadable files moved aside.
    pub quarantined: Vec<String>,
    /// Windows the archive cannot account for.
    pub truncations: Vec<TruncationRecord>,
    /// Compactions interrupted before they were recorded. Nothing was lost:
    /// the sources are intact, so the work is simply discarded and can be
    /// re-run.
    pub abandoned_compactions: Vec<String>,
    /// Superseded files an interrupted tidy left on disk.
    pub swept: Vec<String>,
}

impl RecoveryOutcome {
    /// True when the previous run shut down cleanly.
    pub fn was_clean(&self) -> bool {
        self.adopted.is_empty()
            && self.quarantined.is_empty()
            && self.truncations.is_empty()
            && self.abandoned_compactions.is_empty()
            && self.swept.is_empty()
    }

    pub fn lost_nanos(&self) -> i64 {
        self.truncations.iter().filter_map(|t| t.lost_nanos()).sum()
    }
}

impl std::fmt::Display for RecoveryOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.was_clean() {
            return write!(f, "clean shutdown, nothing to recover");
        }
        write!(
            f,
            "{} file(s) adopted, {} quarantined, {} truncation(s), \
             {} abandoned compaction(s), {} leftover(s) swept",
            self.adopted.len(),
            self.quarantined.len(),
            self.truncations.len(),
            self.abandoned_compactions.len(),
            self.swept.len()
        )?;
        for t in &self.truncations {
            write!(
                f,
                "\n  lost {}: {}",
                t.lost_nanos()
                    .map(|n| format!("{:.3}s", n as f64 / 1e9))
                    .unwrap_or_else(|| "an unknown span".to_string()),
                t.reason
            )?;
        }
        Ok(())
    }
}

/// Recover the `venue`, `symbol`, and `date` a file belongs to from its path.
///
/// The Hive layout is the only record of that for a file whose contents cannot
/// be read, which is exactly the case that matters here.
pub fn parse_partition_path(relative: &str) -> Option<(VenueId, Symbol, String)> {
    let mut venue = None;
    let mut symbol = None;
    let mut date = None;
    for part in Path::new(relative).components() {
        let text = part.as_os_str().to_string_lossy();
        if let Some(v) = text.strip_prefix("venue=") {
            venue = v.parse::<VenueId>().ok();
        } else if let Some(s) = text.strip_prefix("symbol=") {
            symbol = Symbol::parse(s).ok();
        } else if let Some(d) = text.strip_prefix("date=") {
            date = Some(d.to_string());
        }
    }
    Some((venue?, symbol?, date?))
}

fn walk(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            // Skip our own bookkeeping, and anything already set aside.
            if name.starts_with('_') || name.starts_with('.') {
                continue;
            }
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn quarantine(root: &Path, path: &Path) -> Result<String> {
    let dir = root.join(QUARANTINE_DIR);
    std::fs::create_dir_all(&dir)?;
    let flattened = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "__");
    let mut target = dir.join(&flattened);
    // Never overwrite earlier wreckage; a second crash in the same partition
    // would otherwise erase the evidence from the first.
    let mut n = 1;
    while target.exists() {
        target = dir.join(format!("{flattened}.{n}"));
        n += 1;
    }
    std::fs::rename(path, &target)?;
    Ok(target
        .strip_prefix(root)
        .unwrap_or(&target)
        .to_string_lossy()
        .into_owned())
}

/// Bring an archive back to a state where everything it vouches for reads.
///
/// `now_wall` is the restart instant, used as the far end of any lost window.
/// Idempotent: a second pass over a recovered archive does nothing.
pub fn recover(root: impl AsRef<Path>, now_wall: i64) -> Result<RecoveryOutcome> {
    let root = root.as_ref();
    std::fs::create_dir_all(root)?;
    let mut manifest = Manifest::open(root)?;
    let known = manifest.known_paths();
    let mut outcome = RecoveryOutcome::default();

    let mut wreckage: Vec<PathBuf> = Vec::new();
    let mut abandoned: Vec<PathBuf> = Vec::new();

    // Pass one: adopt everything that is genuinely complete. Doing this first
    // means the lost windows computed below are as narrow as the evidence
    // allows, rather than blaming a gap on a file we could have kept.
    for path in walk(root) {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if name.ends_with(PARTIAL_SUFFIX) {
            wreckage.push(path);
            continue;
        }
        if !name.ends_with(".parquet") {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        if known.contains(&relative) {
            continue;
        }
        // An unlisted compaction output is an interrupted compaction, not a
        // file whose manifest entry went missing. Adopting it would double
        // every row in the partition, because its sources are still listed.
        if name.starts_with(DAILY_PREFIX) {
            abandoned.push(path);
            continue;
        }
        match inspect(&path) {
            Ok(summary) => {
                let Some((venue, symbol, date)) = parse_partition_path(&relative) else {
                    wreckage.push(path);
                    continue;
                };
                let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                manifest.record_file(FileRecord {
                    path: relative.clone(),
                    venue: summary.venue.unwrap_or(venue),
                    symbol: summary.symbol.unwrap_or(symbol),
                    date,
                    // Read from the file itself rather than guessed from the
                    // path, which does not encode it.
                    book_level: summary.book_level,
                    // Not encoded in the path; a rebuild falls back to full depth.
                    feed_depth: None,
                    rows: summary.rows,
                    bytes,
                    first_recv_wall: summary.first_recv_wall,
                    last_recv_wall: summary.last_recv_wall,
                    closed_wall: now_wall,
                })?;
                outcome.adopted.push(relative);
            }
            Err(_) => wreckage.push(path),
        }
    }

    // Pass two: everything left cannot be read, so set it aside and say what it
    // cost. The window is bounded by the newest thing we can still vouch for,
    // which is why adoption had to happen first.
    for path in wreckage {
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        let partition = parse_partition_path(&relative);
        let lost_from = partition
            .as_ref()
            .and_then(|(venue, symbol, _)| manifest.last_covered(*venue, symbol));
        let quarantined = quarantine(root, &path)?;
        let record = TruncationRecord {
            venue: partition.as_ref().map(|(v, _, _)| *v),
            symbol: partition.as_ref().map(|(_, s, _)| s.clone()),
            detected_wall: now_wall,
            reason: if relative.ends_with(PARTIAL_SUFFIX) {
                "process died before the file's footer was written".to_string()
            } else {
                "file present but unreadable".to_string()
            },
            quarantined: Some(quarantined.clone()),
            lost_from_wall: lost_from,
            lost_to_wall: now_wall,
            // A footerless Parquet file cannot be read at all, so how many rows
            // it held is not knowable. None says that; zero would lie.
            unrecoverable_rows: None,
        };
        manifest.record_truncation(record.clone())?;
        outcome.quarantined.push(quarantined);
        outcome.truncations.push(record);
    }

    // An interrupted compaction cost nothing but the work: its sources are
    // still there and still vouched for. Set the output aside, no truncation.
    for path in abandoned {
        outcome.abandoned_compactions.push(quarantine(root, &path)?);
    }

    manifest.sync_dir()?;
    drop(manifest);
    outcome.swept = sweep_retired(root)?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{BookDelta, LevelChange};
    use crate::clock::{Stamp, Timestamps};
    use crate::fixed::Fixed;
    use crate::store::reader::ArchiveReader;
    use crate::store::schema::RowBuilder;
    use crate::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};
    use crate::types::Side;
    #[allow(unused_imports)]
    use crate::types::VenueId as _VenueIdForTests;

    const BASE: i64 = 1_787_000_000_000_000_000;

    fn write_some(root: &Path, rows: usize, max_rows_per_file: usize) -> ArchiveWriter {
        write_with(root, rows, max_rows_per_file, 50_000)
    }

    fn write_with(
        root: &Path,
        rows: usize,
        max_rows_per_file: usize,
        row_group_size: usize,
    ) -> ArchiveWriter {
        let mut w = ArchiveWriter::open(WriterConfig {
            max_rows_per_file,
            row_group_size,
            ..WriterConfig::new(root)
        })
        .unwrap();
        let symbol = Symbol::new("BTC", "USD");
        for i in 0..rows {
            let wall = BASE + i as i64 * 1_000_000;
            let key = PartitionKey::new(VenueId::Kraken, &symbol, wall);
            let delta = BookDelta {
                symbol: symbol.clone(),
                changes: vec![LevelChange::new(
                    Side::Bid,
                    Fixed::from_decimal_str("1").unwrap(),
                    Fixed::from_decimal_str("1").unwrap(),
                )],
                seq: Some(i as u64),
                checksum: None,
                stamps: Timestamps::recv_only(Stamp {
                    mono_nanos: wall as u64,
                    wall_nanos: wall,
                }),
                prev_seq: None,
                first_seq: None,
            };
            let mut b = RowBuilder::new();
            b.push_delta(VenueId::Kraken, &delta, i as u64, false);
            w.write_builder(&key, &mut b).unwrap();
        }
        w
    }

    #[test]
    fn a_clean_shutdown_needs_no_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = write_some(dir.path(), 10, 100);
        w.close().unwrap();
        let outcome = recover(dir.path(), BASE + 1_000_000_000).unwrap();
        assert!(outcome.was_clean(), "{outcome}");
        assert_eq!(outcome.to_string(), "clean shutdown, nothing to recover");
    }

    #[test]
    fn an_interrupted_file_is_quarantined_and_its_loss_recorded() {
        let dir = tempfile::tempdir().unwrap();
        // Rotate every 4 rows, write 10, then abandon the writer without
        // closing it. That is what a kill leaves: two complete files and one
        // `.partial` whose footer was never written.
        let w = write_some(dir.path(), 10, 4);
        assert_eq!(w.manifest().files().len(), 2);
        std::mem::forget(w);

        let restart = BASE + 5_000_000_000;
        let outcome = recover(dir.path(), restart).unwrap();
        assert!(!outcome.was_clean());
        assert_eq!(outcome.quarantined.len(), 1, "{outcome}");
        assert_eq!(outcome.truncations.len(), 1);

        let t = &outcome.truncations[0];
        assert_eq!(t.venue, Some(VenueId::Kraken));
        assert_eq!(t.symbol, Some(Symbol::new("BTC", "USD")));
        assert!(t.reason.contains("footer"));
        assert_eq!(t.lost_to_wall, restart);
        // Bounded by the newest complete file, not guessed at.
        assert_eq!(t.lost_from_wall, Some(BASE + 7 * 1_000_000));
        assert!(t.lost_nanos().unwrap() > 0);
        assert_eq!(t.unrecoverable_rows, None);

        // And what survived still reads.
        let report = ArchiveReader::open(dir.path()).unwrap().verify();
        assert!(report.is_clean(), "{report}");
        assert_eq!(report.rows_read, 8);
    }

    #[test]
    fn a_complete_file_the_manifest_missed_is_adopted_not_discarded() {
        // The crash window between renaming a finished file and recording it.
        // The data is perfect; throwing it away would lose real data over
        // bookkeeping.
        let dir = tempfile::tempdir().unwrap();
        let mut w = write_some(dir.path(), 8, 4);
        w.close().unwrap();
        let orphan = w.manifest().files()[1].clone();

        // Rewrite the manifest without its final entry.
        let manifest_path = dir.path().join(crate::store::manifest::MANIFEST_FILE);
        let text = std::fs::read_to_string(&manifest_path).unwrap();
        let kept: Vec<&str> = text.lines().take(1).collect();
        std::fs::write(&manifest_path, format!("{}\n", kept.join("\n"))).unwrap();

        let outcome = recover(dir.path(), BASE + 9_000_000_000).unwrap();
        assert_eq!(outcome.adopted, vec![orphan.path.clone()]);
        assert!(outcome.quarantined.is_empty());
        assert!(outcome.truncations.is_empty(), "nothing was actually lost");

        let reader = ArchiveReader::open(dir.path()).unwrap();
        assert_eq!(reader.files().len(), 2);
        let report = reader.verify();
        assert!(report.is_clean(), "{report}");
        assert_eq!(report.rows_read, 8);
    }

    #[test]
    fn an_unreadable_parquet_file_is_quarantined_rather_than_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = write_some(dir.path(), 8, 4);
        w.close().unwrap();
        let victim = w.manifest().files()[1].clone();

        // Truncate it and forget the manifest ever mentioned it.
        let path = dir.path().join(&victim.path);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 3]).unwrap();
        let manifest_path = dir.path().join(crate::store::manifest::MANIFEST_FILE);
        let text = std::fs::read_to_string(&manifest_path).unwrap();
        std::fs::write(
            &manifest_path,
            format!("{}\n", text.lines().take(1).collect::<Vec<_>>().join("\n")),
        )
        .unwrap();

        let outcome = recover(dir.path(), BASE + 9_000_000_000).unwrap();
        assert!(outcome.adopted.is_empty());
        assert_eq!(outcome.quarantined.len(), 1);
        assert!(outcome.truncations[0].reason.contains("unreadable"));
        assert!(ArchiveReader::open(dir.path()).unwrap().verify().is_clean());
    }

    #[test]
    fn quarantined_files_are_moved_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let w = write_with(dir.path(), 9, 100, 2);
        std::mem::forget(w);
        let outcome = recover(dir.path(), BASE + 9_000_000_000).unwrap();
        let moved = dir.path().join(&outcome.quarantined[0]);
        assert!(
            moved.exists(),
            "wreckage is set aside for a human, never deleted"
        );
    }

    #[test]
    fn an_abandoned_writer_leaves_nothing_readable_however_much_it_wrote() {
        // Measured, and it decides how the guarantee is worded. Arrow buffers
        // the whole file in memory, so a kill leaves a zero-byte file rather
        // than a truncated one, no matter how many row groups had logically
        // completed. There is no partial durability to salvage, which is why
        // rotation frequency is the entire bound on what a crash costs.
        let dir = tempfile::tempdir().unwrap();
        // Nine rows across row groups of two: four groups' worth, and still
        // nothing on disk.
        let w = write_with(dir.path(), 9, 100, 2);
        std::mem::forget(w);

        let partial = super::walk(dir.path())
            .into_iter()
            .find(|p| p.to_string_lossy().ends_with(PARTIAL_SUFFIX))
            .expect("an abandoned writer leaves a partial file");
        assert_eq!(
            std::fs::metadata(&partial).unwrap().len(),
            0,
            "if this ever becomes non-zero, partial recovery is worth revisiting"
        );
        assert!(crate::store::reader::inspect(&partial).is_err());
    }

    #[test]
    fn a_crash_before_any_row_group_flush_still_records_the_loss() {
        // The other end of the same case: nothing had reached disk at all. The
        // file is empty, and the window it covered still has to be named.
        let dir = tempfile::tempdir().unwrap();
        let w = write_with(dir.path(), 2, 100, 50_000);
        std::mem::forget(w);
        let outcome = recover(dir.path(), BASE + 9_000_000_000).unwrap();
        assert_eq!(outcome.quarantined.len(), 1);
        assert_eq!(outcome.truncations.len(), 1);
        // Nothing had ever been vouched for, so how far back the loss goes is
        // unknown rather than zero.
        assert_eq!(outcome.truncations[0].lost_from_wall, None);
        assert_eq!(outcome.truncations[0].lost_nanos(), None);
    }

    #[test]
    fn recovery_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let w = write_some(dir.path(), 10, 4);
        std::mem::forget(w);
        let first = recover(dir.path(), BASE + 9_000_000_000).unwrap();
        assert!(!first.was_clean());
        let second = recover(dir.path(), BASE + 10_000_000_000).unwrap();
        assert!(
            second.was_clean(),
            "a second pass must do nothing: {second}"
        );
    }

    #[test]
    fn two_crashes_in_one_partition_do_not_erase_the_first_wreck() {
        let dir = tempfile::tempdir().unwrap();
        for _ in 0..2 {
            let w = write_some(dir.path(), 2, 100);
            std::mem::forget(w);
            recover(dir.path(), BASE + 9_000_000_000).unwrap();
        }
        let quarantined: Vec<_> = std::fs::read_dir(dir.path().join(QUARANTINE_DIR))
            .unwrap()
            .flatten()
            .collect();
        assert_eq!(quarantined.len(), 2, "the first wreck must survive");
    }

    #[test]
    fn an_interrupted_compaction_is_discarded_without_claiming_a_loss() {
        use crate::store::compact::compact_partition;
        use crate::store::reader::ArchiveReader;
        use crate::store::writer::WriterConfig;

        let dir = tempfile::tempdir().unwrap();
        let mut w = write_some(dir.path(), 20, 5);
        w.close().unwrap();
        let key = PartitionKey::new(VenueId::Kraken, &Symbol::new("BTC", "USD"), BASE);

        // Reconstruct exactly what a crash between renaming the daily file and
        // recording it leaves behind: the daily file present, the manifest
        // never told about it, and every source still on disk and still listed.
        let sources: Vec<(String, Vec<u8>)> = ArchiveReader::open(dir.path())
            .unwrap()
            .files()
            .iter()
            .map(|f| {
                (
                    f.path.clone(),
                    std::fs::read(dir.path().join(&f.path)).unwrap(),
                )
            })
            .collect();
        compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();
        let manifest_path = dir.path().join(crate::store::manifest::MANIFEST_FILE);
        let text = std::fs::read_to_string(&manifest_path).unwrap();
        let kept: Vec<&str> = text.lines().filter(|l| !l.contains("compaction")).collect();
        std::fs::write(&manifest_path, format!("{}\n", kept.join("\n"))).unwrap();
        for (path, bytes) in &sources {
            std::fs::write(dir.path().join(path), bytes).unwrap();
        }

        let outcome = recover(dir.path(), BASE + 9_000_000_000).unwrap();

        assert_eq!(outcome.abandoned_compactions.len(), 1, "{outcome}");
        assert!(
            outcome.truncations.is_empty(),
            "an abandoned compaction loses nothing and must not claim a gap"
        );
        // The sources are untouched and the archive is exactly as it was
        // before the compaction was attempted.
        let reader = ArchiveReader::open(dir.path()).unwrap();
        assert_eq!(reader.files().len(), sources.len());
        assert_eq!(reader.manifest().total_rows(), 20);
        let report = reader.verify();
        assert!(report.is_clean(), "{report}");
    }

    #[test]
    fn a_partition_path_yields_the_partition_it_names() {
        let parsed =
            parse_partition_path("venue=kraken/symbol=BTC-USD/date=2026-08-24/part-1.parquet");
        assert_eq!(
            parsed,
            Some((
                VenueId::Kraken,
                Symbol::new("BTC", "USD"),
                "2026-08-24".to_string()
            ))
        );
        assert_eq!(parse_partition_path("nonsense/part-1.parquet"), None);
    }
}
