//! Compacting many small live files into one daily file.
//!
//! The writer rotates often, because the open file is what a crash costs. That
//! is right for durability and wrong for reading: a day of a busy venue is
//! hundreds of small files, each with its own footer and its own dictionary,
//! and a query pays for every one of them. Compaction is the other half of that
//! trade, run once a day is over.
//!
//! # Crash safety
//!
//! Compaction never leaves the archive unreadable, at any point:
//!
//! 1. The daily file is written under a distinct `daily-` name and renamed into
//!    place. Until step 2 the manifest has never heard of it, so it is not part
//!    of the archive and the sources are untouched.
//! 2. **One** manifest entry records the new file and retires the sources at
//!    the same time. A single append either lands or it does not, so the
//!    archive can never be caught double-counting the rows or missing them.
//! 3. The retired sources are deleted. Interrupted here, the manifest is
//!    already correct and recovery finishes the tidying.
//!
//! An interrupted compaction therefore costs nothing but wasted work, which is
//! why step 1's file is quarantined rather than adopted if it is found
//! unlisted: adopting it would double every row in the partition.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::error::{Error, Result};
use crate::store::manifest::{CompactionRecord, FileRecord, Manifest};
use crate::store::reader::read_batches;
use crate::store::schema::book_schema;
use crate::store::writer::{PARTIAL_SUFFIX, PartitionKey, WriterConfig};
use crate::types::{Symbol, VenueId};

/// Prefix marking a file as the output of compaction.
///
/// Load-bearing: recovery uses it to tell an interrupted compaction from a
/// complete file whose manifest entry was lost, and the two need opposite
/// treatment.
pub const DAILY_PREFIX: &str = "daily-";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionOutcome {
    pub partitions_compacted: usize,
    pub files_before: usize,
    pub files_after: usize,
    pub rows: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// Partitions left alone because there was nothing to gain.
    pub partitions_skipped: usize,
}

impl CompactionOutcome {
    fn merge(&mut self, other: CompactionOutcome) {
        self.partitions_compacted += other.partitions_compacted;
        self.files_before += other.files_before;
        self.files_after += other.files_after;
        self.rows += other.rows;
        self.bytes_before += other.bytes_before;
        self.bytes_after += other.bytes_after;
        self.partitions_skipped += other.partitions_skipped;
    }

    /// Size after compaction as a fraction of size before.
    pub fn size_ratio(&self) -> Option<f64> {
        if self.bytes_before == 0 {
            return None;
        }
        Some(self.bytes_after as f64 / self.bytes_before as f64)
    }
}

impl std::fmt::Display for CompactionOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} partition(s): {} files to {}, {} rows, {:.1} MB to {:.1} MB",
            self.partitions_compacted,
            self.files_before,
            self.files_after,
            self.rows,
            self.bytes_before as f64 / 1e6,
            self.bytes_after as f64 / 1e6
        )?;
        if self.partitions_skipped > 0 {
            write!(f, ", {} already compact", self.partitions_skipped)?;
        }
        Ok(())
    }
}

/// Compact one partition's files into a single daily file.
///
/// A partition already down to one file is left alone: rewriting it would spend
/// IO to produce the same thing, and briefly put the only copy at risk.
pub fn compact_partition(
    root: impl AsRef<Path>,
    key: &PartitionKey,
    config: &WriterConfig,
    now_wall: i64,
) -> Result<CompactionOutcome> {
    let root = root.as_ref();
    let mut manifest = Manifest::open(root)?;

    let mut sources: Vec<FileRecord> = manifest
        .files()
        .iter()
        .filter(|f| {
            f.venue == key.venue
                && f.symbol == key.symbol
                && f.date == key.date
                && f.book_level == key.book_level
                && f.feed_depth == key.feed_depth
        })
        .cloned()
        .collect();
    sources.sort_by_key(|f| (f.first_recv_wall, f.path.clone()));

    if sources.len() < 2 {
        return Ok(CompactionOutcome {
            partitions_skipped: 1,
            ..Default::default()
        });
    }

    let dir = root.join(key.dir());
    std::fs::create_dir_all(&dir)?;
    let name = format!("{DAILY_PREFIX}{}.parquet", key.date);
    let final_path = dir.join(&name);
    let partial = dir.join(format!("{name}{PARTIAL_SUFFIX}"));

    let level = ZstdLevel::try_new(config.zstd_level)
        .map_err(|e| Error::Other(format!("invalid zstd level: {e}")))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .set_max_row_group_row_count(Some(config.row_group_size))
        .set_created_by(format!(
            "tickvault {} (compacted)",
            env!("CARGO_PKG_VERSION")
        ))
        .build();

    let mut rows = 0u64;
    let mut bytes_before = 0u64;
    {
        let sink = File::create(&partial)?;
        let mut writer = ArrowWriter::try_new(sink, book_schema(), Some(props))
            .map_err(|e| Error::Other(format!("opening {}: {e}", partial.display())))?;
        // In time order, so the compacted file reads the same as the sequence
        // of files it replaces.
        for source in &sources {
            bytes_before += source.bytes;
            for batch in read_batches(root.join(&source.path))? {
                rows += batch.num_rows() as u64;
                writer
                    .write(&batch)
                    .map_err(|e| Error::Other(format!("compacting {}: {e}", source.path)))?;
            }
        }
        let sink = writer
            .into_inner()
            .map_err(|e| Error::Other(format!("closing {}: {e}", partial.display())))?;
        sink.sync_all()?;
    }
    std::fs::rename(&partial, &final_path)?;
    File::open(&dir)?.sync_all()?;

    let bytes_after = std::fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);
    let produced = FileRecord {
        path: key.dir().join(&name).to_string_lossy().into_owned(),
        venue: key.venue,
        symbol: key.symbol.clone(),
        date: key.date.clone(),
        book_level: key.book_level,
        feed_depth: key.feed_depth,
        rows,
        bytes: bytes_after,
        first_recv_wall: sources.first().map(|f| f.first_recv_wall).unwrap_or(0),
        last_recv_wall: sources.last().map(|f| f.last_recv_wall).unwrap_or(0),
        closed_wall: now_wall,
    };
    let retired: Vec<String> = sources.iter().map(|f| f.path.clone()).collect();

    // The atomic swap. Everything before this was reversible.
    manifest.record_compaction(CompactionRecord {
        produced,
        retired: retired.clone(),
        at_wall: now_wall,
    })?;

    // Tidying. Safe to interrupt: the manifest already tells the truth, and
    // recovery finishes the job.
    for path in &retired {
        let _ = std::fs::remove_file(root.join(path));
    }

    Ok(CompactionOutcome {
        partitions_compacted: 1,
        files_before: sources.len(),
        files_after: 1,
        rows,
        bytes_before,
        bytes_after,
        partitions_skipped: 0,
    })
}

/// Compact every partition in the archive.
///
/// `exclude_date` skips a day still being written to, since compacting a
/// partition the writer has open would leave its newest file out of the result.
pub fn compact_all(
    root: impl AsRef<Path>,
    config: &WriterConfig,
    now_wall: i64,
    exclude_date: Option<&str>,
) -> Result<CompactionOutcome> {
    let root = root.as_ref();
    let manifest = Manifest::open(root)?;
    // Keyed on the book level too: a venue recorded at both levels on one day
    // holds two different kinds of row that must not be merged into one file.
    type Key = (
        VenueId,
        Symbol,
        String,
        crate::types::BookLevel,
        Option<usize>,
    );
    let mut partitions: BTreeMap<Key, ()> = BTreeMap::new();
    for file in manifest.files() {
        if exclude_date == Some(file.date.as_str()) {
            continue;
        }
        partitions.insert(
            (
                file.venue,
                file.symbol.clone(),
                file.date.clone(),
                file.book_level,
                file.feed_depth,
            ),
            (),
        );
    }

    let mut outcome = CompactionOutcome::default();
    for (venue, symbol, date, book_level, feed_depth) in partitions.into_keys() {
        let key = PartitionKey {
            venue,
            symbol,
            date,
            book_level,
            feed_depth,
        };
        outcome.merge(compact_partition(root, &key, config, now_wall)?);
    }
    Ok(outcome)
}

/// Delete superseded files an interrupted compaction left behind.
///
/// Safe because the manifest already vouches for the file holding their rows,
/// and [`crate::store::reader::ArchiveReader::verify`] checks that it reads.
pub fn sweep_retired(root: impl AsRef<Path>) -> Result<Vec<String>> {
    let root = root.as_ref();
    let manifest = Manifest::open(root)?;
    let mut swept = Vec::new();
    for path in manifest.retired_paths() {
        let full: PathBuf = root.join(path);
        if full.exists() && std::fs::remove_file(&full).is_ok() {
            swept.push(path.clone());
        }
    }
    Ok(swept)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{BookDelta, LevelChange};
    use crate::clock::{Stamp, Timestamps};
    use crate::fixed::Fixed;
    use crate::store::reader::ArchiveReader;
    use crate::store::schema::RowBuilder;
    use crate::store::writer::ArchiveWriter;
    use crate::types::Side;

    const BASE: i64 = 1_787_000_000_000_000_000;

    fn seed(root: &Path, rows: usize, per_file: usize) -> PartitionKey {
        let mut w = ArchiveWriter::open(WriterConfig {
            max_rows_per_file: per_file,
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
                    Fixed::from_decimal_str("78850.12").unwrap(),
                    Fixed::from_decimal_str("0.5").unwrap(),
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
        w.close().unwrap();
        PartitionKey::new(VenueId::Kraken, &symbol, BASE)
    }

    #[test]
    fn many_files_become_one_without_losing_a_row() {
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 40, 5);
        let before = ArchiveReader::open(dir.path()).unwrap();
        assert_eq!(before.files().len(), 8);
        assert_eq!(before.manifest().total_rows(), 40);

        let outcome =
            compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();
        assert_eq!(outcome.files_before, 8);
        assert_eq!(outcome.files_after, 1);
        assert_eq!(outcome.rows, 40);

        let after = ArchiveReader::open(dir.path()).unwrap();
        assert_eq!(after.files().len(), 1);
        assert_eq!(after.manifest().total_rows(), 40);
        let report = after.verify();
        assert!(report.is_clean(), "{report}");
        assert_eq!(report.rows_read, 40);
    }

    #[test]
    fn the_compacted_file_preserves_row_order_and_values() {
        use arrow::array::UInt64Array;
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 20, 3);
        compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();

        let reader = ArchiveReader::open(dir.path()).unwrap();
        let batches = read_batches(reader.path_of(&reader.files()[0])).unwrap();
        let seqs: Vec<u64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("seq")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(seqs, (0..20).collect::<Vec<u64>>(), "order must survive");
    }

    #[test]
    fn retired_source_files_are_removed_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 20, 5);
        let sources: Vec<PathBuf> = ArchiveReader::open(dir.path())
            .unwrap()
            .files()
            .iter()
            .map(|f| dir.path().join(&f.path))
            .collect();
        compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();
        assert!(sources.iter().all(|p| !p.exists()), "sources still present");
    }

    #[test]
    fn a_partition_already_down_to_one_file_is_left_alone() {
        // Rewriting it would spend IO to produce the same bytes, and briefly
        // put the only copy of the data at risk for nothing.
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 10, 1_000);
        let outcome =
            compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();
        assert_eq!(outcome.partitions_skipped, 1);
        assert_eq!(outcome.partitions_compacted, 0);
        assert_eq!(ArchiveReader::open(dir.path()).unwrap().files().len(), 1);
    }

    #[test]
    fn compaction_shrinks_the_archive() {
        // Not the point of compaction, but if it made things bigger that would
        // mean the row groups are being fragmented rather than merged.
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 200, 5);
        let outcome =
            compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();
        assert!(
            outcome.size_ratio().unwrap() < 1.0,
            "compaction grew the archive: {outcome}"
        );
    }

    #[test]
    fn compact_all_skips_the_day_still_being_written() {
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 20, 5);
        let outcome = compact_all(
            dir.path(),
            &WriterConfig::new(dir.path()),
            BASE,
            Some(&key.date),
        )
        .unwrap();
        assert_eq!(outcome.partitions_compacted, 0);
        assert_eq!(ArchiveReader::open(dir.path()).unwrap().files().len(), 4);

        // Without the exclusion it compacts.
        let outcome = compact_all(dir.path(), &WriterConfig::new(dir.path()), BASE, None).unwrap();
        assert_eq!(outcome.partitions_compacted, 1);
    }

    #[test]
    fn compaction_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 20, 5);
        compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();
        let second =
            compact_partition(dir.path(), &key, &WriterConfig::new(dir.path()), BASE).unwrap();
        assert_eq!(second.partitions_skipped, 1);
        assert!(ArchiveReader::open(dir.path()).unwrap().verify().is_clean());
    }

    #[test]
    fn a_leftover_source_from_an_interrupted_tidy_is_swept() {
        let dir = tempfile::tempdir().unwrap();
        let key = seed(dir.path(), 20, 5);
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

        // Put one back, as a crash between the manifest append and the delete
        // would have left it.
        let (path, bytes) = &sources[0];
        std::fs::write(dir.path().join(path), bytes).unwrap();

        let swept = sweep_retired(dir.path()).unwrap();
        assert_eq!(swept, vec![path.clone()]);
        assert!(!dir.path().join(path).exists());
        assert!(ArchiveReader::open(dir.path()).unwrap().verify().is_clean());
    }
}
