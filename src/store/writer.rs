//! The partitioned Parquet writer.
//!
//! # Why files rotate
//!
//! A Parquet file becomes readable only when its footer is written, at close.
//! Everything before that is unrecoverable, so **the open file is the blast
//! radius of a crash**. Rotation is not a tuning knob here, it is the bound on
//! how much a kill can cost, and the defaults are chosen for that rather than
//! for file-size aesthetics.
//!
//! This is stronger than it first sounds, and it was measured rather than
//! assumed. `ArrowWriter` buffers the whole file in memory: killing a writer
//! part way through leaves a **zero-byte** file on disk, not a truncated one,
//! whether or not row groups have logically completed. So there is no partial
//! durability to salvage and no point flushing for safety. Either a file is
//! closed and complete, or its contents never existed as far as any reader is
//! concerned.
//!
//! Which makes the guarantee precise, and worth stating that way in the
//! published dataset: **a crash costs at most one rotation interval per
//! partition, and the manifest names exactly which interval it was.** The raw
//! tape written by [`crate::recorder`] is the separate, line-durable record
//! that a future phase can reprocess to fill such a window; Parquet is the
//! published artifact, and its granularity is the rotation.
//!
//! # The close sequence
//!
//! Each file is written as `.parquet.partial` and renamed on close. The order
//! matters and is chosen so that no crash can lose a *complete* file:
//!
//! 1. write the footer and fsync the data
//! 2. rename `.partial` to `.parquet`, which is atomic
//! 3. fsync the directory, so the rename itself is durable
//! 4. append the manifest entry and fsync it
//!
//! A crash between 2 and 4 leaves a file that is complete and readable but not
//! yet vouched for. Recovery handles that by *reading* it and adopting it,
//! rather than assuming anything unlisted is broken. A crash before 2 leaves a
//! `.partial`, which can never be read and is quarantined.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::clock::format_utc_date;
use crate::error::{Error, Result};
use crate::store::manifest::{FileRecord, Manifest};
use crate::store::schema::{RowBuilder, book_schema};
use crate::types::{Symbol, VenueId};

/// Suffix a file carries until its footer is written and it is renamed.
pub const PARTIAL_SUFFIX: &str = ".partial";

/// How the writer rotates and compresses.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub root: PathBuf,
    /// Rotate once a file holds this many rows.
    pub max_rows_per_file: usize,
    /// Rotate once a file has been open this long.
    ///
    /// This is the real crash bound: a kill can cost at most the rows written
    /// since the last rotation. Short means more, smaller files and a tighter
    /// bound; phase 3's compaction is what puts them back together.
    pub max_file_age: Duration,
    /// Rows per Parquet row group.
    pub row_group_size: usize,
    /// zstd level. Three is a good ratio for order book data without making the
    /// writer the bottleneck, which is the thing this phase is measuring.
    pub zstd_level: i32,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            root: PathBuf::from("archive"),
            max_rows_per_file: 250_000,
            max_file_age: Duration::from_secs(60),
            row_group_size: 50_000,
            zstd_level: 3,
        }
    }
}

impl WriterConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        WriterConfig {
            root: root.into(),
            ..Default::default()
        }
    }
}

/// Which partition a row belongs to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionKey {
    pub venue: VenueId,
    pub symbol: Symbol,
    /// UTC `YYYY-MM-DD`.
    pub date: String,
    /// What the rows in this partition are: aggregated levels or orders.
    pub book_level: crate::types::BookLevel,
    /// Levels a side the feed carried, when it was depth limited.
    ///
    /// Recorded because a rebuild has to truncate exactly as the recorder did.
    /// Kraken's book channel is ten deep and the client maintains that window;
    /// replaying its deltas without it leaves levels that have long since
    /// fallen out of the feed, and the book crosses within seconds.
    pub feed_depth: Option<usize>,
}

impl PartitionKey {
    pub fn new(venue: VenueId, symbol: &Symbol, recv_wall: i64) -> Self {
        Self::at_level(venue, symbol, recv_wall, crate::types::BookLevel::L2)
    }

    pub fn at_level(
        venue: VenueId,
        symbol: &Symbol,
        recv_wall: i64,
        book_level: crate::types::BookLevel,
    ) -> Self {
        PartitionKey {
            venue,
            symbol: symbol.clone(),
            date: format_utc_date(recv_wall),
            book_level,
            feed_depth: None,
        }
    }

    pub fn with_feed_depth(mut self, depth: Option<usize>) -> Self {
        self.feed_depth = depth;
        self
    }

    /// Hive-style relative directory, which pyarrow and polars discover on
    /// their own without being told the layout.
    pub fn dir(&self) -> PathBuf {
        PathBuf::from(format!("venue={}", self.venue.as_str()))
            .join(format!("symbol={}", self.symbol.as_str()))
            .join(format!("date={}", self.date))
    }
}

struct OpenFile {
    writer: ArrowWriter<File>,
    partial: PathBuf,
    final_path: PathBuf,
    relative: String,
    rows: u64,
    first_recv_wall: Option<i64>,
    last_recv_wall: i64,
    opened_mono: std::time::Instant,
}

/// Writes book rows into a partitioned Parquet archive.
///
/// Synchronous by design. It is driven from a dedicated thread on the far side
/// of a bounded channel, so that a slow disk shows up as backpressure to be
/// decided about rather than as latency injected into the socket reader.
pub struct ArchiveWriter {
    config: WriterConfig,
    manifest: Manifest,
    open: BTreeMap<PartitionKey, OpenFile>,
    counter: u64,
    rows_written: u64,
    files_closed: u64,
}

impl ArchiveWriter {
    pub fn open(config: WriterConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.root)?;
        let manifest = Manifest::open(&config.root)?;
        manifest.sync_dir()?;
        Ok(ArchiveWriter {
            config,
            manifest,
            open: BTreeMap::new(),
            counter: 0,
            rows_written: 0,
            files_closed: 0,
        })
    }

    pub fn root(&self) -> &Path {
        &self.config.root
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    pub fn files_closed(&self) -> u64 {
        self.files_closed
    }

    pub fn open_files(&self) -> usize {
        self.open.len()
    }

    fn properties(&self) -> Result<WriterProperties> {
        let level = ZstdLevel::try_new(self.config.zstd_level)
            .map_err(|e| Error::Other(format!("invalid zstd level: {e}")))?;
        Ok(WriterProperties::builder()
            .set_compression(Compression::ZSTD(level))
            .set_max_row_group_row_count(Some(self.config.row_group_size))
            .set_created_by(format!("tickvault {}", env!("CARGO_PKG_VERSION")))
            .build())
    }

    fn open_partition(&mut self, key: &PartitionKey, now_wall: i64) -> Result<()> {
        let dir = self.config.root.join(key.dir());
        std::fs::create_dir_all(&dir)?;
        let name = format!("part-{now_wall}-{:04}.parquet", self.counter);
        self.counter += 1;
        let final_path = dir.join(&name);
        let partial = dir.join(format!("{name}{PARTIAL_SUFFIX}"));
        let file = File::create(&partial)?;
        let writer = ArrowWriter::try_new(file, book_schema(), Some(self.properties()?))
            .map_err(|e| Error::Other(format!("opening {}: {e}", partial.display())))?;
        self.open.insert(
            key.clone(),
            OpenFile {
                writer,
                partial,
                final_path,
                relative: key.dir().join(&name).to_string_lossy().into_owned(),
                rows: 0,
                first_recv_wall: None,
                last_recv_wall: now_wall,
                opened_mono: std::time::Instant::now(),
            },
        );
        Ok(())
    }

    /// Write a batch belonging to one partition.
    ///
    /// `span` is the wall-clock range of the batch, used for the manifest entry
    /// so a reader can find the right file without opening every one.
    pub fn write(
        &mut self,
        key: &PartitionKey,
        batch: &RecordBatch,
        span: (i64, i64),
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if !self.open.contains_key(key) {
            self.open_partition(key, span.0)?;
        }
        {
            let file = self.open.get_mut(key).expect("just opened");
            file.writer
                .write(batch)
                .map_err(|e| Error::Other(format!("writing {}: {e}", file.partial.display())))?;
            file.rows += batch.num_rows() as u64;
            file.first_recv_wall.get_or_insert(span.0);
            file.last_recv_wall = span.1;
        }
        self.rows_written += batch.num_rows() as u64;

        let should_rotate = {
            let file = self.open.get(key).expect("just written");
            file.rows as usize >= self.config.max_rows_per_file
                || file.opened_mono.elapsed() >= self.config.max_file_age
        };
        if should_rotate {
            self.close_partition(key)?;
        }
        Ok(())
    }

    /// Convenience for callers holding a [`RowBuilder`].
    pub fn write_builder(&mut self, key: &PartitionKey, builder: &mut RowBuilder) -> Result<()> {
        let Some(span) = builder.wall_span() else {
            return Ok(());
        };
        let Some(batch) = builder.finish() else {
            return Ok(());
        };
        self.write(key, &batch, span)
    }

    /// Finish one partition's open file, making it readable and vouched for.
    pub fn close_partition(&mut self, key: &PartitionKey) -> Result<()> {
        let Some(file) = self.open.remove(key) else {
            return Ok(());
        };
        let OpenFile {
            writer,
            partial,
            final_path,
            relative,
            rows,
            first_recv_wall,
            last_recv_wall,
            ..
        } = file;

        // 1. Footer, then the data itself is durable.
        let sink = writer
            .into_inner()
            .map_err(|e| Error::Other(format!("closing {}: {e}", partial.display())))?;
        sink.sync_all()?;
        drop(sink);

        // 2. Atomic rename: from here the file is complete and readable.
        std::fs::rename(&partial, &final_path)?;

        // 3. Make the rename itself durable, or a power loss could undo it.
        if let Some(dir) = final_path.parent() {
            File::open(dir)?.sync_all()?;
        }

        // 4. Only now vouch for it.
        let bytes = std::fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);
        let first = first_recv_wall.unwrap_or(last_recv_wall);
        self.manifest.record_file(FileRecord {
            path: relative,
            venue: key.venue,
            symbol: key.symbol.clone(),
            date: key.date.clone(),
            book_level: key.book_level,
            feed_depth: key.feed_depth,
            rows,
            bytes,
            first_recv_wall: first,
            last_recv_wall,
            closed_wall: last_recv_wall,
        })?;
        self.files_closed += 1;
        Ok(())
    }

    /// Close every open file. Call before exiting; a crash is what recovery is
    /// for.
    pub fn close(&mut self) -> Result<()> {
        let keys: Vec<PartitionKey> = self.open.keys().cloned().collect();
        for key in keys {
            self.close_partition(&key)?;
        }
        Ok(())
    }

    /// Rotate any file that has been open longer than the configured age.
    ///
    /// Called on a timer by the writer thread so an idle partition still gets
    /// its data made readable, rather than sitting unreadable until the next
    /// message happens to arrive.
    pub fn rotate_aged(&mut self) -> Result<usize> {
        let due: Vec<PartitionKey> = self
            .open
            .iter()
            .filter(|(_, f)| f.opened_mono.elapsed() >= self.config.max_file_age)
            .map(|(k, _)| k.clone())
            .collect();
        let count = due.len();
        for key in due {
            self.close_partition(&key)?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{BookDelta, LevelChange};
    use crate::clock::{Stamp, Timestamps};
    use crate::fixed::Fixed;
    use crate::types::Side;

    fn key() -> PartitionKey {
        PartitionKey::new(
            VenueId::Kraken,
            &Symbol::new("BTC", "USD"),
            1_787_000_000_000_000_000,
        )
    }

    fn delta(wall: i64) -> BookDelta {
        BookDelta {
            symbol: Symbol::new("BTC", "USD"),
            changes: vec![LevelChange::new(
                Side::Bid,
                Fixed::from_decimal_str("78850.12").unwrap(),
                Fixed::from_decimal_str("0.5").unwrap(),
            )],
            seq: Some(1),
            checksum: None,
            stamps: Timestamps::recv_only(Stamp {
                mono_nanos: wall as u64,
                wall_nanos: wall,
            }),
            prev_seq: None,
            first_seq: None,
        }
    }

    fn write_rows(w: &mut ArchiveWriter, n: usize, base: i64) {
        let key = key();
        for i in 0..n {
            let mut b = RowBuilder::new();
            b.push_delta(VenueId::Kraken, &delta(base + i as i64), i as u64, false);
            w.write_builder(&key, &mut b).unwrap();
        }
    }

    #[test]
    fn a_closed_file_is_renamed_and_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig::new(dir.path())).unwrap();
        write_rows(&mut w, 5, 1_787_000_000_000_000_000);
        assert_eq!(w.open_files(), 1);
        assert!(
            w.manifest().files().is_empty(),
            "not vouched for while open"
        );
        w.close().unwrap();

        assert_eq!(w.manifest().files().len(), 1);
        let record = &w.manifest().files()[0];
        assert_eq!(record.rows, 5);
        assert!(record.bytes > 0);
        assert!(record.path.contains("venue=kraken"));
        assert!(record.path.contains("symbol=BTC-USD"));
        assert!(dir.path().join(&record.path).exists());
    }

    #[test]
    fn no_partial_file_survives_a_clean_close() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig::new(dir.path())).unwrap();
        write_rows(&mut w, 3, 1_787_000_000_000_000_000);
        w.close().unwrap();
        let partials: Vec<_> = walk(dir.path())
            .into_iter()
            .filter(|p| p.to_string_lossy().ends_with(PARTIAL_SUFFIX))
            .collect();
        assert!(partials.is_empty(), "left behind {partials:?}");
    }

    #[test]
    fn rotation_by_row_count_bounds_what_a_crash_can_cost() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig {
            max_rows_per_file: 4,
            ..WriterConfig::new(dir.path())
        })
        .unwrap();
        write_rows(&mut w, 10, 1_787_000_000_000_000_000);
        // Two full files rotated; the rest is still open.
        assert_eq!(w.manifest().files().len(), 2);
        assert!(w.manifest().files().iter().all(|f| f.rows == 4));
        w.close().unwrap();
        assert_eq!(w.manifest().total_rows(), 10);
    }

    #[test]
    fn rotation_by_age_makes_an_idle_partition_readable() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig {
            max_file_age: Duration::from_millis(1),
            ..WriterConfig::new(dir.path())
        })
        .unwrap();
        write_rows(&mut w, 1, 1_787_000_000_000_000_000);
        std::thread::sleep(Duration::from_millis(5));
        w.rotate_aged().unwrap();
        // Asserted as the outcome rather than which code path got there: under
        // load the write itself can outlast the age and rotate on the spot.
        assert_eq!(w.open_files(), 0, "an aged file must not stay open");
        assert_eq!(w.manifest().files().len(), 1);
        assert_eq!(w.manifest().total_rows(), 1);
    }

    #[test]
    fn rows_land_in_the_partition_their_timestamp_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig::new(dir.path())).unwrap();
        let symbol = Symbol::new("BTC", "USD");
        // Either side of a UTC midnight.
        let midnight = crate::clock::parse_rfc3339_nanos("2026-08-25T00:00:00Z").unwrap();
        for wall in [midnight - 1, midnight] {
            let k = PartitionKey::new(VenueId::Kraken, &symbol, wall);
            let mut b = RowBuilder::new();
            b.push_delta(VenueId::Kraken, &delta(wall), 0, false);
            w.write_builder(&k, &mut b).unwrap();
        }
        w.close().unwrap();
        let dates: Vec<&str> = w
            .manifest()
            .files()
            .iter()
            .map(|f| f.date.as_str())
            .collect();
        assert_eq!(
            dates.len(),
            2,
            "a midnight crossing must open a new partition"
        );
        assert!(dates.contains(&"2026-08-24"), "{dates:?}");
        assert!(dates.contains(&"2026-08-25"), "{dates:?}");
    }

    #[test]
    fn separate_symbols_get_separate_partitions_and_stay_open_together() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig::new(dir.path())).unwrap();
        let wall = 1_787_000_000_000_000_000;
        for base in ["BTC", "ETH"] {
            let symbol = Symbol::new(base, "USD");
            let k = PartitionKey::new(VenueId::Kraken, &symbol, wall);
            let mut d = delta(wall);
            d.symbol = symbol;
            let mut b = RowBuilder::new();
            b.push_delta(VenueId::Kraken, &d, 0, false);
            w.write_builder(&k, &mut b).unwrap();
        }
        assert_eq!(w.open_files(), 2);
        w.close().unwrap();
        assert_eq!(w.manifest().files().len(), 2);
    }

    #[test]
    fn writing_nothing_creates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig::new(dir.path())).unwrap();
        let mut empty = RowBuilder::new();
        w.write_builder(&key(), &mut empty).unwrap();
        w.close().unwrap();
        assert!(w.manifest().files().is_empty());
        assert_eq!(w.open_files(), 0);
    }

    pub(crate) fn walk(root: &Path) -> Vec<PathBuf> {
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
                    out.push(path);
                }
            }
        }
        out
    }
}
