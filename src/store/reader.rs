//! Reading the archive back.
//!
//! Exists for three callers: crash recovery, which reads an unlisted file to
//! decide whether it is complete or wreckage; the phase 3 gate, which has to
//! assert that *every* file the manifest vouches for actually opens; and phase
//! 6's query layer, which this is the foundation of.
//!
//! Verification reads the data rather than trusting the metadata. A Parquet
//! footer can claim a row count that the pages do not contain, and the whole
//! point of the manifest is to be checkable.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Error, Result};
use crate::store::manifest::{FileRecord, Manifest};
use crate::types::{Symbol, VenueId};

/// What a Parquet file on disk actually contains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSummary {
    pub rows: u64,
    pub first_recv_wall: i64,
    pub last_recv_wall: i64,
    pub venue: Option<VenueId>,
    pub symbol: Option<Symbol>,
    /// What the rows are, read from the file rather than from its path.
    pub book_level: crate::types::BookLevel,
}

/// Open a file and read it through, returning what is really in it.
///
/// An error here means the file cannot be published, which for recovery is the
/// signal to quarantine it.
pub fn inspect(path: impl AsRef<Path>) -> Result<FileSummary> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Other(format!("{}: unreadable: {e}", path.display())))?
        .build()
        .map_err(|e| Error::Other(format!("{}: unreadable: {e}", path.display())))?;

    let mut rows = 0u64;
    let mut first = None;
    let mut last = None;
    let mut venue = None;
    let mut symbol = None;
    let mut book_level = crate::types::BookLevel::L2;

    for batch in reader {
        let batch =
            batch.map_err(|e| Error::Other(format!("{}: truncated: {e}", path.display())))?;
        rows += batch.num_rows() as u64;
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(col) = batch.column_by_name("recv_wall")
            && let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>()
        {
            for i in 0..ts.len() {
                if ts.is_null(i) {
                    continue;
                }
                let v = ts.value(i);
                first = Some(first.map_or(v, |f: i64| f.min(v)));
                last = Some(last.map_or(v, |l: i64| l.max(v)));
            }
        }
        if venue.is_none()
            && let Some(col) = batch.column_by_name("venue")
            && let Some(s) = col.as_any().downcast_ref::<StringArray>()
            && s.len() > 0
        {
            venue = s.value(0).parse::<VenueId>().ok();
        }
        if let Some(col) = batch.column_by_name("book_level")
            && let Some(levels) = col.as_any().downcast_ref::<arrow::array::UInt8Array>()
            && !levels.is_empty()
            && levels.value(0) == 3
        {
            book_level = crate::types::BookLevel::L3;
        }
        if symbol.is_none()
            && let Some(col) = batch.column_by_name("symbol")
            && let Some(s) = col.as_any().downcast_ref::<StringArray>()
            && s.len() > 0
        {
            symbol = Symbol::parse(s.value(0)).ok();
        }
    }

    Ok(FileSummary {
        book_level,
        rows,
        first_recv_wall: first.unwrap_or(0),
        last_recv_wall: last.unwrap_or(0),
        venue,
        symbol,
    })
}

/// Read a whole file into batches.
pub fn read_batches(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Other(format!("{}: {e}", path.display())))?
        .build()
        .map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Other(format!("{}: {e}", path.display())))
}

/// What a full verification pass found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub files_checked: usize,
    pub rows_read: u64,
    /// Files the manifest vouches for that do not read back, with why.
    pub failures: Vec<(String, String)>,
    /// Files whose real row count disagrees with what the manifest claims.
    pub row_count_mismatches: Vec<(String, u64, u64)>,
}

impl VerifyReport {
    /// True when every file the archive vouches for opened and matched.
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty() && self.row_count_mismatches.is_empty()
    }
}

impl std::fmt::Display for VerifyReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} files, {} rows, {} unreadable, {} row-count mismatches",
            self.files_checked,
            self.rows_read,
            self.failures.len(),
            self.row_count_mismatches.len()
        )?;
        for (path, why) in &self.failures {
            write!(f, "\n  unreadable {path}: {why}")?;
        }
        for (path, claimed, found) in &self.row_count_mismatches {
            write!(f, "\n  {path}: manifest says {claimed} rows, found {found}")?;
        }
        Ok(())
    }
}

/// An archive on disk.
pub struct ArchiveReader {
    root: PathBuf,
    manifest: Manifest,
}

impl ArchiveReader {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let manifest = Manifest::open(&root)?;
        Ok(ArchiveReader { root, manifest })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn files(&self) -> &[FileRecord] {
        self.manifest.files()
    }

    pub fn path_of(&self, record: &FileRecord) -> PathBuf {
        self.root.join(&record.path)
    }

    /// Files covering one partition, oldest first.
    pub fn files_for(&self, venue: VenueId, symbol: &Symbol, date: &str) -> Vec<&FileRecord> {
        let mut out: Vec<&FileRecord> = self
            .manifest
            .files()
            .iter()
            .filter(|f| f.venue == venue && f.symbol == *symbol && f.date == date)
            .collect();
        out.sort_by_key(|f| f.first_recv_wall);
        out
    }

    /// Open every file the manifest vouches for and confirm it reads back.
    ///
    /// This is the assertion the phase 3 gate rests on, so it reads the pages
    /// rather than believing the footer's row count.
    pub fn verify(&self) -> VerifyReport {
        let mut report = VerifyReport::default();
        for record in self.manifest.files() {
            report.files_checked += 1;
            match inspect(self.path_of(record)) {
                Ok(summary) => {
                    report.rows_read += summary.rows;
                    if summary.rows != record.rows {
                        report.row_count_mismatches.push((
                            record.path.clone(),
                            record.rows,
                            summary.rows,
                        ));
                    }
                }
                Err(e) => report.failures.push((record.path.clone(), e.to_string())),
            }
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{BookDelta, LevelChange};
    use crate::clock::{Stamp, Timestamps};
    use crate::fixed::Fixed;
    use crate::store::schema::RowBuilder;
    use crate::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};
    use crate::types::Side;

    fn seed(root: &Path, rows: usize) -> ArchiveWriter {
        let mut w = ArchiveWriter::open(WriterConfig::new(root)).unwrap();
        let symbol = Symbol::new("BTC", "USD");
        let base = 1_787_000_000_000_000_000i64;
        for i in 0..rows {
            let wall = base + i as i64 * 1_000_000;
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
        w
    }

    #[test]
    fn a_written_archive_verifies_clean() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), 20);
        let reader = ArchiveReader::open(dir.path()).unwrap();
        let report = reader.verify();
        assert!(report.is_clean(), "{report}");
        assert_eq!(report.files_checked, 1);
        assert_eq!(report.rows_read, 20);
    }

    #[test]
    fn values_survive_the_round_trip_to_disk_and_back() {
        use arrow::array::Int64Array;
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), 3);
        let reader = ArchiveReader::open(dir.path()).unwrap();
        let batches = read_batches(reader.path_of(&reader.files()[0])).unwrap();
        let prices: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        let expected = Fixed::from_decimal_str("78850.12").unwrap().mantissa();
        assert!(prices.iter().all(|p| *p == expected), "{prices:?}");
    }

    #[test]
    fn inspect_reports_what_is_really_in_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let w = seed(dir.path(), 7);
        let record = &w.manifest().files()[0];
        let summary = inspect(dir.path().join(&record.path)).unwrap();
        assert_eq!(summary.rows, 7);
        assert_eq!(summary.venue, Some(VenueId::Kraken));
        assert_eq!(summary.symbol, Some(Symbol::new("BTC", "USD")));
        assert_eq!(summary.first_recv_wall, record.first_recv_wall);
        assert_eq!(summary.last_recv_wall, record.last_recv_wall);
    }

    #[test]
    fn a_footerless_file_is_reported_unreadable_rather_than_read_partially() {
        // Exactly what a kill mid-write leaves. Anything other than a hard
        // error here would let wreckage into the published dataset.
        let dir = tempfile::tempdir().unwrap();
        let w = seed(dir.path(), 10);
        let path = dir.path().join(&w.manifest().files()[0].path);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();

        assert!(inspect(&path).is_err());
        let report = ArchiveReader::open(dir.path()).unwrap().verify();
        assert!(!report.is_clean());
        assert_eq!(report.failures.len(), 1);
        assert!(report.to_string().contains("unreadable"));
    }

    #[test]
    fn a_missing_file_the_manifest_vouches_for_is_a_failure() {
        let dir = tempfile::tempdir().unwrap();
        let w = seed(dir.path(), 4);
        std::fs::remove_file(dir.path().join(&w.manifest().files()[0].path)).unwrap();
        let report = ArchiveReader::open(dir.path()).unwrap().verify();
        assert!(!report.is_clean());
        assert_eq!(report.failures.len(), 1);
    }

    #[test]
    fn files_for_a_partition_come_back_in_time_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = ArchiveWriter::open(WriterConfig {
            max_rows_per_file: 2,
            ..WriterConfig::new(dir.path())
        })
        .unwrap();
        let symbol = Symbol::new("BTC", "USD");
        let base = 1_787_000_000_000_000_000i64;
        for i in 0..6 {
            let wall = base + i * 1_000_000;
            let key = PartitionKey::new(VenueId::Kraken, &symbol, wall);
            let delta = BookDelta {
                symbol: symbol.clone(),
                changes: vec![LevelChange::new(
                    Side::Bid,
                    Fixed::from_decimal_str("1").unwrap(),
                    Fixed::from_decimal_str("1").unwrap(),
                )],
                seq: None,
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

        let reader = ArchiveReader::open(dir.path()).unwrap();
        // Derive the partition date rather than hard-coding one, so the test
        // does not quietly stop covering anything if the base instant moves.
        let date = crate::clock::format_utc_date(base);
        let files = reader.files_for(VenueId::Kraken, &symbol, &date);
        assert!(files.len() >= 3, "expected several rotations");
        assert!(
            files
                .windows(2)
                .all(|w| w[0].first_recv_wall <= w[1].first_recv_wall),
            "files must come back in time order"
        );
    }
}
