//! The archive's durable index.
//!
//! A Parquet file is only readable once its footer is written, which happens at
//! close. So a process killed mid-write leaves a file that no reader can open,
//! and the archive has no way to tell that from a file it never finished
//! writing on purpose. The manifest is what closes that gap: one append-and-
//! fsync per completed file, so at any instant the set of files known to be
//! complete is exactly the set that is safe to publish.
//!
//! Everything else follows from that. On restart, a `.parquet` on disk that the
//! manifest does not name was interrupted; it is quarantined rather than read,
//! and a truncation record is appended naming the window that was lost. The
//! window is a bound, not an estimate, and the rotation interval is what makes
//! the bound tight.
//!
//! It is a JSONL file rather than anything cleverer because it has to survive a
//! `SIGKILL` between any two bytes. Appending one line and fsyncing it is a
//! thing an operating system will actually promise.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::types::{Symbol, VenueId};

/// Archives written before the L3 columns existed hold aggregated levels.
fn default_book_level() -> crate::types::BookLevel {
    crate::types::BookLevel::L2
}

/// Name of the manifest inside the archive root. The leading underscore keeps
/// Hive-style partition discovery from treating it as data.
pub const MANIFEST_FILE: &str = "_manifest.jsonl";

/// Directory interrupted files are moved to.
pub const QUARANTINE_DIR: &str = "_quarantine";

/// A file the writer finished and closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRecord {
    /// Path relative to the archive root, so the archive can be moved.
    pub path: String,
    pub venue: VenueId,
    pub symbol: Symbol,
    /// UTC `YYYY-MM-DD`, matching the partition directory.
    pub date: String,
    /// Which book level this file holds: aggregated levels or order by order.
    ///
    /// Recorded per file, so the published dataset can state what it holds per
    /// venue per day rather than leaving a consumer to infer it from whether
    /// the order id column happens to be populated.
    #[serde(default = "default_book_level")]
    pub book_level: crate::types::BookLevel,
    /// Levels a side the feed carried, when it was depth limited.
    ///
    /// A rebuild must truncate exactly as the recorder did, so the archive
    /// records the window rather than leaving a reader to guess it.
    #[serde(default)]
    pub feed_depth: Option<usize>,
    pub rows: u64,
    pub bytes: u64,
    pub first_recv_wall: i64,
    pub last_recv_wall: i64,
    pub closed_wall: i64,
}

/// A stretch of time the archive cannot account for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncationRecord {
    pub venue: Option<VenueId>,
    pub symbol: Option<Symbol>,
    /// When we noticed, which is restart time rather than failure time.
    pub detected_wall: i64,
    pub reason: String,
    /// Where the unreadable file was moved, if there was one.
    pub quarantined: Option<String>,
    /// Last instant we can vouch for, from the newest complete file.
    pub lost_from_wall: Option<i64>,
    /// First instant we can vouch for again.
    pub lost_to_wall: i64,
    /// Rows in the interrupted file that could not be recovered.
    ///
    /// Always `None`: a Parquet file without a footer cannot be read at all, so
    /// this records that the count is unknown rather than implying zero.
    pub unrecoverable_rows: Option<u64>,
}

impl TruncationRecord {
    /// Nanoseconds the archive cannot vouch for, when both ends are known.
    pub fn lost_nanos(&self) -> Option<i64> {
        Some(self.lost_to_wall - self.lost_from_wall?)
    }
}

/// Many small files replaced by one, recorded as a single entry.
///
/// One entry rather than two on purpose. Writing "here is the daily file" and
/// "those hourly files are superseded" as separate appends leaves a window
/// where a crash makes the archive either double-count the rows or lose them,
/// depending which order they went in. Recording both facts in one line makes
/// the swap atomic, because a single append either lands or it does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionRecord {
    pub produced: FileRecord,
    /// Paths now superseded. Their rows live in `produced`.
    pub retired: Vec<String>,
    pub at_wall: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ManifestEntry {
    File(FileRecord),
    Truncation(TruncationRecord),
    Compaction(CompactionRecord),
}

/// An append-only index of everything the archive is prepared to stand behind.
#[derive(Debug)]
pub struct Manifest {
    path: PathBuf,
    files: Vec<FileRecord>,
    truncations: Vec<TruncationRecord>,
    /// Paths superseded by compaction. Kept so recovery neither adopts them
    /// back nor mistakes a leftover for something worth keeping.
    retired: BTreeSet<String>,
}

impl Manifest {
    /// Open, creating an empty manifest if the archive is new.
    ///
    /// A malformed trailing line is dropped rather than failing the open: a
    /// kill can land mid-line, and refusing to start because the last write was
    /// half-finished would turn a recoverable crash into an outage. Malformed
    /// lines anywhere else are a real error.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let path = root.join(MANIFEST_FILE);
        let mut files = Vec::new();
        let mut truncations = Vec::new();
        let mut retired: BTreeSet<String> = BTreeSet::new();

        if path.exists() {
            let text = std::fs::read_to_string(&path)?;
            let lines: Vec<&str> = text.lines().collect();
            for (i, line) in lines.iter().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<ManifestEntry>(line) {
                    Ok(ManifestEntry::File(f)) => files.push(f),
                    Ok(ManifestEntry::Truncation(t)) => truncations.push(t),
                    Ok(ManifestEntry::Compaction(c)) => {
                        retired.extend(c.retired.iter().cloned());
                        files.push(c.produced);
                    }
                    Err(e) if i + 1 == lines.len() => {
                        // Torn final line: the process died mid-append.
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "discarding a torn final manifest line"
                        );
                    }
                    Err(e) => {
                        return Err(Error::Other(format!(
                            "{}:{}: corrupt manifest entry: {e}",
                            path.display(),
                            i + 1
                        )));
                    }
                }
            }
        }
        // A retired file is no longer part of the archive, however it was
        // originally recorded.
        files.retain(|f| !retired.contains(&f.path));
        Ok(Manifest {
            path,
            files,
            truncations,
            retired,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn files(&self) -> &[FileRecord] {
        &self.files
    }

    pub fn truncations(&self) -> &[TruncationRecord] {
        &self.truncations
    }

    /// Paths the manifest has an opinion about, live or superseded.
    ///
    /// Recovery uses this to decide what is genuinely unaccounted for, so it
    /// must include retired paths: a superseded file left on disk by an
    /// interrupted compaction is not an orphan to adopt back.
    pub fn known_paths(&self) -> BTreeSet<String> {
        let mut paths: BTreeSet<String> = self.files.iter().map(|f| f.path.clone()).collect();
        paths.extend(self.retired.iter().cloned());
        paths
    }

    /// Paths superseded by compaction, whose bytes may still be on disk.
    pub fn retired_paths(&self) -> &BTreeSet<String> {
        &self.retired
    }

    /// The newest instant covered by a complete file for this partition.
    pub fn last_covered(&self, venue: VenueId, symbol: &Symbol) -> Option<i64> {
        self.files
            .iter()
            .filter(|f| f.venue == venue && f.symbol == *symbol)
            .map(|f| f.last_recv_wall)
            .max()
    }

    pub fn total_rows(&self) -> u64 {
        self.files.iter().map(|f| f.rows).sum()
    }

    fn append(&self, entry: &ManifestEntry) -> Result<()> {
        let mut line = serde_json::to_string(entry)?;
        line.push('\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        // The whole design rests on this. Without the fsync the manifest can
        // lag the files it describes across a power loss, and the archive would
        // vouch for a file that is not there or fail to vouch for one that is.
        file.sync_data()?;
        Ok(())
    }

    /// Record a completed file. Durable before it returns.
    pub fn record_file(&mut self, record: FileRecord) -> Result<()> {
        self.append(&ManifestEntry::File(record.clone()))?;
        self.files.push(record);
        Ok(())
    }

    /// Swap many files for one, atomically. Durable before it returns.
    pub fn record_compaction(&mut self, record: CompactionRecord) -> Result<()> {
        self.append(&ManifestEntry::Compaction(record.clone()))?;
        self.retired.extend(record.retired.iter().cloned());
        self.files.retain(|f| !self.retired.contains(&f.path));
        self.files.push(record.produced);
        Ok(())
    }

    /// Record a stretch we cannot account for. Durable before it returns.
    pub fn record_truncation(&mut self, record: TruncationRecord) -> Result<()> {
        self.append(&ManifestEntry::Truncation(record.clone()))?;
        self.truncations.push(record);
        Ok(())
    }

    /// Fsync the directory holding the manifest.
    ///
    /// Fsyncing a file does not promise its *directory entry* survived, so a
    /// brand new manifest can vanish entirely across a power loss without this.
    pub fn sync_dir(&self) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(path: &str, rows: u64, first: i64, last: i64) -> FileRecord {
        FileRecord {
            path: path.to_string(),
            venue: VenueId::Kraken,
            symbol: Symbol::new("BTC", "USD"),
            date: "2026-08-24".to_string(),
            book_level: crate::types::BookLevel::L2,
            feed_depth: None,
            rows,
            bytes: rows * 40,
            first_recv_wall: first,
            last_recv_wall: last,
            closed_wall: last + 1,
        }
    }

    #[test]
    fn entries_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::open(dir.path()).unwrap();
            m.record_file(record("a.parquet", 10, 100, 200)).unwrap();
            m.record_file(record("b.parquet", 5, 200, 300)).unwrap();
        }
        let m = Manifest::open(dir.path()).unwrap();
        assert_eq!(m.files().len(), 2);
        assert_eq!(m.total_rows(), 15);
        assert_eq!(
            m.last_covered(VenueId::Kraken, &Symbol::new("BTC", "USD")),
            Some(300)
        );
        assert_eq!(
            m.last_covered(VenueId::Coinbase, &Symbol::new("BTC", "USD")),
            None
        );
    }

    #[test]
    fn a_torn_final_line_is_dropped_rather_than_failing_the_open() {
        // Exactly what a kill mid-append leaves behind. Refusing to start would
        // turn a recoverable crash into an outage.
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::open(dir.path()).unwrap();
            m.record_file(record("a.parquet", 10, 100, 200)).unwrap();
        }
        let path = dir.path().join(MANIFEST_FILE);
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str("{\"kind\":\"file\",\"path\":\"b.par");
        std::fs::write(&path, text).unwrap();

        let m = Manifest::open(dir.path()).unwrap();
        assert_eq!(m.files().len(), 1, "the complete entry must survive");
        assert!(!m.known_paths().contains("b.parquet"));
    }

    #[test]
    fn corruption_anywhere_but_the_last_line_is_an_error() {
        // A torn line in the middle cannot be a partial append, so it means
        // something else is wrong and starting anyway would compound it.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(MANIFEST_FILE),
            "not json\n{\"kind\":\"file\",\"path\":\"a\"}\n",
        )
        .unwrap();
        let err = Manifest::open(dir.path()).unwrap_err().to_string();
        assert!(err.contains(":1:"), "the error must name the line: {err}");
    }

    #[test]
    fn a_truncation_records_the_window_it_could_not_cover() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = Manifest::open(dir.path()).unwrap();
        m.record_truncation(TruncationRecord {
            venue: Some(VenueId::Kraken),
            symbol: Some(Symbol::new("BTC", "USD")),
            detected_wall: 1_000,
            reason: "process died mid-write".to_string(),
            quarantined: Some("_quarantine/part-7.parquet".to_string()),
            lost_from_wall: Some(400),
            lost_to_wall: 1_000,
            unrecoverable_rows: None,
        })
        .unwrap();

        let m = Manifest::open(dir.path()).unwrap();
        assert_eq!(m.truncations().len(), 1);
        assert_eq!(m.truncations()[0].lost_nanos(), Some(600));
        assert_eq!(
            m.truncations()[0].unrecoverable_rows,
            None,
            "a footerless file cannot be counted, and None says so"
        );
    }

    #[test]
    fn a_truncation_with_no_prior_coverage_reports_an_open_start() {
        let t = TruncationRecord {
            venue: None,
            symbol: None,
            detected_wall: 10,
            reason: "first run died".to_string(),
            quarantined: None,
            lost_from_wall: None,
            lost_to_wall: 10,
            unrecoverable_rows: None,
        };
        // Not zero. Nothing is known about how far back the loss goes.
        assert_eq!(t.lost_nanos(), None);
    }

    #[test]
    fn compaction_swaps_files_in_one_atomic_entry() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::open(dir.path()).unwrap();
            m.record_file(record("a.parquet", 10, 100, 200)).unwrap();
            m.record_file(record("b.parquet", 5, 200, 300)).unwrap();
            m.record_compaction(CompactionRecord {
                produced: record("daily.parquet", 15, 100, 300),
                retired: vec!["a.parquet".to_string(), "b.parquet".to_string()],
                at_wall: 400,
            })
            .unwrap();
        }
        let m = Manifest::open(dir.path()).unwrap();
        assert_eq!(m.files().len(), 1, "only the compacted file remains live");
        assert_eq!(m.files()[0].path, "daily.parquet");
        assert_eq!(m.total_rows(), 15, "rows must not be double counted");
        // But the old paths are still accounted for, so recovery does not
        // adopt leftovers back into the archive.
        assert!(m.known_paths().contains("a.parquet"));
        assert_eq!(m.retired_paths().len(), 2);
    }

    #[test]
    fn an_empty_archive_opens_clean() {
        let dir = tempfile::tempdir().unwrap();
        let m = Manifest::open(dir.path().join("nested")).unwrap();
        assert!(m.files().is_empty());
        assert!(m.truncations().is_empty());
        assert_eq!(m.total_rows(), 0);
        m.sync_dir().unwrap();
    }
}
