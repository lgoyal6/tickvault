//! Rebuilding a book from the archive.
//!
//! # Ordering
//!
//! The archive is replayed in *arrival* order, and arrival order is defined by
//! the files rather than by sorting a column. Within a partition, files hold
//! non-overlapping increasing time ranges and rows within a file are in the
//! order they arrived, so reading files in manifest order and rows in file
//! order is exact.
//!
//! It matters that this is not a sort on `recv_mono`. That column is a
//! monotonic reading anchored to the *recorder process*, so it restarts at zero
//! every time the recorder does. It is exactly comparable within one run and
//! meaningless across two, which is why it is never used to order anything
//! spanning a restart. `recv_wall` is what orders across processes and venues,
//! and it is a wall clock, so it can step.
//!
//! # Where a rebuild starts
//!
//! Replaying a whole day to answer a question about 23:00 is the obvious
//! implementation and the wrong one. Two things avoid it:
//!
//! - **A snapshot resets the book**, so everything before the last snapshot at
//!   or before the requested instant is irrelevant. This costs no extra storage
//!   and is tried first.
//! - **Checkpoints**, for feeds that snapshot rarely or never. Bitstamp's
//!   order-by-order feed has no snapshot at all, so without them a query at the
//!   end of a day really would replay the whole thing.
//!
//! Both are optimisations of the same answer: `tests/gate_reconstruct.rs`
//! asserts that a rebuild starting from a checkpoint is byte-identical to one
//! that replayed from the beginning.

pub mod checkpoint;

use std::collections::BTreeMap;
#[cfg(test)]
use std::path::{Path, PathBuf};

use crate::book::l3::L3Book;
use crate::book::replay::BookReplayer;
use crate::book::{BookSnapshot, L2Book};
use crate::clock::{Stamp, Timestamps};
use crate::error::Result;
use crate::store::manifest::{FileRecord, TruncationRecord};
use crate::store::reader::ArchiveReader;
use crate::store::rows::{MessageStream, RowStream};
use crate::store::scan::Predicate;
use crate::types::{BookLevel, Side, Symbol, VenueId};

/// What a caller wants rebuilt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub venue: VenueId,
    pub symbol: Symbol,
    /// Wall-clock instant, in nanoseconds since the epoch.
    pub at_wall: i64,
    /// Keep only this many levels a side in the result.
    ///
    /// Applied at the end, never during the replay. Truncating as you go is
    /// wrong: a level pushed out of the window can be updated later and has to
    /// come back, and a book that dropped it would be quietly missing depth.
    pub depth: Option<usize>,
    /// Start from the nearest checkpoint when one is available.
    pub use_checkpoints: bool,
}

impl Request {
    pub fn new(venue: VenueId, symbol: &Symbol, at_wall: i64) -> Self {
        Request {
            venue,
            symbol: symbol.clone(),
            at_wall,
            depth: None,
            use_checkpoints: true,
        }
    }

    pub fn with_depth(mut self, depth: usize) -> Self {
        self.depth = Some(depth);
        self
    }

    pub fn without_checkpoints(mut self) -> Self {
        self.use_checkpoints = false;
        self
    }
}

/// Where the replay began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// From a checkpoint written at this instant.
    Checkpoint(i64),
    /// From a venue snapshot in the archive.
    Snapshot(i64),
    /// From the first row available, because nothing earlier reset the book.
    ///
    /// The book is then only as complete as the stream made it, which for a
    /// feed with no snapshot is the normal case rather than a fault.
    FirstRow(i64),
    /// Nothing in the archive at or before the requested instant.
    Empty,
}

impl Origin {
    pub fn wall(&self) -> Option<i64> {
        match self {
            Origin::Checkpoint(w) | Origin::Snapshot(w) | Origin::FirstRow(w) => Some(*w),
            Origin::Empty => None,
        }
    }
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A raw nanosecond count is not something a reader can place in time,
        // and this string is shown to whoever asks where a book came from.
        let at = |w: &i64| crate::clock::format_rfc3339_nanos(*w);
        match self {
            Origin::Checkpoint(w) => write!(f, "checkpoint at {}", at(w)),
            Origin::Snapshot(w) => write!(f, "venue snapshot at {}", at(w)),
            Origin::FirstRow(w) => write!(f, "first archived row at {}", at(w)),
            Origin::Empty => write!(f, "nothing archived"),
        }
    }
}

/// How much of the rebuild the archive can vouch for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trust {
    /// Rows applied that the recorder had marked suspect.
    pub suspect_rows: u64,
    /// Wall-clock span those rows covered.
    pub suspect_span: Option<(i64, i64)>,
    /// Recorded truncations overlapping the replayed window.
    pub truncations: Vec<TruncationRecord>,
}

impl Trust {
    /// True when nothing in the replay was suspect and no truncation overlapped.
    pub fn is_clean(&self) -> bool {
        self.suspect_rows == 0 && self.truncations.is_empty()
    }
}

impl std::fmt::Display for Trust {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_clean() {
            return f.write_str("clean");
        }
        write!(f, "{} suspect row(s)", self.suspect_rows)?;
        if let Some((a, b)) = self.suspect_span {
            write!(f, " spanning {:.3}s", (b - a) as f64 / 1e9)?;
        }
        if !self.truncations.is_empty() {
            write!(f, ", {} truncation(s) in range", self.truncations.len())?;
        }
        Ok(())
    }
}

/// A book as it stood at an instant, and everything a caller needs to judge it.
#[derive(Debug, Clone)]
pub struct Reconstructed {
    pub venue: VenueId,
    pub symbol: Symbol,
    /// The instant asked for, not the instant of the last applied row.
    pub as_of_wall: i64,
    /// The last row actually applied, which is at or before `as_of_wall`.
    pub last_row_wall: Option<i64>,
    pub book: L2Book,
    /// The order-by-order book, on an L3 partition.
    pub l3: Option<L3Book>,
    pub book_level: BookLevel,
    pub origin: Origin,
    pub rows_applied: u64,
    pub files_read: usize,
    pub trust: Trust,
}

impl Reconstructed {
    /// A stable digest of the rebuilt book.
    ///
    /// This is what the determinism gate hashes. It covers the aggregated book
    /// at full depth, so two rebuilds of the same instant agreeing here have
    /// produced the same book rather than merely a similar one.
    pub fn digest(&self) -> u32 {
        self.book.digest()
    }

    pub fn is_empty(&self) -> bool {
        self.book.is_empty()
    }
}

impl std::fmt::Display for Reconstructed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} at {} [{}]: {} bid / {} ask levels from {} rows in {} file(s), from {}, {}",
            self.venue,
            self.symbol,
            self.as_of_wall,
            self.book_level,
            self.book.level_count(Side::Bid),
            self.book.level_count(Side::Ask),
            self.rows_applied,
            self.files_read,
            self.origin,
            self.trust
        )
    }
}

/// Rebuilds books from an archive.
pub struct Reconstructor {
    reader: ArchiveReader,
}

impl Reconstructor {
    pub fn open(root: impl Into<std::path::PathBuf>) -> Result<Self> {
        Ok(Reconstructor {
            reader: ArchiveReader::open(root)?,
        })
    }

    pub fn reader(&self) -> &ArchiveReader {
        &self.reader
    }

    /// Files covering this partition, oldest first.
    fn files_for(&self, request: &Request) -> Vec<FileRecord> {
        let date = crate::clock::format_utc_date(request.at_wall);
        let mut files: Vec<FileRecord> = self
            .reader
            .files()
            .iter()
            .filter(|f| f.venue == request.venue && f.symbol == request.symbol && f.date == date)
            .cloned()
            .collect();
        files.sort_by_key(|f| (f.first_recv_wall, f.path.clone()));
        files
    }

    /// Truncations overlapping the replayed window.
    fn truncations_in(&self, request: &Request, from: i64) -> Vec<TruncationRecord> {
        self.reader
            .manifest()
            .truncations()
            .iter()
            .filter(|t| {
                t.venue.is_none_or(|v| v == request.venue)
                    && t.symbol.as_ref().is_none_or(|s| *s == request.symbol)
                    && t.lost_to_wall >= from
                    && t.lost_from_wall.unwrap_or(i64::MIN) <= request.at_wall
            })
            .cloned()
            .collect()
    }

    /// Rebuild the book as it stood at the requested instant.
    pub fn at(&self, request: &Request) -> Result<Reconstructed> {
        let files = self.files_for(request);
        let level = files.first().map(|f| f.book_level).unwrap_or(BookLevel::L2);
        // A depth-limited feed expects the client to maintain the window.
        // Replaying its deltas without doing so leaves levels that fell out of
        // the feed long ago, and the book crosses within seconds.
        let feed_depth = files.first().and_then(|f| f.feed_depth);

        let checkpoint = if request.use_checkpoints {
            checkpoint::latest_before(
                self.reader.root(),
                request.venue,
                &request.symbol,
                request.at_wall,
            )?
        } else {
            None
        };

        let mut replayer = BookReplayer::new(request.symbol.clone(), level, feed_depth);
        let mut origin = Origin::Empty;
        // A checkpoint is the book *after* everything at or before its instant,
        // so those rows must not be applied again on top of it. Setting a level
        // twice happens to be idempotent, which is what makes this easy to miss;
        // re-adding an order is not, and would corrupt an L3 rebuild outright.
        let mut after: Option<i64> = None;

        if let Some(ckpt) = &checkpoint {
            replayer.seed_l2(ckpt.bids.clone(), ckpt.asks.clone(), ckpt.at_wall);
            replayer.seed_l3(ckpt.orders.clone(), ckpt.at_wall);
            origin = Origin::Checkpoint(ckpt.at_wall);
            after = Some(ckpt.at_wall);
        }

        // Only files whose recorded span can still contain rows we need. The
        // manifest's first and last recv_wall is a zone map, and this is the
        // read of it; see crate::store::scan.
        let (mut relevant, files_pruned) =
            crate::store::scan::files_in_range(files, &Predicate::range(after, request.at_wall));

        // A snapshot resets the book, so anything before the last one at or
        // before the target is irrelevant. Found by walking files backwards,
        // which is cheap because a file either holds a snapshot or it does not.
        if checkpoint.is_none()
            && let Some((index, wall)) = self.last_snapshot_before(&relevant, request.at_wall)?
        {
            relevant.drain(..index);
            // Everything strictly before that snapshot's instant is moot.
            after = Some(wall - 1);
        }

        let mut stream = MessageStream::new(
            RowStream::new(self.reader.root(), relevant, request.at_wall)
                .manifest_pruned(files_pruned)
                .after(after.unwrap_or(i64::MIN)),
        );
        for message in &mut stream {
            let message = message?;
            if origin == Origin::Empty
                && let Some(first) = message.first()
            {
                origin = if first.event == crate::store::schema::EventKind::Snapshot {
                    Origin::Snapshot(first.recv_wall)
                } else {
                    Origin::FirstRow(first.recv_wall)
                };
            }
            replayer.apply_message(&message);
        }

        let from = origin.wall().unwrap_or(request.at_wall);
        let mut book = replayer.book();
        if let Some(depth) = request.depth {
            // Only now. Truncating during the replay would drop levels that
            // later updates bring back.
            book = truncate(&book, depth);
        }

        Ok(Reconstructed {
            venue: request.venue,
            symbol: request.symbol.clone(),
            as_of_wall: request.at_wall,
            last_row_wall: replayer.last_wall(),
            book,
            l3: replayer.l3().cloned(),
            book_level: level,
            origin,
            rows_applied: replayer.rows(),
            files_read: stream.files_opened(),
            trust: Trust {
                suspect_rows: replayer.suspect_rows(),
                suspect_span: replayer.suspect_span(),
                truncations: self.truncations_in(request, from),
            },
        })
    }

    /// Index of the file holding the last snapshot at or before `at`, and that
    /// snapshot's instant.
    ///
    /// Walks backwards, because the answer is the *last* one and the first
    /// file that has any is therefore the answer. Each file is asked through
    /// [`crate::store::scan::last_snapshot_at_or_before`], which pushes both
    /// halves of the question into Parquet: `event == Snapshot` against the
    /// footer statistics, and `recv_wall <= at` against the page index. Before
    /// that existed this decoded every row of every file it touched, all 24
    /// columns including three that allocate a String per row, to look at one
    /// u8 per message. On an order-by-order feed, which never snapshots at
    /// all, it decoded the whole day to return None.
    fn last_snapshot_before(&self, files: &[FileRecord], at: i64) -> Result<Option<(usize, i64)>> {
        for (index, file) in files.iter().enumerate().rev() {
            let path = self.reader.root().join(&file.path);
            let (found, _) = crate::store::scan::last_snapshot_at_or_before(&path, at)?;
            if let Some(wall) = found {
                return Ok(Some((index, wall)));
            }
        }
        Ok(None)
    }
}

/// Keep only the best `depth` levels a side.
fn truncate(book: &L2Book, depth: usize) -> L2Book {
    let mut out = L2Book::new(book.symbol().clone());
    out.reset_from_snapshot(&BookSnapshot {
        symbol: book.symbol().clone(),
        bids: book.top(Side::Bid, depth),
        asks: book.top(Side::Ask, depth),
        seq: None,
        checksum: None,
        stamps: book
            .last_stamps()
            .unwrap_or(Timestamps::recv_only(Stamp::ZERO)),
    });
    out
}

/// Merge several venues' books into one time-ordered view.
///
/// Ordering is by receipt wall clock, and the tie-break is **stated rather than
/// left to chance**: equal timestamps order by venue name, then by symbol. Two
/// venues genuinely can stamp the same nanosecond, and a caller comparing two
/// runs needs the same answer both times.
pub fn merge_by_receipt(mut books: Vec<Reconstructed>) -> Vec<Reconstructed> {
    books.sort_by(|a, b| {
        a.last_row_wall
            .cmp(&b.last_row_wall)
            .then_with(|| a.venue.as_str().cmp(b.venue.as_str()))
            .then_with(|| a.symbol.as_str().cmp(b.symbol.as_str()))
    });
    books
}

/// Digest a set of reconstructed books as one value.
pub fn digest_all(books: &[Reconstructed]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for book in books {
        hasher.update(book.venue.as_str().as_bytes());
        hasher.update(book.symbol.as_str().as_bytes());
        hasher.update(&book.digest().to_le_bytes());
    }
    hasher.finalize()
}

/// Per-venue books at one instant, keyed for lookup.
pub type Multi = BTreeMap<(VenueId, Symbol), Reconstructed>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{BookDelta, LevelChange};
    use crate::fixed::Fixed;
    use crate::store::schema::RowBuilder;
    use crate::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};

    const BASE: i64 = 1_787_000_000_000_000_000;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    fn sym() -> Symbol {
        Symbol::new("BTC", "USD")
    }

    fn stamps(wall: i64) -> Timestamps {
        Timestamps::new(
            Stamp {
                mono_nanos: wall as u64,
                wall_nanos: wall,
            },
            Some(wall - 1_000),
        )
    }

    fn snapshot(wall: i64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> BookSnapshot {
        BookSnapshot {
            symbol: sym(),
            bids: bids.iter().map(|(p, q)| (f(p), f(q))).collect(),
            asks: asks.iter().map(|(p, q)| (f(p), f(q))).collect(),
            seq: None,
            checksum: None,
            stamps: stamps(wall),
        }
    }

    fn delta(wall: i64, side: Side, price: &str, qty: &str) -> BookDelta {
        BookDelta {
            symbol: sym(),
            changes: vec![LevelChange::new(side, f(price), f(qty))],
            seq: None,
            checksum: None,
            stamps: stamps(wall),
            prev_seq: None,
            first_seq: None,
        }
    }

    /// Build a small archive: a snapshot, then deltas one second apart.
    fn archive(dir: &Path, rows_per_file: usize) -> PathBuf {
        let root = dir.to_path_buf();
        let mut writer = ArchiveWriter::open(WriterConfig {
            max_rows_per_file: rows_per_file,
            ..WriterConfig::new(&root)
        })
        .unwrap();
        let key = PartitionKey::new(VenueId::Kraken, &sym(), BASE);

        let push = |builder: RowBuilder, w: &mut ArchiveWriter| {
            let mut builder = builder;
            w.write_builder(&key, &mut builder).unwrap();
        };

        let mut b = RowBuilder::new();
        b.push_snapshot(
            VenueId::Kraken,
            &snapshot(BASE, &[("100", "1"), ("99", "2")], &[("101", "3")]),
            0,
            false,
        );
        push(b, &mut writer);

        // Ten one-second steps, each raising the best bid's size.
        for i in 1..=10i64 {
            let mut b = RowBuilder::new();
            b.push_delta(
                VenueId::Kraken,
                &delta(BASE + i * 1_000_000_000, Side::Bid, "100", &format!("{i}")),
                i as u64,
                false,
            );
            push(b, &mut writer);
        }
        writer.close().unwrap();
        root
    }

    fn rebuild(root: &Path, at: i64) -> Reconstructed {
        Reconstructor::open(root)
            .unwrap()
            .at(&Request::new(VenueId::Kraken, &sym(), at))
            .unwrap()
    }

    #[test]
    fn a_book_rebuilds_as_it_stood_at_the_instant_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let root = archive(dir.path(), 1_000);

        // After the third delta, the best bid is 3.
        let at = BASE + 3_500_000_000;
        let r = rebuild(&root, at);
        assert_eq!(r.book.best_bid(), Some((f("100"), f("3"))));
        assert_eq!(r.book.best_ask(), Some((f("101"), f("3"))));
        assert_eq!(r.as_of_wall, at);
        assert_eq!(r.last_row_wall, Some(BASE + 3_000_000_000));
        assert!(r.trust.is_clean(), "{}", r.trust);
    }

    #[test]
    fn asking_at_different_instants_gives_different_books() {
        let dir = tempfile::tempdir().unwrap();
        let root = archive(dir.path(), 1_000);
        let early = rebuild(&root, BASE + 1_500_000_000);
        let late = rebuild(&root, BASE + 9_500_000_000);
        assert_eq!(early.book.best_bid(), Some((f("100"), f("1"))));
        assert_eq!(late.book.best_bid(), Some((f("100"), f("9"))));
        assert_ne!(early.digest(), late.digest());
    }

    #[test]
    fn asking_before_anything_was_recorded_yields_an_empty_book() {
        let dir = tempfile::tempdir().unwrap();
        let root = archive(dir.path(), 1_000);
        let r = rebuild(&root, BASE - 1);
        assert!(r.is_empty());
        assert_eq!(r.origin, Origin::Empty);
        assert_eq!(r.rows_applied, 0);
    }

    #[test]
    fn asking_after_everything_yields_the_final_book() {
        let dir = tempfile::tempdir().unwrap();
        let root = archive(dir.path(), 1_000);
        let r = rebuild(&root, BASE + 1_000_000_000_000);
        assert_eq!(r.book.best_bid(), Some((f("100"), f("10"))));
        assert_eq!(r.last_row_wall, Some(BASE + 10_000_000_000));
    }

    #[test]
    fn a_rebuild_starts_from_the_last_snapshot_not_the_beginning() {
        // Two snapshots: the second must make everything before it irrelevant.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let mut writer = ArchiveWriter::open(WriterConfig {
            max_rows_per_file: 2,
            ..WriterConfig::new(&root)
        })
        .unwrap();
        let key = PartitionKey::new(VenueId::Kraken, &sym(), BASE);
        for (i, snap) in [
            snapshot(BASE, &[("50", "1")], &[("51", "1")]),
            snapshot(BASE + 5_000_000_000, &[("100", "7")], &[("101", "8")]),
        ]
        .into_iter()
        .enumerate()
        {
            let mut b = RowBuilder::new();
            b.push_snapshot(VenueId::Kraken, &snap, i as u64, false);
            writer.write_builder(&key, &mut b).unwrap();
        }
        writer.close().unwrap();

        let r = rebuild(&root, BASE + 9_000_000_000);
        assert_eq!(r.book.best_bid(), Some((f("100"), f("7"))));
        assert_eq!(r.origin, Origin::Snapshot(BASE + 5_000_000_000));
        assert!(
            r.book.qty_at(Side::Bid, f("50")).is_none(),
            "the first snapshot must have been discarded, not merged"
        );
    }

    #[test]
    fn depth_is_applied_at_the_end_not_during_the_replay() {
        // A level pushed out of a narrow window has to come back when it is
        // updated later. Truncating as you go would lose it.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let mut writer = ArchiveWriter::open(WriterConfig::new(&root)).unwrap();
        let key = PartitionKey::new(VenueId::Kraken, &sym(), BASE);

        let mut b = RowBuilder::new();
        b.push_snapshot(
            VenueId::Kraken,
            &snapshot(
                BASE,
                &[("100", "1"), ("99", "1"), ("98", "1")],
                &[("101", "1")],
            ),
            0,
            false,
        );
        writer.write_builder(&key, &mut b).unwrap();
        // The deepest level grows to become the best.
        let mut b = RowBuilder::new();
        b.push_delta(
            VenueId::Kraken,
            &delta(BASE + 1_000_000_000, Side::Bid, "98", "9"),
            1,
            false,
        );
        writer.write_builder(&key, &mut b).unwrap();
        writer.close().unwrap();

        let full = rebuild(&root, BASE + 2_000_000_000);
        let limited = Reconstructor::open(&root)
            .unwrap()
            .at(&Request::new(VenueId::Kraken, &sym(), BASE + 2_000_000_000).with_depth(2))
            .unwrap();
        assert_eq!(full.book.level_count(Side::Bid), 3);
        assert_eq!(limited.book.level_count(Side::Bid), 2);
        // Depth-limited must be exactly the truncation of the full book.
        assert_eq!(limited.book.top(Side::Bid, 2), full.book.top(Side::Bid, 2));
        assert_eq!(
            full.book.qty_at(Side::Bid, f("98")),
            Some(f("9")),
            "the deep level's later update must have been applied"
        );
    }

    #[test]
    fn suspect_rows_are_surfaced_rather_than_smoothed_over() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let mut writer = ArchiveWriter::open(WriterConfig::new(&root)).unwrap();
        let key = PartitionKey::new(VenueId::Kraken, &sym(), BASE);

        let mut b = RowBuilder::new();
        b.push_snapshot(
            VenueId::Kraken,
            &snapshot(BASE, &[("100", "1")], &[]),
            0,
            false,
        );
        writer.write_builder(&key, &mut b).unwrap();
        let mut b = RowBuilder::new();
        // Marked suspect by the recorder.
        b.push_delta(
            VenueId::Kraken,
            &delta(BASE + 1_000_000_000, Side::Bid, "100", "5"),
            1,
            true,
        );
        writer.write_builder(&key, &mut b).unwrap();
        writer.close().unwrap();

        let r = rebuild(&root, BASE + 2_000_000_000);
        assert!(!r.trust.is_clean());
        assert_eq!(r.trust.suspect_rows, 1);
        assert_eq!(
            r.trust.suspect_span,
            Some((BASE + 1_000_000_000, BASE + 1_000_000_000))
        );
        // The row is still applied: the caller decides what to do about it.
        assert_eq!(r.book.best_bid(), Some((f("100"), f("5"))));
        assert!(r.trust.to_string().contains("suspect"));
    }

    #[test]
    fn a_rebuild_reads_only_the_files_it_needs() {
        let dir = tempfile::tempdir().unwrap();
        // Two rows per file, so the archive is many small files.
        let root = archive(dir.path(), 2);
        let total = Reconstructor::open(&root).unwrap().reader().files().len();
        assert!(total >= 4, "expected several files, got {total}");

        let early = rebuild(&root, BASE + 1_500_000_000);
        assert!(
            early.files_read < total,
            "reading {} of {total} files to answer an early question",
            early.files_read
        );
    }

    #[test]
    fn merging_venues_orders_by_receipt_with_a_stated_tie_break() {
        let dir = tempfile::tempdir().unwrap();
        let root = archive(dir.path(), 1_000);
        let one = rebuild(&root, BASE + 3_000_000_000);
        let mut two = one.clone();
        two.venue = VenueId::Coinbase;
        let mut three = one.clone();
        three.venue = VenueId::Okx;

        // Same receipt instant on all three, so only the tie-break separates
        // them, and it must be the same order every time.
        let merged = merge_by_receipt(vec![three, one, two]);
        let order: Vec<&str> = merged.iter().map(|r| r.venue.as_str()).collect();
        assert_eq!(order, vec!["coinbase", "kraken", "okx"]);
        assert_eq!(
            digest_all(&merged),
            digest_all(&merge_by_receipt(merged.clone()))
        );
    }
}
