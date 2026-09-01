//! **Scan planner gate.**
//!
//! A planner that prunes is only worth having if it prunes *exactly* the rows
//! nothing wanted. Three properties, in order of how badly they fail:
//!
//! 1. **Equivalence.** A planned scan yields the same rows, in the same order,
//!    as one that reads every row group and every column. Anything else is
//!    silent data loss, which is the failure mode this whole repository
//!    exists to make impossible.
//! 2. **It actually prunes.** A narrow range in the middle of an archive must
//!    read a small fraction of the row groups and pages. Without this the
//!    planner could be a no-op and property 1 would still pass.
//! 3. **It only prunes what is clustered.** The whole thing works because the
//!    archive is written in arrival order, so `recv_wall` is sorted. A
//!    predicate on `price` would prune nothing at all, and this asserts that
//!    rather than letting the headline number imply pruning is free.

mod common;

use std::path::Path;

use tickvault::book::{BookDelta, BookSnapshot, LevelChange};
use tickvault::clock::{Stamp, Timestamps};
use tickvault::fixed::Fixed;
use tickvault::reconstruct::{Reconstructor, Request};
use tickvault::store::reader::ArchiveReader;
use tickvault::store::rows::{Row, RowStream};
use tickvault::store::scan::{self, Predicate, ScanMode};
use tickvault::store::schema::{EventKind, RowBuilder};
use tickvault::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};
use tickvault::types::{Side, Symbol, VenueId};

const VENUE: VenueId = VenueId::Coinbase;
/// Nanoseconds between messages. A tenth of a millisecond, which is the order
/// a busy venue actually moves at.
const TICK: i64 = 100_000;
const EPOCH: i64 = 1_787_000_000_000_000_000;

fn symbol() -> Symbol {
    Symbol::new("BTC", "USD")
}

/// Write an archive of `messages` messages, in arrival order.
///
/// Deliberately not a fixture replay. The fixtures are a few hundred messages,
/// which fits in one row group and one page, and a pruning test over a single
/// row group would assert nothing. Prices walk a random-ish path rather than
/// climbing, because a monotonic price column would be clustered too and would
/// make property 3 pass for the wrong reason.
fn write_archive(
    root: &Path,
    messages: usize,
    levels: usize,
    rows_per_file: usize,
    row_group_size: usize,
    data_page_rows: usize,
    snapshot_every: Option<usize>,
) -> u64 {
    let symbol = symbol();
    let mut writer = ArchiveWriter::open(WriterConfig {
        max_rows_per_file: rows_per_file,
        row_group_size,
        // Stated rather than inherited. A gate whose meaning changes when a
        // default changes is a gate that will one day stop asserting what its
        // name says.
        data_page_rows,
        max_file_age: std::time::Duration::from_secs(86_400),
        ..WriterConfig::new(root)
    })
    .expect("open archive");

    let mut builder = RowBuilder::new();
    let mut rows = 0u64;
    let mut key = None;
    let mut state = 0x2545F491u64;

    for i in 0..messages {
        let wall = EPOCH + i as i64 * TICK;
        let stamps = Timestamps::recv_only(Stamp {
            mono_nanos: i as u64 * TICK as u64,
            wall_nanos: wall,
        });
        key.get_or_insert_with(|| PartitionKey::new(VENUE, &symbol, wall));

        // xorshift, so the price path is reproducible and not monotonic.
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let jitter = (state % 4_000) as i64 - 2_000;

        if snapshot_every.is_some_and(|n| i % n == 0) {
            let mid = 80_000_000_000_000i64 + jitter * 1_000_000;
            builder.push_snapshot(
                VENUE,
                &BookSnapshot {
                    symbol: symbol.clone(),
                    bids: (0..levels)
                        .map(|l| {
                            (
                                Fixed::from_mantissa(mid - (l as i64 + 1) * 100_000),
                                Fixed::from_mantissa(1_000_000),
                            )
                        })
                        .collect(),
                    asks: (0..levels)
                        .map(|l| {
                            (
                                Fixed::from_mantissa(mid + (l as i64 + 1) * 100_000),
                                Fixed::from_mantissa(1_000_000),
                            )
                        })
                        .collect(),
                    seq: Some(i as u64),
                    checksum: None,
                    stamps,
                },
                i as u64,
                false,
            );
            rows += (levels * 2) as u64;
        } else {
            let mid = 80_000_000_000_000i64 + jitter * 1_000_000;
            builder.push_delta(
                VENUE,
                &BookDelta {
                    symbol: symbol.clone(),
                    changes: (0..levels)
                        .map(|l| LevelChange {
                            side: if (i + l).is_multiple_of(2) {
                                Side::Bid
                            } else {
                                Side::Ask
                            },
                            price: Fixed::from_mantissa(mid + (l as i64) * 100_000),
                            qty: Fixed::from_mantissa(((i + l) % 97 + 1) as i64 * 1_000_000),
                        })
                        .collect(),
                    seq: Some(i as u64),
                    first_seq: None,
                    prev_seq: None,
                    checksum: None,
                    stamps,
                },
                i as u64,
                false,
            );
            rows += levels as u64;
        }

        if builder.len() >= 4_096 {
            writer
                .write_builder(key.as_ref().unwrap(), &mut builder)
                .expect("write");
        }
    }
    if !builder.is_empty() {
        writer
            .write_builder(key.as_ref().unwrap(), &mut builder)
            .expect("write");
    }
    writer.close().expect("close");
    rows
}

fn collect(root: &Path, after: i64, until: i64, mode: ScanMode) -> (Vec<Row>, scan::Explain) {
    let reader = ArchiveReader::open(root).expect("open");
    let files = tickvault::store::rows::partition_files(
        &reader,
        VENUE,
        &symbol(),
        &tickvault::clock::format_utc_date(EPOCH),
    );
    let (kept, pruned) = scan::files_in_range(files, &Predicate::range(Some(after), until));
    let mut stream = RowStream::new(root, kept, until)
        .manifest_pruned(pruned)
        .after(after);
    if mode == ScanMode::Unpruned {
        stream = stream.unpruned();
    }
    let rows: Vec<Row> = (&mut stream).map(|r| r.expect("row")).collect();
    let explain = stream.explain().clone();
    (rows, explain)
}

#[test]
fn a_planned_scan_yields_exactly_what_an_unplanned_one_does() {
    let dir = tempfile::tempdir().unwrap();
    let messages = 40_000;
    write_archive(dir.path(), messages, 4, 60_000, 5_000, 1_000, Some(2_000));
    let last = EPOCH + (messages as i64 - 1) * TICK;

    // Ranges chosen to land on and off page and row group boundaries, at both
    // edges and in the middle. A planner that is right in the middle of a
    // page and wrong at its edge is the interesting kind of wrong.
    let spans: Vec<(i64, i64)> = vec![
        (EPOCH - 1, last),                                // everything
        (EPOCH - 1, EPOCH + 10 * TICK),                   // the first few
        (last - 10 * TICK, last),                         // the last few
        (EPOCH + 19_999 * TICK, EPOCH + 20_001 * TICK),   // two messages
        (EPOCH + 5_000 * TICK - 1, EPOCH + 5_000 * TICK), // exactly one
        (EPOCH + 12_345 * TICK, EPOCH + 23_456 * TICK),   // a middling slab
        (last + 1, last + 1_000),                         // past the end
    ];

    for (after, until) in spans {
        let (planned, explain) = collect(dir.path(), after, until, ScanMode::Planned);
        let (unplanned, _) = collect(dir.path(), after, until, ScanMode::Unpruned);
        assert_eq!(
            planned, unplanned,
            "planned and unplanned scans disagree over ({after}, {until}]\n{explain}"
        );
    }
}

#[test]
fn a_narrow_range_reads_a_small_part_of_the_archive() {
    let dir = tempfile::tempdir().unwrap();
    let messages = 40_000;
    write_archive(dir.path(), messages, 4, 60_000, 5_000, 1_000, None);

    // One thousandth of the archive, out of the middle so nothing is pruned
    // for the trivial reason of being at an edge.
    let from = EPOCH + 20_000 * TICK;
    let (rows, explain) = collect(dir.path(), from, from + 40 * TICK, ScanMode::Planned);
    assert!(!rows.is_empty(), "the range should not be empty");

    assert!(
        explain.row_groups_read * 8 < explain.row_groups_total,
        "row group pruning did not happen:\n{explain}"
    );
    assert!(
        explain.pages_total > 0,
        "the archive should carry a page index:\n{explain}"
    );
    assert!(
        explain.pages_read < explain.pages_total,
        "page pruning did not happen:\n{explain}"
    );
    assert!(
        explain.rows_selected * 20 < explain.rows_total,
        "most of the archive was still decoded:\n{explain}"
    );
    assert!(
        explain.columns_projected < explain.columns_in_file,
        "no columns were projected away:\n{explain}"
    );
    assert!(
        explain.bytes_projected * 4 < explain.bytes_total,
        "the scan is still in scope for most of the file's bytes:\n{explain}"
    );
}

/// The honest half of the claim.
///
/// Pruning works here because the archive is written in arrival order, so
/// `recv_wall` is sorted and a range predicate on it lands in a handful of row
/// groups. Nothing else in the schema has that property. A point predicate on
/// `price` would keep almost every row group, because prices move up and down
/// all day and every group's min and max span most of the book.
///
/// This counts, from the footer alone, how many row groups a point predicate
/// on each column would have to read.
#[test]
fn only_the_clustered_column_prunes() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::statistics::Statistics;

    let dir = tempfile::tempdir().unwrap();
    write_archive(dir.path(), 40_000, 4, 200_000, 5_000, 1_000, None);

    let reader = ArchiveReader::open(dir.path()).expect("open");
    let file = reader.files().first().expect("one file").clone();
    let handle = std::fs::File::open(dir.path().join(&file.path)).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(handle).unwrap();
    let meta = builder.metadata();
    let schema = builder.parquet_schema();

    let column = |name: &str| {
        schema
            .columns()
            .iter()
            .position(|c| c.name() == name)
            .unwrap()
    };
    let bounds = |col: usize| -> Vec<(i64, i64)> {
        meta.row_groups()
            .iter()
            .filter_map(|rg| match rg.column(col).statistics() {
                Some(Statistics::Int64(s)) => Some((*s.min_opt()?, *s.max_opt()?)),
                _ => None,
            })
            .collect()
    };

    let walls = bounds(column("recv_wall"));
    let prices = bounds(column("price"));
    assert!(walls.len() > 8, "need several row groups to say anything");
    assert_eq!(walls.len(), prices.len());

    // A point in the middle of a column's overall range, and how many row
    // groups could still contain it.
    let mid = |b: &[(i64, i64)]| {
        let lo = b.iter().map(|(min, _)| *min).min().unwrap();
        let hi = b.iter().map(|(_, max)| *max).max().unwrap();
        lo + (hi - lo) / 2
    };
    let hits = |b: &[(i64, i64)], at: i64| {
        b.iter()
            .filter(|(min, max)| at >= *min && at <= *max)
            .count()
    };

    // The min of a middle row group, which is a value that certainly exists.
    // A computed midpoint can land in the one-tick gap between two groups and
    // hit nothing, which would make this pass for the wrong reason.
    let wall_hits = hits(&walls, walls[walls.len() / 2].0);
    let price_hits = hits(&prices, mid(&prices));

    assert_eq!(
        wall_hits, 1,
        "recv_wall is written in order, so exactly one row group can hold an instant"
    );
    assert!(
        price_hits * 2 > prices.len(),
        "if a price predicate pruned most row groups, the claim that only the \
         clustered column prunes would be wrong, and the headline number would \
         be describing something other than clustering. {price_hits} of {} row \
         groups contain the mid price",
        prices.len()
    );
}

/// The reconstructor's backwards walk, which is what the planner speeds up
/// most.
#[test]
fn the_snapshot_probe_agrees_with_decoding_the_whole_file() {
    let dir = tempfile::tempdir().unwrap();
    let messages = 20_000;
    write_archive(dir.path(), messages, 4, 200_000, 5_000, 1_000, Some(1_500));

    let reader = ArchiveReader::open(dir.path()).expect("open");
    let file = reader.files().first().expect("one file").clone();
    let path = dir.path().join(&file.path);

    for divisor in [1i64, 2, 3, 7, 100] {
        let at = EPOCH + (messages as i64 * TICK) / divisor;
        let (found, explain) = scan::last_snapshot_at_or_before(&path, at).unwrap();
        assert_eq!(
            found,
            naive_last_snapshot(dir.path(), &file.path, at),
            "the probe disagrees with a full decode at {at}\n{explain}"
        );
    }
}

/// A feed that never snapshots is the pathological case: the reconstructor
/// walks every file backwards looking for something that is not there. The
/// footer answers it without a page being read.
#[test]
fn a_file_with_no_snapshot_is_answered_from_its_footer() {
    let dir = tempfile::tempdir().unwrap();
    write_archive(dir.path(), 20_000, 4, 200_000, 5_000, 1_000, None);

    let reader = ArchiveReader::open(dir.path()).expect("open");
    let file = reader.files().first().expect("one file").clone();
    let (found, explain) =
        scan::last_snapshot_at_or_before(&dir.path().join(&file.path), i64::MAX).unwrap();

    assert_eq!(found, None);
    assert_eq!(
        explain.row_groups_read, 0,
        "every row group is deltas, so the event statistic should have skipped all of them:\n{explain}"
    );
    assert_eq!(explain.rows_selected, 0);
    assert!(
        explain.rows_total > 0,
        "the file should not be empty, or this proves nothing"
    );
}

/// A full decode, for the probe to be checked against.
fn naive_last_snapshot(root: &Path, relative: &str, at: i64) -> Option<i64> {
    let batches = tickvault::store::reader::read_batches(root.join(relative)).expect("read");
    let mut best = None;
    for batch in &batches {
        let rows = tickvault::store::rows::decode(batch).expect("decode");
        for row in rows {
            if row.event == EventKind::Snapshot && row.recv_wall <= at {
                best = Some(row.recv_wall);
            }
        }
    }
    best
}

/// Rebuilding must give the same book whether or not the planner ran.
#[test]
fn reconstruction_is_unchanged_by_the_planner() {
    let dir = tempfile::tempdir().unwrap();
    let messages = 20_000;
    write_archive(dir.path(), messages, 4, 60_000, 5_000, 1_000, Some(2_000));
    let reconstructor = Reconstructor::open(dir.path()).unwrap();

    for divisor in [1i64, 2, 3] {
        let at = EPOCH + (messages as i64 * TICK) / divisor;
        let with = reconstructor
            .at(&Request::new(VENUE, &symbol(), at))
            .unwrap();
        let without = reconstructor
            .at(&Request::new(VENUE, &symbol(), at).without_checkpoints())
            .unwrap();
        assert_eq!(
            with.digest(),
            without.digest(),
            "the book at {at} depends on how it was read"
        );
        assert!(with.rows_applied > 0, "nothing was applied at {at}");
    }
}
