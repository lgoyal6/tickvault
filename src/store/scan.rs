//! Planning a scan, and saying what the plan skipped.
//!
//! # What this is for
//!
//! Every question the archive is asked is a range of `recv_wall` within one
//! partition. Until this module existed, answering one meant handing Parquet a
//! file and a shrug: every row group, every column, every page, decoded into
//! [`crate::store::rows::Row`] and then thrown away by a comparison in Rust.
//! The archive was already paying to be indexed and the reader was not
//! reading the index.
//!
//! There are four places a scan can be narrowed, and they are cheapest first:
//!
//! 1. **The partition.** `venue=/symbol=/date=` is a directory, so a query for
//!    one venue never opens another's files at all. This was always true.
//! 2. **The manifest.** Every completed file records its first and last
//!    `recv_wall`, which is a zone map. [`files_in_range`] drops the files
//!    whose span cannot intersect the question, without opening any of them.
//!    Also already true, and duplicated in two callers, which is why it moved
//!    here.
//! 3. **The row group.** Parquet's footer carries per-row-group min and max
//!    for every column, and the writer has always written them. A group whose
//!    `recv_wall` range misses the question holds nothing worth decoding.
//! 4. **The page.** The page index carries the same min and max per data
//!    page, which is 20,000-ish rows rather than 50,000. What it buys over
//!    the row group is the edges of the range, which for a short query is most
//!    of the cost.
//!
//! And orthogonally to all four: **the column**. The archive has 24 of them
//! and a replay reads 14. `venue` and `symbol` are the partition key repeated
//! on every row, `recv_mono` is meaningless across a restart and never
//! ordered on, and `seq`, `first_seq`, `prev_seq`, `checksum` and `skew_ns`
//! exist for the gap report rather than for the book. Not decoding them is
//! free.
//!
//! # Why this works here, and where it would not
//!
//! All of it rests on one property: the archive is written in arrival order,
//! so `recv_wall` is clustered, so a range predicate on it prunes a contiguous
//! run of row groups and pages. That is not a general fact about Parquet, it
//! is a fact about this writer.
//!
//! A predicate on `price` prunes nothing, because prices walk up and down all
//! day and every row group's min and max span most of the book. `Explain`
//! reports the pruning rather than assuming it, so the difference is visible
//! rather than folklore, and `tests/gate_scan.rs` asserts the price case
//! prunes nothing so the claim stays honest.
//!
//! # The one non-time predicate
//!
//! [`Predicate::snapshots_only`] pushes down `event == Snapshot`. It is here
//! because the reconstructor asks it on every rebuild - "does this file
//! contain a snapshot, and when was the last one" - and because the layout
//! answers it for free: `event` is 0 for a snapshot level and 1 for a delta,
//! so a row group whose minimum is 1 holds no snapshot and a file where that
//! is true of every group can be skipped without reading a page. On a feed
//! that never snapshots at all, which is what Bitstamp's order-by-order
//! channel is, that turns a full decode of the day into reading footers.

use std::path::Path;

use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, RowGroupMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;

use crate::error::{Error, Result};
use crate::store::manifest::FileRecord;
use crate::store::schema::EventKind;

/// Columns [`crate::store::rows::decode`] reads.
///
/// Kept next to the planner rather than inferred, because the two have to
/// agree: projecting away a column the decoder needs turns into a runtime
/// error on a file nobody looked at yet. `tests/gate_scan.rs` asserts a
/// projected batch still decodes.
pub const ROW_COLUMNS: &[&str] = &[
    "event",
    "msg_index",
    "recv_wall",
    "venue_ts",
    "side",
    "price",
    "qty",
    "suspect",
    "book_level",
    "order_id",
    "action",
    "queue_index",
    "queue_certainty",
    "traded_qty",
];

/// Columns needed to answer "when was the last snapshot".
const PROBE_COLUMNS: &[&str] = &["event", "recv_wall"];

/// What a scan is looking for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Predicate {
    /// Rows strictly after this instant. `None` means from the beginning.
    ///
    /// Strict, matching the stream: a checkpoint is the book *after*
    /// everything at its instant, so re-applying those rows would double them.
    pub after: Option<i64>,
    /// Rows at or before this instant.
    pub until: i64,
    /// Only rows that came from a full book snapshot.
    pub snapshots_only: bool,
}

impl Predicate {
    pub fn range(after: Option<i64>, until: i64) -> Self {
        Predicate {
            after,
            until,
            snapshots_only: false,
        }
    }

    /// True when a span of `recv_wall` could hold a row this predicate wants.
    ///
    /// Both bounds are inclusive as Parquet reports them, so the test is
    /// against the span rather than against either end.
    pub fn overlaps(&self, min: i64, max: i64) -> bool {
        max > self.after.unwrap_or(i64::MIN) && min <= self.until
    }
}

/// Files whose recorded span could hold a row the predicate wants.
///
/// The manifest's first and last `recv_wall` per file is a zone map, and this
/// is the read of it. Returns the survivors and how many were dropped, which
/// is the first line of an [`Explain`].
pub fn files_in_range(files: Vec<FileRecord>, predicate: &Predicate) -> (Vec<FileRecord>, usize) {
    let before = files.len();
    let kept: Vec<FileRecord> = files
        .into_iter()
        .filter(|f| predicate.overlaps(f.first_recv_wall, f.last_recv_wall))
        .collect();
    let pruned = before - kept.len();
    (kept, pruned)
}

/// Whether a scan uses the plan at all.
///
/// `Unpruned` exists for two reasons and no others: the equivalence gate has
/// to compare a planned scan against an unplanned one over the same files, and
/// the benchmark has to measure the difference. A flag that only a test sets
/// would be speculative; these two both fail without it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanMode {
    Planned,
    Unpruned,
}

/// What a scan touched, and what it did not.
///
/// Every field is a count rather than a ratio, so several files add up and the
/// caller decides what to divide by.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Explain {
    /// Files the manifest offered for this partition.
    pub files_in_partition: usize,
    /// Files dropped on their recorded span, without being opened.
    pub files_pruned: usize,
    /// Files whose footer was read.
    pub files_opened: usize,
    /// Files that turned out to hold nothing wanted, after reading the footer.
    pub files_empty: usize,

    pub row_groups_total: usize,
    pub row_groups_read: usize,

    /// Pages in the read row groups, counted on the column the predicate is
    /// about. Zero when no file carried a page index.
    pub pages_total: usize,
    pub pages_read: usize,

    /// Rows in the whole file.
    pub rows_total: u64,
    /// Rows in the row groups that survived pruning.
    pub rows_in_row_groups: u64,
    /// Rows the page selection asked Parquet to decode.
    pub rows_selected: u64,

    pub columns_in_file: usize,
    pub columns_projected: usize,

    /// Compressed bytes of every column in every row group.
    pub bytes_total: u64,
    /// Compressed bytes of the projected columns in the read row groups.
    ///
    /// An upper bound on what was read rather than an exact figure: page
    /// skipping takes it lower, and attributing that exactly would mean
    /// modelling how each column's pages line up with the selection. The row
    /// counts above are the honest measure of the page level.
    pub bytes_projected: u64,

    /// True when a file carried no page index, so the page level did not run.
    pub without_page_index: bool,
}

impl Explain {
    /// Add another file's counts to this one.
    pub fn absorb(&mut self, other: &Explain) {
        self.files_opened += other.files_opened;
        self.files_empty += other.files_empty;
        self.row_groups_total += other.row_groups_total;
        self.row_groups_read += other.row_groups_read;
        self.pages_total += other.pages_total;
        self.pages_read += other.pages_read;
        self.rows_total += other.rows_total;
        self.rows_in_row_groups += other.rows_in_row_groups;
        self.rows_selected += other.rows_selected;
        self.columns_in_file = self.columns_in_file.max(other.columns_in_file);
        self.columns_projected = self.columns_projected.max(other.columns_projected);
        self.bytes_total += other.bytes_total;
        self.bytes_projected += other.bytes_projected;
        self.without_page_index |= other.without_page_index;
    }

    fn pct(part: u64, whole: u64) -> f64 {
        if whole == 0 {
            0.0
        } else {
            100.0 * part as f64 / whole as f64
        }
    }
}

impl std::fmt::Display for Explain {
    /// The plan, cheapest step first, with what each one removed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "manifest    {:>8} of {:>8} files      ({:.1}% pruned on recorded span)",
            self.files_opened,
            self.files_in_partition,
            Self::pct(self.files_pruned as u64, self.files_in_partition as u64)
        )?;
        writeln!(
            f,
            "row groups  {:>8} of {:>8} groups     ({:.1}% pruned on footer statistics)",
            self.row_groups_read,
            self.row_groups_total,
            Self::pct(
                (self.row_groups_total - self.row_groups_read) as u64,
                self.row_groups_total as u64
            )
        )?;
        if self.pages_total > 0 {
            writeln!(
                f,
                "pages       {:>8} of {:>8} pages      ({:.1}% pruned on the page index)",
                self.pages_read,
                self.pages_total,
                Self::pct(
                    (self.pages_total - self.pages_read) as u64,
                    self.pages_total as u64
                )
            )?;
        } else if self.without_page_index {
            writeln!(f, "pages          no page index in this archive")?;
        }
        writeln!(
            f,
            "rows        {:>8} of {:>8} rows       ({:.2}% decoded)",
            self.rows_selected,
            self.rows_total,
            Self::pct(self.rows_selected, self.rows_total)
        )?;
        write!(
            f,
            "columns     {:>8} of {:>8} columns    ({:.1}% of bytes in scope)",
            self.columns_projected,
            self.columns_in_file,
            Self::pct(self.bytes_projected, self.bytes_total)
        )
    }
}

/// A plan for one file.
pub struct FilePlan {
    pub row_groups: Vec<usize>,
    pub selection: Option<RowSelection>,
    pub projection: ProjectionMask,
    pub explain: Explain,
}

impl FilePlan {
    pub fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }
}

/// Open a file and plan a scan of it.
///
/// The footer and the page index are read here; nothing else is.
pub fn open_planned(
    path: &Path,
    predicate: &Predicate,
    mode: ScanMode,
) -> Result<(ParquetRecordBatchReader, Explain)> {
    let (builder, mut explain) = open_builder(path, mode)?;
    if mode == ScanMode::Unpruned {
        explain.row_groups_read = explain.row_groups_total;
        explain.rows_in_row_groups = explain.rows_total;
        explain.rows_selected = explain.rows_total;
        explain.columns_projected = explain.columns_in_file;
        explain.bytes_projected = explain.bytes_total;
        let reader = builder
            .build()
            .map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;
        return Ok((reader, explain));
    }

    let plan = plan_file(
        predicate,
        ROW_COLUMNS,
        builder.metadata(),
        builder.parquet_schema(),
    );
    explain.absorb(&plan.explain);
    let mut builder = builder
        .with_projection(plan.projection)
        .with_row_groups(plan.row_groups);
    if let Some(selection) = plan.selection {
        builder = builder.with_row_selection(selection);
    }
    let reader = builder
        .build()
        .map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;
    Ok((reader, explain))
}

fn open_builder(
    path: &Path,
    mode: ScanMode,
) -> Result<(ParquetRecordBatchReaderBuilder<std::fs::File>, Explain)> {
    let file = std::fs::File::open(path)?;
    // The page index costs one extra read of a few kilobytes at the tail of
    // the file, and is not read at all when nothing will use it.
    //
    // Optional rather than Required. An archive written before page-level
    // statistics were the default carries none, and refusing to read it would
    // turn an optimisation into a compatibility break; the planner already
    // falls back to row group pruning when the index is absent.
    let policy = match mode {
        ScanMode::Planned => PageIndexPolicy::Optional,
        ScanMode::Unpruned => PageIndexPolicy::Skip,
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(policy);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;

    let meta = builder.metadata();
    let mut explain = Explain {
        files_opened: 1,
        columns_in_file: builder.parquet_schema().num_columns(),
        ..Default::default()
    };
    for rg in meta.row_groups() {
        explain.row_groups_total += 1;
        explain.rows_total += rg.num_rows() as u64;
        explain.bytes_total += rg.compressed_size().max(0) as u64;
    }
    Ok((builder, explain))
}

/// Decide which row groups, pages and columns of one file to read.
fn plan_file(
    predicate: &Predicate,
    columns: &[&str],
    meta: &ParquetMetaData,
    schema: &SchemaDescriptor,
) -> FilePlan {
    let projection = ProjectionMask::columns(schema, columns.iter().copied());
    let wall = column_index_of(schema, "recv_wall");
    let event = column_index_of(schema, "event");
    let projected: Vec<usize> = (0..schema.num_columns())
        .filter(|i| projection.leaf_included(*i))
        .collect();

    let mut explain = Explain {
        columns_in_file: schema.num_columns(),
        columns_projected: projected.len(),
        ..Default::default()
    };

    let mut row_groups = Vec::new();
    for (index, rg) in meta.row_groups().iter().enumerate() {
        if !row_group_matches(predicate, rg, wall, event) {
            continue;
        }
        row_groups.push(index);
        explain.rows_in_row_groups += rg.num_rows() as u64;
        for &col in &projected {
            explain.bytes_projected += rg.column(col).compressed_size().max(0) as u64;
        }
    }
    explain.row_groups_read = row_groups.len();

    let selection = page_selection(predicate, meta, &row_groups, wall, event, &mut explain);
    if selection.is_none() {
        explain.rows_selected = explain.rows_in_row_groups;
    }

    FilePlan {
        row_groups,
        selection,
        projection,
        explain,
    }
}

/// Whether a row group's footer statistics allow it to hold a wanted row.
///
/// Absent statistics mean "read it": a file written by something that did not
/// record them is a file we cannot prune, not a file we may skip.
fn row_group_matches(
    predicate: &Predicate,
    rg: &RowGroupMetaData,
    wall: Option<usize>,
    event: Option<usize>,
) -> bool {
    if let Some(col) = wall
        && let Some((min, max)) = int_stats(rg.column(col).statistics())
        && !predicate.overlaps(min, max)
    {
        return false;
    }
    if predicate.snapshots_only
        && let Some(col) = event
        && let Some((min, _)) = int_stats(rg.column(col).statistics())
        && min > EventKind::Snapshot as u8 as i64
    {
        // Every row in this group is a delta.
        return false;
    }
    true
}

/// A row selection over the chosen row groups, from the page index.
///
/// `None` when there is no page index, or when it cannot narrow anything: an
/// all-select selection would cost the reader work and save it none.
fn page_selection(
    predicate: &Predicate,
    meta: &ParquetMetaData,
    row_groups: &[usize],
    wall: Option<usize>,
    event: Option<usize>,
    explain: &mut Explain,
) -> Option<RowSelection> {
    let (Some(column_index), Some(offset_index), Some(wall)) =
        (meta.column_index(), meta.offset_index(), wall)
    else {
        explain.without_page_index = !row_groups.is_empty();
        return None;
    };

    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut selected_rows = 0u64;
    let mut pruned_any = false;

    for &rg in row_groups {
        let rows = meta.row_groups()[rg].num_rows();
        let locations = offset_index[rg][wall].page_locations();
        let walls = &column_index[rg][wall];
        let events = event.map(|col| &column_index[rg][col]);

        explain.pages_total += locations.len();

        for page in 0..locations.len() {
            let start = locations[page].first_row_index;
            let end = locations
                .get(page + 1)
                .map(|next| next.first_row_index)
                .unwrap_or(rows);
            let count = (end - start).max(0) as usize;
            if count == 0 {
                continue;
            }

            let mut keep = match page_bounds(walls, page) {
                Some((min, max)) => predicate.overlaps(min, max),
                // A page with no recorded bounds has to be read.
                None => true,
            };
            if keep
                && predicate.snapshots_only
                && let Some(events) = events
                && let Some((min, _)) = page_bounds(events, page)
                && min > EventKind::Snapshot as u8 as i64
            {
                keep = false;
            }

            if keep {
                explain.pages_read += 1;
                selected_rows += count as u64;
                push(&mut selectors, RowSelector::select(count));
            } else {
                pruned_any = true;
                push(&mut selectors, RowSelector::skip(count));
            }
        }
    }

    if !pruned_any {
        return None;
    }
    explain.rows_selected = selected_rows;
    Some(RowSelection::from(selectors))
}

/// Append a selector, merging with the last one when it says the same thing.
///
/// Parquet walks this list; a run of a thousand one-page selectors is a
/// thousand decisions to make instead of one.
fn push(selectors: &mut Vec<RowSelector>, next: RowSelector) {
    match selectors.last_mut() {
        Some(last) if last.skip == next.skip => last.row_count += next.row_count,
        _ => selectors.push(next),
    }
}

/// Min and max of one page, as integers, whatever width they were stored at.
///
/// `recv_wall` is a nanosecond timestamp and lands in Parquet as INT64;
/// `event` is a `u8`, which Parquet has no type for, so arrow writes it as
/// INT32. Both are wanted here and neither is worth a separate code path.
fn page_bounds(index: &ColumnIndexMetaData, page: usize) -> Option<(i64, i64)> {
    match index {
        ColumnIndexMetaData::INT64(idx) => Some((*idx.min_value(page)?, *idx.max_value(page)?)),
        ColumnIndexMetaData::INT32(idx) => {
            Some((*idx.min_value(page)? as i64, *idx.max_value(page)? as i64))
        }
        _ => None,
    }
}

fn int_stats(stats: Option<&Statistics>) -> Option<(i64, i64)> {
    match stats? {
        Statistics::Int64(s) => Some((*s.min_opt()?, *s.max_opt()?)),
        Statistics::Int32(s) => Some((*s.min_opt()? as i64, *s.max_opt()? as i64)),
        _ => None,
    }
}

fn column_index_of(schema: &SchemaDescriptor, name: &str) -> Option<usize> {
    schema.columns().iter().position(|c| c.name() == name)
}

/// The last snapshot at or before an instant, and what finding it cost.
///
/// This is the query the reconstructor asks of every file it walks backwards
/// through, and before this module it was answered by decoding the file: all
/// 24 columns of every row group, including three `Utf8` columns that
/// allocate a `String` per row on an order-by-order archive, to look at one
/// `u8`. Now it is two columns, and on a feed that never snapshots it is
/// nothing at all: `event` has a minimum of 1 in every row group, so the plan
/// selects no row groups and the file is answered from its footer.
pub fn last_snapshot_at_or_before(path: &Path, at: i64) -> Result<(Option<i64>, Explain)> {
    let predicate = Predicate {
        after: None,
        until: at,
        snapshots_only: true,
    };
    let (builder, mut explain) = open_builder(path, ScanMode::Planned)?;
    let plan = plan_file(
        &predicate,
        PROBE_COLUMNS,
        builder.metadata(),
        builder.parquet_schema(),
    );
    explain.absorb(&plan.explain);
    if plan.is_empty() {
        explain.files_empty = 1;
        return Ok((None, explain));
    }

    let mut builder = builder
        .with_projection(plan.projection)
        .with_row_groups(plan.row_groups);
    if let Some(selection) = plan.selection {
        builder = builder.with_row_selection(selection);
    }
    let reader = builder
        .build()
        .map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;

    let mut best: Option<i64> = None;
    for batch in reader {
        let batch = batch.map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;
        let events = batch
            .column_by_name("event")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt8Array>())
            .ok_or_else(|| Error::Other("archive row is missing column event".into()))?;
        let walls = batch
            .column_by_name("recv_wall")
            .and_then(|c| {
                c.as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            })
            .ok_or_else(|| Error::Other("archive row is missing column recv_wall".into()))?;
        for i in 0..batch.num_rows() {
            // The page index prunes whole pages, not rows, so the row-level
            // test still has to run. It is two comparisons on decoded values
            // that are already in registers.
            if events.value(i) == EventKind::Snapshot as u8 && walls.value(i) <= at {
                best = Some(walls.value(i));
            }
        }
    }
    Ok((best, explain))
}
