//! **Schema compatibility gate.**
//!
//! A published archive outlives the code that wrote it. Files recorded last
//! month are read by this month's binary, and the wheel on PyPI is read by
//! whoever installed it. So the schema has exactly two kinds of change:
//!
//! - **Additive.** A new column appears. Old files simply lack it, new files
//!   carry it, and both read. This has to keep working without a migration,
//!   because there is no migration: the files are already published.
//! - **Breaking.** A column changes type, disappears, or keeps its type and
//!   changes meaning. The last of those is the dangerous one, because nothing
//!   about it is visible in the Arrow schema: a `price` at a scale of 1e-6
//!   read as one at 1e-9 is a number a thousand times too small, and it is a
//!   perfectly valid `Int64` all the way down.
//!
//! The gate asserts that additive changes read and breaking ones are *refused*.
//! Refusing is the feature. An archive that answers a query with a wrong number
//! is worse than one that will not answer at all.

mod common;

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use tickvault::book::BookDelta;
use tickvault::book::LevelChange;
use tickvault::clock::{Stamp, Timestamps};
use tickvault::fixed::Fixed;
use tickvault::reconstruct::{Reconstructor, Request};
use tickvault::store::manifest::{FileRecord, Manifest};
use tickvault::store::reader::ArchiveReader;
use tickvault::store::rows::decode;
use tickvault::store::schema::{RowBuilder, book_schema};
use tickvault::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};
use tickvault::types::{BookLevel, Side, Symbol, VenueId};

const EPOCH: i64 = 1_787_000_000_000_000_000;
const TICK: i64 = 1_000_000;
const ROWS: usize = 8;

fn f(s: &str) -> Fixed {
    Fixed::from_decimal_str(s).expect("decimal literal")
}

fn symbol() -> Symbol {
    Symbol::new("BTC", "USD")
}

/// A batch in exactly the schema the current code writes. This is the fixture
/// every case below is a variation on.
fn current_batch() -> RecordBatch {
    let mut builder = RowBuilder::new();
    for i in 0..ROWS {
        builder.push_delta(
            VenueId::Coinbase,
            &BookDelta {
                symbol: symbol(),
                changes: vec![LevelChange::new(
                    Side::Bid,
                    f("60000")
                        .checked_add(Fixed::from_mantissa(i as i64))
                        .unwrap(),
                    f("1.5"),
                )],
                seq: Some(i as u64 + 1),
                checksum: None,
                stamps: Timestamps::new(
                    Stamp {
                        mono_nanos: i as u64 * TICK as u64,
                        wall_nanos: EPOCH + i as i64 * TICK,
                    },
                    Some(EPOCH + i as i64 * TICK - 1_000),
                ),
                prev_seq: None,
                first_seq: None,
            },
            i as u64,
            false,
        );
    }
    builder.finish().expect("batch")
}

/// Write `batch` into the archive at `root` by hand, under a partition key it
/// may not match, and vouch for it in the manifest.
///
/// Deliberately bypasses [`ArchiveWriter`], which would reject a batch whose
/// schema is not the current one before it ever reached a file. The point of
/// this gate is what happens to a file that is *already on disk*.
fn plant(root: &Path, batch: &RecordBatch) -> FileRecord {
    let key = PartitionKey::new(VenueId::Coinbase, &symbol(), EPOCH);
    let dir = root.join(key.dir());
    std::fs::create_dir_all(&dir).expect("mkdir");
    let name = format!("part-planted-{}.parquet", batch.num_columns());
    let path = dir.join(&name);
    {
        let sink = File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(sink, batch.schema(), None).expect("open writer");
        writer.write(batch).expect("write");
        writer.close().expect("close");
    }
    let record = FileRecord {
        path: key.dir().join(&name).to_string_lossy().into_owned(),
        venue: VenueId::Coinbase,
        symbol: symbol(),
        date: key.date.clone(),
        book_level: BookLevel::L2,
        feed_depth: None,
        rows: batch.num_rows() as u64,
        bytes: std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
        first_recv_wall: EPOCH,
        last_recv_wall: EPOCH + (ROWS as i64 - 1) * TICK,
        closed_wall: EPOCH + ROWS as i64 * TICK,
    };
    let mut manifest = Manifest::open(root).expect("manifest");
    manifest.record_file(record.clone()).expect("append");
    record
}

/// The same batch with one extra nullable column appended: the additive case.
fn with_extra_column(batch: &RecordBatch) -> RecordBatch {
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(
        Field::new("conn_epoch", DataType::UInt64, true).with_metadata(HashMap::from([(
            "doc".to_string(),
            "which connection generation produced this row".to_string(),
        )])),
    );
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from(
        (0..batch.num_rows())
            .map(|_| Some(7u64))
            .collect::<Vec<_>>(),
    )));
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .expect("extended batch")
}

/// The same batch with `price` turned into a float: the breaking case that a
/// naive reader is most likely to attempt.
fn with_float_price(batch: &RecordBatch) -> RecordBatch {
    let idx = batch.schema().index_of("price").expect("price");
    let ints = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("price is an integer");
    let floats: Float64Array = (0..ints.len())
        .map(|i| Some(ints.value(i) as f64 / 1e9))
        .collect();

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[idx] = Field::new("price", DataType::Float64, false);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[idx] = Arc::new(floats);
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .expect("float batch")
}

// ---------------------------------------------------------------------------
// Additive changes.
// ---------------------------------------------------------------------------

/// A file carrying a column this code has never heard of reads exactly as if it
/// did not, and the reader does not have to be told.
#[test]
fn a_file_with_an_unknown_extra_column_still_reads() {
    let old = current_batch();
    let new = with_extra_column(&old);
    assert_eq!(new.num_columns(), old.num_columns() + 1);

    let from_old = decode(&old).expect("decode current");
    let from_new = decode(&new).expect("decode extended");
    assert_eq!(
        from_old, from_new,
        "an added column must not change what the existing ones mean"
    );
}

/// And the columns the current code added are themselves the additive case
/// looked at from the other side: a file written before they existed is missing
/// them, and reads as the default rather than as an error.
#[test]
fn a_file_written_before_a_column_existed_still_reads() {
    let batch = current_batch();
    // Drop every column the L3 work added, which is exactly what a phase 3
    // archive on disk looks like today.
    let dropped = [
        "book_level",
        "order_id",
        "action",
        "queue_index",
        "queue_qty_ahead",
        "queue_certainty",
        "size_change_explained",
        "traded_qty",
    ];
    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|i| !dropped.contains(&batch.schema().field(*i).name().as_str()))
        .collect();
    let fields: Vec<Field> = keep
        .iter()
        .map(|i| batch.schema().field(*i).clone())
        .collect();
    let columns: Vec<ArrayRef> = keep.iter().map(|i| batch.column(*i).clone()).collect();
    let older = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .expect("older batch");
    assert_eq!(older.num_columns(), batch.num_columns() - dropped.len());

    let rows = decode(&older).expect("an older archive must still decode");
    assert_eq!(rows.len(), ROWS);
    // The absent columns come back as what they mean when absent, not as an
    // invented value: aggregated levels, no order identity, no traded size.
    assert!(rows.iter().all(|r| r.book_level == BookLevel::L2));
    assert!(rows.iter().all(|r| r.order_id.is_none()));
    assert!(rows.iter().all(|r| r.traded_qty.is_none()));
    // And the columns that did exist are unchanged.
    let current = decode(&batch).expect("decode current");
    assert!(
        rows.iter()
            .zip(&current)
            .all(|(a, b)| a.price == b.price && a.qty == b.qty && a.recv_wall == b.recv_wall)
    );
}

/// The whole read path, not only the decoder: an archive holding an extended
/// file verifies, streams, and rebuilds the same book as one holding the
/// current schema.
#[test]
fn an_archive_with_an_extended_file_rebuilds_identically() {
    let baseline = {
        let dir = tempfile::tempdir().expect("tempdir");
        plant(dir.path(), &current_batch());
        rebuild(dir.path())
    };
    let extended = {
        let dir = tempfile::tempdir().expect("tempdir");
        plant(dir.path(), &with_extra_column(&current_batch()));
        rebuild(dir.path())
    };
    assert_eq!(
        baseline.0, extended.0,
        "the manifest must verify either way"
    );
    assert_eq!(
        baseline.1, extended.1,
        "the rebuilt book digest must not depend on a column nothing reads"
    );
    assert_eq!(baseline.2, extended.2, "and the same rows must be applied");
}

/// Verify, then rebuild, returning `(clean, digest, rows_applied)`.
fn rebuild(root: &Path) -> (bool, u32, u64) {
    let clean = ArchiveReader::open(root).expect("open").verify().is_clean();
    let rc = Reconstructor::open(root).expect("open");
    let built = rc
        .at(&Request::new(
            VenueId::Coinbase,
            &symbol(),
            EPOCH + ROWS as i64 * TICK,
        ))
        .expect("rebuild");
    (clean, built.digest(), built.rows_applied)
}

// ---------------------------------------------------------------------------
// Breaking changes.
// ---------------------------------------------------------------------------

/// A `price` column that has become a float is refused by name, not read as
/// something else.
#[test]
fn a_column_whose_type_changed_is_refused_by_name() {
    let broken = with_float_price(&current_batch());
    let err = decode(&broken).expect_err("a float price must not decode");
    let text = err.to_string();
    assert!(
        text.contains("price"),
        "the refusal must name the column: {text}"
    );
}

/// A column that has gone away is refused too, rather than defaulting.
///
/// The distinction from the additive case is which columns are optional. A
/// missing `queue_index` means "this feed could not say"; a missing `price`
/// means the file is not this dataset.
#[test]
fn a_missing_required_column_is_refused() {
    let batch = current_batch();
    let idx = batch.schema().index_of("qty").expect("qty");
    let keep: Vec<usize> = (0..batch.num_columns()).filter(|i| *i != idx).collect();
    let fields: Vec<Field> = keep
        .iter()
        .map(|i| batch.schema().field(*i).clone())
        .collect();
    let columns: Vec<ArrayRef> = keep.iter().map(|i| batch.column(*i).clone()).collect();
    let broken = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch");

    let err = decode(&broken).expect_err("a file with no qty column is not this dataset");
    assert!(err.to_string().contains("qty"), "{err}");
}

/// A file whose schema is not the current one cannot be compacted into a file
/// that claims to be.
///
/// Compaction copies batches through a writer opened on the *current* schema.
/// Copying an extended batch through it would either lose the extra column
/// silently or write a file whose footer disagrees with its pages, so it has to
/// fail, and it has to say which file it choked on.
#[test]
fn compacting_a_file_of_another_schema_fails_loudly() {
    use tickvault::store::compact::compact_partition;

    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    plant(root, &current_batch());
    plant(root, &with_extra_column(&current_batch()));
    assert_eq!(ArchiveReader::open(root).expect("open").files().len(), 2);

    let key = PartitionKey::new(VenueId::Coinbase, &symbol(), EPOCH);
    let result = compact_partition(
        root,
        &key,
        &WriterConfig::new(root),
        EPOCH + 86_400_000_000_000,
    );
    let err = result.expect_err("compacting mismatched schemas must not silently succeed");
    let text = err.to_string();
    assert!(
        text.contains("compacting") && text.contains("conn_epoch"),
        "the failure must name the file and the column it would have dropped: {text}"
    );

    // And the archive is untouched: the manifest still vouches for both files
    // and neither was retired behind a failed swap.
    let after = ArchiveReader::open(root).expect("open");
    assert_eq!(
        after.files().len(),
        2,
        "a refused compaction must change nothing"
    );
}

// ---------------------------------------------------------------------------
// The change that is invisible in the schema.
// ---------------------------------------------------------------------------

/// A file that declares a different price scale is refused, because nothing
/// about it looks wrong.
///
/// `price` is an `Int64` either way. A file written at a scale of 1e-6 and read
/// as 1e-9 produces prices a thousand times too small, in the right type, with
/// no null, no error and no clue. The scale is written into the file's own
/// metadata precisely so this is checkable, so it has to actually be checked.
#[test]
fn a_file_declaring_a_different_price_scale_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();

    let batch = current_batch();
    let mut meta = batch.schema().metadata().clone();
    meta.insert("tickvault.price_scale".to_string(), "6".to_string());
    let rescaled = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
            meta,
        )),
        batch.columns().to_vec(),
    )
    .expect("rescaled batch");
    plant(root, &rescaled);

    let report = ArchiveReader::open(root).expect("open").verify();
    assert!(
        !report.is_clean(),
        "a file at another scale must not verify clean: {report}"
    );
    assert_eq!(report.incompatible.len(), 1, "{report}");
    assert!(
        report.incompatible[0].1.contains("1e-6") && report.incompatible[0].1.contains("1e-9"),
        "the refusal must say both scales: {}",
        report.incompatible[0].1
    );

    // The rows themselves decode perfectly well, which is the whole problem:
    // a thousandth of the real price, in the right type, with no error.
    let rows = decode(&rescaled).expect("the batch still decodes");
    assert_eq!(rows[0].price, f("60000"), "read as if it were 1e-9");
}

/// The current schema declares its own scale and version, which is what makes
/// the check above possible at all.
#[test]
fn the_schema_declares_what_a_reader_needs_to_check() {
    let schema = book_schema();
    assert_eq!(
        schema.metadata().get("tickvault.price_scale"),
        Some(&tickvault::fixed::SCALE.to_string())
    );
    assert!(schema.metadata().contains_key("tickvault.version"));
    // And a file written by the current writer passes its own check.
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let mut writer = ArchiveWriter::open(WriterConfig {
        max_file_age: std::time::Duration::from_secs(86_400),
        ..WriterConfig::new(root)
    })
    .expect("open archive");
    let key = PartitionKey::new(VenueId::Coinbase, &symbol(), EPOCH);
    let batch = current_batch();
    writer
        .write(&key, &batch, (EPOCH, EPOCH + ROWS as i64 * TICK))
        .expect("write");
    writer.close_partition(&key).expect("close");
    let report = ArchiveReader::open(root).expect("open").verify();
    assert!(report.is_clean(), "{report}");
}

#[allow(dead_code)]
fn _uses_common() {
    let _ = common::btc_usd();
}
