//! The archive schema.
//!
//! One row per **level change**, not per message. Repeating the message
//! metadata across a message's levels costs almost nothing once Parquet
//! dictionary- and run-length-encodes it, and it means a consumer can answer
//! "what was the size at this price at this time" with a filter rather than by
//! unnesting a list column. `msg_index` groups the levels that arrived
//! together, so nothing is lost by flattening.
//!
//! # Prices are integers
//!
//! `price` and `qty` are exact signed integers counting 1e-9 units, not floats
//! and not decimals. This is the same decision as [`crate::fixed`] and for the
//! same reason: the project's whole claim is that the data can be trusted, and
//! a float round trip quietly breaks Kraken's checksum validation. The scale is
//! declared in each field's metadata so the file is self-describing, and
//! `docs/schema.md` states it too.
//!
//! The cost is that a naive reader gets integers rather than prices. That is a
//! deliberate trade: a wrong number that looks right is worse than a right
//! number that needs dividing.
//!
//! # `suspect` is part of the data
//!
//! Every row carries whether it fell inside a suspect window. A consumer that
//! wants only trustworthy data writes `filter(col("suspect") == False)` and is
//! done, without having to join against the gap report. The gap report says
//! *why* and *how much*; this column is what makes acting on it one line.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Int64Builder, RecordBatch, StringBuilder, UInt8Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::book::{BookDelta, BookSnapshot};
use crate::types::{Side, VenueId};

/// Decimal places held by the `price` and `qty` columns.
pub const PRICE_SCALE: u32 = crate::fixed::SCALE;

/// What kind of event a row came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventKind {
    /// A level as it stood in a full book snapshot.
    Snapshot = 0,
    /// A level changed by an incremental update.
    Delta = 1,
}

impl EventKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(EventKind::Snapshot),
            1 => Some(EventKind::Delta),
            _ => None,
        }
    }
}

fn scaled(name: &str, doc: &str) -> Field {
    let mut meta = HashMap::new();
    meta.insert("scale".to_string(), PRICE_SCALE.to_string());
    meta.insert("unit".to_string(), format!("1e-{PRICE_SCALE}"));
    meta.insert("doc".to_string(), doc.to_string());
    Field::new(name, DataType::Int64, false).with_metadata(meta)
}

fn documented(name: &str, ty: DataType, nullable: bool, doc: &str) -> Field {
    Field::new(name, ty, nullable)
        .with_metadata(HashMap::from([("doc".to_string(), doc.to_string())]))
}

/// The archive's Arrow schema.
pub fn book_schema() -> SchemaRef {
    let ts = || DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let fields = vec![
        documented("venue", DataType::Utf8, false, "recording venue"),
        documented("symbol", DataType::Utf8, false, "canonical BASE-QUOTE pair"),
        documented(
            "event",
            DataType::UInt8,
            false,
            "0 = snapshot level, 1 = delta level",
        ),
        documented(
            "msg_index",
            DataType::UInt64,
            false,
            "groups the levels that arrived in one message",
        ),
        documented(
            "recv_mono",
            DataType::UInt64,
            false,
            "monotonic nanoseconds since the recorder started; order the archive by this",
        ),
        documented(
            "recv_wall",
            ts(),
            false,
            "wall clock at receipt, for comparing across machines",
        ),
        documented(
            "venue_ts",
            ts(),
            true,
            "the venue's own timestamp, never reconciled with ours",
        ),
        documented(
            "skew_ns",
            DataType::Int64,
            true,
            "recv_wall minus venue_ts; negative means the clocks disagree, and it is kept as-is",
        ),
        documented("side", DataType::UInt8, false, "0 = bid, 1 = ask"),
        scaled("price", "price as an exact count of 1e-9 units"),
        scaled("qty", "resting quantity; zero means the level was removed"),
        documented(
            "seq",
            DataType::UInt64,
            true,
            "the venue's identifier for this message, where it has one",
        ),
        documented(
            "first_seq",
            DataType::UInt64,
            true,
            "first identifier covered, on venues whose messages span a range",
        ),
        documented(
            "prev_seq",
            DataType::UInt64,
            true,
            "identifier this message claims to follow, on chained feeds",
        ),
        documented(
            "checksum",
            DataType::UInt32,
            true,
            "the venue's own book checksum, where it publishes one",
        ),
        documented(
            "suspect",
            DataType::Boolean,
            false,
            "true when this row fell inside a suspect window; filter on it",
        ),
        documented(
            "book_level",
            DataType::UInt8,
            false,
            "2 = aggregated price levels, 3 = order by order",
        ),
        documented(
            "order_id",
            DataType::Utf8,
            true,
            "the venue's order identifier, on L3 rows only",
        ),
        documented(
            "action",
            DataType::Utf8,
            true,
            "add, modify, cancel, or execute, on L3 rows only",
        ),
        documented(
            "queue_index",
            DataType::UInt32,
            true,
            "orders ahead of this one at its price; null when not inferable",
        ),
        scaled(
            "queue_qty_ahead",
            "quantity that must trade before this order does; null when not inferable",
        )
        .with_nullable(true),
        documented(
            "queue_certainty",
            DataType::Utf8,
            true,
            "observed, seeded, or unknown: how much the queue position is worth",
        ),
        documented(
            "size_change_explained",
            DataType::Boolean,
            true,
            "false when the venue's own numbers do not account for the size change",
        ),
        documented(
            "traded_qty",
            DataType::Int64,
            true,
            "quantity this event traded, 1e-9 units; null when the feed cannot say",
        ),
    ];

    let meta = HashMap::from([
        ("tickvault.price_scale".to_string(), PRICE_SCALE.to_string()),
        (
            "tickvault.note".to_string(),
            "price and qty are exact integers at 1e-9; divide, do not cast".to_string(),
        ),
        (
            "tickvault.version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        ),
    ]);
    Arc::new(Schema::new_with_metadata(fields, meta))
}

/// Accumulates rows and turns them into a [`RecordBatch`].
pub struct RowBuilder {
    schema: SchemaRef,
    venue: StringBuilder,
    symbol: StringBuilder,
    event: UInt8Builder,
    msg_index: UInt64Builder,
    recv_mono: UInt64Builder,
    recv_wall: Int64Builder,
    venue_ts: Int64Builder,
    skew_ns: Int64Builder,
    side: UInt8Builder,
    price: Int64Builder,
    qty: Int64Builder,
    seq: UInt64Builder,
    first_seq: UInt64Builder,
    prev_seq: UInt64Builder,
    checksum: UInt32Builder,
    suspect: BooleanBuilder,
    book_level: UInt8Builder,
    order_id: StringBuilder,
    action: StringBuilder,
    queue_index: UInt32Builder,
    queue_qty_ahead: Int64Builder,
    queue_certainty: StringBuilder,
    size_change_explained: BooleanBuilder,
    traded_qty: Int64Builder,
    rows: usize,
    first_recv_wall: Option<i64>,
    last_recv_wall: Option<i64>,
}

impl Default for RowBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RowBuilder {
    pub fn new() -> Self {
        RowBuilder {
            schema: book_schema(),
            venue: StringBuilder::new(),
            symbol: StringBuilder::new(),
            event: UInt8Builder::new(),
            msg_index: UInt64Builder::new(),
            recv_mono: UInt64Builder::new(),
            recv_wall: Int64Builder::new(),
            venue_ts: Int64Builder::new(),
            skew_ns: Int64Builder::new(),
            side: UInt8Builder::new(),
            price: Int64Builder::new(),
            qty: Int64Builder::new(),
            seq: UInt64Builder::new(),
            first_seq: UInt64Builder::new(),
            prev_seq: UInt64Builder::new(),
            checksum: UInt32Builder::new(),
            suspect: BooleanBuilder::new(),
            book_level: UInt8Builder::new(),
            order_id: StringBuilder::new(),
            action: StringBuilder::new(),
            queue_index: UInt32Builder::new(),
            queue_qty_ahead: Int64Builder::new(),
            queue_certainty: StringBuilder::new(),
            size_change_explained: BooleanBuilder::new(),
            traded_qty: Int64Builder::new(),
            rows: 0,
            first_recv_wall: None,
            last_recv_wall: None,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Wall-clock span of the rows accumulated so far, for the manifest.
    pub fn wall_span(&self) -> Option<(i64, i64)> {
        Some((self.first_recv_wall?, self.last_recv_wall?))
    }

    #[allow(clippy::too_many_arguments)]
    fn push_row(
        &mut self,
        venue: VenueId,
        symbol: &str,
        kind: EventKind,
        msg_index: u64,
        stamps: crate::clock::Timestamps,
        side: Side,
        price: crate::Fixed,
        qty: crate::Fixed,
        seq: Option<u64>,
        first_seq: Option<u64>,
        prev_seq: Option<u64>,
        checksum: Option<u32>,
        suspect: bool,
    ) {
        self.venue.append_value(venue.as_str());
        self.symbol.append_value(symbol);
        self.event.append_value(kind as u8);
        self.msg_index.append_value(msg_index);
        self.recv_mono.append_value(stamps.recv.mono_nanos);
        self.recv_wall.append_value(stamps.recv.wall_nanos);
        self.venue_ts.append_option(stamps.venue_nanos);
        self.skew_ns.append_option(stamps.skew_nanos());
        self.side.append_value(match side {
            Side::Bid => 0,
            Side::Ask => 1,
        });
        self.price.append_value(price.mantissa());
        self.qty.append_value(qty.mantissa());
        self.seq.append_option(seq);
        self.first_seq.append_option(first_seq);
        self.prev_seq.append_option(prev_seq);
        self.checksum.append_option(checksum);
        self.suspect.append_value(suspect);
        // L2 rows leave every L3 column null rather than filling in a
        // plausible-looking zero.
        self.book_level.append_value(2);
        self.order_id.append_null();
        self.action.append_null();
        self.queue_index.append_null();
        self.queue_qty_ahead.append_null();
        self.queue_certainty.append_null();
        self.size_change_explained.append_null();
        // An aggregated feed shows a level shrinking without saying whether it
        // traded, so the answer is unknown rather than zero.
        self.traded_qty.append_null();

        self.rows += 1;
        let wall = stamps.recv.wall_nanos;
        self.first_recv_wall.get_or_insert(wall);
        self.last_recv_wall = Some(wall);
    }

    /// Flatten a snapshot into one row per level.
    pub fn push_snapshot(
        &mut self,
        venue: VenueId,
        snapshot: &BookSnapshot,
        msg_index: u64,
        suspect: bool,
    ) {
        let symbol = snapshot.symbol.as_str().to_string();
        for (side, levels) in [(Side::Bid, &snapshot.bids), (Side::Ask, &snapshot.asks)] {
            for (price, qty) in levels {
                self.push_row(
                    venue,
                    &symbol,
                    EventKind::Snapshot,
                    msg_index,
                    snapshot.stamps,
                    side,
                    *price,
                    *qty,
                    snapshot.seq,
                    None,
                    None,
                    snapshot.checksum,
                    suspect,
                );
            }
        }
    }

    /// Flatten a delta into one row per changed level.
    pub fn push_delta(&mut self, venue: VenueId, delta: &BookDelta, msg_index: u64, suspect: bool) {
        let symbol = delta.symbol.as_str().to_string();
        for change in &delta.changes {
            self.push_row(
                venue,
                &symbol,
                EventKind::Delta,
                msg_index,
                delta.stamps,
                change.side,
                change.price,
                change.qty,
                delta.seq,
                delta.first_seq,
                delta.prev_seq,
                delta.checksum,
                suspect,
            );
        }
    }

    /// Record one order-by-order event, with its inferred queue position.
    ///
    /// The certainty rides alongside the position on purpose: a consumer that
    /// filters on `queue_certainty == "observed"` gets only positions that were
    /// watched rather than assumed, in one expression.
    pub fn push_order(
        &mut self,
        venue: VenueId,
        event: &crate::book::l3::OrderEvent,
        position: Option<crate::book::l3::QueuePosition>,
        traded: Option<crate::Fixed>,
        msg_index: u64,
        suspect: bool,
    ) {
        self.venue.append_value(venue.as_str());
        self.symbol.append_value(event.symbol.as_str());
        self.event.append_value(EventKind::Delta as u8);
        self.msg_index.append_value(msg_index);
        self.recv_mono.append_value(event.stamps.recv.mono_nanos);
        self.recv_wall.append_value(event.stamps.recv.wall_nanos);
        self.venue_ts.append_option(event.stamps.venue_nanos);
        self.skew_ns.append_option(event.stamps.skew_nanos());
        self.side.append_value(match event.side {
            Side::Bid => 0,
            Side::Ask => 1,
        });
        self.price.append_value(event.price.mantissa());
        self.qty.append_value(event.qty.mantissa());
        // The chained token is 128 bits; the low 64 identify the message well
        // enough for a reader, and the full chain is the recorder's business.
        self.seq.append_option(event.event_token.map(|t| t as u64));
        self.first_seq.append_null();
        self.prev_seq
            .append_option(event.prev_event_token.map(|t| t as u64));
        self.checksum.append_null();
        self.suspect.append_value(suspect);
        self.book_level.append_value(3);
        self.order_id.append_value(event.order_id.as_str());
        self.action.append_value(event.action.as_str());
        self.queue_index
            .append_option(position.map(|p| p.index as u32));
        self.queue_qty_ahead
            .append_option(position.map(|p| p.qty_ahead.mantissa()));
        self.queue_certainty
            .append_option(position.map(|p| p.certainty.as_str()));
        self.size_change_explained
            .append_value(event.size_change_explained);
        // The row carries what is still resting, which on a full fill is zero.
        // The amount that changed hands is only knowable while the previous
        // state of the order is in hand, so the book works it out and it is
        // written here rather than being recomputed from the archive later.
        self.traded_qty.append_option(traded.map(|q| q.mantissa()));

        self.rows += 1;
        let wall = event.stamps.recv.wall_nanos;
        self.first_recv_wall.get_or_insert(wall);
        self.last_recv_wall = Some(wall);
    }

    /// Produce a batch and reset. Returns `None` when nothing was accumulated.
    pub fn finish(&mut self) -> Option<RecordBatch> {
        if self.rows == 0 {
            return None;
        }
        let ts_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.venue.finish()),
            Arc::new(self.symbol.finish()),
            Arc::new(self.event.finish()),
            Arc::new(self.msg_index.finish()),
            Arc::new(self.recv_mono.finish()),
            Arc::new(
                arrow::compute::cast(&self.recv_wall.finish(), &ts_type)
                    .expect("nanoseconds cast to a nanosecond timestamp"),
            ),
            Arc::new(
                arrow::compute::cast(&self.venue_ts.finish(), &ts_type)
                    .expect("nanoseconds cast to a nanosecond timestamp"),
            ),
            Arc::new(self.skew_ns.finish()),
            Arc::new(self.side.finish()),
            Arc::new(self.price.finish()),
            Arc::new(self.qty.finish()),
            Arc::new(self.seq.finish()),
            Arc::new(self.first_seq.finish()),
            Arc::new(self.prev_seq.finish()),
            Arc::new(self.checksum.finish()),
            Arc::new(self.suspect.finish()),
            Arc::new(self.book_level.finish()),
            Arc::new(self.order_id.finish()),
            Arc::new(self.action.finish()),
            Arc::new(self.queue_index.finish()),
            Arc::new(self.queue_qty_ahead.finish()),
            Arc::new(self.queue_certainty.finish()),
            Arc::new(self.size_change_explained.finish()),
            Arc::new(self.traded_qty.finish()),
        ];
        self.rows = 0;
        self.first_recv_wall = None;
        self.last_recv_wall = None;
        Some(
            RecordBatch::try_new(Arc::clone(&self.schema), columns)
                .expect("builders match the schema they were built from"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::LevelChange;
    use crate::clock::{Stamp, Timestamps};
    use crate::fixed::Fixed;
    use crate::types::Symbol;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    fn stamps(wall: i64, venue: Option<i64>) -> Timestamps {
        Timestamps::new(
            Stamp {
                mono_nanos: wall as u64,
                wall_nanos: wall,
            },
            venue,
        )
    }

    fn delta() -> BookDelta {
        BookDelta {
            symbol: Symbol::new("BTC", "USD"),
            changes: vec![
                LevelChange::new(Side::Bid, f("78850.12"), f("0.5")),
                LevelChange::new(Side::Ask, f("78850.13"), f("0")),
            ],
            seq: Some(42),
            checksum: Some(7),
            stamps: stamps(1_700_000_000_000_000_000, Some(1_699_999_999_000_000_000)),
            prev_seq: Some(41),
            first_seq: Some(40),
        }
    }

    #[test]
    fn the_schema_declares_its_own_decimal_scale() {
        // A published file has to be readable without the README.
        let schema = book_schema();
        assert_eq!(
            schema.metadata().get("tickvault.price_scale"),
            Some(&"9".to_string())
        );
        let price = schema.field_with_name("price").unwrap();
        assert_eq!(price.metadata().get("scale"), Some(&"9".to_string()));
        assert_eq!(price.metadata().get("unit"), Some(&"1e-9".to_string()));
        assert_eq!(price.data_type(), &DataType::Int64);
    }

    #[test]
    fn a_delta_becomes_one_row_per_changed_level() {
        let mut b = RowBuilder::new();
        b.push_delta(VenueId::Kraken, &delta(), 7, false);
        assert_eq!(b.len(), 2);
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), book_schema().fields().len());
        assert!(b.is_empty(), "finish must reset the builder");
        assert!(b.finish().is_none(), "an empty builder yields no batch");
    }

    #[test]
    fn prices_survive_the_round_trip_as_exact_integers() {
        use arrow::array::Int64Array;
        let mut b = RowBuilder::new();
        b.push_delta(VenueId::Kraken, &delta(), 0, false);
        let batch = b.finish().unwrap();
        let price = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(price.value(0), f("78850.12").mantissa());
        // And back again, exactly.
        assert_eq!(Fixed::from_mantissa(price.value(0)), f("78850.12"));
    }

    #[test]
    fn a_removed_level_is_a_zero_quantity_row_not_an_absent_one() {
        use arrow::array::Int64Array;
        let mut b = RowBuilder::new();
        b.push_delta(VenueId::Kraken, &delta(), 0, false);
        let batch = b.finish().unwrap();
        let qty = batch
            .column_by_name("qty")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(qty.value(1), 0, "the ask was removed and must be recorded");
    }

    #[test]
    fn clock_skew_is_stored_signed_and_unclamped() {
        use arrow::array::Int64Array;
        let mut d = delta();
        // Venue clock ahead of ours.
        d.stamps = stamps(1_000, Some(1_500));
        let mut b = RowBuilder::new();
        b.push_delta(VenueId::Kraken, &d, 0, false);
        let batch = b.finish().unwrap();
        let skew = batch
            .column_by_name("skew_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(skew.value(0), -500);
    }

    #[test]
    fn a_venue_that_sends_no_timestamp_leaves_nulls_rather_than_zeros() {
        use arrow::array::Int64Array;
        let mut d = delta();
        d.stamps = Timestamps::recv_only(Stamp {
            mono_nanos: 5,
            wall_nanos: 5,
        });
        let mut b = RowBuilder::new();
        b.push_delta(VenueId::Kraken, &d, 0, false);
        let batch = b.finish().unwrap();
        for column in ["venue_ts", "skew_ns"] {
            assert!(
                batch.column_by_name(column).unwrap().is_null(0),
                "{column} must be null, not zero; a zero would read as 1970"
            );
        }
        let _ = Int64Array::from(vec![0i64]);
    }

    #[test]
    fn a_snapshot_flattens_both_sides() {
        let snapshot = BookSnapshot {
            symbol: Symbol::new("BTC", "USD"),
            bids: vec![(f("100"), f("1")), (f("99"), f("2"))],
            asks: vec![(f("101"), f("3"))],
            seq: Some(9),
            checksum: None,
            stamps: stamps(1, None),
        };
        let mut b = RowBuilder::new();
        b.push_snapshot(VenueId::Okx, &snapshot, 3, true);
        assert_eq!(b.len(), 3);
        let batch = b.finish().unwrap();
        use arrow::array::{BooleanArray, UInt8Array};
        let event = batch
            .column_by_name("event")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert!((0..3).all(|i| event.value(i) == EventKind::Snapshot as u8));
        let suspect = batch
            .column_by_name("suspect")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(
            (0..3).all(|i| suspect.value(i)),
            "a snapshot taken inside a suspect window must be marked"
        );
    }

    #[test]
    fn the_wall_span_covers_every_row_accumulated() {
        let mut b = RowBuilder::new();
        assert_eq!(b.wall_span(), None);
        let mut early = delta();
        early.stamps = stamps(100, None);
        let mut late = delta();
        late.stamps = stamps(900, None);
        b.push_delta(VenueId::Kraken, &early, 0, false);
        b.push_delta(VenueId::Kraken, &late, 1, false);
        assert_eq!(b.wall_span(), Some((100, 900)));
        b.finish();
        assert_eq!(b.wall_span(), None, "the span resets with the builder");
    }
}
