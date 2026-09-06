//! Emit the shared point-in-time fixture, and TickVault's own answers to it.
//!
//! One Parquet file and one JSON file. The Parquet is the feature table every
//! implementation reads; the JSON is what [`tickvault::features`] says the
//! answers are. Feast and a versioned DuckDB view read the same Parquet and are
//! compared against the same JSON, so the comparison is between implementations
//! rather than between fixtures.
//!
//! Run: `cargo run --example pit_fixture -- <out-dir>`
//!
//! One feature, `last_bid_price`, on purpose. C21 is about which timestamp a
//! read filters on, and a second feature would multiply the rows without
//! testing another rule.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use tickvault::features::{Entity, Feature, FeatureStore, FeatureView, Freshness};
use tickvault::fixed::Fixed;
use tickvault::store::rows::Row;
use tickvault::store::schema::EventKind;
use tickvault::types::{BookLevel, Side, Symbol, VenueId};

const SECOND: i64 = 1_000_000_000;
const TTL: i64 = 10 * SECOND;
/// Rows on the ordinary part of the tape. Enough that a timing number is not
/// all constant overhead.
const TAPE_ROWS: i64 = 20_000;
/// Where the leak rows sit, in seconds from the tape's start.
const LEAK_AT: i64 = 10_000;

fn bid(price: i64, event_time: Option<i64>, available_at: i64) -> Row {
    Row {
        event: EventKind::Delta,
        msg_index: 0,
        recv_wall: available_at,
        venue_ts: event_time,
        side: Side::Bid,
        price: Fixed::from_mantissa(price),
        qty: Fixed::from_mantissa(1),
        suspect: false,
        book_level: BookLevel::L2,
        order_id: None,
        action: None,
        queue_index: None,
        queue_certainty: None,
        traded_qty: None,
    }
}

/// The tape. Deterministic, so every implementation is judged on one dataset.
///
/// Ordinary rows reach us a quarter second after the venue stamps them. Three
/// rows are different and are the point of the exercise:
///
/// - a **leak** row, stamped before `LEAK_AT` but not arriving until after it,
/// - a **stale** stretch, where nothing new arrives for a minute,
/// - an **unstamped** row, from a venue that sent no timestamp at all.
fn tape() -> Vec<Row> {
    let mut rows = Vec::with_capacity(TAPE_ROWS as usize + 3);
    for i in 0..TAPE_ROWS {
        let event_time = i * SECOND;
        if i == LEAK_AT {
            // The venue's clock runs ahead of ours: it stamps this 2s in the
            // past relative to the read we are about to take, but the message
            // does not reach us until 3s after it. Nobody at LEAK_AT had it.
            rows.push(bid(
                999_999,
                Some(LEAK_AT * SECOND - 2 * SECOND),
                LEAK_AT * SECOND + 3 * SECOND,
            ));
            continue;
        }
        rows.push(bid(
            1_000_000 + i,
            Some(event_time),
            event_time + SECOND / 4,
        ));
    }
    // A venue that sends no timestamp of its own. It lands late, so the stale
    // probe below is not entangled with it.
    rows.push(bid(2_000_000, None, TAPE_ROWS * SECOND + 100 * SECOND));
    rows
}

/// The instants every implementation is asked about.
fn as_of_points() -> Vec<i64> {
    let mut points = vec![
        // Before anything arrived.
        -SECOND,
        // The leak instant, and either side of it.
        LEAK_AT * SECOND - SECOND,
        LEAK_AT * SECOND,
        LEAK_AT * SECOND + SECOND,
        LEAK_AT * SECOND + 4 * SECOND,
        // The instant the skew row becomes available. Its availability is
        // later than the rows that arrived after it, so this is where an
        // implementation that orders by wall clock instead of by arrival
        // sequence gives a different answer.
        LEAK_AT * SECOND + 3 * SECOND,
        // Deep into the tape, where the answer is unremarkable.
        (TAPE_ROWS / 2) * SECOND,
        // Fifty seconds after the last stamped row and before the unstamped one
        // arrives: the newest value is genuinely old, and nothing else is
        // competing to be latest.
        TAPE_ROWS * SECOND + 50 * SECOND,
        // Past the unstamped row too.
        TAPE_ROWS * SECOND + 150 * SECOND,
    ];
    // A spread of ordinary instants, so the comparison is not three edge cases.
    for i in (0..TAPE_ROWS).step_by(997) {
        points.push(i * SECOND + SECOND / 2);
    }
    points.sort_unstable();
    points.dedup();
    points
}

fn freshness_name(f: Freshness) -> &'static str {
    match f {
        Freshness::Fresh { .. } => "fresh",
        Freshness::Stale { .. } => "stale",
        Freshness::AgeUnknown => "age_unknown",
        Freshness::Missing => "missing",
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out: PathBuf = std::env::args()
        .nth(1)
        .ok_or("usage: pit_fixture <out-dir>")?
        .into();
    std::fs::create_dir_all(&out)?;

    let rows = tape();
    let symbol = Symbol::new("BTC", "USD");
    let entity = Entity::new(VenueId::Kraken, &symbol);
    let view = FeatureView::new(Feature::LastBidPrice, TTL);

    // ---- the shared feature table -----------------------------------------
    //
    // Two timestamp columns, named for what they mean rather than for what a
    // particular tool calls them.
    let ts = || DataType::Timestamp(TimeUnit::Nanosecond, None);
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("event_time", ts(), true),
        Field::new("available_at", ts(), false),
        // The archive's own order, carried explicitly. `available_at` is a wall
        // reading and NTP can step it backwards, so it cannot be trusted to
        // order arrivals; the recorder orders by its monotonic clock and this
        // column is that order. A reader that sorts by `available_at` instead
        // will disagree wherever the two differ.
        Field::new("arrival_seq", DataType::Int64, false),
        Field::new("last_bid_price", DataType::Int64, false),
    ]));

    let mut entity_ids = StringBuilder::new();
    let mut event_times = Int64Builder::new();
    let mut available = Int64Builder::new();
    let mut arrival = Int64Builder::new();
    let mut values = Int64Builder::new();
    let entity_id = format!("{}:{}", VenueId::Kraken, symbol.as_str());
    for (seq, row) in rows.iter().enumerate() {
        entity_ids.append_value(&entity_id);
        event_times.append_option(row.venue_ts);
        available.append_value(row.recv_wall);
        arrival.append_value(seq as i64);
        values.append_value(row.price.mantissa());
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(entity_ids.finish()),
            Arc::new(arrow::compute::cast(&event_times.finish(), &ts())?),
            Arc::new(arrow::compute::cast(&available.finish(), &ts())?),
            Arc::new(arrival.finish()),
            Arc::new(values.finish()),
        ],
    )?;
    let parquet_path = out.join("features.parquet");
    let mut writer = ArrowWriter::try_new(File::create(&parquet_path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    // ---- TickVault's answers ----------------------------------------------
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &symbol, &rows);

    // Time the same 29 questions the other two implementations are asked, so
    // the three numbers are comparable. Repeated, because a single pass over
    // 20k rows is short enough to be mostly noise.
    let points = as_of_points();
    let mut best = f64::MAX;
    for _ in 0..5 {
        let start = std::time::Instant::now();
        for as_of in &points {
            std::hint::black_box(store.as_of(view, &entity, *as_of));
        }
        best = best.min(start.elapsed().as_secs_f64());
    }
    println!("tickvault_as_of_all_points_s {best:.6}");

    let mut answers = Vec::new();
    for as_of in as_of_points() {
        let historical = store.as_of(view, &entity, as_of);
        // The online store materialized to the same instant. A separate code
        // path; its agreement is asserted here as well as in the gate.
        let online = store.materialize(as_of);
        let latest = online.latest(view, &entity);
        assert_eq!(
            historical.observation(),
            latest.observation(),
            "historical and online disagree at {as_of}"
        );

        let mut record = HashMap::new();
        record.insert("as_of".to_string(), serde_json::json!(as_of));
        record.insert(
            "value".to_string(),
            serde_json::json!(historical.observation().map(|o| o.value)),
        );
        record.insert(
            "event_time".to_string(),
            serde_json::json!(historical.observation().and_then(|o| o.event_time)),
        );
        record.insert(
            "available_at".to_string(),
            serde_json::json!(historical.observation().map(|o| o.available_at)),
        );
        record.insert(
            "freshness".to_string(),
            serde_json::json!(freshness_name(historical.freshness)),
        );
        record.insert(
            "fresh_value".to_string(),
            serde_json::json!(historical.fresh_value()),
        );
        answers.push(record);
    }

    let manifest = serde_json::json!({
        "entity_id": entity_id,
        "feature": Feature::LastBidPrice.name(),
        "ttl_nanos": TTL,
        "rows": rows.len(),
        "leak_row": {
            "value": 999_999,
            "event_time": LEAK_AT * SECOND - 2 * SECOND,
            "available_at": LEAK_AT * SECOND + 3 * SECOND,
            "note": "stamped before the read, arrived after it",
        },
        "answers": answers,
        "tickvault_as_of_all_points_s": best,
    });
    std::fs::write(
        out.join("tickvault_answers.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;

    println!(
        "wrote {} rows to {} and {} answers",
        rows.len(),
        parquet_path.display(),
        answers.len()
    );
    Ok(())
}
