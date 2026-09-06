//! Point-in-time features and serving parity.
//!
//! Four things are asserted here, and they are the whole of the capability:
//!
//! 1. A point-in-time read filters on **availability** time, so a row that had
//!    not arrived by `as_of` cannot be read at `as_of`, no matter what the
//!    venue's own timestamp claims. The leak case is constructed rather than
//!    hoped for.
//! 2. Historical retrieval and the online/latest read agree at the same
//!    instant, by two different code paths.
//! 3. A stale value announces itself; a consumer cannot receive an old number
//!    while believing it is current.
//! 4. Where the venue sent no timestamp, the age is reported unknown rather
//!    than invented.

use tickvault::features::{Entity, Feature, FeatureStore, FeatureView, Freshness};
use tickvault::fixed::Fixed;
use tickvault::store::rows::Row;
use tickvault::store::schema::EventKind;
use tickvault::types::{BookLevel, Side, Symbol, VenueId};

const SECOND: i64 = 1_000_000_000;

fn btc() -> Symbol {
    Symbol::new("BTC", "USD")
}

fn entity() -> Entity {
    Entity::new(VenueId::Kraken, &btc())
}

fn view() -> FeatureView {
    // Ten seconds: long enough that the parity rows are all fresh, short enough
    // that the staleness case is unambiguous.
    FeatureView::new(Feature::LastBidPrice, 10 * SECOND)
}

/// One bid level, with its two times stated separately.
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

/// What a point-in-time join looks like when it trusts the venue's clock.
///
/// This is the implementation under suspicion, written out plainly so the test
/// compares two real behaviours rather than asserting against a comment. It is
/// the join every feature store performs by default: filter on the event
/// timestamp column, take the latest.
fn naive_event_time_join(rows: &[Row], as_of: i64) -> Option<i64> {
    rows.iter()
        .filter(|row| row.venue_ts.is_some_and(|ts| ts <= as_of))
        .max_by_key(|row| row.venue_ts.unwrap())
        .map(|row| row.price.mantissa())
}

#[test]
fn a_row_that_had_not_arrived_yet_cannot_be_read_at_that_instant() {
    // The venue's clock runs 5s ahead of ours, so it stamps an event with a
    // time we have not reached, and the message lands 3s after `as_of`. This
    // is not contrived: the archive records `skew_ns` signed and unclamped
    // precisely because it goes negative in the wild.
    let as_of = 1_000 * SECOND;
    let rows = vec![
        // Arrived well before as_of. This is the only honest answer.
        bid(100, Some(as_of - 30 * SECOND), as_of - 29 * SECOND),
        // Venue says it happened 2s BEFORE as_of, but it reached us 3s AFTER.
        // At `as_of` this row did not exist here.
        bid(999, Some(as_of - 2 * SECOND), as_of + 3 * SECOND),
    ];

    // The naive join takes the bait: it sees a venue timestamp inside the
    // window and returns a price nobody could have had.
    assert_eq!(
        naive_event_time_join(&rows, as_of),
        Some(999),
        "the leak has to be real for refusing it to mean anything"
    );

    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);
    let read = store.as_of(view(), &entity(), as_of);

    let observation = read.observation().expect("the earlier row is available");
    assert_eq!(
        observation.value, 100,
        "as_of must return the last value that had actually arrived, not the \
         one the venue stamped into the past after the fact"
    );
    assert!(
        observation.available_at <= as_of,
        "availability {} must not exceed as_of {as_of}",
        observation.available_at
    );
}

#[test]
fn the_refusal_survives_the_leaking_row_arriving_first_in_the_file() {
    // Same leak, but the unavailable row sits earlier in arrival order, so a
    // fix that merely took "the last row in the file" would pass by accident.
    let as_of = 1_000 * SECOND;
    let rows = vec![
        bid(999, Some(as_of - 2 * SECOND), as_of + 3 * SECOND),
        bid(100, Some(as_of - 30 * SECOND), as_of - 29 * SECOND),
    ];
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);
    assert_eq!(
        store
            .as_of(view(), &entity(), as_of)
            .observation()
            .map(|o| o.value),
        Some(100)
    );
}

#[test]
fn historical_and_online_reads_agree_at_the_same_instant() {
    // A tape with ordinary latency: each event reaches us a few hundred
    // milliseconds after the venue stamps it.
    let mut rows = Vec::new();
    for i in 0..40i64 {
        let event_time = i * SECOND;
        rows.push(bid(1_000 + i, Some(event_time), event_time + SECOND / 4));
    }
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);

    // Every instant on the tape, not one convenient one.
    for step in 0..=(40 * 4) {
        let watermark = step * SECOND / 4;
        let online = store.materialize(watermark);
        for feature in Feature::ALL {
            let v = FeatureView::new(*feature, 10 * SECOND);
            let historical = store.as_of(v, &entity(), watermark);
            let latest = online.latest(v, &entity());
            assert_eq!(
                historical.observation(),
                latest.observation(),
                "training and serving disagree at watermark {watermark} for {}",
                feature.name()
            );
            assert_eq!(
                historical.freshness, latest.freshness,
                "the same value must carry the same verdict at {watermark}"
            );
        }
    }
}

#[test]
fn parity_holds_when_the_wall_clock_steps_backwards() {
    // NTP corrects the recorder's wall clock backwards mid-tape, so
    // `available_at` is no longer monotonic in arrival order. A materializer
    // that stopped at the first arrival past the watermark would truncate here
    // and disagree with the historical read.
    let rows = vec![
        bid(10, Some(0), 10 * SECOND),
        // The step back: this arrived later but stamps earlier.
        bid(20, Some(SECOND), 4 * SECOND),
        bid(30, Some(2 * SECOND), 6 * SECOND),
    ];
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);

    for watermark in [0, 3, 4, 5, 6, 9, 10, 11].map(|s| s * SECOND) {
        let online = store.materialize(watermark);
        assert_eq!(
            store.as_of(view(), &entity(), watermark).observation(),
            online.latest(view(), &entity()).observation(),
            "historical and online disagree at watermark {watermark} across a \
             backwards clock step"
        );
    }

    // And the answer itself is the arrival-ordered one: at t=6s the rows that
    // had arrived are the 4s and 6s ones, and the last to arrive wins.
    assert_eq!(
        store
            .as_of(view(), &entity(), 6 * SECOND)
            .observation()
            .map(|o| o.value),
        Some(30)
    );
}

#[test]
fn a_stale_value_says_so_and_will_not_hand_over_its_number() {
    let as_of = 100 * SECOND;
    // Stamped 40s before the read, against a 10s TTL.
    let rows = vec![bid(500, Some(as_of - 40 * SECOND), as_of - 39 * SECOND)];
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);

    let read = store.as_of(view(), &entity(), as_of);
    match read.freshness {
        Freshness::Stale {
            age_nanos,
            ttl_nanos,
        } => {
            assert_eq!(age_nanos, 40 * SECOND);
            assert_eq!(ttl_nanos, 10 * SECOND);
        }
        other => panic!("a 40s-old value under a 10s TTL must be stale, got {other:?}"),
    }
    assert_eq!(
        read.fresh_value(),
        None,
        "a stale read must not hand back a bare number"
    );
    // It is still reachable, but only by a caller who takes the verdict too.
    assert_eq!(read.value_with_freshness().map(|(v, _)| v), Some(500));

    // The online path reports it identically.
    let online = store.materialize(as_of);
    assert_eq!(online.latest(view(), &entity()).freshness, read.freshness);
    assert_eq!(online.latest(view(), &entity()).fresh_value(), None);
}

#[test]
fn a_fresh_value_inside_the_ttl_is_handed_over() {
    // The negative half of the staleness test: the withholding above must be
    // the TTL working, not the accessor always returning None.
    let as_of = 100 * SECOND;
    let rows = vec![bid(500, Some(as_of - 2 * SECOND), as_of - SECOND)];
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);
    let read = store.as_of(view(), &entity(), as_of);
    assert_eq!(
        read.freshness,
        Freshness::Fresh {
            age_nanos: 2 * SECOND
        }
    );
    assert_eq!(read.fresh_value(), Some(500));
}

#[test]
fn a_venue_that_sent_no_timestamp_gets_an_unknown_age_not_a_fresh_one() {
    let as_of = 100 * SECOND;
    let rows = vec![bid(500, None, as_of - SECOND)];
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);

    let read = store.as_of(view(), &entity(), as_of);
    assert_eq!(read.freshness, Freshness::AgeUnknown);
    assert!(
        !read.freshness.is_fresh(),
        "an unmeasurable age must not pass as fresh"
    );
    assert_eq!(
        read.fresh_value(),
        None,
        "a value whose age cannot be computed must not be handed over as current"
    );
    assert_eq!(read.value_with_freshness().map(|(v, _)| v), Some(500));
}

#[test]
fn an_instant_before_anything_arrived_is_missing_rather_than_zero() {
    let rows = vec![bid(500, Some(50 * SECOND), 51 * SECOND)];
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);

    let read = store.as_of(view(), &entity(), 10 * SECOND);
    assert_eq!(read.freshness, Freshness::Missing);
    assert_eq!(read.observation(), None);
    assert_eq!(read.fresh_value(), None);
    // The online store at the same instant holds nothing at all.
    assert!(store.materialize(10 * SECOND).is_empty());
}

#[test]
fn a_removed_level_does_not_become_a_zero_priced_feature() {
    // qty == 0 means the level went away. Treating that as an observation
    // would publish a price of zero as the latest bid.
    let mut removal = bid(500, Some(50 * SECOND), 51 * SECOND);
    removal.qty = Fixed::from_mantissa(0);
    let rows = vec![bid(400, Some(40 * SECOND), 41 * SECOND), removal];
    let mut store = FeatureStore::new();
    store.ingest(VenueId::Kraken, &btc(), &rows);
    assert_eq!(
        store
            .as_of(view(), &entity(), 60 * SECOND)
            .observation()
            .map(|o| o.value),
        Some(400)
    );
}
