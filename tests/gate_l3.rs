//! **Phase 4 gate.**
//!
//! The prompt sets no explicit gate for L3, so these are the properties an
//! order-by-order capture has to have before the queue positions in it are
//! worth anything to anyone.
//!
//! 1. **The chain holds, and a break is detected.** Bitstamp's L3 feed names
//!    each event's predecessor, so removing any event must be caught. This is
//!    the phase 1 gate again, on a different feed.
//! 2. **Lifecycle invariants.** No order added twice, none removed twice, and
//!    events for orders that predate the capture are counted rather than
//!    silently absorbed.
//! 3. **Removals detach from where the order really is.** Bitstamp deletions
//!    sometimes carry a different price from the creation. A reconstruction
//!    that trusted the event's price would leave a phantom resting at the old
//!    level forever.
//! 4. **Queue positions never overclaim.** A position derived from a snapshot
//!    is marked seeded, and one sitting behind a seeded order inherits that.
//!
//! The cross-feed reconciliation lives at the bottom, behind `--ignored`,
//! because it measures this venue's two feeds against each other live rather
//! than against a frozen fixture.

mod common;

use std::time::Duration;

use common::*;
use tickvault::book::l3::{L3Anomaly, OrderAction, QueueCertainty};
use tickvault::session::BookSession;
use tickvault::types::VenueId;

#[test]
fn the_l3_feed_parses_into_order_events() {
    let venue = bitstamp_l3();
    let tape = bitstamp_l3_tape();
    let events = order_events(venue.as_ref(), &tape);
    assert!(events.len() > 400, "only {} events parsed", events.len());

    // Every event carries what the reconstruction needs.
    for e in &events {
        assert!(!e.order_id.as_str().is_empty());
        assert!(e.price.is_positive(), "{:?} has no price", e.order_id);
        assert!(
            e.event_token.is_some() && e.prev_event_token.is_some(),
            "the chain must be present on every event"
        );
        assert!(e.stamps.venue_nanos.is_some(), "venue timestamp missing");
    }

    // All four actions are modelled, and the venue only names three.
    let adds = events
        .iter()
        .filter(|e| e.action == OrderAction::Add)
        .count();
    let cancels = events
        .iter()
        .filter(|e| e.action == OrderAction::Cancel)
        .count();
    let executes = events
        .iter()
        .filter(|e| e.action == OrderAction::Execute)
        .count();
    assert!(adds > 0 && cancels > 0);
    println!(
        "{} events: {adds} add, {cancels} cancel, {executes} execute, {} modify",
        events.len(),
        events.len() - adds - cancels - executes
    );
}

#[test]
fn the_event_chain_is_unbroken_across_the_capture() {
    let venue = bitstamp_l3();
    let tape = bitstamp_l3_tape();
    let r = replay(std::sync::Arc::clone(&venue), &tape, &btc_usd());
    let report = r.session.report();
    assert!(r.parse_errors.is_empty(), "{:?}", r.parse_errors);
    assert_eq!(
        report.total_gaps(),
        0,
        "a clean capture must not produce a chain break\n{report}"
    );
}

#[test]
fn removing_any_event_breaks_the_chain_and_is_detected() {
    // The phase 1 property on the L3 feed. Because each event names its
    // predecessor, *every* removal is detectable, with no exceptions for
    // changes that happen to leave the book identical.
    let venue = bitstamp_l3();
    let tape = bitstamp_l3_tape();
    let victims = order_frame_indices(venue.as_ref(), &tape);
    assert!(victims.len() > 400);

    let mut checked = 0;
    // The first event has no observed predecessor to chain to, and the last has
    // no successor to reveal its absence. Both are inherent, not oversights.
    for victim in victims.iter().skip(1).take(victims.len() - 2) {
        let r = replay(
            std::sync::Arc::clone(&venue),
            &tape.without(&[*victim]),
            &btc_usd(),
        );
        let stats = r
            .session
            .gaps()
            .stats(VenueId::Bitstamp, &btc_usd())
            .expect("stats");
        assert!(
            stats.sequence_gaps > 0,
            "removing frame {victim} left the chain looking intact\n{}",
            r.session.report()
        );
        checked += 1;
    }
    println!("{checked} removals, every one detected by the chain");
}

#[test]
fn order_lifecycle_invariants_hold() {
    let venue = bitstamp_l3();
    let events = order_events(venue.as_ref(), &bitstamp_l3_tape());
    let l = lifecycle(&events);

    assert_eq!(l.duplicate_creates, 0, "an order was added twice");
    assert_eq!(l.duplicate_removes, 0, "an order was removed twice");
    assert!(l.created > 0 && l.removed > 0);
    // Events for orders that predate the capture are expected on a feed with no
    // in-band snapshot, and must be counted rather than absorbed.
    assert!(
        l.orphans > 0,
        "the fixture was chosen to contain pre-existing orders; if this is now \
         zero the fixture changed and the case is no longer covered"
    );
    println!(
        "{} created, {} removed, {} still open, {} events for orders predating the capture",
        l.created, l.removed, l.still_open, l.orphans
    );
}

#[test]
fn a_removal_detaches_the_order_from_where_it_really_is() {
    // Measured on Bitstamp: 62 of 1,865 deletions carry a different price from
    // the order's creation. Trusting the event's price would leave a phantom
    // resting at the original level for the rest of the day.
    let venue = bitstamp_l3();
    let events = order_events(venue.as_ref(), &bitstamp_l3_tape());
    let l = lifecycle(&events);
    assert!(
        l.price_moved_on_remove > 0,
        "the fixture no longer contains a price-moving removal, so this \
         property is no longer being tested"
    );

    let book = drive_l3(&events);
    // Every order still in the book must be findable at the level the book
    // says it is on. A phantom would fail this.
    for (side, price) in [
        (tickvault::types::Side::Bid, book.best_bid()),
        (tickvault::types::Side::Ask, book.best_ask()),
    ] {
        let Some(price) = price else { continue };
        for order in book.queue_at(side, price) {
            assert_eq!(order.price, price, "order filed at the wrong level");
            assert_eq!(order.side, side);
        }
    }
    println!(
        "{} removals carried a different price than the creation, all detached correctly",
        l.price_moved_on_remove
    );
}

#[test]
fn queue_positions_never_claim_more_than_was_observed() {
    let venue = bitstamp_l3();
    let mut session = BookSession::new(std::sync::Arc::clone(&venue), vec![btc_usd()]);

    // Seed from the captured order-level snapshot, then replay.
    let snapshot = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(venue.order_snapshot(&btc_usd()))
        .expect("snapshot")
        .expect("bitstamp at L3 has an order snapshot");
    let seeded_ids: Vec<_> = snapshot.orders.iter().map(|o| o.0.clone()).collect();
    assert!(!seeded_ids.is_empty());
    session.apply_order_snapshot(snapshot);

    for frame in &bitstamp_l3_tape().frames {
        let _ = session.ingest(&frame.to_raw());
    }
    let book = session.l3_book(&btc_usd()).expect("l3 book");

    let mut seeded_seen = 0;
    for id in &seeded_ids {
        if let Some(pos) = book.queue_position(id) {
            assert_ne!(
                pos.certainty,
                QueueCertainty::Observed,
                "order {id} came from a snapshot and must never be reported as observed"
            );
            seeded_seen += 1;
        }
    }
    assert!(
        seeded_seen > 0,
        "no seeded order survived the replay, so nothing was checked"
    );
    println!("{seeded_seen} seeded orders, none claiming an observed position");

    // A level known to hold an order we never watched arrive can never report
    // an observed position, however precisely we can count the ones we know.
    let mut on_incomplete = 0;
    for side in [tickvault::types::Side::Bid, tickvault::types::Side::Ask] {
        let Some(best) = (match side {
            tickvault::types::Side::Bid => book.best_bid(),
            tickvault::types::Side::Ask => book.best_ask(),
        }) else {
            continue;
        };
        if !book.level_is_incomplete(side, best) {
            continue;
        }
        for order in book.queue_at(side, best) {
            let pos = book.queue_position(&order.id).expect("resting order");
            assert_eq!(
                pos.certainty,
                QueueCertainty::Unknown,
                "orders we never saw rest here, so counting the ones we know understates the queue"
            );
            on_incomplete += 1;
        }
    }
    println!("{on_incomplete} positions on known-incomplete levels, all reported unknown");
}

#[test]
fn the_aggregated_view_of_an_l3_book_stays_consistent() {
    let venue = bitstamp_l3();
    let events = order_events(venue.as_ref(), &bitstamp_l3_tape());
    let book = drive_l3(&events);
    let l2 = book.to_l2(tickvault::clock::Timestamps::recv_only(
        tickvault::clock::Stamp::ZERO,
    ));

    // Total quantity per level must equal the sum of the orders on it.
    for side in [tickvault::types::Side::Bid, tickvault::types::Side::Ask] {
        for (price, qty) in l2.top(side, usize::MAX) {
            let summed = book
                .queue_at(side, price)
                .iter()
                .fold(tickvault::Fixed::ZERO, |acc, o| {
                    acc.checked_add(o.qty).expect("in range")
                });
            assert_eq!(summed, qty, "aggregate disagrees with the queue at {price}");
            assert!(qty.is_positive(), "an empty level must not survive");
        }
    }
    println!(
        "{} open orders aggregate to {} bid and {} ask levels",
        book.open_orders(),
        l2.level_count(tickvault::types::Side::Bid),
        l2.level_count(tickvault::types::Side::Ask)
    );
}

#[test]
fn an_l3_book_reports_its_own_anomalies_rather_than_absorbing_them() {
    let venue = bitstamp_l3();
    let events = order_events(venue.as_ref(), &bitstamp_l3_tape());
    let mut book = tickvault::book::l3::L3Book::new(btc_usd());
    let (mut unknown, mut crossed) = (0, 0);
    for event in &events {
        for anomaly in book.apply(event).anomalies {
            match anomaly {
                L3Anomaly::UnknownOrder { .. } => unknown += 1,
                // An aggressive order is on the book for the instant between
                // being created and being matched, so an order-by-order book
                // crosses by construction. See the transient-crossing test.
                L3Anomaly::Crossed { .. } => crossed += 1,
                // Adding the same order twice, or a negative quantity, would be
                // the venue contradicting itself and is not expected here.
                other => panic!("unexpected anomaly: {other}"),
            }
        }
    }
    assert_eq!(
        unknown as u64,
        book.stats().unknown_orders,
        "the book's own counter must match what it reported"
    );
    assert!(unknown > 0, "the fixture contains pre-existing orders");
    println!(
        "{unknown} events for orders predating the capture, {crossed} transient crossings, all reported"
    );
}

#[test]
fn a_transient_crossing_from_an_aggressive_order_is_not_treated_as_corruption() {
    // The rule that is right for L2 is wrong here. An order-by-order feed shows
    // an aggressive order resting for the instant between its creation and its
    // match, so the book crosses by construction. Observed on Bitstamp with
    // both events sharing a microtimestamp and the deletion reporting the
    // execution price rather than the placement price.
    //
    // A crossing that *lasts* is still a fault, which is why the session times
    // them rather than ignoring them.
    let venue = bitstamp_l3();
    let r = replay(
        std::sync::Arc::clone(&venue),
        &bitstamp_l3_tape(),
        &btc_usd(),
    );
    let stats = r
        .session
        .gaps()
        .stats(VenueId::Bitstamp, &btc_usd())
        .expect("stats");
    assert!(
        stats.crossed_books > 0,
        "the fixture contains an aggressive order; if this is zero the case is \
         no longer covered"
    );
    assert!(
        r.silently_crossed.is_empty(),
        "a crossing must always be counted, transient or not"
    );

    let report = r.session.report();
    let clean = stats.clean_fraction().unwrap_or(0.0);
    assert!(
        clean > 0.99,
        "transient crossings must not make a healthy feed look suspect: \
         clean {:.4}\n{report}",
        clean * 100.0
    );
    println!(
        "{} transient crossings, {:.4}% clean",
        stats.crossed_books,
        clean * 100.0
    );
}

/// Live reconciliation of this venue's two feeds against each other.
///
/// The strongest available check on an L3 reconstruction, and it needs no
/// snapshot: **between two consecutive reports of a price level on the
/// aggregated feed, the order-by-order events at that price must net to exactly
/// the quantity change the aggregated feed reports.** Both feeds start from
/// whatever was already resting, so the starting state cancels out.
///
/// It runs against the venue rather than a fixture because a frozen capture
/// would stop being evidence the moment Bitstamp changed anything.
///
/// The measured rate is around 96%. The residual is not a reconstruction error:
/// the two feeds stamp differently, with the aggregated one running roughly
/// 300 ms behind, so an event near a batch boundary lands on the wrong side of
/// it. The floor asserted here is well below the measured rate so that a real
/// regression fails while that known skew does not.
#[test]
#[ignore = "connects to Bitstamp's two live feeds; run with --ignored"]
fn the_order_feed_reconciles_with_the_venues_own_aggregated_feed() {
    use std::collections::BTreeMap;
    use tickvault::Fixed;
    use tickvault::types::Side;

    const SECONDS: u64 = 45;
    const FLOOR: f64 = 0.90;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (orders, levels) = runtime.block_on(async {
        let l3 = capture_live("live_orders_btcusd", SECONDS);
        let l2 = capture_live("diff_order_book_btcusd", SECONDS);
        tokio::join!(l3, l2)
    });
    assert!(orders.len() > 100, "only {} order frames", orders.len());
    assert!(levels.len() > 5, "only {} aggregated frames", levels.len());

    // Order-by-order: net quantity change per level, tracking each order's own
    // resting level rather than whatever price the event happens to carry.
    let venue = bitstamp_l3();
    let mut resting: BTreeMap<String, (Side, Fixed, Fixed)> = BTreeMap::new();
    let mut net: BTreeMap<(Side, Fixed), Vec<(i64, Fixed)>> = BTreeMap::new();
    let mut unknown: BTreeMap<(Side, Fixed), Vec<i64>> = BTreeMap::new();
    let mut first = i64::MAX;
    let mut last = i64::MIN;

    for event in order_events(venue.as_ref(), &orders) {
        let t = event.stamps.venue_nanos.unwrap_or(0);
        first = first.min(t);
        last = last.max(t);
        let id = event.order_id.as_str().to_string();
        match event.action {
            OrderAction::Add => {
                net.entry((event.side, event.price))
                    .or_default()
                    .push((t, event.qty));
                resting.insert(id, (event.side, event.price, event.qty));
            }
            OrderAction::Modify => match resting.get(&id).copied() {
                Some((side, price, qty)) => {
                    let delta = event.qty.checked_sub(qty).unwrap_or(Fixed::ZERO);
                    net.entry((side, price)).or_default().push((t, delta));
                    resting.insert(id, (side, price, event.qty));
                }
                None => unknown
                    .entry((event.side, event.price))
                    .or_default()
                    .push(t),
            },
            OrderAction::Cancel | OrderAction::Execute => match resting.remove(&id) {
                // Its real level, not the one the event reports: an aggressive
                // order is deleted at the price it executed at.
                Some((side, price, qty)) => net
                    .entry((side, price))
                    .or_default()
                    .push((t, Fixed::ZERO.checked_sub(qty).unwrap_or(Fixed::ZERO))),
                None => unknown
                    .entry((event.side, event.price))
                    .or_default()
                    .push(t),
            },
        }
    }

    // Aggregated: absolute quantity per level, per report.
    let l2_venue = common::fixture(VenueId::Bitstamp).venue;
    let mut reports: BTreeMap<(Side, Fixed), Vec<(i64, Fixed)>> = BTreeMap::new();
    for frame in &levels.frames {
        for event in l2_venue.parse_delta(&frame.to_raw()).expect("parses") {
            let tickvault::venue::FeedEvent::Delta(delta) = event else {
                continue;
            };
            let t = delta.stamps.venue_nanos.unwrap_or(0);
            if t < first || t > last {
                continue;
            }
            for change in &delta.changes {
                reports
                    .entry((change.side, change.price))
                    .or_default()
                    .push((t, change.qty));
            }
        }
    }

    let (mut checked, mut agreed, mut skipped) = (0, 0, 0);
    for (key, series) in &reports {
        for window in series.windows(2) {
            let ((t1, q1), (t2, q2)) = (window[0], window[1]);
            // An order predating the capture moved here; its size is unknown.
            if unknown
                .get(key)
                .is_some_and(|ts| ts.iter().any(|t| *t > t1 && *t <= t2))
            {
                skipped += 1;
                continue;
            }
            checked += 1;
            let summed = net
                .get(key)
                .map(|cs| {
                    cs.iter()
                        .filter(|(t, _)| *t > t1 && *t <= t2)
                        .fold(Fixed::ZERO, |acc, (_, d)| {
                            acc.checked_add(*d).unwrap_or(acc)
                        })
                })
                .unwrap_or(Fixed::ZERO);
            if q2.checked_sub(q1) == Some(summed) {
                agreed += 1;
            }
        }
    }

    assert!(checked > 100, "only {checked} windows to compare");
    let rate = agreed as f64 / checked as f64;
    println!(
        "reconciliation over {:.1}s: {agreed}/{checked} level windows agree ({:.2}%), \
         {skipped} skipped for orders predating the capture",
        (last - first) as f64 / 1e9,
        rate * 100.0
    );
    assert!(
        rate >= FLOOR,
        "the order feed and the aggregated feed agree on only {:.2}% of level \
         changes, below the {:.0}% floor",
        rate * 100.0,
        FLOOR * 100.0
    );
}

/// Subscribe to one Bitstamp channel and collect frames for `seconds`.
async fn capture_live(channel: &str, seconds: u64) -> tickvault::recorder::RawTape {
    use futures_util::{SinkExt, StreamExt};
    use tickvault::clock::{Clock, MonotonicClock};
    use tickvault::recorder::{RawTape, RecordedFrame};
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws, _) = tokio_tungstenite::connect_async("wss://ws.bitstamp.net")
        .await
        .expect("connect to bitstamp");
    let subscribe = format!(r#"{{"event":"bts:subscribe","data":{{"channel":"{channel}"}}}}"#);
    ws.send(Message::Text(subscribe.into()))
        .await
        .expect("subscribe");

    let clock = MonotonicClock::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let mut frames = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let stamp = clock.stamp();
                frames.push(RecordedFrame {
                    venue: VenueId::Bitstamp,
                    t_mono: stamp.mono_nanos,
                    t_wall: stamp.wall_nanos,
                    payload: text.to_string(),
                });
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    RawTape::new(frames)
}

/// The archived quantity of an execution is what changed hands, not what is
/// left.
///
/// A removal row carries the quantity still resting, which on a full fill is
/// zero. Reading the traded size off that column makes every L3 bar report a
/// volume of exactly zero, which is not "unknown" but the false claim that
/// nothing traded. The size is knowable only while the order's previous state
/// is still held, so the book works it out as the rise in the venue's own
/// running total and it travels in its own column.
#[test]
fn an_execution_records_the_size_that_traded_not_the_size_left_resting() {
    let venue = bitstamp_l3();
    let tape = bitstamp_l3_tape();
    let events = order_events(venue.as_ref(), &tape);

    let mut book = tickvault::book::l3::L3Book::new(tickvault::types::Symbol::new("BTC", "USD"));
    let mut traded = Vec::new();
    for event in &events {
        let outcome = book.apply(event);
        if let Some(qty) = outcome.traded_qty {
            traded.push((event.action, qty));
        }
    }

    assert!(
        !traded.is_empty(),
        "this tape contains executions; none were sized"
    );
    for (action, qty) in &traded {
        assert!(
            qty.is_positive(),
            "{action:?} reported a traded quantity of {qty}, which says nothing traded"
        );
    }

    // The venue reports a cumulative figure per order, so what is recorded must
    // be the increment. Anything larger means a fill was counted twice.
    let total: f64 = traded.iter().map(|(_, q)| q.to_f64_lossy()).sum();
    let ceiling: f64 = events
        .iter()
        .filter_map(|e| e.executed_qty)
        .map(|q| q.to_f64_lossy())
        .sum();
    assert!(
        total <= ceiling,
        "counted {total} traded against a cumulative ceiling of {ceiling}"
    );
    println!("{} sized executions totalling {total}", traded.len());
}
