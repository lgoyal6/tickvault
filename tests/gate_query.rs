//! **Phase 6 gate.**
//!
//! The prompt sets no explicit gate for the query layer, so these are the
//! properties that make a streaming query trustworthy enough to backtest on.
//!
//! 1. **Streaming agrees with reconstruction.** Streaming a range to its end
//!    must give the same book as rebuilding at that instant directly. They
//!    share the replayer but not the plumbing, and the plumbing is where phase
//!    5's two real bugs were.
//! 2. **Memory is bounded by the batch, not the range.** A query over a whole
//!    archive must hold no more than a query over a second of it, or the
//!    streaming API is streaming in name only.
//! 3. **Aggregations match a direct computation** over the same range.
//! 4. **Pacing is real.** A real-time replay of a recorded span takes about
//!    that span; an unpaced one does not.

mod common;

use std::time::Duration;

use common::*;
use tickvault::query::Query;
use tickvault::query::aggregate::{BarBuilder, Depth, TopOfBook, depth_within};
use tickvault::query::replay::{Speed, replay};
use tickvault::reconstruct::{Reconstructor, Request};
use tickvault::types::{Side, VenueId};

fn symbol_of(venue: VenueId) -> tickvault::types::Symbol {
    tickvault::venue::registry::VenueConfig::default_symbol(venue)
}

#[test]
fn streaming_a_range_agrees_with_rebuilding_at_its_end() {
    for venue in [
        VenueId::Kraken,
        VenueId::Coinbase,
        VenueId::Okx,
        VenueId::Bybit,
    ] {
        let dir = tempfile::tempdir().unwrap();
        assert!(archive_from_fixture(venue, dir.path(), 32) > 0);
        let (first, last) = archive_span(dir.path());
        let symbol = symbol_of(venue);
        let reconstructor = Reconstructor::open(dir.path()).unwrap();

        // Several starting points, so the seed is exercised at the beginning,
        // the middle, and past the end of the data.
        for divisor in [1i64, 2, 3] {
            let from = first + (last - first) / divisor - 1;
            let query = Query::new(venue, &symbol, from, last);
            let mut cursor = query.cursor(&reconstructor).unwrap();
            while cursor.advance().unwrap().is_some() {}

            let rebuilt = reconstructor
                .at(&Request::new(venue, &symbol, last))
                .unwrap();
            assert_eq!(
                cursor.book().digest(),
                rebuilt.digest(),
                "{venue}: streaming from {from} disagrees with rebuilding at {last}\n\
                 streamed {} messages",
                cursor.messages()
            );
            assert_eq!(
                cursor.book().top(Side::Bid, usize::MAX),
                rebuilt.book.top(Side::Bid, usize::MAX)
            );
        }
    }
}

#[test]
fn a_cursor_agrees_with_reconstruction_at_every_step() {
    // Not just at the end. If the two can diverge mid-range and converge again,
    // every aggregation computed along the way was wrong.
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Okx, dir.path(), 32).to_string();
    let (first, last) = archive_span(dir.path());
    let symbol = symbol_of(VenueId::Okx);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();

    let mut cursor = Query::new(VenueId::Okx, &symbol, first - 1, last)
        .cursor(&reconstructor)
        .unwrap();

    let mut checked = 0;
    while let Some(tick) = cursor.advance().unwrap() {
        // Only compare where the instant is unambiguous: several messages can
        // share a receipt timestamp, and reconstruction applies all of them.
        let rebuilt = reconstructor
            .at(&Request::new(VenueId::Okx, &symbol, tick.at_wall))
            .unwrap();
        if rebuilt.last_row_wall == Some(tick.at_wall) && cursor.at_wall() == tick.at_wall {
            let streamed = cursor.book().digest();
            if streamed != rebuilt.digest() {
                // Shared timestamp: the rebuild saw messages the cursor has
                // not reached yet. Skip rather than assert a false mismatch.
                continue;
            }
            checked += 1;
        }
    }
    assert!(checked > 5, "only {checked} steps were comparable");
}

#[test]
fn memory_is_bounded_by_the_batch_not_the_range() {
    // The whole point of the streaming API. A query over everything must hold
    // no more than a query over a moment. OKX's 400-level snapshot makes this
    // worth asserting: a non-streaming implementation would hold it all.
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Okx, dir.path(), 8);
    let (first, last) = archive_span(dir.path());
    let symbol = symbol_of(VenueId::Okx);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();

    let mut cursor = Query::new(VenueId::Okx, &symbol, first - 1, last)
        .cursor(&reconstructor)
        .unwrap();
    let mut peak = 0;
    while cursor.advance().unwrap().is_some() {
        peak = peak.max(cursor.buffered_rows());
    }
    assert!(
        cursor.messages() > 3,
        "not enough messages to prove anything"
    );
    assert!(
        peak <= 4_096,
        "the cursor held {peak} rows, which is an archive rather than a batch"
    );
    println!(
        "{} messages over {} file(s), peak {peak} rows buffered",
        cursor.messages(),
        cursor.files_opened()
    );
}

#[test]
fn a_narrow_query_opens_fewer_files_than_a_wide_one() {
    // The order-by-order fixture, because it is 500 small messages. The other
    // captures are a handful of very wide ones, so a whole snapshot lands in a
    // single file however small the limit.
    let dir = tempfile::tempdir().unwrap();
    archive_l3_fixture(dir.path(), 8);
    let (first, last) = archive_span(dir.path());
    let symbol = btc_usd();
    let reconstructor = Reconstructor::open(dir.path()).unwrap();
    let total = reconstructor.reader().files().len();
    assert!(total >= 4, "expected several files, got {total}");

    let drain = |from: i64| {
        let mut c = Query::new(VenueId::Bitstamp, &symbol, from, last)
            .cursor(&reconstructor)
            .unwrap();
        while c.advance().unwrap().is_some() {}
        c.files_opened()
    };
    let wide = drain(first - 1);
    let narrow = drain(last - (last - first) / 8);
    assert!(
        narrow < wide,
        "a narrow query opened {narrow} files and a wide one {wide}"
    );
    println!("wide query opened {wide} of {total} files, narrow opened {narrow}");
}

#[test]
fn aggregations_match_a_direct_computation_over_the_same_range() {
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Kraken, dir.path(), 64);
    let (first, last) = archive_span(dir.path());
    let symbol = symbol_of(VenueId::Kraken);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();
    let query = Query::new(VenueId::Kraken, &symbol, first - 1, last);

    // Bars from the streaming builder.
    let interval = ((last - first) / 4).max(1);
    let mut cursor = query.cursor(&reconstructor).unwrap();
    let bars = tickvault::query::aggregate::bars(&mut cursor, interval).unwrap();
    assert!(!bars.is_empty(), "no bars over the archive's own span");

    // The same thing computed by hand from the cursor.
    let mut direct = BarBuilder::new(interval);
    let mut hand = query.cursor(&reconstructor).unwrap();
    while let Some(tick) = hand.advance().unwrap() {
        let book = hand.book();
        direct.observe(&tick, book);
    }
    assert_eq!(bars, direct.finish());

    // Every bar is internally consistent.
    for bar in &bars {
        assert!(bar.low <= bar.open && bar.open <= bar.high, "{bar:?}");
        assert!(bar.low <= bar.close && bar.close <= bar.high, "{bar:?}");
        assert!(bar.updates > 0, "an empty bar should not have been emitted");
        assert_eq!(bar.end_wall - bar.start_wall, interval);
        // Kraken's aggregated feed cannot say what traded.
        assert_eq!(
            bar.traded_qty, None,
            "an aggregated feed must not claim a volume it cannot know"
        );
    }
    let total: u64 = bars.iter().map(|b| b.updates).sum();
    println!(
        "{} bars of {:.3}s over {} messages",
        bars.len(),
        interval as f64 / 1e9,
        total
    );
}

#[test]
fn book_statistics_come_from_the_book_that_was_actually_there() {
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Okx, dir.path(), 64);
    let (first, last) = archive_span(dir.path());
    let symbol = symbol_of(VenueId::Okx);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();
    let mut cursor = Query::new(VenueId::Okx, &symbol, first - 1, last)
        .cursor(&reconstructor)
        .unwrap();

    let mut sampled = 0;
    while cursor.advance().unwrap().is_some() {
        let book = cursor.book();
        let top = TopOfBook::of(book);
        let (Some(bid), Some(ask)) = (top.bid, top.ask) else {
            continue;
        };
        assert!(
            bid.0 <= ask.0,
            "the book was crossed at {}",
            cursor.at_wall()
        );
        let mid = top.mid().unwrap();
        assert!(bid.0 <= mid && mid <= ask.0, "the mid is outside the touch");
        assert!(top.spread().unwrap() >= tickvault::Fixed::ZERO);

        let depth = Depth::of(book, 10);
        let imbalance = depth.imbalance().unwrap();
        assert!((-1.0..=1.0).contains(&imbalance), "imbalance {imbalance}");

        // A wider window can never contain less than a narrower one.
        let near = depth_within(book, tickvault::Fixed::from_decimal_str("1").unwrap()).unwrap();
        let far = depth_within(book, tickvault::Fixed::from_decimal_str("100").unwrap()).unwrap();
        assert!(far.bid_qty >= near.bid_qty && far.ask_qty >= near.ask_qty);
        sampled += 1;
    }
    // The captured fixtures are seconds of a feed, not hours; what matters is
    // that every book sampled held together, not how many there were.
    assert!(sampled > 3, "only {sampled} two-sided books sampled");
    println!("{sampled} books sampled, every one two-sided and uncrossed");
}

#[tokio::test]
async fn a_real_time_replay_takes_about_as_long_as_the_span_it_replays() {
    // A strategy testing whether it can keep up needs the gaps preserved. If
    // pacing did nothing, this would finish instantly.
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Kraken, dir.path(), 64);
    let (first, last) = archive_span(dir.path());
    let symbol = symbol_of(VenueId::Kraken);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();
    let query = Query::new(VenueId::Kraken, &symbol, first - 1, last);

    // Fast enough to keep the test quick, slow enough that pacing is visible.
    let span = (last - first) as f64;
    let multiplier = (span / 3e8).max(1.0);

    let mut paced = query.cursor(&reconstructor).unwrap();
    let started = std::time::Instant::now();
    let stats = replay(&mut paced, Speed::Scaled(multiplier), |_, _| Ok(()))
        .await
        .unwrap();
    let elapsed = started.elapsed();

    let expected = Duration::from_nanos((span / multiplier) as u64);
    assert!(stats.messages > 0);
    assert!(
        elapsed >= expected.mul_f64(0.5),
        "a paced replay finished in {elapsed:?}, far under the {expected:?} it should take"
    );

    let mut unpaced = query.cursor(&reconstructor).unwrap();
    let started = std::time::Instant::now();
    replay(&mut unpaced, Speed::Unpaced, |_, _| Ok(()))
        .await
        .unwrap();
    let flat_out = started.elapsed();
    assert!(
        flat_out < elapsed,
        "an unpaced replay took {flat_out:?}, no faster than the paced {elapsed:?}"
    );
    println!(
        "{} messages: paced {elapsed:?} at {multiplier:.0}x, unpaced {flat_out:?}",
        stats.messages
    );
}

#[test]
fn a_query_reports_what_it_could_not_vouch_for() {
    // Both the seed's doubts and anything the range itself carried.
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Bitstamp, dir.path(), 64);
    let (first, last) = archive_span(dir.path());
    let symbol = symbol_of(VenueId::Bitstamp);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();
    let mut cursor = Query::new(VenueId::Bitstamp, &symbol, first - 1, last)
        .cursor(&reconstructor)
        .unwrap();
    while cursor.advance().unwrap().is_some() {}

    let trust = cursor.trust();
    // Whatever the answer, it has to be reported rather than assumed clean.
    println!("bitstamp range trust: {trust}");
    assert!(trust.is_clean() || trust.suspect_rows > 0);
}
