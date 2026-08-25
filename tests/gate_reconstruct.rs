//! **Phase 5 gate.**
//!
//! > Reconstructing the same timestamp twice produces a byte-identical book.
//! > Hash it and assert in CI.
//!
//! That is the stated gate, and on its own it is weaker than it sounds: a
//! reconstruction that is deterministically *wrong* passes it. So it is checked
//! alongside the two properties that make determinism worth having.
//!
//! - **Determinism.** The same request twice, and from a freshly opened
//!   reconstructor, gives the same book: same digest and the same levels.
//! - **Checkpoints are an optimisation, not a variation.** A rebuild that
//!   starts from a checkpoint must be identical to one that replayed
//!   everything. If they can disagree, the checkpoint is a second
//!   implementation of the book and one of them is wrong.
//! - **Depth is a view, not a different book.** A depth-limited rebuild must
//!   equal the truncation of the full one, which is what makes truncating only
//!   at output observable rather than a claim.
//!
//! The archive under test is produced by the ordinary ingest path over bytes a
//! venue actually sent, not from rows invented to suit the test.

mod common;

use common::*;
use tickvault::reconstruct::{Reconstructor, Request, checkpoint};
use tickvault::types::{Side, VenueId};

/// Sample instants across an archive's span, including both ends.
fn probe_instants(span: (i64, i64), n: usize) -> Vec<i64> {
    let (first, last) = span;
    let width = (last - first).max(1);
    let mut out: Vec<i64> = (0..n)
        .map(|i| first + width * i as i64 / n.max(1) as i64)
        .collect();
    out.push(first - 1);
    out.push(last);
    out.push(last + width);
    out
}

#[test]
fn rebuilding_the_same_instant_twice_gives_the_same_book() {
    for venue in [VenueId::Kraken, VenueId::Coinbase, VenueId::Okx] {
        let dir = tempfile::tempdir().unwrap();
        let rows = archive_from_fixture(venue, dir.path(), 64);
        assert!(rows > 0, "{venue} archived nothing");
        let span = archive_span(dir.path());
        let symbol = tickvault::venue::registry::VenueConfig::default_symbol(venue);

        for at in probe_instants(span, 12) {
            let request = Request::new(venue, &symbol, at);
            // Two separate reconstructors, so nothing is carried between them.
            let a = Reconstructor::open(dir.path())
                .unwrap()
                .at(&request)
                .unwrap();
            let b = Reconstructor::open(dir.path())
                .unwrap()
                .at(&request)
                .unwrap();

            assert_eq!(a.digest(), b.digest(), "{venue} at {at}: digests differ");
            assert_eq!(
                a.book.top(Side::Bid, usize::MAX),
                b.book.top(Side::Bid, usize::MAX),
                "{venue} at {at}: bids differ"
            );
            assert_eq!(
                a.book.top(Side::Ask, usize::MAX),
                b.book.top(Side::Ask, usize::MAX),
                "{venue} at {at}: asks differ"
            );
            assert_eq!(a.rows_applied, b.rows_applied);
            assert_eq!(a.origin, b.origin);
        }
    }
}

#[test]
fn a_rebuild_is_not_accidentally_constant() {
    // Determinism is worth nothing if every instant returns the same book. The
    // digest has to actually move as the day does.
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Kraken, dir.path(), 64);
    let span = archive_span(dir.path());
    let symbol = tickvault::venue::registry::VenueConfig::default_symbol(VenueId::Kraken);
    let r = Reconstructor::open(dir.path()).unwrap();

    let digests: Vec<u32> = probe_instants(span, 10)
        .into_iter()
        .map(|at| {
            r.at(&Request::new(VenueId::Kraken, &symbol, at))
                .unwrap()
                .digest()
        })
        .collect();
    let distinct: std::collections::BTreeSet<u32> = digests.iter().copied().collect();
    assert!(
        distinct.len() > 2,
        "only {} distinct books across the whole archive: {digests:?}",
        distinct.len()
    );
}

#[test]
fn a_checkpointed_rebuild_equals_a_full_replay() {
    // The property that makes checkpoints an optimisation rather than a second,
    // disagreeing implementation of the book.
    for venue in [VenueId::Kraken, VenueId::Coinbase] {
        let dir = tempfile::tempdir().unwrap();
        archive_from_fixture(venue, dir.path(), 64);
        let span = archive_span(dir.path());
        let symbol = tickvault::venue::registry::VenueConfig::default_symbol(venue);
        let reconstructor = Reconstructor::open(dir.path()).unwrap();

        // Checkpoint a third of the way through, then ask about later instants.
        let mid = span.0 + (span.1 - span.0) / 3;
        let at_mid = reconstructor
            .at(&Request::new(venue, &symbol, mid).without_checkpoints())
            .unwrap();
        checkpoint::write(
            dir.path(),
            &checkpoint::Checkpoint::of_l2(venue, &at_mid.book, mid),
        )
        .unwrap();

        let mut compared = 0;
        for at in probe_instants(span, 8).into_iter().filter(|t| *t > mid) {
            let full = reconstructor
                .at(&Request::new(venue, &symbol, at).without_checkpoints())
                .unwrap();
            let from_checkpoint = reconstructor.at(&Request::new(venue, &symbol, at)).unwrap();

            assert_eq!(
                full.digest(),
                from_checkpoint.digest(),
                "{venue} at {at}: a checkpointed rebuild disagrees with a full replay\n\
                 full: {full}\ncheckpointed: {from_checkpoint}"
            );
            assert_eq!(
                full.book.top(Side::Bid, usize::MAX),
                from_checkpoint.book.top(Side::Bid, usize::MAX)
            );
            assert_eq!(
                full.book.top(Side::Ask, usize::MAX),
                from_checkpoint.book.top(Side::Ask, usize::MAX)
            );
            compared += 1;
        }
        assert!(compared > 0, "{venue}: no instants after the checkpoint");
    }
}

#[test]
fn a_checkpoint_actually_saves_work() {
    // Otherwise it is a correct answer produced the slow way, and the whole
    // point was not replaying a day to answer a question about its end.
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Coinbase, dir.path(), 16);
    let span = archive_span(dir.path());
    let symbol = tickvault::venue::registry::VenueConfig::default_symbol(VenueId::Coinbase);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();

    let late = span.1;
    let full = reconstructor
        .at(&Request::new(VenueId::Coinbase, &symbol, late).without_checkpoints())
        .unwrap();

    // Checkpoint most of the way through.
    let mid = span.0 + (span.1 - span.0) * 3 / 4;
    let at_mid = reconstructor
        .at(&Request::new(VenueId::Coinbase, &symbol, mid).without_checkpoints())
        .unwrap();
    checkpoint::write(
        dir.path(),
        &checkpoint::Checkpoint::of_l2(VenueId::Coinbase, &at_mid.book, mid),
    )
    .unwrap();

    let cheap = reconstructor
        .at(&Request::new(VenueId::Coinbase, &symbol, late))
        .unwrap();
    assert_eq!(full.digest(), cheap.digest());
    assert!(
        cheap.rows_applied < full.rows_applied,
        "the checkpoint saved nothing: {} rows either way",
        full.rows_applied
    );
    println!(
        "full replay {} rows from {} file(s); from checkpoint {} rows from {} file(s)",
        full.rows_applied, full.files_read, cheap.rows_applied, cheap.files_read
    );
}

#[test]
fn a_depth_limited_rebuild_is_the_truncation_of_the_full_one() {
    let dir = tempfile::tempdir().unwrap();
    archive_from_fixture(VenueId::Okx, dir.path(), 128);
    let span = archive_span(dir.path());
    let symbol = tickvault::venue::registry::VenueConfig::default_symbol(VenueId::Okx);
    let reconstructor = Reconstructor::open(dir.path()).unwrap();

    let mut checked = 0;
    for at in probe_instants(span, 6) {
        let full = reconstructor
            .at(&Request::new(VenueId::Okx, &symbol, at))
            .unwrap();
        for depth in [1usize, 5, 20] {
            let limited = reconstructor
                .at(&Request::new(VenueId::Okx, &symbol, at).with_depth(depth))
                .unwrap();
            for side in [Side::Bid, Side::Ask] {
                assert_eq!(
                    limited.book.top(side, usize::MAX),
                    full.book.top(side, depth),
                    "depth {depth} at {at} on the {side} side is not the truncation"
                );
            }
            assert!(limited.book.level_count(Side::Bid) <= depth);
            assert!(limited.book.level_count(Side::Ask) <= depth);
        }
        checked += 1;
    }
    assert!(checked > 0);
}

#[test]
fn an_order_by_order_archive_rebuilds_too() {
    // The L3 path has no snapshot to restart from, which is exactly the case
    // checkpoints exist for.
    let dir = tempfile::tempdir().unwrap();
    let f = bitstamp_l3();
    let symbol = btc_usd();
    let _ = &f;
    let rows = archive_l3_fixture(dir.path(), 256);
    assert!(rows > 0);

    let span = archive_span(dir.path());
    let reconstructor = Reconstructor::open(dir.path()).unwrap();
    let request = Request::new(VenueId::Bitstamp, &symbol, span.1);

    let a = reconstructor.at(&request).unwrap();
    let b = reconstructor.at(&request).unwrap();
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.book_level, tickvault::types::BookLevel::L3);
    assert!(a.l3.is_some(), "an L3 archive must rebuild the order book");
    assert!(
        a.l3.as_ref().unwrap().open_orders() > 0,
        "no orders survived the rebuild"
    );
    // The aggregated view is the order book aggregated, not a separate answer.
    let l3 = a.l3.as_ref().unwrap();
    for (price, qty) in a.book.top(Side::Bid, usize::MAX) {
        let summed = l3
            .queue_at(Side::Bid, price)
            .iter()
            .fold(tickvault::Fixed::ZERO, |acc, o| {
                acc.checked_add(o.qty).expect("in range")
            });
        assert_eq!(summed, qty, "aggregate disagrees with the queue at {price}");
    }
    println!(
        "rebuilt {} orders into {} bid levels from {} rows",
        l3.open_orders(),
        a.book.level_count(Side::Bid),
        a.rows_applied
    );
}
