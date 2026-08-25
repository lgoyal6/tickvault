//! **Phase 1 gate.**
//!
//! > Replay a recorded raw feed with messages deliberately removed and assert
//! > every gap is detected. Assert the book never goes crossed without being
//! > flagged.
//!
//! The gate is stated as three properties, in increasing order of how much they
//! actually protect a downstream user.
//!
//! 1. **Detection.** On a counter-based feed, every removed message is detected
//!    and counted exactly.
//! 2. **No silent corruption.** On a checksum-based feed the count is not
//!    knowable, so the property becomes: if the book diverges from what it
//!    should have been, we always know. A loss that provably left the book
//!    identical is not a lie, and is allowed to pass unremarked.
//! 3. **Never claims clean.** Whatever was removed, however it was removed, the
//!    report never describes the affected window as clean. This is the one a
//!    backtest actually depends on.
//!
//! The blind spots are tested too, as tests rather than as prose, because a
//! limit that is only written down in a README stops being true the first time
//! someone changes a default.

mod common;

use common::*;
use tickvault::gap::SuspectCause;
use tickvault::recorder::RawTape;
use tickvault::types::VenueId;

/// The window a drop can be detected in. A message removed from the very end of
/// a recording has no successor to reveal its absence, and a message removed
/// before the first one we ever saw has no predecessor. Both are inherent, and
/// both get their own test below rather than being quietly excluded.
fn droppable(tape: &RawTape) -> std::ops::Range<usize> {
    1..tape.len() - 1
}

/// Indices of Kraken *book update* frames. Deliberately specific: Kraken's
/// `status` frame is also `"type":"update"`, and a looser match would have this
/// sweep "dropping" a connection banner and expecting a checksum to notice.
fn kraken_updates(tape: &RawTape) -> Vec<usize> {
    let mut v = tape.indices_containing(r#""channel":"book","type":"update""#);
    // The final frame has no successor to reveal its absence.
    v.retain(|i| *i + 1 < tape.len());
    v
}

fn assert_never_claims_clean(r: &Replay, context: &str) {
    let report = r.session.report();
    assert!(
        !report.is_spotless(),
        "{context}: messages were removed but the report calls the window clean\n{report}"
    );
}

// ------------------------------------------------------------ clean tapes --

#[test]
fn an_untouched_tape_reports_no_gaps_on_either_venue() {
    for (name, venue, tape) in [
        ("coinbase synthetic", coinbase(), coinbase_tape(200, 7)),
        (
            "kraken synthetic",
            kraken(10),
            kraken_tape(200, 10, 7, false),
        ),
        ("coinbase captured", coinbase(), real_coinbase_tape()),
        ("kraken captured", kraken(10), real_kraken_tape()),
    ] {
        let r = replay(venue, &tape, &btc_usd());
        let report = r.session.report();
        assert!(
            r.parse_errors.is_empty(),
            "{name}: unparsed frames {:?}",
            r.parse_errors
        );
        assert_eq!(report.total_gaps(), 0, "{name}: phantom gap\n{report}");
        assert_eq!(report.total_missing(), 0, "{name}\n{report}");
        assert!(r.silently_crossed.is_empty(), "{name}");
        assert!(
            r.session.is_ready(&btc_usd()),
            "{name}: book should be trusted after a clean run\n{report}"
        );
        assert!(
            report.is_spotless(),
            "{name}: a clean tape must produce a spotless report\n{report}"
        );
    }
}

// -------------------------------------------- property 1: exact detection --

#[test]
fn coinbase_detects_every_single_removed_frame_and_counts_it_exactly() {
    let tape = coinbase_tape(120, 11);
    let mut checked = 0;
    for victim in droppable(&tape) {
        let r = replay(coinbase(), &tape.without(&[victim]), &btc_usd());
        let stats = r
            .session
            .gaps()
            .stats(VenueId::Coinbase, &btc_usd())
            .expect("stats");
        assert_eq!(
            stats.sequence_gaps,
            1,
            "dropping frame {victim} produced {} gaps, expected exactly 1\n{}",
            stats.sequence_gaps,
            r.session.report()
        );
        assert_eq!(
            stats.messages_missing, 1,
            "dropping frame {victim} should account for exactly one lost message"
        );
        assert_never_claims_clean(&r, &format!("coinbase drop {victim}"));
        checked += 1;
    }
    assert!(checked > 100, "the sweep must actually cover the tape");
}

#[test]
fn coinbase_accounts_for_every_message_in_a_multi_frame_loss() {
    let tape = coinbase_tape(150, 23);
    for seed in 0..40u64 {
        let mut rng = Rng::new(seed);
        let range = droppable(&tape);
        // Never remove the final frame: its loss is undetectable by
        // construction and would make this a test of that instead.
        let mut victims: Vec<usize> = range.clone().filter(|_| rng.chance(6)).collect();
        victims.retain(|i| *i < tape.len() - 1);
        if victims.is_empty() {
            continue;
        }
        let r = replay(coinbase(), &tape.without(&victims), &btc_usd());
        let stats = r
            .session
            .gaps()
            .stats(VenueId::Coinbase, &btc_usd())
            .expect("stats");
        assert_eq!(
            stats.messages_missing,
            victims.len() as u64,
            "seed {seed}: removed {} frames, report accounts for {}\n{}",
            victims.len(),
            stats.messages_missing,
            r.session.report()
        );
        assert_never_claims_clean(&r, &format!("coinbase seed {seed}"));
    }
}

#[test]
fn coinbase_detects_a_removed_frame_in_a_captured_tape() {
    // The same property against bytes the venue actually sent, including the
    // subscription acknowledgement that shares the connection's counter.
    let tape = real_coinbase_tape();
    for victim in droppable(&tape) {
        let r = replay(coinbase(), &tape.without(&[victim]), &btc_usd());
        let stats = r
            .session
            .gaps()
            .stats(VenueId::Coinbase, &btc_usd())
            .expect("stats");
        assert_eq!(
            stats.messages_missing,
            1,
            "captured tape, dropped frame {victim}\n{}",
            r.session.report()
        );
    }
}

// ------------------------------------ property 2: no silent book corruption --

/// The first replay step at which the book differs from what it should have
/// been, or `None` if removing the message changed nothing at all.
///
/// That second case is not a failure and not a gap. If a lost update is
/// immediately overwritten by the next one touching the same level, the archive
/// ends up holding exactly the right book. Reporting it as a gap would tell a
/// user to discard data that is perfectly good, which is its own kind of
/// dishonesty.
fn first_divergence(reference: &Replay, dropped: &Replay, victim: usize) -> Option<usize> {
    (victim..dropped.digests.len()).find(|&j| dropped.digests[j] != reference.digests[j + 1])
}

#[test]
fn kraken_never_corrupts_a_book_without_saying_so() {
    let tape = kraken_tape(120, 10, 31, false);
    let reference = replay(kraken(10), &tape, &btc_usd());
    let victims = kraken_updates(&tape);
    assert!(
        victims.len() > 100,
        "the sweep must actually cover the tape"
    );

    let (mut detected, mut inconsequential) = (0, 0);
    for victim in &victims {
        let dropped = replay(kraken(10), &tape.without(&[*victim]), &btc_usd());
        match first_divergence(&reference, &dropped, *victim) {
            Some(j) => {
                assert!(
                    dropped.detected_by[j],
                    "removing frame {victim} left the book wrong from replay step {j} and \
                     nothing was flagged\n{}",
                    dropped.session.report()
                );
                assert_never_claims_clean(&dropped, &format!("kraken drop {victim}"));
                detected += 1;
            }
            None => {
                // Nothing was lost that the book records, so the run really is
                // clean and must be reported that way.
                let report = dropped.session.report();
                assert!(
                    report.is_spotless(),
                    "removing frame {victim} changed no book state, so flagging it would \
                     send users to discard good data\n{report}"
                );
                inconsequential += 1;
            }
        }
    }
    assert_eq!(detected + inconsequential, victims.len());
    assert!(
        detected > 0,
        "the sweep detected nothing, which is suspicious"
    );
    println!(
        "kraken depth 10: {detected}/{} losses altered the book and every one was caught; \
         {inconsequential} were overwritten before they mattered",
        victims.len()
    );
}

#[test]
fn kraken_at_depth_ten_has_no_undetectable_book_corruption() {
    // The claim the depth default rests on. At depth ten the checksum covers
    // every level the feed can touch, so the count of losses that changed the
    // book and escaped notice is exactly zero, not merely small.
    let tape = kraken_tape(80, 10, 47, false);
    let reference = replay(kraken(10), &tape, &btc_usd());
    let mut escaped = Vec::new();
    for victim in kraken_updates(&tape) {
        let dropped = replay(kraken(10), &tape.without(&[victim]), &btc_usd());
        if let Some(j) = first_divergence(&reference, &dropped, victim)
            && !dropped.detected_by[j]
        {
            escaped.push(victim);
        }
    }
    assert!(
        escaped.is_empty(),
        "depth 10 is supposed to be the depth at which nothing escapes, but frames {escaped:?} did"
    );
}

#[test]
fn kraken_detects_a_removed_frame_in_a_captured_tape() {
    // The same property against bytes Kraken actually sent, checksums included.
    let tape = real_kraken_tape();
    let reference = replay(kraken(10), &tape, &btc_usd());
    let book_updates = kraken_updates(&tape);
    assert!(!book_updates.is_empty(), "fixture has no book updates");
    let mut checked = 0;
    for victim in book_updates {
        let dropped = replay(kraken(10), &tape.without(&[victim]), &btc_usd());
        if let Some(j) = first_divergence(&reference, &dropped, victim) {
            assert!(
                dropped.detected_by[j],
                "captured tape: removing frame {victim} corrupted the book unnoticed\n{}",
                dropped.session.report()
            );
            checked += 1;
        }
    }
    assert!(
        checked > 0,
        "no removal from the captured tape altered the book, so this proved nothing"
    );
}

// -------------------------------------------------- the declared blind spot --

#[test]
fn kraken_beyond_the_checksum_window_is_genuinely_blind_and_says_so() {
    // The limitation the capability matrix declares, demonstrated rather than
    // asserted: subscribed at depth 100, a lost update touching only levels
    // past the tenth changes the book and leaves the checksum matching.
    let depth = 100;
    let tape = kraken_tape(60, depth, 53, true);
    let reference = replay(kraken(depth), &tape, &btc_usd());

    let mut undetected_and_wrong = 0;
    for victim in kraken_updates(&tape) {
        let dropped = replay(kraken(depth), &tape.without(&[victim]), &btc_usd());
        if let Some(j) = first_divergence(&reference, &dropped, victim)
            && !dropped.detected_by[j]
        {
            undetected_and_wrong += 1;
        }
    }
    assert!(
        undetected_and_wrong > 0,
        "expected deep-level losses to be invisible at depth {depth}; if this now passes, \
         the checksum window changed and the published blind spot is out of date"
    );

    // Having established the blind spot is real, assert the dataset admits it.
    let session = tickvault::session::BookSession::new(kraken(depth), vec![btc_usd()]);
    let limits = session.report().limits;
    assert!(
        limits.iter().any(|l| l.venue == VenueId::Kraken
            && l.consequence.contains("undetectable")
            && l.scope.contains(&depth.to_string())),
        "depth {depth} hides losses and the report does not declare it: {limits:#?}"
    );

    // And that the declaration is absent at depth ten, where it does not apply.
    let shallow = tickvault::session::BookSession::new(kraken(10), vec![btc_usd()]);
    assert!(
        !shallow
            .report()
            .limits
            .iter()
            .any(|l| l.consequence.contains("undetectable")),
        "depth 10 has no checksum blind spot and should not claim one"
    );
    println!("depth {depth}: {undetected_and_wrong} deep losses invisible to the venue checksum");
}

// -------------------------------------------------- inherent edge of a tape --

#[test]
fn a_loss_at_either_end_of_a_recording_is_admitted_rather_than_detected() {
    let tape = coinbase_tape(60, 61);

    // The last frame has no successor, so nothing can reveal its absence.
    let tail = replay(coinbase(), &tape.without(&[tape.len() - 1]), &btc_usd());
    let tail_stats = tail
        .session
        .gaps()
        .stats(VenueId::Coinbase, &btc_usd())
        .expect("stats");
    assert_eq!(
        tail_stats.sequence_gaps, 0,
        "a loss at the tail cannot be detected, and pretending otherwise would be worse"
    );

    // The first frame has no predecessor. It is also the subscription frame, so
    // its loss is invisible to the counter, but the snapshot still anchors us.
    let head = replay(coinbase(), &tape.without(&[0]), &btc_usd());
    let head_stats = head
        .session
        .gaps()
        .stats(VenueId::Coinbase, &btc_usd())
        .expect("stats");
    assert_eq!(head_stats.sequence_gaps, 0);

    // Losing the snapshot itself is different: it is detected, and the book is
    // never marked ready, so nothing downstream can mistake it for good data.
    let no_snapshot = replay(coinbase(), &tape.without(&[1]), &btc_usd());
    assert!(
        !no_snapshot.session.is_ready(&btc_usd()),
        "without a snapshot the book must never be presented as trustworthy"
    );
    assert_never_claims_clean(&no_snapshot, "coinbase without a snapshot");
}

// ---------------------------------- the second half of the gate: crossing --

#[test]
fn the_book_is_never_crossed_without_being_flagged() {
    // Random drop patterns are the cheapest way to manufacture the disordered
    // updates that produce a crossed book.
    let cases: Vec<(&str, std::sync::Arc<dyn tickvault::venue::Venue>, RawTape)> = vec![
        ("coinbase", coinbase(), coinbase_tape(120, 71)),
        ("kraken-10", kraken(10), kraken_tape(120, 10, 71, false)),
        ("kraken-100", kraken(100), kraken_tape(120, 100, 71, false)),
        // A tape that genuinely crosses, so the sweep is not vacuous.
        (
            "kraken-venue-bug",
            kraken(10),
            kraken_tape_with_venue_bug(120, 10, 71),
        ),
    ];
    for (name, venue, tape) in cases {
        let mut total_crossed = 0;
        // The undropped tape first. On the bug tape a drop is caught before the
        // crossing update is ever reached, so without this case the sweep would
        // silently stop exercising the thing it is named after.
        let mut victim_sets: Vec<Vec<usize>> = vec![Vec::new()];
        for seed in 0..60u64 {
            let mut rng = Rng::new(seed ^ 0xC0FFEE);
            victim_sets.push(droppable(&tape).filter(|_| rng.chance(5)).collect());
        }
        for (case, victims) in victim_sets.iter().enumerate() {
            let r = replay(
                std::sync::Arc::clone(&venue),
                &tape.without(victims),
                &btc_usd(),
            );
            assert!(
                r.silently_crossed.is_empty(),
                "{name} case {case}: crossed book at frames {:?} with nothing flagged\n{}",
                r.silently_crossed,
                r.session.report()
            );
            total_crossed += r.crossed_frames.len();
        }
        if name == "kraken-venue-bug" {
            assert!(
                total_crossed > 0,
                "the bug tape must actually cross the book, or this sweep proves nothing"
            );
        }
        println!("{name}: {total_crossed} crossed-book frames, all flagged");
    }
}

#[test]
fn a_crossed_book_is_caught_even_when_the_venue_checksum_agrees_with_it() {
    // The reason the crossed-book invariant is not redundant with sequence
    // validation. Here the venue sends a bid above its own best ask *and* a
    // checksum that correctly describes that crossed book, so every sequence
    // check passes. Only the book's own invariant is left to notice, and if it
    // is ever removed this test is the thing that fails.
    let tape = kraken_tape_with_venue_bug(30, 10, 83);
    let r = replay(kraken(10), &tape, &btc_usd());

    let stats = r
        .session
        .gaps()
        .stats(VenueId::Kraken, &btc_usd())
        .expect("stats");
    assert_eq!(
        stats.checksum_divergences,
        0,
        "the checksum agrees with the crossed book, so this must not be a divergence\n{}",
        r.session.report()
    );
    assert_eq!(stats.crossed_books, 1, "{}", r.session.report());
    assert!(r.silently_crossed.is_empty());
    assert!(
        !r.session.is_ready(&btc_usd()),
        "a crossed book must not stay trusted"
    );
    assert!(
        r.session
            .gaps()
            .windows()
            .iter()
            .any(|w| matches!(w.cause, SuspectCause::Anomaly(_))
                || w.also.iter().any(|c| matches!(c, SuspectCause::Anomaly(_)))),
        "a crossed book must open or extend a suspect window"
    );
}

// ---------------------------------------------- no stitching across a hole --

#[test]
fn deltas_are_not_stitched_onto_a_book_known_to_be_stale() {
    // The failure this whole phase exists to prevent: continuing to apply
    // updates across a detected gap, producing a book that looks continuous and
    // is wrong.
    let tape = coinbase_tape(60, 97);
    let victim = 20;
    let r = replay(coinbase(), &tape.without(&[victim]), &btc_usd());

    assert!(
        !r.session.is_ready(&btc_usd()),
        "after an undetected re-snapshot the book must not be presented as good"
    );
    // The book stops moving at the gap, rather than drifting further from truth.
    let frozen = &r.digests[victim..];
    assert!(
        frozen.windows(2).all(|w| w[0] == w[1]),
        "the book kept changing after a detected gap: {:?}",
        &frozen[..frozen.len().min(8)]
    );

    let stats = r
        .session
        .gaps()
        .stats(VenueId::Coinbase, &btc_usd())
        .expect("stats");
    assert!(
        stats.unverifiable >= 1,
        "messages arriving inside a suspect window must be counted as unverifiable"
    );
    assert!(
        stats.suspect_nanos > 0,
        "the suspect window must have duration"
    );
}

#[test]
fn losing_the_snapshot_yields_no_book_rather_than_a_wrong_one() {
    // Dropping the snapshot is not a gap in the delta stream, it is the absence
    // of a baseline. There is nothing for a checksum to disagree with, so the
    // right behaviour is not detection but refusal: never mark the book ready,
    // count every subsequent message as unverifiable, and never call it clean.
    for (name, venue, tape, snapshot_frame) in [
        (
            "kraken",
            kraken(10),
            kraken_tape(40, 10, 101, false),
            1usize,
        ),
        ("coinbase", coinbase(), coinbase_tape(40, 101), 1usize),
    ] {
        let r = replay(venue, &tape.without(&[snapshot_frame]), &btc_usd());
        let report = r.session.report();
        assert!(
            !r.session.is_ready(&btc_usd()),
            "{name}: no snapshot, yet the book is presented as trustworthy\n{report}"
        );
        assert!(!report.is_spotless(), "{name}\n{report}");
        let stats = r
            .session
            .gaps()
            .stats(r.session.venue_id(), &btc_usd())
            .expect("stats");
        assert!(
            stats.unverifiable > 0,
            "{name}: messages with no baseline must be counted unverifiable\n{report}"
        );
        assert_eq!(
            stats.clean_fraction(),
            Some(0.0),
            "{name}: with no baseline, no part of the window is vouched for\n{report}"
        );
    }
}

#[test]
fn a_delete_for_an_absent_level_is_judged_per_venue_not_universally() {
    // The two venues genuinely disagree here, and getting it wrong is not a
    // cosmetic mistake. Treating Coinbase's idempotent deletes as corruption
    // made a live recorder rebuild a 4.8 MB book every two seconds and report a
    // perfectly healthy feed as 2% clean.

    // Coinbase: noise. Counted, but the book stays trusted.
    let tape = coinbase_tape_with_redundant_delete(40, 131);
    let r = replay(coinbase(), &tape, &btc_usd());
    let stats = r
        .session
        .gaps()
        .stats(VenueId::Coinbase, &btc_usd())
        .expect("stats");
    assert_eq!(
        stats.deletes_of_absent_levels, 1,
        "it must still be counted"
    );
    assert_eq!(stats.sequence_gaps, 0);
    assert!(
        r.session.is_ready(&btc_usd()),
        "a delete Coinbase always sends must not invalidate the book\n{}",
        r.session.report()
    );
    assert_eq!(
        stats.resnapshots, 1,
        "only the initial snapshot; the redundant delete must not force another"
    );

    // Kraken: evidence. It never deletes what it did not publish, so this means
    // we missed the add.
    let tape = kraken_tape_with_redundant_delete(40, 10, 131);
    let r = replay(kraken(10), &tape, &btc_usd());
    let stats = r
        .session
        .gaps()
        .stats(VenueId::Kraken, &btc_usd())
        .expect("stats");
    assert_eq!(stats.deletes_of_absent_levels, 1);
    assert_eq!(
        stats.checksum_divergences, 0,
        "the checksum still matches, so the absent-level rule is the only signal"
    );
    assert!(
        !r.session.is_ready(&btc_usd()),
        "on Kraken this is evidence of a missed message\n{}",
        r.session.report()
    );
    assert!(!r.session.report().is_spotless());
}
