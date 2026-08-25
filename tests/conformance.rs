//! **The conformance suite every venue implementation must pass.**
//!
//! Phase 2's real goal is not six venues, it is that adding the seventh is
//! mechanical rather than archaeological. That only holds if there is a single
//! battery that says what "a venue" means, and if it is run against all of them
//! rather than against whichever one was written last.
//!
//! Every test here iterates [`VenueId::ALL`]. Adding a venue to that list and
//! dropping a captured fixture beside the others is the whole checklist; if the
//! new venue is wrong, this file says which of these properties it broke.
//!
//! Note that several assertions are *conditional on declared capabilities*
//! rather than universal. A venue that says it cannot detect loss is not
//! required to detect loss; it is required to say so, and to never report a
//! message as verified. That is the difference between a conformance suite and
//! a set of assumptions.

mod common;

use common::*;
use tickvault::sequence::SeqState;
use tickvault::types::VenueId;
use tickvault::venue::{
    DecimalEncoding, FeedDepth, FeedEvent, Keepalive, SequenceScope, SnapshotSource,
    ValidationScheme, ValidationTiming,
};

// ------------------------------------------------------------- identity --

#[test]
fn every_venue_agrees_with_itself_about_who_it_is() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        assert_eq!(f.venue.id(), *id);
        assert_eq!(f.caps().id, *id);
        assert_eq!(id.as_str().parse::<VenueId>().unwrap(), *id);
    }
    // And no two venues share a name, which the archive partitions on.
    let mut names: Vec<&str> = VenueId::ALL.iter().map(|v| v.as_str()).collect();
    names.sort();
    let count = names.len();
    names.dedup();
    assert_eq!(names.len(), count, "two venues share a partition name");
}

// ------------------------------------------------------------- symbols --

#[test]
fn every_venue_round_trips_the_symbols_it_records() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        let spelled = f.venue.venue_symbol(&f.symbol);
        assert!(
            !spelled.as_str().is_empty(),
            "{id} produced an empty symbol"
        );
        assert_eq!(
            f.venue.canonical_symbol(spelled.as_str()).unwrap(),
            f.symbol,
            "{id} could not read back its own spelling {spelled}"
        );
    }
}

#[test]
fn every_venue_states_which_dollar_it_quotes() {
    // USD and USDT are different assets at different prices. A caller asking
    // for "Bitcoin against the dollar" has to be told which one it will get.
    for id in VenueId::ALL {
        let f = fixture(*id);
        let quote = f.symbol.quote();
        assert!(
            quote == "USD" || quote == "USDT",
            "{id} default symbol {} is neither dollar",
            f.symbol
        );
    }
}

// ------------------------------------------------------------ subscribe --

#[test]
fn every_venue_builds_subscribe_frames_that_name_its_symbols() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        let frames = f.venue.subscribe(std::slice::from_ref(&f.symbol)).unwrap();
        assert!(!frames.is_empty(), "{id} produced no subscribe frame");
        let spelled = f.venue.venue_symbol(&f.symbol).0;
        let joined = frames.join(" ");
        assert!(
            joined.contains(&spelled) || joined.contains(&spelled.to_ascii_lowercase()),
            "{id} subscribe frames do not mention {spelled}: {joined}"
        );
        for frame in &frames {
            serde_json::from_str::<serde_json::Value>(frame)
                .unwrap_or_else(|e| panic!("{id} subscribe frame is not valid JSON: {e}\n{frame}"));
        }
    }
}

#[test]
fn every_venue_refuses_an_empty_subscription() {
    // Subscribing to nothing and then reporting a clean run would be the most
    // flattering possible bug.
    for id in VenueId::ALL {
        let f = fixture(*id);
        assert!(
            f.venue.subscribe(&[]).is_err(),
            "{id} accepted an empty symbol list"
        );
    }
}

// ------------------------------------------------------------- parsing --

#[test]
fn every_venue_parses_every_frame_it_actually_sent_us() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        assert!(!f.tape.is_empty(), "{id} has no captured frames");
        for (i, recorded) in f.tape.frames.iter().enumerate() {
            f.venue.parse_delta(&recorded.to_raw()).unwrap_or_else(|e| {
                panic!(
                    "{id} cannot parse its own captured frame {i}: {e}\n{}",
                    &recorded.payload[..recorded.payload.len().min(300)]
                )
            });
        }
    }
}

#[test]
fn every_venue_produces_book_events_from_its_capture() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        let mut snapshots = 0;
        let mut deltas = 0;
        let mut orders = 0;
        for recorded in &f.tape.frames {
            for event in f.venue.parse_delta(&recorded.to_raw()).unwrap() {
                match event {
                    FeedEvent::Snapshot(_) => snapshots += 1,
                    FeedEvent::Delta(_) => deltas += 1,
                    FeedEvent::Order(_) => orders += 1,
                    FeedEvent::Control(_) => {}
                }
            }
        }
        assert_eq!(
            orders, 0,
            "{id}'s default capture is aggregated, so it should yield no order events"
        );
        assert!(deltas > 0, "{id} produced no deltas from its capture");
        match f.caps().snapshot_source {
            SnapshotSource::InBand => assert!(
                snapshots > 0,
                "{id} claims an in-band snapshot but its capture has none"
            ),
            SnapshotSource::Rest | SnapshotSource::None => {
                assert_eq!(snapshots, 0, "{id} claims no in-band snapshot but sent one")
            }
        }
    }
}

// ------------------------------------------- capability self-consistency --

#[test]
fn declared_capabilities_do_not_contradict_each_other() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        let c = f.caps();

        // A check on the book *after* an update can only be a checksum, and a
        // checksum can only be checked after applying.
        assert_eq!(
            c.timing == ValidationTiming::AfterApply,
            matches!(c.validation, ValidationScheme::Checksum { .. }),
            "{id}: validation timing and scheme disagree"
        );

        // A range scheme needs a snapshot to anchor to, and the only anchor is
        // a REST book carrying a last update id.
        if matches!(c.validation, ValidationScheme::UpdateIdRange) {
            assert_eq!(
                c.snapshot_source,
                SnapshotSource::Rest,
                "{id}: an update-id range has nothing to anchor to in band"
            );
        }

        // A venue with no usable snapshot must publish order-by-order events,
        // because an aggregated feed with no starting point is unusable.
        if c.snapshot_source == SnapshotSource::None {
            assert_eq!(
                c.book_level,
                tickvault::types::BookLevel::L3,
                "{id} has no snapshot to start from and is not order by order"
            );
        }

        // A venue that cannot detect loss must say so out loud.
        if !c.can_detect_loss() {
            assert!(
                !c.detection_limits.is_empty(),
                "{id} cannot detect loss and declares no blind spot"
            );
        }

        // Every venue must declare at least one thing it cannot see. A venue
        // with no blind spots has not been looked at hard enough.
        assert!(
            !c.detection_limits.is_empty(),
            "{id} declares no detection limits at all"
        );
        for limit in &c.detection_limits {
            assert_eq!(limit.venue, *id, "{id} declares a limit for another venue");
            assert!(!limit.scope.is_empty() && !limit.consequence.is_empty());
        }

        // A keepalive we never send is worse than none at all.
        if let Keepalive::Text { payload, every } = c.keepalive {
            assert!(!payload.is_empty(), "{id} keepalive payload is empty");
            assert!(
                every.as_secs() >= 1 && every.as_secs() < 60,
                "{id} keepalive interval {every:?} is not a plausible heartbeat"
            );
        }

        assert!(c.budget.capacity() > 0, "{id} can carry no symbols");
        assert!(
            !c.budget.connect_interval.is_zero() && !c.budget.rest_interval.is_zero(),
            "{id} has no rate limit at all"
        );
        assert!(
            !c.requires_auth,
            "{id} needs a key; this project is keyless"
        );

        if let FeedDepth::Limited(n) = c.feed_depth {
            assert!(n > 0, "{id} declares a zero-deep feed");
        }
    }
}

#[test]
fn each_venue_builds_the_validator_its_scheme_names() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        let state = f.caps().new_seq_state();
        let matches = match f.caps().validation {
            ValidationScheme::Counter { .. } => matches!(state, SeqState::Counter(_)),
            ValidationScheme::Checksum { .. } => matches!(state, SeqState::Checksum(_)),
            ValidationScheme::Chained => matches!(state, SeqState::Chain(_)),
            ValidationScheme::UpdateIdRange => matches!(state, SeqState::Range(_)),
            ValidationScheme::MonotonicTimestamp => matches!(state, SeqState::Timestamp(_)),
            ValidationScheme::None => matches!(state, SeqState::Unverifiable),
        };
        assert!(matches, "{id} builds a validator its scheme does not name");
    }
}

#[test]
fn deltas_carry_whatever_their_scheme_needs_to_validate_them() {
    // The three identifier facts are orthogonal, and a venue that declares a
    // scheme without populating its field would validate nothing while looking
    // like it validated everything.
    for id in VenueId::ALL {
        let f = fixture(*id);
        let deltas: Vec<_> = f
            .tape
            .frames
            .iter()
            .flat_map(|r| f.venue.parse_delta(&r.to_raw()).unwrap())
            .filter_map(|e| match e {
                FeedEvent::Delta(d) => Some(d),
                _ => None,
            })
            .collect();
        assert!(!deltas.is_empty());
        match f.caps().validation {
            ValidationScheme::Counter { .. } => assert!(
                deltas.iter().all(|d| d.seq.is_some()),
                "{id} is a counter venue with unnumbered deltas"
            ),
            ValidationScheme::Checksum { .. } => assert!(
                deltas.iter().all(|d| d.checksum.is_some()),
                "{id} is a checksum venue with unchecksummed deltas"
            ),
            ValidationScheme::Chained => assert!(
                deltas
                    .iter()
                    .all(|d| d.seq.is_some() && d.prev_seq.is_some()),
                "{id} is a chained venue whose deltas name no predecessor"
            ),
            ValidationScheme::UpdateIdRange => assert!(
                deltas
                    .iter()
                    .all(|d| d.first_seq.is_some() && d.seq.is_some()),
                "{id} is a range venue whose deltas carry no range"
            ),
            ValidationScheme::MonotonicTimestamp => assert!(
                deltas.iter().all(|d| d.stamps.venue_nanos.is_some()),
                "{id} has only timestamps to go on and its deltas carry none"
            ),
            ValidationScheme::None => {}
        }
    }
}

#[test]
fn a_venue_sending_bare_json_numbers_is_declared_as_such() {
    // The encoding decides whether a value survives parsing exactly, which for
    // a checksum venue is the difference between working and never matching.
    for id in VenueId::ALL {
        let f = fixture(*id);
        let sample = f
            .tape
            .frames
            .iter()
            .find(|r| r.payload.contains("price") || r.payload.contains("\"b\""))
            .map(|r| r.payload.clone())
            .unwrap_or_default();
        if f.caps().decimals == DecimalEncoding::JsonNumbers && !sample.is_empty() {
            assert!(
                sample.contains("\"price\":7") || sample.contains("\"qty\":0"),
                "{id} claims bare numbers but its capture looks quoted"
            );
        }
    }
}

// ---------------------------------------------------------- replay --

#[test]
fn every_venue_replays_its_own_capture_without_a_phantom_gap() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        let r = f.replay();
        let report = r.session.report();
        assert!(
            r.parse_errors.is_empty(),
            "{id}: unparsed frames {:?}",
            r.parse_errors
        );
        assert_eq!(
            report.total_gaps(),
            0,
            "{id} invented a gap replaying its own clean capture\n{report}"
        );
        assert!(
            r.silently_crossed.is_empty(),
            "{id} crossed its book without flagging it at frames {:?}",
            r.silently_crossed
        );
    }
}

#[test]
fn a_venue_that_cannot_detect_loss_never_reports_a_message_as_verified() {
    for id in VenueId::ALL {
        let f = fixture(*id);
        let r = f.replay();
        let stats = r
            .session
            .gaps()
            .stats(*id, &f.symbol)
            .unwrap_or_else(|| panic!("{id} produced no stats"));
        if !f.caps().can_detect_loss() {
            assert_eq!(
                stats.verified_fraction(),
                Some(0.0),
                "{id} cannot detect loss yet reports messages as verified\n{}",
                r.session.report()
            );
        }
    }
}

// -------------------------------------------------- the drop gate, per venue --

#[test]
fn every_venue_that_claims_to_detect_loss_actually_does() {
    // Phase 1's gate, generalised. For each venue that says it can detect loss,
    // remove one book message at a time and require that any removal which
    // alters the book is noticed. Removals that change nothing are allowed to
    // pass unremarked, because the archive still holds the right book.
    for id in VenueId::ALL {
        let f = fixture(*id);
        if !f.caps().can_detect_loss() {
            continue;
        }
        let reference = f.replay();
        let victims = f.delta_indices();
        assert!(
            !victims.is_empty(),
            "{id}: no removable book message in the capture"
        );

        let (mut caught, mut harmless) = (0, 0);
        for victim in &victims {
            let dropped = f.replay_tape(&f.tape.without(&[*victim]));
            match (*victim..dropped.digests.len())
                .find(|&j| dropped.digests[j] != reference.digests[j + 1])
            {
                Some(j) => {
                    assert!(
                        dropped.detected_by[j],
                        "{id}: removing frame {victim} left the book wrong from step {j} and \
                         nothing was flagged\n{}",
                        dropped.session.report()
                    );
                    caught += 1;
                }
                None => harmless += 1,
            }
        }
        println!("{id}: {caught} detected, {harmless} left the book unchanged");
        assert!(
            caught > 0,
            "{id} claims to detect loss but nothing in the sweep was detectable"
        );
    }
}

#[test]
fn a_venue_that_cannot_detect_loss_is_honest_about_a_removed_message() {
    // The other half. Bitstamp cannot notice a dropped message, and the correct
    // behaviour is not to pretend otherwise: it must not report the run as
    // verified, and it must have said in advance that this would happen.
    for id in VenueId::ALL {
        let f = fixture(*id);
        if f.caps().can_detect_loss() {
            continue;
        }
        let victims = f.delta_indices();
        assert!(!victims.is_empty());
        let dropped = f.replay_tape(&f.tape.without(&[victims[0]]));
        let report = dropped.session.report();
        assert!(
            !report.is_spotless(),
            "{id} lost a message and called the result spotless\n{report}"
        );
        assert!(
            f.caps()
                .detection_limits
                .iter()
                .any(|l| l.consequence.contains("undetectable")
                    || l.consequence.contains("no sequence")),
            "{id} cannot detect loss and never said so"
        );
    }
}

// -------------------------------------------------------------- budgeting --

#[test]
fn every_venue_plans_a_subscription_layout_that_matches_its_scope() {
    let symbols: Vec<tickvault::types::Symbol> = (0..5)
        .map(|i| tickvault::types::Symbol::new(&format!("A{i}"), "USD"))
        .collect();
    for id in VenueId::ALL {
        let f = fixture(*id);
        let caps = f.caps();
        let plan = caps
            .plan_subscriptions(&symbols)
            .unwrap_or_else(|e| panic!("{id} cannot plan five symbols: {e}"));
        assert_eq!(plan.symbol_count(), 5, "{id} dropped symbols from the plan");
        assert!(
            plan.connections
                .iter()
                .all(|c| c.len() <= caps.budget.subscriptions_per_connection),
            "{id} overfilled a socket"
        );
        assert!(plan.connection_count() <= caps.budget.max_connections);

        // A venue whose counter belongs to the socket must spread, because
        // there one gap costs every symbol sharing it.
        if caps.scope == SequenceScope::PerConnection {
            assert_eq!(
                plan.worst_case_blast_radius(true),
                1,
                "{id} numbers the connection but packed symbols onto one socket"
            );
        }
    }
}
