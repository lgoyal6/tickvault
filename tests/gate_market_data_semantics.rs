//! **Market-data semantics gate.**
//!
//! Four properties that a book archive is worthless without, and that all fail
//! the same way when they fail: quietly, producing a number that looks like a
//! price and is not one.
//!
//! 1. **A quote currency is part of the instrument.** `BTC-USD` and `BTC-USDT`
//!    trade tens of dollars apart. Anything that lets one become the other, or
//!    lets rows of one be replayed as the other, produces a book that is wrong
//!    without being empty, which is the worst kind of wrong.
//! 2. **Venue order and arrival order are different questions.** The venue's
//!    own identifier says what the venue emitted; the receipt clock says what
//!    reached us. An archive that keeps only one of them cannot answer the
//!    other, and an archive that conflates them answers both wrongly.
//! 3. **A reconnect ends an epoch.** Sequence numbers are only comparable
//!    inside one connection. Carrying a validator across a reconnect makes a
//!    stale number look continuous, which is a lost message reported as clean.
//! 4. **Prices are exact.** A venue quotes decimals; we store integers at a
//!    declared scale. A value we cannot hold exactly is refused, never rounded.
//!
//! Every case here asserts the *visible* failure. Rejecting loudly is the
//! property; silently normalising is the bug.

mod common;

use std::path::Path;

use tickvault::book::{BookDelta, BookSnapshot, L2Book, LevelChange};
use tickvault::clock::{ManualClock, Stamp, Timestamps};
use tickvault::fixed::{Fixed, ParseFixedError, SCALE};
use tickvault::reconstruct::{Reconstructor, Request};
use tickvault::sequence::{CounterState, SeqVerdict, TimestampState};
use tickvault::store::manifest::{FileRecord, Manifest};
use tickvault::store::reader::ArchiveReader;
use tickvault::store::schema::{RowBuilder, book_schema};
use tickvault::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};
use tickvault::symbols::{SymbolError, mappers};
use tickvault::types::{BookLevel, Side, Symbol, VenueId};

const EPOCH: i64 = 1_787_000_000_000_000_000;
const TICK: i64 = 1_000_000;

fn f(s: &str) -> Fixed {
    Fixed::from_decimal_str(s).expect("decimal literal")
}

fn stamps(wall: i64) -> Timestamps {
    Timestamps::new(
        Stamp {
            mono_nanos: (wall - EPOCH) as u64,
            wall_nanos: wall,
        },
        Some(wall - 1_000),
    )
}

/// Write one partition of `messages` one-level deltas at `price`.
///
/// Deliberately takes the symbol and the venue separately from the partition
/// key it writes under, so a test can produce the mislabelling case on purpose.
fn write_partition(
    root: &Path,
    row_venue: VenueId,
    row_symbol: &Symbol,
    key_venue: VenueId,
    key_symbol: &Symbol,
    price: &str,
    messages: usize,
) -> FileRecord {
    let mut writer = ArchiveWriter::open(WriterConfig {
        max_rows_per_file: 1_000_000,
        max_file_age: std::time::Duration::from_secs(86_400),
        ..WriterConfig::new(root)
    })
    .expect("open archive");

    let key = PartitionKey::new(key_venue, key_symbol, EPOCH);
    let mut builder = RowBuilder::new();
    for i in 0..messages {
        let wall = EPOCH + i as i64 * TICK;
        builder.push_snapshot(
            row_venue,
            &BookSnapshot {
                symbol: row_symbol.clone(),
                bids: vec![(f(price), f("1"))],
                asks: vec![(f(price).checked_add(f("1")).unwrap(), f("1"))],
                seq: Some(i as u64 + 1),
                checksum: None,
                stamps: stamps(wall),
            },
            i as u64,
            false,
        );
    }
    let span = builder.wall_span().expect("rows");
    let batch = builder.finish().expect("batch");
    writer.write(&key, &batch, span).expect("write");
    writer.close_partition(&key).expect("close");
    writer
        .manifest()
        .files()
        .last()
        .cloned()
        .expect("one record")
}

// ---------------------------------------------------------------------------
// 1. A quote currency is part of the instrument.
// ---------------------------------------------------------------------------

/// The canonical form keeps dollars and Tether apart, in both directions, on
/// every venue that lists both.
#[test]
fn dollars_and_tether_never_collapse_into_one_instrument() {
    let usd = Symbol::new("BTC", "USD");
    let usdt = Symbol::new("BTC", "USDT");
    assert_ne!(usd, usdt);
    assert_eq!(usd.quote(), "USD");
    assert_eq!(usdt.quote(), "USDT");

    // Forward: each venue spells them differently, and never identically.
    for mut mapper in [
        mappers::coinbase(),
        mappers::kraken(),
        mappers::okx(),
        mappers::bybit(),
        mappers::binance_us(),
        mappers::bitstamp(),
    ] {
        let a = mapper.to_venue(&usd);
        let b = mapper.to_venue(&usdt);
        assert_ne!(a, b, "a venue spelling must not merge USD and USDT");

        // Reverse: the concatenated venues are where the split is a guess, so
        // check the guess lands on the right quote asset.
        mapper.register_all(&[usd.clone(), usdt.clone()]);
        assert_eq!(mapper.to_canonical(a.as_str()).unwrap(), usd);
        assert_eq!(mapper.to_canonical(b.as_str()).unwrap(), usdt);
    }
}

/// A concatenated spelling that splits two ways is refused, not guessed at.
#[test]
fn an_ambiguous_venue_spelling_is_refused_rather_than_guessed() {
    let mapper = mappers::bybit();
    // XBTUSD is XBT+USD, and equally XB+TUSD once TrueUSD is a known quote.
    match mapper.to_canonical("XBTUSD") {
        Err(SymbolError::Ambiguous { candidates, .. }) => {
            assert!(candidates.len() >= 2, "{candidates:?}");
        }
        other => panic!("an ambiguous spelling must not resolve silently: {other:?}"),
    }
    // And a spelling whose quote asset we do not know at all.
    assert!(matches!(
        mapper.to_canonical("BTCZZZ"),
        Err(SymbolError::Unsplittable { .. })
    ));
    // A canonical parse of a concatenated string is not attempted at all.
    assert!(Symbol::parse("BTCUSD").is_err());
}

/// The archive keeps the two instruments in separate partitions, and a rebuild
/// of one never sees the other's prices.
#[test]
fn the_archive_partitions_on_the_quote_currency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let usd = Symbol::new("BTC", "USD");
    let usdt = Symbol::new("BTC", "USDT");

    write_partition(root, VenueId::Okx, &usd, VenueId::Okx, &usd, "60000", 4);
    write_partition(root, VenueId::Okx, &usdt, VenueId::Okx, &usdt, "60123", 4);

    let reader = ArchiveReader::open(root).expect("open");
    let paths: Vec<&str> = reader.files().iter().map(|f| f.path.as_str()).collect();
    assert!(paths.iter().any(|p| p.contains("symbol=BTC-USD/")));
    assert!(paths.iter().any(|p| p.contains("symbol=BTC-USDT/")));

    let rc = Reconstructor::open(root).expect("open");
    let a = rc
        .at(&Request::new(VenueId::Okx, &usd, EPOCH + 3 * TICK))
        .expect("rebuild usd");
    let b = rc
        .at(&Request::new(VenueId::Okx, &usdt, EPOCH + 3 * TICK))
        .expect("rebuild usdt");
    assert_eq!(a.book.best_bid().unwrap().0, f("60000"));
    assert_eq!(b.book.best_bid().unwrap().0, f("60123"));
}

/// A file whose rows are a different instrument than the manifest claims is
/// caught by verification rather than replayed into the wrong book.
///
/// This is the case the partitioning alone cannot cover. The manifest is what
/// selects files for a rebuild, so a record that names the wrong instrument
/// hands a `BTC-USDT` tape to a `BTC-USD` rebuild, and the replayer stamps its
/// own symbol on every row it applies. Nothing downstream can notice.
#[test]
fn a_file_whose_rows_are_a_different_instrument_is_caught() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let usd = Symbol::new("BTC", "USD");
    let usdt = Symbol::new("BTC", "USDT");

    // Real Tether rows, written under the Tether partition.
    let record = write_partition(root, VenueId::Okx, &usdt, VenueId::Okx, &usdt, "60123", 4);
    let record_path = record.path.clone();

    // A second manifest entry for the same file, claiming it is the dollar
    // pair. However it arises -- a hand-edited manifest, a repair tool, a
    // partition key built from the wrong variable -- this is what it looks
    // like on disk.
    let mut manifest = Manifest::open(root).expect("manifest");
    manifest
        .record_file(FileRecord {
            symbol: usd.clone(),
            ..record
        })
        .expect("append");

    let report = ArchiveReader::open(root).expect("open").verify();
    assert!(
        !report.is_clean(),
        "a mislabelled file must not verify clean: {report}"
    );
    assert_eq!(
        report.mislabelled.len(),
        1,
        "verification must name the mislabelled file: {report}"
    );
    let (path, claimed, found) = &report.mislabelled[0];
    assert_eq!(path, &record_path);
    assert!(claimed.ends_with("BTC-USD"), "{claimed}");
    assert!(found.ends_with("BTC-USDT"), "{found}");
}

/// The same check on the venue, which partitions the archive for the same
/// reason and is mixed up the same way.
#[test]
fn a_file_recorded_from_a_different_venue_is_caught() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let usd = Symbol::new("BTC", "USD");

    let record = write_partition(
        root,
        VenueId::Bitstamp,
        &usd,
        VenueId::Bitstamp,
        &usd,
        "60000",
        4,
    );
    let mut manifest = Manifest::open(root).expect("manifest");
    manifest
        .record_file(FileRecord {
            venue: VenueId::Coinbase,
            ..record
        })
        .expect("append");

    let report = ArchiveReader::open(root).expect("open").verify();
    assert_eq!(
        report.mislabelled.len(),
        1,
        "verification must name the file recorded from another venue: {report}"
    );
    let (_, claimed, found) = &report.mislabelled[0];
    assert!(claimed.starts_with("coinbase"), "{claimed}");
    assert!(found.starts_with("bitstamp"), "{found}");
}

// ---------------------------------------------------------------------------
// 2. Venue order and arrival order are different questions.
// ---------------------------------------------------------------------------

/// A message that arrives late still carries the venue's own numbering, and the
/// archive keeps arrival order rather than sorting by it.
#[test]
fn the_archive_keeps_arrival_order_and_venue_order_separately() {
    let mut builder = RowBuilder::new();
    // Arrival order 1, 2, 3. Venue timestamps 1, 3, 2: the third message was
    // emitted before the second and overtook it on the wire.
    let venue_ts = [EPOCH + TICK, EPOCH + 3 * TICK, EPOCH + 2 * TICK];
    for (i, vts) in venue_ts.iter().enumerate() {
        builder.push_delta(
            VenueId::Coinbase,
            &BookDelta {
                symbol: Symbol::new("BTC", "USD"),
                changes: vec![LevelChange::new(Side::Bid, f("60000"), f("1"))],
                seq: Some(i as u64 + 1),
                checksum: None,
                stamps: Timestamps::new(
                    Stamp {
                        mono_nanos: i as u64 * TICK as u64,
                        wall_nanos: EPOCH + i as i64 * TICK,
                    },
                    Some(*vts),
                ),
                prev_seq: None,
                first_seq: None,
            },
            i as u64,
            false,
        );
    }
    let batch = builder.finish().expect("batch");

    let col = |name: &str| batch.column_by_name(name).expect(name).clone();
    let recv = col("recv_wall");
    let recv = recv
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .expect("recv_wall is a timestamp");
    let vts = col("venue_ts");
    let vts = vts
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .expect("venue_ts is a timestamp");
    let skew = col("skew_ns");
    let skew = skew
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("skew_ns is an integer");

    // Arrival order is the row order and is monotonic.
    let arrival: Vec<i64> = (0..recv.len()).map(|i| recv.value(i)).collect();
    assert!(arrival.windows(2).all(|w| w[0] < w[1]), "{arrival:?}");
    // Venue order is not, and was not quietly repaired to match.
    let emitted: Vec<i64> = (0..vts.len()).map(|i| vts.value(i)).collect();
    assert_eq!(emitted, venue_ts.to_vec());
    assert!(emitted[1] > emitted[2], "the overtaking must survive");
    // The disagreement is a column, not a correction: the last message is
    // stamped as having arrived *before* the venue says it was emitted, and
    // the negative skew is kept rather than clamped to zero.
    assert!(
        skew.value(1) < 0,
        "a message stamped ahead of its own arrival keeps a negative skew: {}",
        skew.value(1)
    );
}

/// The two loss detectors answer different questions and neither is allowed to
/// answer the other's.
#[test]
fn a_counter_gap_and_an_out_of_order_arrival_are_different_verdicts() {
    // A counter feed sees a missing message and can say exactly how many.
    let mut counter = CounterState::new(1, 0);
    counter.anchor(1);
    assert_eq!(counter.observe(2), SeqVerdict::InOrder);
    match counter.observe(5) {
        SeqVerdict::Gap(evidence) => {
            assert_eq!(evidence.size.messages(), 2, "{evidence:?}");
        }
        other => panic!("a counter must report a gap with a size: {other:?}"),
    }

    // A timestamp-only feed cannot see a gap at all. The most it can report is
    // that the order was wrong, which is not the same claim.
    // A timestamp-only feed does not even claim `InOrder` for a well-ordered
    // message: the most it knows is that it saw nothing wrong, which is not
    // the same as having checked.
    let mut ts = TimestampState::new();
    for stamp in [EPOCH + TICK, EPOCH + 5 * TICK] {
        assert!(
            matches!(ts.observe(stamp), SeqVerdict::Unverifiable(_)),
            "a feed with no sequence must never report a message as verified"
        );
    }
    match ts.observe(EPOCH + 3 * TICK) {
        SeqVerdict::OutOfOrder { previous, observed } => {
            assert_eq!(previous, EPOCH + 5 * TICK);
            assert_eq!(observed, EPOCH + 3 * TICK);
        }
        other => panic!("a timestamp feed must not claim a gap it cannot see: {other:?}"),
    }
    // Both mean the book is no longer trustworthy; only one is evidence of how
    // much went missing.
    assert!(
        SeqVerdict::OutOfOrder {
            previous: 2,
            observed: 1
        }
        .requires_resnapshot()
    );
}

// ---------------------------------------------------------------------------
// 3. A reconnect ends an epoch.
// ---------------------------------------------------------------------------

/// A validator does not survive the connection it was validating.
///
/// Coinbase's `sequence_num` is scoped to one socket. Comparing a number from
/// the new connection against one from the old is comparing two different
/// counters, and whichever way it comes out the answer is meaningless: it
/// either invents a gap that did not happen or -- the case that matters --
/// declares a stale number continuous and reports the outage as clean.
#[test]
fn a_reconnect_forgets_the_previous_connections_numbering() {
    let tape = common::coinbase_tape(40, 11);
    let mut session =
        tickvault::session::BookSession::new(common::coinbase(), vec![common::btc_usd()]);

    // First connection: the whole tape, cleanly.
    for recorded in &tape.frames {
        session.ingest(&recorded.to_raw()).expect("parse");
    }
    assert!(session.is_ready(&common::btc_usd()));
    assert_eq!(session.report().total_gaps(), 0, "the tape is clean");

    // The socket drops, and the same tape arrives again on a new connection
    // with its numbering restarted from zero. Against the old connection's
    // counter that is a reset or a mountain of duplicates; against a forgotten
    // one it is simply a fresh feed.
    session.on_disconnect(Stamp {
        mono_nanos: 10_000_000_000,
        wall_nanos: EPOCH,
    });
    assert!(
        !session.is_ready(&common::btc_usd()),
        "a dropped socket must invalidate the book until a snapshot arrives"
    );
    for recorded in &tape.frames {
        session.ingest(&recorded.to_raw()).expect("parse");
    }

    let report = session.report();
    assert_eq!(
        report.total_gaps(),
        0,
        "a restarted counter on a new connection is not a gap:\n{report}"
    );
    assert_eq!(
        report.total_missing(),
        0,
        "and nothing may be claimed missing:\n{report}"
    );
}

/// The outage itself is still recorded, so the epoch boundary is visible in the
/// data rather than being papered over by the re-anchoring above.
///
/// This is the half that stops "forget the old numbering" from becoming "forget
/// that anything happened". Whatever the venue did to its counters while the
/// socket was down, the recorder cannot vouch for that window and says so.
#[test]
fn the_gap_between_two_connections_is_recorded_as_downtime() {
    let tape = common::coinbase_tape(20, 3);
    let mut session =
        tickvault::session::BookSession::new(common::coinbase(), vec![common::btc_usd()]);
    for recorded in &tape.frames {
        session.ingest(&recorded.to_raw()).expect("parse");
    }

    let before = session
        .gaps()
        .stats(VenueId::Coinbase, &common::btc_usd())
        .expect("stats")
        .disconnects;
    session.on_disconnect(Stamp {
        mono_nanos: 10_000_000_000,
        wall_nanos: EPOCH,
    });
    let after = session
        .gaps()
        .stats(VenueId::Coinbase, &common::btc_usd())
        .expect("stats")
        .disconnects;
    assert_eq!(after, before + 1, "a disconnect must be counted");

    let report = session.report();
    assert!(
        !report.is_spotless(),
        "a recording with an outage in it must never read as spotless:\n{report}"
    );
}

/// `recv_mono` is scoped to one recorder process and the archive does not say
/// which one, so it cannot order rows across a restart.
///
/// This is a real limit of the published schema, asserted rather than written
/// down: the column's own documentation says to order by it, and that is only
/// true inside one run. `recv_wall` is what survives, and the replayer already
/// refuses to read `recv_mono` for exactly this reason. A test rather than a
/// note, because a limit that lives only in prose stops being true the first
/// time someone changes a default.
#[test]
fn the_monotonic_clock_does_not_survive_a_recorder_restart() {
    // Two runs of the same recorder, an hour apart on the wall clock, each
    // starting its monotonic clock from zero.
    let mut builder = RowBuilder::new();
    for (run, wall_base) in [(0u64, EPOCH), (1, EPOCH + 3_600_000_000_000)] {
        for i in 0..3u64 {
            builder.push_delta(
                VenueId::Coinbase,
                &BookDelta {
                    symbol: Symbol::new("BTC", "USD"),
                    changes: vec![LevelChange::new(Side::Bid, f("60000"), f("1"))],
                    seq: Some(i + 1),
                    checksum: None,
                    stamps: Timestamps::new(
                        Stamp {
                            // Each run restarts its monotonic clock at zero.
                            mono_nanos: i * TICK as u64,
                            wall_nanos: wall_base + i as i64 * TICK,
                        },
                        None,
                    ),
                    prev_seq: None,
                    first_seq: None,
                },
                run * 3 + i,
                false,
            );
        }
    }
    let batch = builder.finish().expect("batch");
    let mono = batch.column_by_name("recv_mono").expect("recv_mono");
    let mono = mono
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("recv_mono is an unsigned integer");
    let values: Vec<u64> = (0..mono.len()).map(|i| mono.value(i)).collect();
    assert!(
        !values.windows(2).all(|w| w[0] <= w[1]),
        "recv_mono is documented as the ordering column but restarts per run: {values:?}"
    );

    // The wall clock does survive, which is why it is what a rebuild uses.
    let rows = tickvault::store::rows::decode(&batch).expect("decode");
    let walls: Vec<i64> = rows.iter().map(|r| r.recv_wall).collect();
    assert!(walls.windows(2).all(|w| w[0] <= w[1]), "{walls:?}");

    // And the venue's own numbering restarts with the run, so it is not a
    // global order either: nothing in the archive distinguishes run 0's
    // message 1 from run 1's.
    let seq = batch.column_by_name("seq").expect("seq");
    let seq = seq
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("seq is an unsigned integer");
    let seqs: Vec<u64> = (0..seq.len()).map(|i| seq.value(i)).collect();
    assert_eq!(seqs, vec![1, 2, 3, 1, 2, 3]);
    assert!(
        !seqs.windows(2).all(|w| w[0] < w[1]),
        "sequence numbers are only monotonic inside one connection"
    );
}

// ---------------------------------------------------------------------------
// 4. Prices are exact.
// ---------------------------------------------------------------------------

/// A decimal we cannot hold exactly is refused, not rounded.
#[test]
fn an_over_precise_quote_is_refused_rather_than_rounded() {
    assert_eq!(SCALE, 9);
    // Nine decimals is the boundary and is exact.
    assert_eq!(f("0.123456789").mantissa(), 123_456_789);
    // Ten is not, and rounding it would discard what the venue sent.
    match Fixed::from_decimal_str("0.1234567891") {
        Err(ParseFixedError::TooPrecise { decimals, .. }) => assert_eq!(decimals, 10),
        other => panic!("an over-precise decimal must be refused: {other:?}"),
    }
    // Including via scientific notation, which is how a small size arrives.
    assert!(matches!(
        Fixed::from_decimal_str("1.234567891e-1"),
        Err(ParseFixedError::TooPrecise { .. })
    ));
    // A magnitude we cannot hold is refused too, rather than saturating.
    assert!(matches!(
        Fixed::from_decimal_str("99999999999.0"),
        Err(ParseFixedError::OutOfRange(_))
    ));
}

/// Prices survive the archive as the exact integers they went in as, and the
/// file says what scale they are on.
#[test]
fn the_archive_stores_prices_as_exact_integers_at_a_declared_scale() {
    use arrow::datatypes::DataType;

    let schema = book_schema();
    for name in ["price", "qty"] {
        let field = schema.field_with_name(name).expect(name);
        assert_eq!(
            field.data_type(),
            &DataType::Int64,
            "{name} must never be a float"
        );
        assert_eq!(field.metadata().get("scale"), Some(&SCALE.to_string()));
    }
    assert_eq!(
        schema.metadata().get("tickvault.price_scale"),
        Some(&SCALE.to_string())
    );

    // A price whose last digit only exists at full scale round-trips intact.
    let exact = f("60000.123456789");
    let mut builder = RowBuilder::new();
    builder.push_delta(
        VenueId::Kraken,
        &BookDelta {
            symbol: Symbol::new("BTC", "USD"),
            changes: vec![LevelChange::new(Side::Bid, exact, f("0.000000001"))],
            seq: None,
            checksum: None,
            stamps: stamps(EPOCH),
            prev_seq: None,
            first_seq: None,
        },
        0,
        false,
    );
    let batch = builder.finish().expect("batch");
    let rows = tickvault::store::rows::decode(&batch).expect("decode");
    assert_eq!(rows[0].price, exact);
    assert_eq!(rows[0].price.mantissa(), 60_000_123_456_789);
    assert_eq!(rows[0].qty.mantissa(), 1);
}

/// A book keyed by exact price never merges two distinct levels, which is what
/// a float key would do at the far end of the scale.
#[test]
fn two_prices_a_single_unit_apart_stay_two_levels() {
    let mut book = L2Book::new(Symbol::new("BTC", "USD"));
    let a = Fixed::from_mantissa(60_000_123_456_789);
    let b = Fixed::from_mantissa(60_000_123_456_790);
    book.apply_delta(&BookDelta {
        symbol: Symbol::new("BTC", "USD"),
        changes: vec![
            LevelChange::new(Side::Bid, a, f("1")),
            LevelChange::new(Side::Bid, b, f("2")),
        ],
        seq: None,
        checksum: None,
        stamps: stamps(EPOCH),
        prev_seq: None,
        first_seq: None,
    });
    assert_eq!(book.level_count(Side::Bid), 2);
    assert_eq!(book.best_bid().unwrap(), (b, f("2")));
}

/// Two prices `f64` cannot tell apart, at a magnitude a venue in the matrix
/// actually quotes.
///
/// `f64` holds integers exactly only up to 2^53, which at a scale of 1e-9 is a
/// price of about 9,007,199. Kraken lists BTC/JPY, which trades an order of
/// magnitude above that, so this is the live case rather than a contrived one:
/// past that price a float book silently merges adjacent levels.
#[test]
fn a_float_cannot_hold_the_prices_a_yen_book_quotes() {
    let a = Fixed::from_mantissa(9_007_199_254_740_992);
    let b = Fixed::from_mantissa(9_007_199_254_740_993);
    assert_ne!(a, b, "exactly, they are different prices");
    assert_eq!(
        a.to_f64_lossy(),
        b.to_f64_lossy(),
        "as floats they are the same number, which is why nothing here uses one"
    );

    // And the book keeps them apart, which a float-keyed book could not.
    let mut book = L2Book::new(Symbol::new("BTC", "JPY"));
    book.apply_delta(&BookDelta {
        symbol: Symbol::new("BTC", "JPY"),
        changes: vec![
            LevelChange::new(Side::Bid, a, f("1")),
            LevelChange::new(Side::Bid, b, f("2")),
        ],
        seq: None,
        checksum: None,
        stamps: stamps(EPOCH),
        prev_seq: None,
        first_seq: None,
    });
    assert_eq!(book.level_count(Side::Bid), 2);
}

// Silence the unused-import warning from the shared harness, which this gate
// only borrows the tempfile dependency from.
#[allow(dead_code)]
fn _uses_common() {
    let _ = common::btc_usd();
    let _: Option<ManualClock> = None;
    let _: Option<BookLevel> = None;
}
