//! Harness for the phase 1 gate.
//!
//! Two kinds of tape feed the gate. Fixtures captured verbatim from the live
//! feeds prove the parsers and the Kraken checksum agree with reality. Synthetic
//! tapes, built here in the same wire shapes, give the volume and the precise
//! control a drop experiment needs: to assert that *every* lost message is
//! detected you have to know exactly which message was lost.

#![allow(dead_code)]

use std::sync::Arc;

use tickvault::book::checksum::kraken_checksum;
use tickvault::book::{BookSnapshot, L2Book, LevelChange};
use tickvault::clock::{ManualClock, Stamp, Timestamps};
use tickvault::fixed::Fixed;
use tickvault::recorder::{RawTape, RecordedFrame};
use tickvault::session::BookSession;
use tickvault::transport::CannedFetch;
use tickvault::types::{Side, Symbol, VenueId};
use tickvault::venue::Venue;
use tickvault::venue::coinbase::Coinbase;
use tickvault::venue::kraken::{Kraken, Precision};

/// Kraken's own precision for BTC/USD, read from its AssetPairs endpoint:
/// `pair_decimals: 1, lot_decimals: 8`.
pub const KRAKEN_BTC_USD: Precision = Precision { price: 1, qty: 8 };

pub fn btc_usd() -> Symbol {
    Symbol::new("BTC", "USD")
}

pub fn f(s: &str) -> Fixed {
    Fixed::from_decimal_str(s).expect("decimal literal")
}

pub fn coinbase() -> Arc<dyn Venue> {
    Arc::new(Coinbase::new(
        CannedFetch::new().shared(),
        Arc::new(ManualClock::default()),
    ))
}

pub fn kraken(depth: usize) -> Arc<dyn Venue> {
    Arc::new(
        Kraken::new(
            CannedFetch::new().shared(),
            Arc::new(ManualClock::default()),
            depth,
        )
        .expect("valid depth")
        .with_precision(btc_usd(), KRAKEN_BTC_USD),
    )
}

/// A tiny deterministic generator. Seeded so a failing case can be reproduced
/// from the seed printed in the assertion message.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15).max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    pub fn chance(&mut self, one_in: u64) -> bool {
        one_in > 0 && self.next_u64().is_multiple_of(one_in)
    }
}

fn frame(venue: VenueId, index: usize, payload: String) -> RecordedFrame {
    RecordedFrame {
        venue,
        t_mono: index as u64 * 1_000_000,
        t_wall: 1_700_000_000_000_000_000 + index as i64 * 1_000_000,
        payload,
    }
}

fn iso(index: usize) -> String {
    // A valid RFC 3339 instant that advances by a millisecond per frame.
    let millis = index % 1000;
    let secs = index / 1000 % 60;
    format!("2026-08-24T22:00:{secs:02}.{millis:03}Z")
}

// ---------------------------------------------------------------- Coinbase --

/// A Coinbase Advanced Trade tape: one `l2_data` snapshot followed by
/// `updates` numbered messages, in the exact wire shape captured from
/// `advanced-trade-ws.coinbase.com`.
pub fn coinbase_tape(updates: usize, seed: u64) -> RawTape {
    let mut rng = Rng::new(seed);
    let mut frames = Vec::with_capacity(updates + 2);

    frames.push(frame(
        VenueId::Coinbase,
        0,
        r#"{"channel":"subscriptions","timestamp":"2026-08-24T22:00:00.000Z","sequence_num":0,"events":[{"subscriptions":{"level2":["BTC-USD"]}}]}"#.to_string(),
    ));

    let mut levels: Vec<(i64, i64)> = Vec::new();
    let mut snapshot_updates = Vec::new();
    for i in 0..20 {
        // Prices in whole cents, so `cents()` can render them exactly.
        let bid = 5_000_000 - i * 10;
        let ask = 5_000_100 + i * 10;
        levels.push((bid, ask));
        snapshot_updates.push(format!(
            r#"{{"side":"bid","event_time":"{}","price_level":"{}","new_quantity":"{}"}}"#,
            iso(0),
            cents(bid),
            "1.00000000"
        ));
        snapshot_updates.push(format!(
            r#"{{"side":"offer","event_time":"{}","price_level":"{}","new_quantity":"{}"}}"#,
            iso(0),
            cents(ask),
            "1.00000000"
        ));
    }
    frames.push(frame(
        VenueId::Coinbase,
        1,
        format!(
            r#"{{"channel":"l2_data","timestamp":"{}","sequence_num":1,"events":[{{"type":"snapshot","product_id":"BTC-USD","updates":[{}]}}]}}"#,
            iso(0),
            snapshot_updates.join(",")
        ),
    ));

    for n in 0..updates {
        let idx = rng.below(levels.len());
        let (bid, ask) = levels[idx];
        let (side, price) = if rng.chance(2) {
            ("bid", bid)
        } else {
            ("offer", ask)
        };
        // A quantity that changes every time, so no update is a silent no-op.
        let qty = format!("{}.{:08}", 1 + (n % 4), (n * 7919) % 100_000_000);
        frames.push(frame(
            VenueId::Coinbase,
            n + 2,
            format!(
                r#"{{"channel":"l2_data","timestamp":"{}","sequence_num":{},"events":[{{"type":"update","product_id":"BTC-USD","updates":[{{"side":"{}","event_time":"{}","price_level":"{}","new_quantity":"{}"}}]}}]}}"#,
                iso(n + 1),
                n + 2,
                side,
                iso(n + 1),
                cents(price),
                qty
            ),
        ));
    }
    RawTape::new(frames)
}

fn cents(v: i64) -> String {
    format!("{}.{:02}", v / 100, v % 100)
}

// ------------------------------------------------------------------ Kraken --

/// A Kraken v2 tape whose checksums are genuine: each one is computed over the
/// book state that message produces, exactly as the venue would.
///
/// `deep_only` restricts updates to levels past the checksum window, which is
/// how the blind spot at depth greater than ten is demonstrated rather than
/// asserted.
pub fn kraken_tape(updates: usize, depth: usize, seed: u64, deep_only: bool) -> RawTape {
    kraken_tape_with_book(updates, depth, seed, deep_only).0
}

/// A Kraken tape that ends with a *correctly checksummed crossed book*.
///
/// This is the case the crossed-book invariant exists for, and it cannot be
/// produced by dropping messages: both venues catch a loss before the book can
/// cross. Here the venue itself sends a bid above the ask and a checksum that
/// genuinely matches that state, so sequence validation is satisfied and the
/// only thing standing between the archive and a nonsense book is the book's
/// own invariant.
pub fn kraken_tape_with_venue_bug(updates: usize, depth: usize, seed: u64) -> RawTape {
    let (mut tape, mut book) = kraken_tape_with_book(updates, depth, seed, false);
    let (best_ask, _) = book.best_ask().expect("seeded book has asks");
    // A bid one tick above the best ask.
    let price = best_ask.checked_add(f("0.1")).expect("in range");
    let qty = f("0.12345678");
    let mut outcome = Default::default();
    book.apply_change(LevelChange::new(Side::Bid, price, qty), &mut outcome);
    assert!(
        book.is_crossed(),
        "the bug tape must actually cross the book"
    );
    let checksum = kraken_checksum(&book, KRAKEN_BTC_USD.price, KRAKEN_BTC_USD.qty);

    let index = tape.len();
    tape.push(frame(
        VenueId::Kraken,
        index,
        format!(
            r#"{{"channel":"book","type":"update","data":[{{"symbol":"BTC/USD","bids":[{{"price":{},"qty":{}}}],"asks":[],"checksum":{},"timestamp":"{}"}}]}}"#,
            price.to_decimal_string(KRAKEN_BTC_USD.price),
            qty.to_decimal_string(KRAKEN_BTC_USD.qty),
            checksum.value,
            iso(index)
        ),
    ));
    tape
}

fn kraken_tape_with_book(
    updates: usize,
    depth: usize,
    seed: u64,
    deep_only: bool,
) -> (RawTape, L2Book) {
    let mut rng = Rng::new(seed);
    let mut frames = Vec::new();
    let mut book = L2Book::with_depth_limit(btc_usd(), depth);

    frames.push(frame(
        VenueId::Kraken,
        0,
        r#"{"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online","api_version":"v2","connection_id":1}]}"#.to_string(),
    ));

    let bid_price = |i: usize| f(&format!("{}.{}", 78_800 - i as i64 / 10, 9 - (i % 10)));
    let ask_price = |i: usize| f(&format!("{}.{}", 78_801 + i as i64 / 10, i % 10));
    let qty_of = |n: usize| f(&format!("0.{:08}", 1_000_000 + (n * 7919) % 80_000_000));

    let bids: Vec<(Fixed, Fixed)> = (0..depth).map(|i| (bid_price(i), qty_of(i))).collect();
    let asks: Vec<(Fixed, Fixed)> = (0..depth).map(|i| (ask_price(i), qty_of(i + 7))).collect();
    let snapshot = BookSnapshot {
        symbol: btc_usd(),
        bids: bids.clone(),
        asks: asks.clone(),
        seq: None,
        checksum: None,
        stamps: Timestamps::recv_only(Stamp::ZERO),
    };
    book.reset_from_snapshot(&snapshot);
    let snap_checksum = kraken_checksum(&book, KRAKEN_BTC_USD.price, KRAKEN_BTC_USD.qty);
    assert_eq!(
        snap_checksum.lossy_levels, 0,
        "synthetic tape must be renderable at Kraken precision"
    );

    let level_json = |levels: &[(Fixed, Fixed)]| {
        levels
            .iter()
            .map(|(p, q)| {
                format!(
                    r#"{{"price":{},"qty":{}}}"#,
                    p.to_decimal_string(KRAKEN_BTC_USD.price),
                    q.to_decimal_string(KRAKEN_BTC_USD.qty)
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    };

    frames.push(frame(
        VenueId::Kraken,
        1,
        format!(
            r#"{{"channel":"book","type":"snapshot","data":[{{"symbol":"BTC/USD","bids":[{}],"asks":[{}],"checksum":{},"timestamp":"{}"}}]}}"#,
            level_json(&bids),
            level_json(&asks),
            snap_checksum.value,
            iso(0)
        ),
    ));

    for n in 0..updates {
        // Where in the book to poke. Past the tenth level when we are
        // deliberately hiding from the checksum.
        let floor = if deep_only { 10 } else { 0 };
        assert!(depth > floor, "cannot target deep levels in a shallow book");
        let idx = floor + rng.below(depth - floor);
        let bid_side = rng.chance(2);
        let (side, price) = if bid_side {
            (Side::Bid, bid_price(idx))
        } else {
            (Side::Ask, ask_price(idx))
        };
        // A quantity guaranteed different from what is there now, so every
        // update genuinely moves the book.
        let mut qty = qty_of(n + 1_000);
        if book.qty_at(side, price) == Some(qty) {
            qty = qty.checked_add(f("0.00000001")).expect("in range");
        }

        let mut outcome = Default::default();
        book.apply_change(LevelChange::new(side, price, qty), &mut outcome);
        let checksum = kraken_checksum(&book, KRAKEN_BTC_USD.price, KRAKEN_BTC_USD.qty);

        let entry = format!(
            r#"{{"price":{},"qty":{}}}"#,
            price.to_decimal_string(KRAKEN_BTC_USD.price),
            qty.to_decimal_string(KRAKEN_BTC_USD.qty)
        );
        let (bid_arr, ask_arr) = if bid_side {
            (entry.as_str(), "")
        } else {
            ("", entry.as_str())
        };
        frames.push(frame(
            VenueId::Kraken,
            n + 2,
            format!(
                r#"{{"channel":"book","type":"update","data":[{{"symbol":"BTC/USD","bids":[{}],"asks":[{}],"checksum":{},"timestamp":"{}"}}]}}"#,
                bid_arr,
                ask_arr,
                checksum.value,
                iso(n + 1)
            ),
        ));
    }
    (RawTape::new(frames), book)
}

// ----------------------------------------------------------- real fixtures --

/// Load a file of raw payloads, one per line, as a tape.
pub fn fixture_tape(venue: VenueId, name: &str) -> RawTape {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()));
    RawTape::new(
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
            .map(|(i, line)| frame(venue, i, line.to_string()))
            .collect(),
    )
}

/// One captured Coinbase connection: the `l2_data` snapshot, the updates that
/// followed it, and the `subscriptions` acknowledgement that arrived numbered
/// between them. Captured in a single session so the sequence numbers are the
/// venue's real ones; only the snapshot's level list is trimmed.
pub fn real_coinbase_tape() -> RawTape {
    fixture_tape(VenueId::Coinbase, "coinbase_l2.jsonl")
}

pub fn real_kraken_tape() -> RawTape {
    fixture_tape(VenueId::Kraken, "kraken_book.jsonl")
}

// -------------------------------------------------------------- the replay --

/// What one replay produced, frame by frame.
pub struct Replay {
    pub session: BookSession,
    /// Book digest after each ingested frame.
    pub digests: Vec<u32>,
    /// Whether any loss had been detected by the end of each frame.
    pub detected_by: Vec<bool>,
    /// Frames where the book was crossed after ingest.
    pub crossed_frames: Vec<usize>,
    /// Frames where a crossed book was *not* accompanied by a flag. Must be
    /// empty; this is half the gate.
    pub silently_crossed: Vec<usize>,
    pub parse_errors: Vec<String>,
}

impl Replay {
    pub fn detected(&self) -> bool {
        *self.detected_by.last().unwrap_or(&false)
    }

    pub fn final_digest(&self) -> u32 {
        *self.digests.last().unwrap_or(&0)
    }
}

/// Feed a tape through a real [`BookSession`] and record what happened at every
/// step. The session under test is the same one the live recorder runs.
pub fn replay(venue: Arc<dyn Venue>, tape: &RawTape, symbol: &Symbol) -> Replay {
    let venue_id = venue.id();
    let session = BookSession::new(venue, vec![symbol.clone()]);
    drive(session, venue_id, tape, symbol)
}

/// Drive an already-constructed session over a tape, recording every step.
pub fn drive(
    mut session: BookSession,
    venue_id: VenueId,
    tape: &RawTape,
    symbol: &Symbol,
) -> Replay {
    let mut digests = Vec::with_capacity(tape.len());
    let mut detected_by = Vec::with_capacity(tape.len());
    let mut crossed_frames = Vec::new();
    let mut silently_crossed = Vec::new();
    let mut parse_errors = Vec::new();
    let mut last = Stamp::ZERO;

    for (i, recorded) in tape.frames.iter().enumerate() {
        let raw = recorded.to_raw();
        last = raw.stamp;
        if let Err(e) = session.ingest(&raw) {
            parse_errors.push(e.to_string());
        }

        let book = session.book(symbol).expect("subscribed symbol has a book");
        digests.push(book.digest());

        if book.is_crossed() {
            crossed_frames.push(i);
            // The invariant is that a crossing is always *counted*. Whether it
            // also invalidates the book is a venue-level decision: at L2 a
            // crossing means missing data, while an order-by-order feed crosses
            // by construction every time an aggressive order arrives.
            let stats = session.gaps().stats(venue_id, symbol);
            let counted = stats.map(|s| s.crossed_books > 0).unwrap_or(false);
            if !counted {
                silently_crossed.push(i);
            }
        }

        let stats = session.gaps().stats(venue_id, symbol);
        detected_by.push(
            stats
                .map(|s| s.sequence_gaps > 0 || s.checksum_divergences > 0)
                .unwrap_or(false),
        );
    }
    session.seal(last);

    Replay {
        session,
        digests,
        detected_by,
        crossed_frames,
        silently_crossed,
        parse_errors,
    }
}

/// A Coinbase tape ending in a delete for a price the venue never published.
///
/// Coinbase does this constantly: 210 times in one 30-second capture. It must
/// not be treated as loss.
pub fn coinbase_tape_with_redundant_delete(updates: usize, seed: u64) -> RawTape {
    let mut tape = coinbase_tape(updates, seed);
    let index = tape.len();
    tape.push(frame(
        VenueId::Coinbase,
        index,
        format!(
            r#"{{"channel":"l2_data","timestamp":"{}","sequence_num":{},"events":[{{"type":"update","product_id":"BTC-USD","updates":[{{"side":"offer","event_time":"{}","price_level":"77777.77","new_quantity":"0"}}]}}]}}"#,
            iso(index),
            updates + 2,
            iso(index)
        ),
    ));
    tape
}

/// A Kraken tape ending in a delete for a price it never published, carrying
/// the checksum of the *unchanged* book.
///
/// Deleting a level we do not hold changes nothing, so the venue's own checksum
/// still matches. That isolates the anomaly: the only thing that can react is
/// the absent-level rule, which on Kraken is real loss evidence.
pub fn kraken_tape_with_redundant_delete(updates: usize, depth: usize, seed: u64) -> RawTape {
    let (mut tape, book) = kraken_tape_with_book(updates, depth, seed, false);
    let unchanged = kraken_checksum(&book, KRAKEN_BTC_USD.price, KRAKEN_BTC_USD.qty);
    let absent = f("77777.7");
    assert_eq!(
        book.qty_at(Side::Bid, absent),
        None,
        "the test price must genuinely be absent"
    );
    let index = tape.len();
    tape.push(frame(
        VenueId::Kraken,
        index,
        format!(
            r#"{{"channel":"book","type":"update","data":[{{"symbol":"BTC/USD","bids":[{{"price":{},"qty":0.00000000}}],"asks":[],"checksum":{},"timestamp":"{}"}}]}}"#,
            absent.to_decimal_string(KRAKEN_BTC_USD.price),
            unchanged.value,
            iso(index)
        ),
    ));
    tape
}

// ------------------------------------------------- per-venue fixtures --

use tickvault::venue::SnapshotSource;
use tickvault::venue::registry::{self, VenueConfig};

/// A venue built offline, plus the frames captured from it.
pub struct Fixture {
    pub id: VenueId,
    pub venue: Arc<dyn Venue>,
    pub tape: RawTape,
    pub symbol: Symbol,
    /// The REST snapshot, for venues that have no in-band one.
    pub rest_snapshot: Option<String>,
}

/// Where each venue's captured frames live, and what REST body it needs.
fn fixture_files(id: VenueId) -> (&'static str, Option<(&'static str, &'static str)>) {
    match id {
        VenueId::Coinbase => ("coinbase_l2.jsonl", None),
        VenueId::Kraken => ("kraken_book.jsonl", None),
        VenueId::Okx => ("okx_books.jsonl", None),
        VenueId::Bybit => ("bybit_orderbook.jsonl", None),
        // (fixture, (url needle, snapshot file))
        VenueId::BinanceUs => (
            "binance_us_depth.jsonl",
            Some(("depth", "binance_us_snapshot.json")),
        ),
        VenueId::Bitstamp => (
            "bitstamp_diff.jsonl",
            Some(("order_book", "bitstamp_snapshot.json")),
        ),
    }
}

fn read_fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()))
}

/// Build a venue offline and load its captured frames.
pub fn fixture(id: VenueId) -> Fixture {
    let (tape_file, rest) = fixture_files(id);
    let symbol = VenueConfig::default_symbol(id);
    let rest_snapshot = rest.map(|(_, file)| read_fixture(file));

    let mut http = CannedFetch::new();
    if let (Some((needle, _)), Some(body)) = (rest, rest_snapshot.as_deref()) {
        http = http.on(needle, body);
    }

    let config = VenueConfig {
        symbols: vec![symbol.clone()],
        kraken_precision: Some(Precision { price: 1, qty: 8 }),
        ..VenueConfig::default()
    };
    let venue =
        registry::build_offline(id, &config, http.shared(), Arc::new(ManualClock::default()))
            .unwrap_or_else(|e| panic!("{id} failed to build offline: {e}"));

    Fixture {
        id,
        venue,
        tape: fixture_tape(id, tape_file),
        symbol,
        rest_snapshot,
    }
}

impl Fixture {
    pub fn caps(&self) -> &tickvault::venue::VenueCapabilities {
        self.venue.capabilities()
    }

    /// Indices of frames carrying book deltas, determined by parsing.
    pub fn delta_indices(&self) -> Vec<usize> {
        let mut v = registry::delta_frame_indices(self.venue.as_ref(), &self.tape);
        // A message removed from the very end has no successor to reveal it.
        v.retain(|i| *i + 1 < self.tape.len());
        v
    }

    /// Replay a tape through a real session, mirroring what the runner does:
    /// subscribe, then fetch the REST snapshot for venues that need one.
    pub fn replay_tape(&self, tape: &RawTape) -> Replay {
        let mut session = BookSession::new(Arc::clone(&self.venue), vec![self.symbol.clone()]);
        if self.caps().snapshot_source == SnapshotSource::Rest {
            let snapshot =
                block_on(self.venue.snapshot(&self.symbol)).expect("canned REST snapshot");
            session.apply_rest_snapshot(snapshot);
        }
        drive(session, self.venue.id(), tape, &self.symbol)
    }

    pub fn replay(&self) -> Replay {
        self.replay_tape(&self.tape)
    }
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

// --------------------------------------------------------- L3 fixtures --

use tickvault::book::l3::{L3Book, OrderAction, OrderEvent};
use tickvault::types::BookLevel;
use tickvault::venue::bitstamp::Bitstamp;

/// The Bitstamp order-by-order venue, built offline with its captured
/// order-level snapshot canned as the REST response.
pub fn bitstamp_l3() -> Arc<dyn Venue> {
    let snapshot = read_fixture("bitstamp_snapshot_l3.json");
    Arc::new(
        Bitstamp::new(
            CannedFetch::new().on("order_book", &snapshot).shared(),
            Arc::new(ManualClock::default()),
        )
        .with_book_level(BookLevel::L3)
        .with_order_seed(true)
        .with_symbols(&[btc_usd()]),
    )
}

/// The captured `live_orders` frames.
pub fn bitstamp_l3_tape() -> RawTape {
    fixture_tape(VenueId::Bitstamp, "bitstamp_live_orders.jsonl")
}

/// Parse a tape into order events, ignoring control frames.
pub fn order_events(venue: &dyn Venue, tape: &RawTape) -> Vec<OrderEvent> {
    tape.frames
        .iter()
        .flat_map(|f| venue.parse_delta(&f.to_raw()).expect("parses"))
        .filter_map(|e| match e {
            tickvault::venue::FeedEvent::Order(o) => Some(*o),
            _ => None,
        })
        .collect()
}

/// Replay order events into a bare book, without a session.
pub fn drive_l3(events: &[OrderEvent]) -> L3Book {
    let mut book = L3Book::new(btc_usd());
    for event in events {
        book.apply(event);
    }
    book
}

/// Indices of frames that parse into an order event.
pub fn order_frame_indices(venue: &dyn Venue, tape: &RawTape) -> Vec<usize> {
    tape.frames
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            venue
                .parse_delta(&f.to_raw())
                .map(|es| {
                    es.iter()
                        .any(|e| matches!(e, tickvault::venue::FeedEvent::Order(_)))
                })
                .unwrap_or(false)
        })
        .map(|(i, _)| i)
        .collect()
}

/// Lifecycle bookkeeping over a stream of order events.
#[derive(Debug, Default)]
pub struct Lifecycle {
    pub created: usize,
    pub removed: usize,
    pub duplicate_creates: usize,
    pub duplicate_removes: usize,
    /// Events for orders whose creation predates the capture.
    pub orphans: usize,
    /// Removals whose price differs from where the order was created.
    pub price_moved_on_remove: usize,
    pub still_open: usize,
}

pub fn lifecycle(events: &[OrderEvent]) -> Lifecycle {
    use std::collections::{HashMap, HashSet};
    let mut open: HashMap<String, tickvault::Fixed> = HashMap::new();
    let mut removed: HashSet<String> = HashSet::new();
    let mut l = Lifecycle::default();
    for e in events {
        let id = e.order_id.as_str().to_string();
        match e.action {
            OrderAction::Add => {
                if open.contains_key(&id) {
                    l.duplicate_creates += 1;
                }
                open.insert(id, e.price);
                l.created += 1;
            }
            OrderAction::Modify => {
                if !open.contains_key(&id) {
                    l.orphans += 1;
                }
            }
            OrderAction::Cancel | OrderAction::Execute => {
                match open.remove(&id) {
                    Some(created_at) => {
                        if created_at != e.price {
                            l.price_moved_on_remove += 1;
                        }
                        l.removed += 1;
                    }
                    None => l.orphans += 1,
                }
                if !removed.insert(id) {
                    l.duplicate_removes += 1;
                }
            }
        }
    }
    l.still_open = open.len();
    l
}

// ------------------------------------------------- archives for rebuilding --

use tickvault::store::writer::{ArchiveWriter, WriterConfig};

/// Replay a venue's captured fixture into a real Parquet archive.
///
/// The rebuild tests then work from an archive produced by the ordinary ingest
/// path over bytes the venue actually sent, rather than from rows invented to
/// suit them.
pub fn archive_from_fixture(id: VenueId, root: &std::path::Path, rows_per_file: usize) -> u64 {
    archive_from_fixture_with_digest(id, root, rows_per_file).0
}

/// As above, plus the digest of the book the *recorder* ended up holding.
///
/// A rebuild that disagrees with it has read the archive differently from the
/// process that wrote it, which no amount of determinism would reveal.
pub fn archive_from_fixture_with_digest(
    id: VenueId,
    root: &std::path::Path,
    rows_per_file: usize,
) -> (u64, u32) {
    let f = fixture(id);
    let mut session = BookSession::new(Arc::clone(&f.venue), vec![f.symbol.clone()]);
    session.enable_archive(64);
    if f.caps().snapshot_source == SnapshotSource::Rest {
        let snapshot = block_on(f.venue.snapshot(&f.symbol)).expect("canned snapshot");
        session.apply_rest_snapshot(snapshot);
    }

    let mut writer = ArchiveWriter::open(WriterConfig {
        max_rows_per_file: rows_per_file,
        ..WriterConfig::new(root)
    })
    .expect("open archive");

    let mut rows = 0u64;
    let mut drain = |session: &mut BookSession, writer: &mut ArchiveWriter, all: bool| {
        let batches = if all {
            session.flush_archive()
        } else {
            session.take_archive_batches()
        };
        for (key, batch, span) in batches {
            rows += batch.num_rows() as u64;
            writer.write(&key, &batch, span).expect("write");
        }
    };

    for frame in &f.tape.frames {
        let _ = session.ingest(&frame.to_raw());
        drain(&mut session, &mut writer, false);
    }
    drain(&mut session, &mut writer, true);
    writer.close().expect("close archive");
    let digest = session
        .book(&f.symbol)
        .map(|b| b.digest())
        .unwrap_or_default();
    (rows, digest)
}

/// The wall-clock span an archive covers, from its manifest.
pub fn archive_span(root: &std::path::Path) -> (i64, i64) {
    let reader = tickvault::store::reader::ArchiveReader::open(root).expect("open");
    let first = reader
        .files()
        .iter()
        .map(|f| f.first_recv_wall)
        .min()
        .expect("a non-empty archive");
    let last = reader
        .files()
        .iter()
        .map(|f| f.last_recv_wall)
        .max()
        .expect("a non-empty archive");
    (first, last)
}

/// Archive the Bitstamp order-by-order fixture through the ordinary ingest path.
///
/// Five hundred small messages, which is what makes it the right fixture for
/// anything about file counts or streaming behaviour: the other captures are a
/// handful of very wide messages.
pub fn archive_l3_fixture(root: &std::path::Path, rows_per_file: usize) -> u64 {
    let venue = bitstamp_l3();
    let symbol = btc_usd();
    let mut session = BookSession::new(Arc::clone(&venue), vec![symbol]);
    session.enable_archive(8);
    let mut writer = ArchiveWriter::open(WriterConfig {
        max_rows_per_file: rows_per_file,
        ..WriterConfig::new(root)
    })
    .expect("open archive");

    let mut rows = 0;
    for frame in &bitstamp_l3_tape().frames {
        let _ = session.ingest(&frame.to_raw());
        for (key, batch, span) in session.take_archive_batches() {
            rows += batch.num_rows() as u64;
            writer.write(&key, &batch, span).expect("write");
        }
    }
    for (key, batch, span) in session.flush_archive() {
        rows += batch.num_rows() as u64;
        writer.write(&key, &batch, span).expect("write");
    }
    writer.close().expect("close");
    rows
}
