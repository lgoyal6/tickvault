//! The L2 order book and the invariants it must never violate silently.

pub mod checksum;

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::clock::Timestamps;
use crate::fixed::Fixed;
use crate::types::{Side, Symbol};

/// One price level's new resting quantity. A quantity of zero removes the level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LevelChange {
    pub side: Side,
    pub price: Fixed,
    pub qty: Fixed,
}

impl LevelChange {
    pub fn new(side: Side, price: Fixed, qty: Fixed) -> Self {
        LevelChange { side, price, qty }
    }

    /// True when this change deletes the level rather than resizing it.
    pub fn is_removal(&self) -> bool {
        self.qty.is_zero()
    }
}

/// A complete book state as the venue described it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    pub symbol: Symbol,
    pub bids: Vec<(Fixed, Fixed)>,
    pub asks: Vec<(Fixed, Fixed)>,
    /// The venue's sequence number at the moment of the snapshot, if it has one.
    pub seq: Option<u64>,
    /// The venue's own book checksum, if it publishes one.
    pub checksum: Option<u32>,
    pub stamps: Timestamps,
}

/// An incremental update to a book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookDelta {
    pub symbol: Symbol,
    pub changes: Vec<LevelChange>,
    /// The venue's sequence number for this message, if it has one.
    pub seq: Option<u64>,
    /// The venue's checksum of the book *after* this update, if it sends one.
    pub checksum: Option<u32>,
    pub stamps: Timestamps,
    /// The identifier of the message this one claims to follow.
    ///
    /// Only a chained feed populates this. It is what lets continuity be
    /// checked by identity rather than by assuming a step size, which is the
    /// difference between OKX's scheme and a plain counter.
    pub prev_seq: Option<u64>,
    /// The first identifier this message covers, when a message spans a range.
    ///
    /// Only a range feed populates this. Binance batches many book changes into
    /// one message and reports the span, so `seq` alone would understate how
    /// much a gap swallowed.
    pub first_seq: Option<u64>,
}

/// A book state that should not be possible, and what it was when we saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookAnomaly {
    /// The best bid is strictly above the best ask. On a single venue this is
    /// not a market condition, it is missing data.
    Crossed { best_bid: Fixed, best_ask: Fixed },
    /// Best bid equals best ask. Briefly legitimate, still worth counting.
    Locked { price: Fixed },
    /// A venue sent a level with a quantity below zero.
    NegativeQty {
        side: Side,
        price: Fixed,
        qty: Fixed,
    },
    /// A delete arrived for a price we were not holding. For a venue without
    /// sequence numbers this is often the first visible sign of a lost message.
    RemovedMissingLevel { side: Side, price: Fixed },
}

impl BookAnomaly {
    /// Whether this anomaly means the book can no longer be trusted, as opposed
    /// to being merely unusual.
    pub fn corrupts_book(&self) -> bool {
        matches!(
            self,
            BookAnomaly::Crossed { .. }
                | BookAnomaly::NegativeQty { .. }
                | BookAnomaly::RemovedMissingLevel { .. }
        )
    }
}

impl fmt::Display for BookAnomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BookAnomaly::Crossed { best_bid, best_ask } => {
                write!(f, "crossed book: bid {best_bid} above ask {best_ask}")
            }
            BookAnomaly::Locked { price } => write!(f, "locked book at {price}"),
            BookAnomaly::NegativeQty { side, price, qty } => {
                write!(f, "negative quantity {qty} on {side} at {price}")
            }
            BookAnomaly::RemovedMissingLevel { side, price } => {
                write!(f, "delete for absent {side} level at {price}")
            }
        }
    }
}

/// What applying a delta did to the book.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyOutcome {
    /// Levels inserted or resized.
    pub applied: usize,
    /// Levels deleted.
    pub removed: usize,
    /// Levels dropped because they fell outside a depth-limited feed's window.
    pub truncated: usize,
    /// Everything that looked wrong. Empty is the normal case.
    pub anomalies: Vec<BookAnomaly>,
}

impl ApplyOutcome {
    pub fn is_clean(&self) -> bool {
        self.anomalies.is_empty()
    }

    /// True when the book is no longer a faithful copy of the venue's.
    pub fn corrupted(&self) -> bool {
        self.anomalies.iter().any(BookAnomaly::corrupts_book)
    }
}

/// An aggregated price-level book for one instrument on one venue.
///
/// Bids and asks are kept in separate ordered maps keyed by exact price, so
/// "best bid" is a map lookup rather than a scan, and two books built from the
/// same messages in the same order are byte-identical. Phase 5's determinism
/// gate depends on that.
#[derive(Debug, Clone)]
pub struct L2Book {
    symbol: Symbol,
    bids: BTreeMap<Fixed, Fixed>,
    asks: BTreeMap<Fixed, Fixed>,
    /// Depth-limited feeds require the client to truncate after each update.
    /// Kraken is one: keeping levels past the window leaves stale prices below
    /// the checksum's reach, where they are wrong and invisible at once.
    depth_limit: Option<usize>,
    last_stamps: Option<Timestamps>,
    updates_applied: u64,
}

impl L2Book {
    pub fn new(symbol: Symbol) -> Self {
        L2Book {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            depth_limit: None,
            last_stamps: None,
            updates_applied: 0,
        }
    }

    /// A book fed by a depth-limited channel, truncated to `depth` per side.
    pub fn with_depth_limit(symbol: Symbol, depth: usize) -> Self {
        L2Book {
            depth_limit: Some(depth),
            ..L2Book::new(symbol)
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn depth_limit(&self) -> Option<usize> {
        self.depth_limit
    }

    pub fn updates_applied(&self) -> u64 {
        self.updates_applied
    }

    pub fn last_stamps(&self) -> Option<Timestamps> {
        self.last_stamps
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    pub fn level_count(&self, side: Side) -> usize {
        self.side(side).len()
    }

    fn side(&self, side: Side) -> &BTreeMap<Fixed, Fixed> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<Fixed, Fixed> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    pub fn best_bid(&self) -> Option<(Fixed, Fixed)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    pub fn best_ask(&self) -> Option<(Fixed, Fixed)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }

    /// Best ask minus best bid, or `None` if either side is empty.
    pub fn spread(&self) -> Option<Fixed> {
        match (self.best_bid(), self.best_ask()) {
            (Some((b, _)), Some((a, _))) => a.checked_sub(b),
            _ => None,
        }
    }

    /// Resting quantity at an exact price, or `None` if there is no such level.
    pub fn qty_at(&self, side: Side, price: Fixed) -> Option<Fixed> {
        self.side(side).get(&price).copied()
    }

    /// The best `n` levels, best price first.
    pub fn top(&self, side: Side, n: usize) -> Vec<(Fixed, Fixed)> {
        match side {
            Side::Bid => self
                .bids
                .iter()
                .rev()
                .take(n)
                .map(|(p, q)| (*p, *q))
                .collect(),
            Side::Ask => self.asks.iter().take(n).map(|(p, q)| (*p, *q)).collect(),
        }
    }

    /// Discard everything and rebuild from a venue snapshot.
    pub fn reset_from_snapshot(&mut self, snapshot: &BookSnapshot) -> ApplyOutcome {
        self.bids.clear();
        self.asks.clear();
        self.updates_applied = 0;

        let mut outcome = ApplyOutcome::default();
        for (price, qty) in &snapshot.bids {
            self.insert_checked(Side::Bid, *price, *qty, &mut outcome);
        }
        for (price, qty) in &snapshot.asks {
            self.insert_checked(Side::Ask, *price, *qty, &mut outcome);
        }
        self.truncate_to_depth(&mut outcome);
        self.check_cross(&mut outcome);
        self.last_stamps = Some(snapshot.stamps);
        outcome
    }

    /// Apply a batch of level changes.
    ///
    /// The crossed-book check runs once, after the whole batch. A venue that
    /// moves both sides in one message is transiently crossed halfway through
    /// applying it, and flagging that would bury the real crossings in noise.
    pub fn apply_delta(&mut self, delta: &BookDelta) -> ApplyOutcome {
        let mut outcome = ApplyOutcome::default();
        for change in &delta.changes {
            self.apply_change(*change, &mut outcome);
        }
        self.truncate_to_depth(&mut outcome);
        self.check_cross(&mut outcome);
        self.last_stamps = Some(delta.stamps);
        self.updates_applied += 1;
        outcome
    }

    /// Apply a single level change. Exposed for tests and for L3 aggregation in
    /// phase 4.
    pub fn apply_change(&mut self, change: LevelChange, outcome: &mut ApplyOutcome) {
        if change.qty.is_negative() {
            outcome.anomalies.push(BookAnomaly::NegativeQty {
                side: change.side,
                price: change.price,
                qty: change.qty,
            });
            return;
        }
        if change.is_removal() {
            if self.side_mut(change.side).remove(&change.price).is_some() {
                outcome.removed += 1;
            } else {
                // A delete for a level we never had. Either we missed the add,
                // or the venue is echoing a level that fell out of a depth
                // window. Both are worth counting; neither is fatal alone.
                outcome.anomalies.push(BookAnomaly::RemovedMissingLevel {
                    side: change.side,
                    price: change.price,
                });
            }
        } else {
            self.side_mut(change.side).insert(change.price, change.qty);
            outcome.applied += 1;
        }
    }

    fn insert_checked(&mut self, side: Side, price: Fixed, qty: Fixed, outcome: &mut ApplyOutcome) {
        if qty.is_negative() {
            outcome
                .anomalies
                .push(BookAnomaly::NegativeQty { side, price, qty });
            return;
        }
        if qty.is_zero() {
            // Some venues pad snapshots with zero levels. Skip, do not flag.
            return;
        }
        self.side_mut(side).insert(price, qty);
        outcome.applied += 1;
    }

    fn truncate_to_depth(&mut self, outcome: &mut ApplyOutcome) {
        let Some(limit) = self.depth_limit else {
            return;
        };
        while self.bids.len() > limit {
            // Worst bid is the lowest price.
            if let Some((&price, _)) = self.bids.iter().next() {
                self.bids.remove(&price);
                outcome.truncated += 1;
            }
        }
        while self.asks.len() > limit {
            // Worst ask is the highest price.
            if let Some((&price, _)) = self.asks.iter().next_back() {
                self.asks.remove(&price);
                outcome.truncated += 1;
            }
        }
    }

    fn check_cross(&self, outcome: &mut ApplyOutcome) {
        if let (Some((bid, _)), Some((ask, _))) = (self.best_bid(), self.best_ask()) {
            if bid > ask {
                outcome.anomalies.push(BookAnomaly::Crossed {
                    best_bid: bid,
                    best_ask: ask,
                });
            } else if bid == ask {
                outcome.anomalies.push(BookAnomaly::Locked { price: bid });
            }
        }
    }

    /// The book's own integrity check, independent of any venue checksum.
    pub fn is_crossed(&self) -> bool {
        matches!(
            (self.best_bid(), self.best_ask()),
            (Some((b, _)), Some((a, _))) if b > a
        )
    }

    /// A stable digest of the entire book, for the phase 5 determinism gate and
    /// for comparing a replayed book against a live one.
    pub fn digest(&self) -> u32 {
        checksum::book_digest(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Stamp;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    fn sym() -> Symbol {
        Symbol::new("BTC", "USD")
    }

    fn stamps() -> Timestamps {
        Timestamps::recv_only(Stamp {
            mono_nanos: 1,
            wall_nanos: 1,
        })
    }

    fn delta(changes: Vec<LevelChange>) -> BookDelta {
        BookDelta {
            symbol: sym(),
            changes,
            seq: None,
            checksum: None,
            stamps: stamps(),
            prev_seq: None,
            first_seq: None,
        }
    }

    fn seeded() -> L2Book {
        let mut book = L2Book::new(sym());
        let snap = BookSnapshot {
            symbol: sym(),
            bids: vec![(f("99"), f("1")), (f("98"), f("2")), (f("97"), f("3"))],
            asks: vec![(f("101"), f("1")), (f("102"), f("2")), (f("103"), f("3"))],
            seq: Some(10),
            checksum: None,
            stamps: stamps(),
        };
        let outcome = book.reset_from_snapshot(&snap);
        assert!(outcome.is_clean(), "{outcome:?}");
        book
    }

    #[test]
    fn best_prices_come_from_the_right_end_of_each_side() {
        let book = seeded();
        assert_eq!(book.best_bid(), Some((f("99"), f("1"))));
        assert_eq!(book.best_ask(), Some((f("101"), f("1"))));
        assert_eq!(book.spread(), Some(f("2")));
    }

    #[test]
    fn top_n_is_ordered_best_first_on_both_sides() {
        let book = seeded();
        assert_eq!(
            book.top(Side::Bid, 2),
            vec![(f("99"), f("1")), (f("98"), f("2"))]
        );
        assert_eq!(
            book.top(Side::Ask, 2),
            vec![(f("101"), f("1")), (f("102"), f("2"))]
        );
        // Asking for more than exists yields what exists, not an error.
        assert_eq!(book.top(Side::Bid, 99).len(), 3);
    }

    #[test]
    fn zero_quantity_removes_a_level() {
        let mut book = seeded();
        let out = book.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("99"), f("0"))]));
        assert_eq!(out.removed, 1);
        assert!(out.is_clean(), "{out:?}");
        assert_eq!(book.best_bid(), Some((f("98"), f("2"))));
    }

    #[test]
    fn deleting_an_absent_level_is_flagged_not_ignored() {
        let mut book = seeded();
        let out = book.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("50"), f("0"))]));
        assert_eq!(
            out.anomalies,
            vec![BookAnomaly::RemovedMissingLevel {
                side: Side::Bid,
                price: f("50")
            }]
        );
        assert!(out.corrupted());
    }

    #[test]
    fn crossed_book_is_always_flagged() {
        let mut book = seeded();
        let out = book.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("105"), f("1"))]));
        assert_eq!(
            out.anomalies,
            vec![BookAnomaly::Crossed {
                best_bid: f("105"),
                best_ask: f("101")
            }]
        );
        assert!(book.is_crossed());
    }

    #[test]
    fn locked_book_is_reported_separately_from_crossed() {
        let mut book = seeded();
        let out = book.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("101"), f("1"))]));
        assert_eq!(out.anomalies, vec![BookAnomaly::Locked { price: f("101") }]);
        assert!(!book.is_crossed());
        assert!(!out.corrupted(), "a locked book is unusual, not corrupt");
    }

    #[test]
    fn a_batch_that_moves_both_sides_is_not_transiently_flagged() {
        // Bids and asks both step up by 5. Applying the new bid first makes the
        // book momentarily crossed; only the end state should be judged.
        let mut book = seeded();
        let out = book.apply_delta(&delta(vec![
            LevelChange::new(Side::Bid, f("104"), f("1")),
            LevelChange::new(Side::Ask, f("101"), f("0")),
            LevelChange::new(Side::Ask, f("102"), f("0")),
            LevelChange::new(Side::Ask, f("103"), f("0")),
            LevelChange::new(Side::Ask, f("106"), f("1")),
        ]));
        assert!(out.is_clean(), "{out:?}");
        assert_eq!(book.best_bid(), Some((f("104"), f("1"))));
        assert_eq!(book.best_ask(), Some((f("106"), f("1"))));
    }

    #[test]
    fn negative_quantity_is_rejected_without_touching_the_book() {
        let mut book = seeded();
        let out = book.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("99"), f("-1"))]));
        assert!(matches!(
            out.anomalies.as_slice(),
            [BookAnomaly::NegativeQty { .. }]
        ));
        assert_eq!(book.qty_at(Side::Bid, f("99")), Some(f("1")));
    }

    #[test]
    fn depth_limited_book_truncates_the_worst_levels_on_each_side() {
        let mut book = L2Book::with_depth_limit(sym(), 2);
        let out = book.reset_from_snapshot(&BookSnapshot {
            symbol: sym(),
            bids: vec![(f("99"), f("1")), (f("98"), f("2")), (f("97"), f("3"))],
            asks: vec![(f("101"), f("1")), (f("102"), f("2")), (f("103"), f("3"))],
            seq: None,
            checksum: None,
            stamps: stamps(),
        });
        assert_eq!(out.truncated, 2);
        assert_eq!(book.level_count(Side::Bid), 2);
        assert_eq!(book.level_count(Side::Ask), 2);
        // The levels kept are the best ones.
        assert_eq!(book.qty_at(Side::Bid, f("97")), None);
        assert_eq!(book.qty_at(Side::Ask, f("103")), None);
        assert_eq!(book.best_bid(), Some((f("99"), f("1"))));
    }

    #[test]
    fn a_new_best_level_pushes_the_worst_one_out_of_the_window() {
        let mut book = L2Book::with_depth_limit(sym(), 2);
        book.reset_from_snapshot(&BookSnapshot {
            symbol: sym(),
            bids: vec![(f("99"), f("1")), (f("98"), f("2"))],
            asks: vec![(f("101"), f("1")), (f("102"), f("2"))],
            seq: None,
            checksum: None,
            stamps: stamps(),
        });
        let out = book.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("99.5"), f("4"))]));
        assert_eq!(out.truncated, 1);
        assert_eq!(
            book.top(Side::Bid, 5),
            vec![(f("99.5"), f("4")), (f("99"), f("1"))]
        );
    }

    #[test]
    fn snapshot_reset_discards_prior_state_entirely() {
        let mut book = seeded();
        book.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("1"), f("9"))]));
        book.reset_from_snapshot(&BookSnapshot {
            symbol: sym(),
            bids: vec![(f("10"), f("1"))],
            asks: vec![(f("11"), f("1"))],
            seq: Some(99),
            checksum: None,
            stamps: stamps(),
        });
        assert_eq!(book.level_count(Side::Bid), 1);
        assert_eq!(book.qty_at(Side::Bid, f("1")), None);
        assert_eq!(book.updates_applied(), 0);
    }

    #[test]
    fn identical_message_orders_give_identical_digests() {
        let a = seeded();
        let b = seeded();
        assert_eq!(a.digest(), b.digest());
        let mut c = seeded();
        c.apply_delta(&delta(vec![LevelChange::new(Side::Bid, f("99"), f("5"))]));
        assert_ne!(a.digest(), c.digest());
    }
}
