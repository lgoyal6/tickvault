//! Order-by-order book.
//!
//! An L2 book knows there is 0.5 BTC resting at a price. An L3 book knows it is
//! three orders of 0.2, 0.2 and 0.1, which arrived in that order, and that a
//! fourth order joining now sits behind all of them. That difference is the
//! whole reason microstructure work wants L3: **queue position is not derivable
//! from aggregated depth at all.**
//!
//! # What can and cannot be inferred
//!
//! Under price-time priority, an order we watched arrive goes to the back of
//! its level and its position from then on is a fact we observed, not a guess.
//! Two situations break that, and both are marked rather than smoothed over:
//!
//! - **Orders that predate us.** A feed that starts empty has no idea what is
//!   already resting. Seeding from a snapshot gives the orders but their
//!   relative order is the venue's listing order, which is an assumption. It is
//!   a well-evidenced one for Bitstamp: across 591 price levels holding more
//!   than one order, its REST book listed every single one in ascending order
//!   id, and its order ids increase with time. Evidenced is still not observed,
//!   so those orders carry [`QueueCertainty::Seeded`].
//! - **Size increases.** Raising an order's quantity forfeits time priority on
//!   most venues, but not all, and where it lands is not something the feed
//!   says. Those orders become [`QueueCertainty::Unknown`].
//!
//! [`L3Book::queue_position`] returns the certainty alongside the number, so a
//! caller cannot use the position without seeing how much it is worth.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::book::{BookSnapshot, L2Book};
use crate::clock::{Stamp, Timestamps};
use crate::fixed::Fixed;
use crate::types::{Side, Symbol};

/// A venue's identifier for a resting order.
///
/// A string because the venues disagree: Bitstamp numbers them, Coinbase uses
/// UUIDs. Parsing them into something narrower would only invite one of them to
/// stop fitting.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OrderId(pub String);

impl OrderId {
    pub fn new(id: impl Into<String>) -> Self {
        OrderId(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What happened to one order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderAction {
    /// A new order joined the book.
    Add,
    /// Its resting quantity changed while it stayed on the book.
    Modify,
    /// It left the book without being fully traded.
    Cancel,
    /// It left the book having been fully traded.
    Execute,
}

impl OrderAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            OrderAction::Add => "add",
            OrderAction::Modify => "modify",
            OrderAction::Cancel => "cancel",
            OrderAction::Execute => "execute",
        }
    }

    /// True when the order leaves the book.
    pub const fn removes(self) -> bool {
        matches!(self, OrderAction::Cancel | OrderAction::Execute)
    }
}

impl fmt::Display for OrderAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One order-by-order event from a venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderEvent {
    pub symbol: Symbol,
    pub order_id: OrderId,
    pub action: OrderAction,
    pub side: Side,
    pub price: Fixed,
    /// Quantity still resting after this event.
    pub qty: Fixed,
    /// Size when the order was created, where the venue reports it.
    pub original_qty: Option<Fixed>,
    /// Cumulative quantity traded, where the venue reports it.
    pub executed_qty: Option<Fixed>,
    pub stamps: Timestamps,
    /// Chained event token, where the venue publishes one.
    pub event_token: Option<u128>,
    pub prev_event_token: Option<u128>,
    /// Whether the venue's own numbers account for the change in size.
    ///
    /// False means the resting quantity moved by more than the reported trade
    /// explains. Measured on Bitstamp: 19 of 3,999 events. When this is false
    /// the difference between an execution and a cancellation is not decidable
    /// from the feed, and nothing downstream should pretend otherwise.
    pub size_change_explained: bool,
}

/// How much a queue position is worth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueCertainty {
    /// We watched this order arrive. Its place is observed, not inferred.
    Observed,
    /// It came from a snapshot. Its place is the venue's listing order, which
    /// is an assumption about how the venue lists rather than something seen.
    Seeded,
    /// Something happened that price-time priority does not determine.
    Unknown,
}

impl QueueCertainty {
    /// True only when the position is something we saw rather than assumed.
    pub const fn is_observed(self) -> bool {
        matches!(self, QueueCertainty::Observed)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            QueueCertainty::Observed => "observed",
            QueueCertainty::Seeded => "seeded",
            QueueCertainty::Unknown => "unknown",
        }
    }
}

/// An order resting on the book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub price: Fixed,
    pub qty: Fixed,
    pub original_qty: Option<Fixed>,
    pub executed_qty: Option<Fixed>,
    pub arrived: Stamp,
    pub certainty: QueueCertainty,
}

/// Where an order sits in its price level's queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuePosition {
    /// Orders ahead of it at the same price. Zero means next to trade.
    pub index: usize,
    /// Quantity that must trade before this order does.
    pub qty_ahead: Fixed,
    /// How much the two numbers above are worth.
    pub certainty: QueueCertainty,
}

/// A state the book should not reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L3Anomaly {
    /// A change or removal for an order we never saw added. On a feed with no
    /// snapshot this is normal at first and stops once the pre-existing orders
    /// have all traded or been cancelled.
    UnknownOrder { id: OrderId, action: OrderAction },
    /// The same order added twice without being removed in between.
    DuplicateAdd { id: OrderId },
    /// An event reports a different price than the order was resting at.
    ///
    /// On Bitstamp this is usually not a modification at all: an aggressive
    /// order that matches immediately is deleted at the price it *executed* at
    /// rather than where it was placed. Either way the order has to be detached
    /// from where it actually is, never from where the event says.
    PriceMoved { id: OrderId, from: Fixed, to: Fixed },
    /// A quantity below zero.
    NegativeQty { id: OrderId, qty: Fixed },
    /// The best bid is above the best ask.
    Crossed { best_bid: Fixed, best_ask: Fixed },
}

impl fmt::Display for L3Anomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            L3Anomaly::UnknownOrder { id, action } => {
                write!(f, "{action} for unknown order {id}")
            }
            L3Anomaly::DuplicateAdd { id } => write!(f, "order {id} added twice"),
            L3Anomaly::PriceMoved { id, from, to } => {
                write!(f, "order {id} moved from {from} to {to}")
            }
            L3Anomaly::NegativeQty { id, qty } => {
                write!(f, "order {id} has negative quantity {qty}")
            }
            L3Anomaly::Crossed { best_bid, best_ask } => {
                write!(f, "crossed book: bid {best_bid} above ask {best_ask}")
            }
        }
    }
}

/// Running totals for the gap report and the published load figures.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct L3Stats {
    pub added: u64,
    pub modified: u64,
    pub cancelled: u64,
    pub executed: u64,
    /// Events for orders we never saw added.
    pub unknown_orders: u64,
    /// Events whose size change the venue's own numbers do not explain.
    pub unexplained_size_changes: u64,
    pub peak_open_orders: u64,
}

/// What applying one event did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct L3Outcome {
    pub applied: bool,
    pub anomalies: Vec<L3Anomaly>,
    /// How much this event traded, where the feed lets that be worked out.
    ///
    /// The venue reports a *cumulative* traded amount per order, so the size of
    /// one execution is the rise since the last event for that order. That
    /// subtraction can only be done here, while the previous state of the order
    /// is still in hand; by the time a row reaches the archive it carries the
    /// quantity still resting, which on a full fill is zero.
    ///
    /// `None` means the event was not an execution, or was one on an order we
    /// never watched arrive, in which case what it had been resting for is not
    /// known. Zero would say nothing traded.
    pub traded_qty: Option<Fixed>,
}

impl L3Outcome {
    pub fn is_clean(&self) -> bool {
        self.anomalies.is_empty()
    }
}

/// An order-by-order book for one instrument.
#[derive(Debug, Clone)]
pub struct L3Book {
    symbol: Symbol,
    orders: HashMap<OrderId, Order>,
    /// Price to the queue of order ids at it, front first.
    bids: BTreeMap<Fixed, VecDeque<OrderId>>,
    asks: BTreeMap<Fixed, VecDeque<OrderId>>,
    /// Levels known to hold orders we never saw arrive.
    ///
    /// A feed with no usable snapshot starts empty, so a level can contain
    /// orders that predate the recording. We learn that a level is incomplete
    /// when an event arrives for an order there that we never saw added. From
    /// then on, no queue position at that price can be called observed: the
    /// index would be counted among the orders we happen to know about and
    /// would understate the real queue.
    ///
    /// A level clears once it empties completely, because there is then nothing
    /// left that we did not watch arrive.
    incomplete: std::collections::HashSet<(Side, Fixed)>,
    stats: L3Stats,
}

impl L3Book {
    pub fn new(symbol: Symbol) -> Self {
        L3Book {
            symbol,
            orders: HashMap::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            incomplete: std::collections::HashSet::new(),
            stats: L3Stats::default(),
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn stats(&self) -> L3Stats {
        self.stats
    }

    pub fn open_orders(&self) -> usize {
        self.orders.len()
    }

    /// Levels we know are missing orders that predate the recording.
    pub fn incomplete_levels(&self) -> usize {
        self.incomplete.len()
    }

    /// True when this level is known to hold orders we never watched arrive.
    pub fn level_is_incomplete(&self, side: Side, price: Fixed) -> bool {
        self.incomplete.contains(&(side, price))
    }

    pub fn order(&self, id: &OrderId) -> Option<&Order> {
        self.orders.get(id)
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    fn levels(&self, side: Side) -> &BTreeMap<Fixed, VecDeque<OrderId>> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<Fixed, VecDeque<OrderId>> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    pub fn best_bid(&self) -> Option<Fixed> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Fixed> {
        self.asks.keys().next().copied()
    }

    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(b), Some(a)) if b > a)
    }

    /// Orders resting at a price, in queue order.
    pub fn queue_at(&self, side: Side, price: Fixed) -> Vec<&Order> {
        self.levels(side)
            .get(&price)
            .map(|queue| queue.iter().filter_map(|id| self.orders.get(id)).collect())
            .unwrap_or_default()
    }

    /// Where an order sits in its level's queue, and how much that is worth.
    ///
    /// The certainty is the *weakest* of the order's own and that of everything
    /// ahead of it: knowing exactly where you are in a queue is no use if the
    /// sizes ahead of you are assumed. This is the difference between a queue
    /// position a strategy can act on and one that merely looks precise.
    pub fn queue_position(&self, id: &OrderId) -> Option<QueuePosition> {
        let order = self.orders.get(id)?;
        let queue = self.levels(order.side).get(&order.price)?;
        let mut qty_ahead = Fixed::ZERO;
        let mut certainty = order.certainty;
        if self.level_is_incomplete(order.side, order.price) {
            // Orders we never saw are resting here, so counting the ones we
            // know about would understate the queue.
            certainty = QueueCertainty::Unknown;
        }
        for (index, ahead) in queue.iter().enumerate() {
            if ahead == id {
                return Some(QueuePosition {
                    index,
                    qty_ahead,
                    certainty,
                });
            }
            if let Some(other) = self.orders.get(ahead) {
                qty_ahead = qty_ahead.checked_add(other.qty).unwrap_or(Fixed::MAX);
                if !other.certainty.is_observed() {
                    certainty = match certainty {
                        QueueCertainty::Unknown => QueueCertainty::Unknown,
                        _ => other.certainty,
                    };
                }
            }
        }
        None
    }

    /// Seed from a snapshot of individual orders, in the venue's listing order.
    ///
    /// Everything seeded is marked [`QueueCertainty::Seeded`]: the relative
    /// order is the venue's, and taking it for queue order is an assumption.
    pub fn seed(
        &mut self,
        orders: impl IntoIterator<Item = (OrderId, Side, Fixed, Fixed)>,
        at: Stamp,
    ) {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
        for (id, side, price, qty) in orders {
            if qty.is_zero() || qty.is_negative() {
                continue;
            }
            self.levels_mut(side)
                .entry(price)
                .or_default()
                .push_back(id.clone());
            self.orders.insert(
                id.clone(),
                Order {
                    id,
                    side,
                    price,
                    qty,
                    original_qty: Some(qty),
                    executed_qty: None,
                    arrived: at,
                    certainty: QueueCertainty::Seeded,
                },
            );
        }
        self.stats.peak_open_orders = self.stats.peak_open_orders.max(self.orders.len() as u64);
    }

    /// Apply one order-by-order event.
    pub fn apply(&mut self, event: &OrderEvent) -> L3Outcome {
        let mut outcome = L3Outcome::default();
        if !event.size_change_explained {
            self.stats.unexplained_size_changes += 1;
        }
        if event.qty.is_negative() {
            outcome.anomalies.push(L3Anomaly::NegativeQty {
                id: event.order_id.clone(),
                qty: event.qty,
            });
            return outcome;
        }

        match event.action {
            OrderAction::Add => self.add(event, &mut outcome),
            OrderAction::Modify => self.modify(event, &mut outcome),
            OrderAction::Cancel | OrderAction::Execute => self.remove(event, &mut outcome),
        }

        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask())
            && bid > ask
        {
            outcome.anomalies.push(L3Anomaly::Crossed {
                best_bid: bid,
                best_ask: ask,
            });
        }
        self.stats.peak_open_orders = self.stats.peak_open_orders.max(self.orders.len() as u64);
        outcome
    }

    fn add(&mut self, event: &OrderEvent, outcome: &mut L3Outcome) {
        if self.orders.contains_key(&event.order_id) {
            outcome.anomalies.push(L3Anomaly::DuplicateAdd {
                id: event.order_id.clone(),
            });
            return;
        }
        if event.qty.is_zero() {
            // An order resting for nothing is not on the book.
            return;
        }
        // The back of the queue: this is the observation that makes queue
        // position a fact rather than an inference.
        self.levels_mut(event.side)
            .entry(event.price)
            .or_default()
            .push_back(event.order_id.clone());
        self.orders.insert(
            event.order_id.clone(),
            Order {
                id: event.order_id.clone(),
                side: event.side,
                price: event.price,
                qty: event.qty,
                original_qty: event.original_qty,
                executed_qty: event.executed_qty,
                arrived: event.stamps.recv,
                certainty: QueueCertainty::Observed,
            },
        );
        self.stats.added += 1;
        outcome.applied = true;
    }

    fn modify(&mut self, event: &OrderEvent, outcome: &mut L3Outcome) {
        let Some(existing) = self.orders.get(&event.order_id).cloned() else {
            outcome.anomalies.push(L3Anomaly::UnknownOrder {
                id: event.order_id.clone(),
                action: event.action,
            });
            self.stats.unknown_orders += 1;
            self.incomplete.insert((event.side, event.price));
            return;
        };

        if existing.price != event.price {
            // A price change is a cancel and an add wearing one event. Treat it
            // as such: it leaves its old queue and joins the back of the new
            // one, which is what price-time priority does.
            outcome.anomalies.push(L3Anomaly::PriceMoved {
                id: event.order_id.clone(),
                from: existing.price,
                to: event.price,
            });
            self.detach(&existing);
            self.levels_mut(event.side)
                .entry(event.price)
                .or_default()
                .push_back(event.order_id.clone());
        } else if event.qty > existing.qty {
            // Raising size forfeits time priority on most venues but not all,
            // and the feed does not say where it lands. Move it to the back and
            // stop claiming to know where it is.
            self.detach(&existing);
            self.levels_mut(event.side)
                .entry(event.price)
                .or_default()
                .push_back(event.order_id.clone());
            if let Some(order) = self.orders.get_mut(&event.order_id) {
                order.certainty = QueueCertainty::Unknown;
            }
        }

        if let Some(order) = self.orders.get_mut(&event.order_id) {
            order.price = event.price;
            order.side = event.side;
            order.qty = event.qty;
            order.executed_qty = event.executed_qty.or(order.executed_qty);
            order.original_qty = order.original_qty.or(event.original_qty);
        }
        self.stats.modified += 1;
        outcome.applied = true;
        // A partial fill arrives as a change in size, not as an execution, so
        // the traded amount has to be read off the cumulative figure here as
        // well or every partial fill goes unrecorded.
        let filled = traded_since(&existing, event);
        if !filled.is_zero() {
            outcome.traded_qty = Some(filled);
        }

        if event.qty.is_zero() {
            // A modification down to nothing is a removal in all but name.
            let order = self.orders.get(&event.order_id).cloned();
            if let Some(order) = order {
                self.detach(&order);
                self.orders.remove(&event.order_id);
            }
        }
    }

    fn remove(&mut self, event: &OrderEvent, outcome: &mut L3Outcome) {
        let Some(existing) = self.orders.remove(&event.order_id) else {
            outcome.anomalies.push(L3Anomaly::UnknownOrder {
                id: event.order_id.clone(),
                action: event.action,
            });
            self.stats.unknown_orders += 1;
            // Something was resting here that we never watched arrive.
            self.incomplete.insert((event.side, event.price));
            return;
        };
        self.detach(&existing);
        match event.action {
            OrderAction::Execute => {
                self.stats.executed += 1;
                outcome.traded_qty = Some(traded_since(&existing, event));
            }
            _ => self.stats.cancelled += 1,
        }
        outcome.applied = true;
    }
}

/// How much an order traded between the state we held and this event.
///
/// The venue publishes a running total per order, so one execution is the rise
/// in that total. Where it is not published at all, an execution that removes
/// the order traded whatever it had been resting for, which is the next best
/// thing the feed supports and is not a guess about anything unobserved.
fn traded_since(existing: &Order, event: &OrderEvent) -> Fixed {
    match (event.executed_qty, existing.executed_qty) {
        (Some(now), Some(before)) => now
            .checked_sub(before)
            .unwrap_or(Fixed::ZERO)
            .max(Fixed::ZERO),
        // Seeded orders carry no history, so the venue's running total may
        // count fills from before the recording started. Only the part that
        // can be accounted for by what we watched resting is claimed.
        (Some(now), None) => now.min(existing.qty),
        (None, _) if event.action == OrderAction::Execute => existing.qty,
        (None, _) => Fixed::ZERO,
    }
}

impl L3Book {
    /// Take an order out of its level's queue, dropping the level if empty.
    fn detach(&mut self, order: &Order) {
        let levels = self.levels_mut(order.side);
        if let Some(queue) = levels.get_mut(&order.price) {
            queue.retain(|id| id != &order.id);
            if queue.is_empty() {
                levels.remove(&order.price);
                // Nothing is left here that we did not watch arrive, so the
                // level starts clean again.
                self.incomplete.remove(&(order.side, order.price));
            }
        }
    }

    /// Remove seeded orders that cross the book, and say how many.
    ///
    /// A real order book is never crossed, so an order sitting through the
    /// opposite side is wrong. When the crossing involves an order we only
    /// *assumed* was there, the assumption is what gives way: Bitstamp's REST
    /// book contains orders its stream never retracts, and seeding from it
    /// plants phantoms that no deletion ever arrives for. Measured: seeding a
    /// 25 second recording left the book crossed on 6,097 of 6,596 events,
    /// against a single transient crossing when starting from empty.
    ///
    /// It stops the moment the crossing is between two orders we watched
    /// arrive. That is a real crossing, and it stays flagged rather than being
    /// tidied away.
    pub fn prune_crossed_seeded(&mut self) -> usize {
        let mut pruned = 0;
        loop {
            let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) else {
                return pruned;
            };
            if bid <= ask {
                return pruned;
            }
            // Drop whichever side of the crossing we did not observe. If we
            // observed both, the crossing is real and not ours to remove.
            let bid_seeded = self.level_is_seeded(Side::Bid, bid);
            let ask_seeded = self.level_is_seeded(Side::Ask, ask);
            let side = match (bid_seeded, ask_seeded) {
                (true, _) => Side::Bid,
                (false, true) => Side::Ask,
                (false, false) => return pruned,
            };
            let price = if side == Side::Bid { bid } else { ask };
            let ids: Vec<OrderId> = self
                .levels(side)
                .get(&price)
                .map(|q| q.iter().cloned().collect())
                .unwrap_or_default();
            for id in ids {
                if let Some(order) = self.orders.get(&id).cloned()
                    && order.certainty == QueueCertainty::Seeded
                {
                    self.orders.remove(&id);
                    self.detach(&order);
                    pruned += 1;
                }
            }
            // Nothing removable at that level; leave the crossing flagged.
            if self.levels(side).contains_key(&price) {
                return pruned;
            }
        }
    }

    /// True when any order at this level came from a snapshot rather than being
    /// watched arrive.
    fn level_is_seeded(&self, side: Side, price: Fixed) -> bool {
        self.levels(side)
            .get(&price)
            .map(|q| {
                q.iter().any(|id| {
                    self.orders
                        .get(id)
                        .map(|o| o.certainty == QueueCertainty::Seeded)
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    }

    /// Aggregate into price levels: the L2 view of this book.
    ///
    /// This is what lets an L3 capture be checked against the venue's own L2
    /// feed, and what lets everything built on [`L2Book`] keep working.
    pub fn to_l2(&self, at: Timestamps) -> L2Book {
        let mut book = L2Book::new(self.symbol.clone());
        let aggregate = |levels: &BTreeMap<Fixed, VecDeque<OrderId>>,
                         orders: &HashMap<OrderId, Order>| {
            levels
                .iter()
                .filter_map(|(price, queue)| {
                    let total = queue
                        .iter()
                        .filter_map(|id| orders.get(id))
                        .fold(Fixed::ZERO, |acc, o| {
                            acc.checked_add(o.qty).unwrap_or(Fixed::MAX)
                        });
                    (!total.is_zero()).then_some((*price, total))
                })
                .collect::<Vec<_>>()
        };
        book.reset_from_snapshot(&BookSnapshot {
            symbol: self.symbol.clone(),
            bids: aggregate(&self.bids, &self.orders),
            asks: aggregate(&self.asks, &self.orders),
            seq: None,
            checksum: None,
            stamps: at,
        });
        book
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    fn sym() -> Symbol {
        Symbol::new("BTC", "USD")
    }

    fn at(n: u64) -> Stamp {
        Stamp {
            mono_nanos: n,
            wall_nanos: n as i64,
        }
    }

    fn event(
        id: &str,
        action: OrderAction,
        side: Side,
        price: &str,
        qty: &str,
        n: u64,
    ) -> OrderEvent {
        OrderEvent {
            symbol: sym(),
            order_id: OrderId::new(id),
            action,
            side,
            price: f(price),
            qty: f(qty),
            original_qty: Some(f(qty)),
            executed_qty: None,
            stamps: Timestamps::recv_only(at(n)),
            event_token: Some(n as u128),
            prev_event_token: (n > 0).then_some(n as u128 - 1),
            size_change_explained: true,
        }
    }

    fn seeded_book() -> L3Book {
        let mut book = L3Book::new(sym());
        book.apply(&event("a", OrderAction::Add, Side::Bid, "100", "1", 1));
        book.apply(&event("b", OrderAction::Add, Side::Bid, "100", "2", 2));
        book.apply(&event("c", OrderAction::Add, Side::Bid, "100", "3", 3));
        book.apply(&event("d", OrderAction::Add, Side::Ask, "101", "1", 4));
        book
    }

    #[test]
    fn orders_queue_in_the_order_they_arrived() {
        let book = seeded_book();
        let queue: Vec<&str> = book
            .queue_at(Side::Bid, f("100"))
            .iter()
            .map(|o| o.id.as_str())
            .collect();
        assert_eq!(queue, vec!["a", "b", "c"]);
    }

    #[test]
    fn queue_position_counts_the_quantity_that_must_trade_first() {
        let book = seeded_book();
        let pos = book.queue_position(&OrderId::new("c")).unwrap();
        assert_eq!(pos.index, 2);
        // 1 + 2 ahead of it.
        assert_eq!(pos.qty_ahead, f("3"));
        assert_eq!(pos.certainty, QueueCertainty::Observed);

        let front = book.queue_position(&OrderId::new("a")).unwrap();
        assert_eq!(front.index, 0);
        assert_eq!(front.qty_ahead, Fixed::ZERO);
    }

    #[test]
    fn removing_an_order_promotes_everything_behind_it() {
        let mut book = seeded_book();
        book.apply(&event("a", OrderAction::Execute, Side::Bid, "100", "0", 5));
        let pos = book.queue_position(&OrderId::new("c")).unwrap();
        assert_eq!(pos.index, 1);
        assert_eq!(pos.qty_ahead, f("2"));
        assert_eq!(book.stats().executed, 1);
    }

    #[test]
    fn a_seeded_order_never_claims_an_observed_position() {
        // The listing order of a snapshot is the venue's, not something we saw.
        let mut book = L3Book::new(sym());
        book.seed(
            [
                (OrderId::new("x"), Side::Bid, f("100"), f("1")),
                (OrderId::new("y"), Side::Bid, f("100"), f("2")),
            ],
            at(0),
        );
        let pos = book.queue_position(&OrderId::new("y")).unwrap();
        assert_eq!(pos.index, 1);
        assert_eq!(pos.certainty, QueueCertainty::Seeded);
        assert!(!pos.certainty.is_observed());
    }

    #[test]
    fn an_observed_order_behind_a_seeded_one_inherits_the_weaker_certainty() {
        // Knowing exactly where you are in the queue is worth nothing if the
        // sizes ahead of you are assumed.
        let mut book = L3Book::new(sym());
        book.seed([(OrderId::new("old"), Side::Bid, f("100"), f("5"))], at(0));
        book.apply(&event("new", OrderAction::Add, Side::Bid, "100", "1", 1));

        let new = book.queue_position(&OrderId::new("new")).unwrap();
        assert_eq!(new.index, 1);
        assert_eq!(new.qty_ahead, f("5"));
        assert_eq!(
            new.certainty,
            QueueCertainty::Seeded,
            "its own arrival was observed, but what is ahead of it was not"
        );
    }

    #[test]
    fn raising_size_forfeits_the_position_and_says_so() {
        let mut book = seeded_book();
        // `a` grows from 1 to 5.
        let mut grow = event("a", OrderAction::Modify, Side::Bid, "100", "5", 5);
        grow.original_qty = Some(f("1"));
        book.apply(&grow);

        let queue: Vec<&str> = book
            .queue_at(Side::Bid, f("100"))
            .iter()
            .map(|o| o.id.as_str())
            .collect();
        assert_eq!(queue, vec!["b", "c", "a"], "a must lose its place");
        let pos = book.queue_position(&OrderId::new("a")).unwrap();
        assert_eq!(
            pos.certainty,
            QueueCertainty::Unknown,
            "where a re-sized order lands is not something the feed says"
        );
    }

    #[test]
    fn shrinking_an_order_keeps_its_place() {
        let mut book = seeded_book();
        let shrink = event("b", OrderAction::Modify, Side::Bid, "100", "1", 5);
        book.apply(&shrink);
        let queue: Vec<&str> = book
            .queue_at(Side::Bid, f("100"))
            .iter()
            .map(|o| o.id.as_str())
            .collect();
        assert_eq!(queue, vec!["a", "b", "c"]);
        let pos = book.queue_position(&OrderId::new("b")).unwrap();
        assert_eq!(pos.certainty, QueueCertainty::Observed);
        assert_eq!(book.order(&OrderId::new("b")).unwrap().qty, f("1"));
    }

    #[test]
    fn an_event_for_an_unknown_order_is_flagged_not_ignored() {
        // Normal on a feed with no snapshot, and it has to be visible so a
        // consumer knows the book is not yet complete.
        let mut book = L3Book::new(sym());
        let outcome = book.apply(&event(
            "ghost",
            OrderAction::Cancel,
            Side::Bid,
            "100",
            "0",
            1,
        ));
        assert_eq!(
            outcome.anomalies,
            vec![L3Anomaly::UnknownOrder {
                id: OrderId::new("ghost"),
                action: OrderAction::Cancel
            }]
        );
        assert_eq!(book.stats().unknown_orders, 1);
        assert!(book.is_empty());
    }

    #[test]
    fn adding_the_same_order_twice_is_flagged() {
        let mut book = seeded_book();
        let outcome = book.apply(&event("a", OrderAction::Add, Side::Bid, "100", "1", 9));
        assert_eq!(
            outcome.anomalies,
            vec![L3Anomaly::DuplicateAdd {
                id: OrderId::new("a")
            }]
        );
        assert_eq!(book.queue_at(Side::Bid, f("100")).len(), 3);
    }

    #[test]
    fn a_price_change_moves_the_order_and_is_flagged() {
        let mut book = seeded_book();
        let outcome = book.apply(&event("a", OrderAction::Modify, Side::Bid, "99", "1", 5));
        assert!(matches!(
            outcome.anomalies.as_slice(),
            [L3Anomaly::PriceMoved { .. }]
        ));
        assert_eq!(book.queue_at(Side::Bid, f("100")).len(), 2);
        assert_eq!(book.queue_at(Side::Bid, f("99")).len(), 1);
    }

    #[test]
    fn a_crossed_book_is_flagged() {
        let mut book = seeded_book();
        let outcome = book.apply(&event("z", OrderAction::Add, Side::Bid, "102", "1", 6));
        assert!(
            outcome
                .anomalies
                .iter()
                .any(|a| matches!(a, L3Anomaly::Crossed { .. }))
        );
        assert!(book.is_crossed());
    }

    #[test]
    fn the_l2_view_aggregates_the_queue() {
        let book = seeded_book();
        let l2 = book.to_l2(Timestamps::recv_only(at(9)));
        assert_eq!(l2.best_bid(), Some((f("100"), f("6"))));
        assert_eq!(l2.best_ask(), Some((f("101"), f("1"))));
        assert_eq!(l2.level_count(Side::Bid), 1);
    }

    #[test]
    fn the_l2_view_tracks_removals() {
        let mut book = seeded_book();
        book.apply(&event("a", OrderAction::Cancel, Side::Bid, "100", "0", 5));
        book.apply(&event("d", OrderAction::Cancel, Side::Ask, "101", "0", 6));
        let l2 = book.to_l2(Timestamps::recv_only(at(9)));
        assert_eq!(l2.best_bid(), Some((f("100"), f("5"))));
        assert_eq!(l2.best_ask(), None, "the only ask left the book");
    }

    #[test]
    fn an_empty_level_disappears_rather_than_lingering_at_zero() {
        let mut book = seeded_book();
        for id in ["a", "b", "c"] {
            book.apply(&event(id, OrderAction::Cancel, Side::Bid, "100", "0", 7));
        }
        assert_eq!(book.best_bid(), None);
        assert!(book.queue_at(Side::Bid, f("100")).is_empty());
        assert_eq!(
            book.to_l2(Timestamps::recv_only(at(9)))
                .level_count(Side::Bid),
            0
        );
    }

    #[test]
    fn unexplained_size_changes_are_counted() {
        // Measured on Bitstamp: the resting quantity sometimes moves by more
        // than the reported trade explains, and then execute versus cancel is
        // not decidable from the feed.
        let mut book = seeded_book();
        let mut odd = event("b", OrderAction::Modify, Side::Bid, "100", "1", 5);
        odd.size_change_explained = false;
        book.apply(&odd);
        assert_eq!(book.stats().unexplained_size_changes, 1);
    }

    #[test]
    fn a_modify_to_zero_removes_the_order() {
        let mut book = seeded_book();
        book.apply(&event("b", OrderAction::Modify, Side::Bid, "100", "0", 5));
        assert!(book.order(&OrderId::new("b")).is_none());
        let queue: Vec<&str> = book
            .queue_at(Side::Bid, f("100"))
            .iter()
            .map(|o| o.id.as_str())
            .collect();
        assert_eq!(queue, vec!["a", "c"]);
    }

    #[test]
    fn peak_open_orders_is_tracked_for_the_load_report() {
        let mut book = seeded_book();
        assert_eq!(book.stats().peak_open_orders, 4);
        for id in ["a", "b", "c", "d"] {
            book.apply(&event(id, OrderAction::Cancel, Side::Bid, "100", "0", 8));
        }
        assert_eq!(book.stats().peak_open_orders, 4, "peak, not current");
    }
}
