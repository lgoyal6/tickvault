//! Our own orders against the recorded book, and how they fill.
//!
//! # Queue position here is approximate, and the name says so
//!
//! Every field that carries one is called `approx_queue_ahead`, because on an
//! aggregated feed it cannot be anything better. When an order rests at price
//! `p`, the quantity ahead of it is taken to be the size the venue was
//! displaying at `p`, which is a sum over orders we never saw individually. It
//! is then reduced by every observed decrease of that displayed size.
//!
//! That reduction is the approximation. A level shrinking on an aggregated feed
//! is either a trade or a cancellation and **the venue never says which**. Both
//! shorten the queue, so both reduce `approx_queue_ahead`; only a trade can
//! fill us. So the decrease is attributed proportionally: the share of it the
//! feed reports as traded is what can reach our order, and the rest is treated
//! as cancelled. On every recording in this experiment `traded_qty` is null, so
//! that share is nothing, and no maker fill here comes from a level decrease.
//! Writing a zero traded quantity instead of null would turn "the feed cannot
//! say" into "nothing traded", which is a claim and a false one.
//!
//! What does fill a resting order on this data is a **crossing**: the opposite
//! best arriving at or through the order's price is direct evidence that
//! somebody traded there. That volume consumes `approx_queue_ahead` first and
//! then fills us. It is a conservative rule, and it is the only fill evidence an
//! aggregated feed actually contains.
//!
//! # Two more approximations, stated rather than buried
//!
//! A marketable order walks the recorded book but does not remove those levels
//! from it, so the recording is not made shallower by our own trading. Order
//! size is held small for that reason and `extra_slippage_bps` stands in for the
//! impact that is not modelled.
//!
//! A snapshot message rebuilds the whole book, so any queue position held
//! across one is reseeded from the size the snapshot displays rather than
//! carried over. That is the archive's own `seeded` rather than `observed`
//! certainty, and it is counted.

use tickvault::book::L2Book;
use tickvault::{Fixed, Side};

use crate::costs::Costs;

pub type OwnOrderId = u64;

/// Whether a fill took liquidity or provided it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liquidity {
    Maker,
    Taker,
}

impl Liquidity {
    pub fn as_str(self) -> &'static str {
        match self {
            Liquidity::Maker => "maker",
            Liquidity::Taker => "taker",
        }
    }
}

/// What produced a maker fill. Recorded because the two are worth very
/// different amounts of trust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillCause {
    /// Volume the feed positively reported as traded, past our queue.
    ObservedTrade,
    /// The opposite best arrived at or through our price.
    Crossing,
    /// We crossed the spread ourselves.
    OwnAggression,
}

impl FillCause {
    pub fn as_str(self) -> &'static str {
        match self {
            FillCause::ObservedTrade => "observed_trade",
            FillCause::Crossing => "crossing",
            FillCause::OwnAggression => "own_aggression",
        }
    }
}

/// An order of ours resting in the book.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub id: OwnOrderId,
    pub side: Side,
    pub price: Fixed,
    pub size: f64,
    pub remaining: f64,
    /// Quantity believed to be ahead of this order at its price. Approximate;
    /// see the module documentation for exactly how much so.
    pub approx_queue_ahead: f64,
    pub placed_at: i64,
    pub entry_latency_ms: f64,
    /// True once any part of the queue in front of this order came from a
    /// snapshot rather than from watching it arrive.
    pub queue_reseeded: bool,
}

/// One simulated execution.
#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: OwnOrderId,
    pub at_recv_wall: i64,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub liquidity: Liquidity,
    pub cause: FillCause,
    pub approx_queue_ahead_at_fill: f64,
    pub latency_ms: f64,
}

/// What an order was, before it reached the venue.
#[derive(Debug, Clone, Copy)]
enum PendingKind {
    Maker { price: Fixed },
    Taker,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    id: OwnOrderId,
    side: Side,
    size: f64,
    kind: PendingKind,
    effective_at: i64,
    entry_latency_ms: f64,
}

/// A level as it stood before a message was applied.
#[derive(Debug, Clone, Copy)]
pub struct LevelBefore {
    pub side: Side,
    pub price: Fixed,
    pub qty: Fixed,
}

/// How many of each kind of thing happened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MarketCounts {
    pub orders_submitted: u64,
    pub orders_filled: u64,
    pub orders_fully_filled: u64,
    pub orders_cancelled: u64,
    pub maker_fills_from_trade: u64,
    pub maker_fills_from_crossing: u64,
    pub taker_fills: u64,
    /// Marketable orders the recorded book was too shallow to fill completely.
    pub taker_short_fills: u64,
    /// Queue positions reseeded from a snapshot rather than watched.
    pub queue_reseeds: u64,
    /// Messages that reported a traded quantity while no level shrank, so the
    /// archive cannot say which level it happened at.
    pub traded_qty_unlocated: u64,
}

/// The simulated exchange side of one run.
#[derive(Debug, Clone)]
pub struct Market {
    costs: Costs,
    tick: Fixed,
    next_id: OwnOrderId,
    pending: Vec<Pending>,
    pending_cancels: Vec<(i64, OwnOrderId)>,
    resting: Vec<RestingOrder>,
    pub fills: Vec<Fill>,
    pub cash: f64,
    pub inventory: f64,
    pub fees: f64,
    pub turnover: f64,
    pub counts: MarketCounts,
    last_mid: Option<f64>,
}

/// Round a price onto the venue's own grid, always away from the mid.
///
/// A bid rounds down and an ask rounds up, so rounding can only make a quote
/// more passive. Rounding the other way would invent fills.
pub fn round_to_tick(price: f64, tick: Fixed, side: Side) -> Fixed {
    let t = tick.mantissa().max(1);
    let raw = (price * 1e9).round() as i64;
    let remainder = raw.rem_euclid(t);
    let floor = raw - remainder;
    let mantissa = match side {
        Side::Bid => floor,
        Side::Ask => {
            if remainder == 0 {
                floor
            } else {
                floor + t
            }
        }
    };
    Fixed::from_mantissa(mantissa)
}

fn f(value: Fixed) -> f64 {
    value.to_f64_lossy()
}

impl Market {
    pub fn new(costs: Costs, tick: Fixed) -> Self {
        Market {
            costs,
            tick,
            next_id: 1,
            pending: Vec::new(),
            pending_cancels: Vec::new(),
            resting: Vec::new(),
            fills: Vec::new(),
            cash: 0.0,
            inventory: 0.0,
            fees: 0.0,
            turnover: 0.0,
            counts: MarketCounts::default(),
            last_mid: None,
        }
    }

    pub fn tick(&self) -> Fixed {
        self.tick
    }

    pub fn resting(&self) -> &[RestingOrder] {
        &self.resting
    }

    /// Size resting on one side, in base units.
    pub fn resting_size(&self, side: Side) -> f64 {
        self.resting
            .iter()
            .filter(|o| o.side == side)
            .map(|o| o.remaining)
            .sum()
    }

    /// Size submitted but not yet resting, on one side.
    pub fn pending_size(&self, side: Side) -> f64 {
        self.pending
            .iter()
            .filter(|p| p.side == side)
            .map(|p| p.size)
            .sum()
    }

    pub fn has_work_outstanding(&self) -> bool {
        !self.pending.is_empty() || !self.resting.is_empty()
    }

    /// Submit a resting quote. It reaches the book after `entry_nanos`.
    pub fn submit_maker(
        &mut self,
        side: Side,
        price: Fixed,
        size: f64,
        now: i64,
        entry_nanos: i64,
        entry_latency_ms: f64,
    ) -> OwnOrderId {
        let id = self.next_id;
        self.next_id += 1;
        self.counts.orders_submitted += 1;
        self.pending.push(Pending {
            id,
            side,
            size,
            kind: PendingKind::Maker { price },
            effective_at: now + entry_nanos,
            entry_latency_ms,
        });
        id
    }

    /// Submit a marketable order. It walks the book after `entry_nanos`.
    pub fn submit_taker(
        &mut self,
        side: Side,
        size: f64,
        now: i64,
        entry_nanos: i64,
        entry_latency_ms: f64,
    ) -> OwnOrderId {
        let id = self.next_id;
        self.next_id += 1;
        self.counts.orders_submitted += 1;
        self.pending.push(Pending {
            id,
            side,
            size,
            kind: PendingKind::Taker,
            effective_at: now + entry_nanos,
            entry_latency_ms,
        });
        id
    }

    /// Cancel a resting order. The cancel reaches the venue after `entry_nanos`
    /// too, so an order can still fill in the meantime. That is the whole point
    /// of modelling latency at all.
    pub fn cancel(&mut self, id: OwnOrderId, now: i64, entry_nanos: i64) {
        self.pending_cancels.push((now + entry_nanos, id));
    }

    /// Cancel everything, as the kill switch does.
    pub fn cancel_all(&mut self, now: i64, entry_nanos: i64) {
        let ids: Vec<OwnOrderId> = self
            .resting
            .iter()
            .map(|o| o.id)
            .chain(self.pending.iter().map(|p| p.id))
            .collect();
        for id in ids {
            self.cancel(id, now, entry_nanos);
        }
    }

    /// Change a resting order's price or size.
    ///
    /// Modelled as the venue does it: a reprice loses queue position entirely,
    /// and so does an increase in size. Only a reduction keeps it.
    pub fn modify(
        &mut self,
        id: OwnOrderId,
        new_price: Fixed,
        new_size: f64,
        now: i64,
        entry_nanos: i64,
        entry_latency_ms: f64,
    ) -> Option<OwnOrderId> {
        let index = self.resting.iter().position(|o| o.id == id)?;
        let existing = self.resting[index].clone();
        if new_price == existing.price && new_size <= existing.remaining {
            let order = &mut self.resting[index];
            order.remaining = new_size;
            order.size = new_size;
            return Some(id);
        }
        self.resting.remove(index);
        self.counts.orders_cancelled += 1;
        let new_id = self.submit_maker(
            existing.side,
            new_price,
            new_size,
            now,
            entry_nanos,
            entry_latency_ms,
        );
        Some(new_id)
    }

    /// Promote everything whose latency has elapsed.
    ///
    /// Called after a message has been applied, so an order that becomes live
    /// at the same instant cannot fill from the message that arrived then. That
    /// is the conservative direction.
    pub fn activate(&mut self, book: &L2Book, now: i64) {
        let mut due: Vec<(i64, OwnOrderId)> = Vec::new();
        self.pending_cancels.retain(|(at, id)| {
            if *at <= now {
                due.push((*at, *id));
                false
            } else {
                true
            }
        });
        for (_, id) in due {
            if let Some(index) = self.resting.iter().position(|o| o.id == id) {
                self.resting.remove(index);
                self.counts.orders_cancelled += 1;
            } else if let Some(index) = self.pending.iter().position(|p| p.id == id) {
                self.pending.remove(index);
                self.counts.orders_cancelled += 1;
            }
        }

        let mut ready: Vec<Pending> = Vec::new();
        self.pending.retain(|p| {
            if p.effective_at <= now {
                ready.push(*p);
                false
            } else {
                true
            }
        });
        for p in ready {
            match p.kind {
                PendingKind::Maker { price } => {
                    // Everything the venue is showing at this price is ahead of
                    // us: it was there first, and we cannot see the individual
                    // orders that make it up.
                    let ahead = book.qty_at(p.side, price).map(f).unwrap_or(0.0);
                    self.resting.push(RestingOrder {
                        id: p.id,
                        side: p.side,
                        price,
                        size: p.size,
                        remaining: p.size,
                        approx_queue_ahead: ahead,
                        placed_at: p.effective_at,
                        entry_latency_ms: p.entry_latency_ms,
                        queue_reseeded: false,
                    });
                }
                PendingKind::Taker => {
                    self.walk(p.id, p.side, p.size, book, now, p.entry_latency_ms);
                }
            }
        }
    }

    /// Consume the recorded book with a marketable order.
    fn walk(
        &mut self,
        id: OwnOrderId,
        side: Side,
        size: f64,
        book: &L2Book,
        now: i64,
        latency_ms: f64,
    ) {
        let opposite = match side {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        };
        let buying = side == Side::Bid;
        let levels = book.top(opposite, usize::MAX);
        let mut left = size;
        let mut any = false;
        for (price, qty) in levels {
            if left <= 0.0 {
                break;
            }
            let available = f(qty);
            if available <= 0.0 {
                continue;
            }
            let take = left.min(available);
            let paid = self.costs.slipped(buying, f(price));
            let fee = self.costs.taker_fee(paid * take);
            self.record_fill(Fill {
                order_id: id,
                at_recv_wall: now,
                side,
                price: paid,
                size: take,
                fee,
                liquidity: Liquidity::Taker,
                cause: FillCause::OwnAggression,
                // A marketable order joins no queue. Zero here is a fact about
                // the order, not a missing value.
                approx_queue_ahead_at_fill: 0.0,
                latency_ms,
            });
            left -= take;
            any = true;
        }
        if any {
            self.counts.taker_fills += 1;
            self.counts.orders_filled += 1;
            if left <= 1e-12 {
                self.counts.orders_fully_filled += 1;
            }
        }
        if left > 1e-12 {
            self.counts.taker_short_fills += 1;
        }
    }

    /// Reseed every queue position from a snapshot.
    ///
    /// A snapshot rebuilds the book, so a position held across one is the
    /// venue's listing order rather than something watched. The archive calls
    /// that `seeded` instead of `observed`, and so does this.
    pub fn reseed_queues(&mut self, book: &L2Book) {
        for order in &mut self.resting {
            order.approx_queue_ahead = book.qty_at(order.side, order.price).map(f).unwrap_or(0.0);
            if !order.queue_reseeded {
                order.queue_reseeded = true;
                self.counts.queue_reseeds += 1;
            }
        }
    }

    /// Work out what a message did to our resting orders.
    ///
    /// `before` holds the levels the message touched as they stood beforehand;
    /// `book` is the book the message produced. `traded` is what the feed says
    /// changed hands in this message, which is `None` on every aggregated feed.
    pub fn on_message(
        &mut self,
        before: &[LevelBefore],
        traded: Option<Fixed>,
        book: &L2Book,
        now: i64,
    ) {
        self.deplete_queues(before, traded, book, now);
        self.fill_from_crossings(book, now);
        self.last_mid = mid_of(book);
    }

    /// Reduce queue positions by what the level lost, and fill from whatever
    /// the feed positively reports as traded past our place in the queue.
    ///
    /// The split is by trade evidence, not pro rata: the reported traded
    /// quantity consumes the queue and is the only thing that can reach our
    /// order, and whatever decrease is left over is treated as a cancellation,
    /// which shortens the queue and fills nothing. A feed can report more
    /// traded than the displayed decrease, because orders can arrive and trade
    /// inside one message, so the traded quantity rather than the decrease is
    /// what bounds a fill.
    ///
    /// A message that reports a traded quantity while no level shrank leaves it
    /// unlocated. It is counted and not used: the feed said something traded
    /// and the book does not say where, and picking a level would be inventing
    /// the answer.
    fn deplete_queues(
        &mut self,
        before: &[LevelBefore],
        traded: Option<Fixed>,
        book: &L2Book,
        now: i64,
    ) {
        let mut decreases: Vec<(Side, Fixed, f64)> = Vec::new();
        let mut total_decrease = 0.0f64;
        for level in before {
            let after = book.qty_at(level.side, level.price).map(f).unwrap_or(0.0);
            let decrease = f(level.qty) - after;
            if decrease > 0.0 {
                decreases.push((level.side, level.price, decrease));
                total_decrease += decrease;
            }
        }
        let traded_total = traded.map(f).unwrap_or(0.0);
        if decreases.is_empty() {
            if traded_total > 0.0 {
                self.counts.traded_qty_unlocated += 1;
            }
            return;
        }

        let mut fills: Vec<Fill> = Vec::new();
        for (side, price, decrease) in decreases {
            // The share of this message's traded quantity that belongs to this
            // level, in proportion to how much the level lost. On an aggregated
            // feed `traded_total` is zero because the feed said nothing, which
            // is why no fill comes out of here.
            let traded_here = if total_decrease > 0.0 {
                traded_total * decrease / total_decrease
            } else {
                0.0
            };
            for order in &mut self.resting {
                if order.side != side || order.price != price || order.remaining <= 0.0 {
                    continue;
                }
                let queue_before = order.approx_queue_ahead;
                let consumed_by_trade = queue_before.min(traded_here);
                let fillable = traded_here - consumed_by_trade;
                // Whatever the level lost beyond what traded was cancelled. It
                // shortens the queue and fills nothing, and the feed does not
                // say which orders it was.
                let cancelled = (decrease - traded_here).max(0.0);
                order.approx_queue_ahead = (queue_before - consumed_by_trade - cancelled).max(0.0);
                if fillable <= 0.0 {
                    continue;
                }
                let take = order.remaining.min(fillable);
                if take <= 0.0 {
                    continue;
                }
                let notional = f(price) * take;
                fills.push(Fill {
                    order_id: order.id,
                    at_recv_wall: now,
                    side,
                    price: f(price),
                    size: take,
                    fee: self.costs.maker_fee(notional),
                    liquidity: Liquidity::Maker,
                    cause: FillCause::ObservedTrade,
                    approx_queue_ahead_at_fill: queue_before,
                    latency_ms: order.entry_latency_ms,
                });
                order.remaining -= take;
            }
        }
        for fill in fills {
            self.counts.maker_fills_from_trade += 1;
            self.settle_maker_fill(fill);
        }
        self.resting.retain(|o| o.remaining > 1e-12);
    }

    /// Fill resting orders the market has traded at or through.
    ///
    /// The opposite best arriving at or through our price is the one piece of
    /// positive trade evidence an aggregated feed carries. That volume consumes
    /// the queue in front of us first.
    fn fill_from_crossings(&mut self, book: &L2Book, now: i64) {
        let mut fills: Vec<Fill> = Vec::new();
        for order in &mut self.resting {
            if order.remaining <= 0.0 {
                continue;
            }
            let available = match order.side {
                Side::Bid => book
                    .top(Side::Ask, usize::MAX)
                    .into_iter()
                    .take_while(|(price, _)| *price <= order.price)
                    .map(|(_, qty)| f(qty))
                    .sum::<f64>(),
                Side::Ask => book
                    .top(Side::Bid, usize::MAX)
                    .into_iter()
                    .take_while(|(price, _)| *price >= order.price)
                    .map(|(_, qty)| f(qty))
                    .sum::<f64>(),
            };
            if available <= 0.0 {
                continue;
            }
            let queue_before = order.approx_queue_ahead;
            let from_queue = queue_before.min(available);
            order.approx_queue_ahead = queue_before - from_queue;
            let past_queue = available - from_queue;
            if past_queue <= 0.0 {
                continue;
            }
            let take = order.remaining.min(past_queue);
            if take <= 0.0 {
                continue;
            }
            let notional = f(order.price) * take;
            fills.push(Fill {
                order_id: order.id,
                at_recv_wall: now,
                side: order.side,
                price: f(order.price),
                size: take,
                fee: self.costs.maker_fee(notional),
                liquidity: Liquidity::Maker,
                cause: FillCause::Crossing,
                approx_queue_ahead_at_fill: queue_before,
                latency_ms: order.entry_latency_ms,
            });
            order.remaining -= take;
        }
        for fill in fills {
            self.counts.maker_fills_from_crossing += 1;
            self.settle_maker_fill(fill);
        }
        self.resting.retain(|o| o.remaining > 1e-12);
    }

    fn settle_maker_fill(&mut self, fill: Fill) {
        let fully = self
            .resting
            .iter()
            .find(|o| o.id == fill.order_id)
            .map(|o| o.remaining <= 1e-12)
            .unwrap_or(true);
        self.counts.orders_filled += 1;
        if fully {
            self.counts.orders_fully_filled += 1;
        }
        self.record_fill(fill);
    }

    fn record_fill(&mut self, fill: Fill) {
        let notional = fill.price * fill.size;
        match fill.side {
            Side::Bid => {
                self.cash -= notional;
                self.inventory += fill.size;
            }
            Side::Ask => {
                self.cash += notional;
                self.inventory -= fill.size;
            }
        }
        self.cash -= fill.fee;
        self.fees += fill.fee;
        self.turnover += notional.abs();
        self.fills.push(fill);
    }

    /// Cash plus inventory marked at `mid`.
    ///
    /// `None` when there is inventory to mark and no mid to mark it at. A
    /// one-sided book has no mid, and writing a zero there would be a claim
    /// about a price nobody quoted.
    pub fn equity(&self, mid: Option<f64>) -> Option<f64> {
        if self.inventory.abs() <= 1e-12 {
            return Some(self.cash);
        }
        mid.or(self.last_mid)
            .map(|m| self.cash + self.inventory * m)
    }

    pub fn last_mid(&self) -> Option<f64> {
        self.last_mid
    }
}

/// The mid of a book, or nothing.
///
/// A one-sided book has no mid. The archive's schema says so and so does this.
pub fn mid_of(book: &L2Book) -> Option<f64> {
    match (book.best_bid(), book.best_ask()) {
        (Some((bid, _)), Some((ask, _))) => Some((f(bid) + f(ask)) / 2.0),
        _ => None,
    }
}

/// The levels a message is about to touch, as they stand now.
pub fn levels_before(book: &L2Book, rows: &[tickvault::store::rows::Row]) -> Vec<LevelBefore> {
    rows.iter()
        .map(|row| LevelBefore {
            side: row.side,
            price: row.price,
            qty: book.qty_at(row.side, row.price).unwrap_or(Fixed::ZERO),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickvault::Symbol;
    use tickvault::book::{BookDelta, LevelChange};
    use tickvault::clock::{Stamp, Timestamps};

    fn sym() -> Symbol {
        Symbol::new("BTC", "USD")
    }

    fn tick() -> Fixed {
        // A cent.
        Fixed::from_mantissa(10_000_000)
    }

    fn costs() -> Costs {
        Costs {
            taker_fee_bps: 5.0,
            maker_rebate_bps: 0.0,
            extra_slippage_bps: 0.5,
        }
    }

    fn px(units: i64) -> Fixed {
        Fixed::from_mantissa(units * 1_000_000_000)
    }

    fn qty(thousandths: i64) -> Fixed {
        Fixed::from_mantissa(thousandths * 1_000_000)
    }

    fn apply(book: &mut L2Book, changes: Vec<LevelChange>, at: i64) {
        book.apply_delta(&BookDelta {
            symbol: sym(),
            changes,
            seq: None,
            checksum: None,
            stamps: Timestamps::recv_only(Stamp {
                mono_nanos: 0,
                wall_nanos: at,
            }),
            prev_seq: None,
            first_seq: None,
        });
    }

    /// A two-sided book: bids at 100 and 99, asks at 101 and 102.
    fn book() -> L2Book {
        let mut book = L2Book::new(sym());
        apply(
            &mut book,
            vec![
                LevelChange::new(Side::Bid, px(100), qty(2_000)),
                LevelChange::new(Side::Bid, px(99), qty(3_000)),
                LevelChange::new(Side::Ask, px(101), qty(2_000)),
                LevelChange::new(Side::Ask, px(102), qty(3_000)),
            ],
            1,
        );
        book
    }

    #[test]
    fn a_quote_is_rounded_away_from_the_mid() {
        // 100.004 as a bid must not become 100.01: that would be a better price
        // than asked for and would invent fills.
        assert_eq!(round_to_tick(100.004, tick(), Side::Bid), px(100));
        assert_eq!(
            round_to_tick(100.004, tick(), Side::Ask).mantissa(),
            100_010_000_000
        );
        assert_eq!(round_to_tick(100.0, tick(), Side::Ask), px(100));
    }

    #[test]
    fn an_order_does_not_exist_until_its_latency_has_elapsed() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 1_000, 5_000_000, 5.0);
        market.activate(&book, 1_000);
        assert!(market.resting().is_empty(), "it cannot be there yet");
        market.activate(&book, 6_000_001);
        assert_eq!(market.resting().len(), 1);
    }

    #[test]
    fn a_resting_order_starts_behind_everything_the_venue_displays() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        // Two units were displayed at 100 before we arrived.
        assert!((market.resting()[0].approx_queue_ahead - 2.0).abs() < 1e-9);
    }

    #[test]
    fn a_quote_inside_the_spread_has_nothing_ahead_of_it() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        market.submit_maker(
            Side::Bid,
            Fixed::from_mantissa(100_500_000_000),
            0.005,
            0,
            0,
            3.0,
        );
        market.activate(&book, 0);
        assert_eq!(market.resting()[0].approx_queue_ahead, 0.0);
    }

    #[test]
    fn a_level_decrease_shortens_the_queue_and_fills_nothing_without_trade_evidence() {
        // The whole aggregated-feed problem in one test. Two units vanish from
        // in front of us and the feed does not say whether they traded or were
        // cancelled, so the queue shortens and we do not fill.
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        let rows_before = vec![LevelBefore {
            side: Side::Bid,
            price: px(100),
            qty: qty(2_000),
        }];
        apply(
            &mut book,
            vec![LevelChange::new(Side::Bid, px(100), qty(500))],
            2,
        );
        market.on_message(&rows_before, None, &book, 2);
        assert!(market.fills.is_empty(), "no trade evidence, no fill");
        assert!((market.resting()[0].approx_queue_ahead - 0.5).abs() < 1e-9);
    }

    #[test]
    fn a_trade_that_reaches_exactly_our_place_in_the_queue_does_not_fill_us() {
        // We are behind all two units the venue displayed. A trade of exactly
        // two units consumes the queue and reaches us with nothing left, so it
        // fills nothing. Filling here would be giving ourselves a place in the
        // queue we did not have.
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        let before = vec![LevelBefore {
            side: Side::Bid,
            price: px(100),
            qty: qty(2_000),
        }];
        apply(
            &mut book,
            vec![LevelChange::new(Side::Bid, px(100), Fixed::ZERO)],
            2,
        );
        market.on_message(&before, Some(qty(2_000)), &book, 2);
        assert!(market.fills.is_empty());
        assert_eq!(market.resting()[0].approx_queue_ahead, 0.0);
    }

    #[test]
    fn traded_volume_past_the_queue_fills_the_order() {
        // A feed that reports executions can report more traded than the
        // displayed level lost, because orders arrive and trade inside one
        // message. Three units trade where two were displayed: two consume the
        // queue in front of us and the third reaches us.
        //
        // This path is implemented and inert on every recording in this
        // experiment, which is exactly why it is tested rather than assumed.
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        let before = vec![LevelBefore {
            side: Side::Bid,
            price: px(100),
            qty: qty(2_000),
        }];
        apply(
            &mut book,
            vec![LevelChange::new(Side::Bid, px(100), Fixed::ZERO)],
            2,
        );
        market.on_message(&before, Some(qty(3_000)), &book, 2);
        assert_eq!(market.counts.maker_fills_from_trade, 1);
        let fill = &market.fills[0];
        assert_eq!(fill.cause, FillCause::ObservedTrade);
        assert!((fill.size - 0.005).abs() < 1e-9);
        // The queue we were behind is what the fill records, not zero.
        assert!((fill.approx_queue_ahead_at_fill - 2.0).abs() < 1e-9);
        assert!((market.inventory - 0.005).abs() < 1e-9);
    }

    #[test]
    fn a_traded_quantity_with_nowhere_to_put_it_is_counted_not_guessed() {
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        // The level grows, and the feed still claims something traded. Where it
        // traded is not recoverable, so nothing fills.
        let before = vec![LevelBefore {
            side: Side::Bid,
            price: px(100),
            qty: qty(2_000),
        }];
        apply(
            &mut book,
            vec![LevelChange::new(Side::Bid, px(100), qty(4_000))],
            2,
        );
        market.on_message(&before, Some(qty(1_000)), &book, 2);
        assert!(market.fills.is_empty());
        assert_eq!(market.counts.traded_qty_unlocated, 1);
        assert!((market.resting()[0].approx_queue_ahead - 2.0).abs() < 1e-9);
    }

    #[test]
    fn a_cancellation_ahead_of_us_never_fills_us_however_large_it_is() {
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        let before = vec![LevelBefore {
            side: Side::Bid,
            price: px(100),
            qty: qty(2_000),
        }];
        // The level empties and the feed says nothing traded, which it can only
        // say on a feed that reports executions at all.
        apply(
            &mut book,
            vec![LevelChange::new(Side::Bid, px(100), Fixed::ZERO)],
            2,
        );
        market.on_message(&before, Some(Fixed::ZERO), &book, 2);
        assert!(market.fills.is_empty());
        assert_eq!(market.resting()[0].approx_queue_ahead, 0.0);
    }

    #[test]
    fn a_crossing_fills_a_resting_bid_at_its_own_price() {
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        // Quote inside the spread, so nothing is ahead of us.
        let price = Fixed::from_mantissa(100_500_000_000);
        market.submit_maker(Side::Bid, price, 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        // The ask side comes down through our bid.
        let before = vec![LevelBefore {
            side: Side::Ask,
            price: px(100),
            qty: Fixed::ZERO,
        }];
        apply(
            &mut book,
            vec![LevelChange::new(Side::Ask, px(100), qty(1_000))],
            3,
        );
        market.on_message(&before, None, &book, 3);
        assert_eq!(market.counts.maker_fills_from_crossing, 1);
        let fill = &market.fills[0];
        assert_eq!(fill.cause, FillCause::Crossing);
        // A maker gets the price it quoted, not the crossing price.
        assert!((fill.price - 100.5).abs() < 1e-9);
        assert_eq!(fill.liquidity, Liquidity::Maker);
    }

    #[test]
    fn a_crossing_consumes_the_queue_before_it_reaches_us() {
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        // At the touch, behind two units.
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        let before = vec![LevelBefore {
            side: Side::Ask,
            price: px(100),
            qty: Fixed::ZERO,
        }];
        // One unit crosses, which is less than the two ahead of us.
        apply(
            &mut book,
            vec![LevelChange::new(Side::Ask, px(100), qty(1_000))],
            3,
        );
        market.on_message(&before, None, &book, 3);
        assert!(market.fills.is_empty(), "the queue was not exhausted");
        assert!((market.resting()[0].approx_queue_ahead - 1.0).abs() < 1e-9);
    }

    #[test]
    fn a_partial_fill_leaves_the_rest_resting() {
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        let price = Fixed::from_mantissa(100_500_000_000);
        market.submit_maker(Side::Bid, price, 0.010, 0, 0, 3.0);
        market.activate(&book, 0);
        let before = vec![LevelBefore {
            side: Side::Ask,
            price: px(100),
            qty: Fixed::ZERO,
        }];
        // Only six thousandths of a unit is available to cross.
        apply(
            &mut book,
            vec![LevelChange::new(Side::Ask, px(100), qty(6))],
            3,
        );
        market.on_message(&before, None, &book, 3);
        assert_eq!(market.fills.len(), 1);
        assert!((market.fills[0].size - 0.006).abs() < 1e-9);
        assert_eq!(market.resting().len(), 1);
        assert!((market.resting()[0].remaining - 0.004).abs() < 1e-9);
        assert_eq!(market.counts.orders_fully_filled, 0);
    }

    #[test]
    fn a_marketable_order_walks_the_book_and_pays_for_it() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        // Three units, so it eats all of the 101 level and part of 102.
        market.submit_taker(Side::Bid, 3.5, 0, 0, 3.0);
        market.activate(&book, 0);
        assert_eq!(market.fills.len(), 2);
        assert!(market.fills[0].price > 101.0, "slippage moves against us");
        assert!(market.fills[1].price > 102.0);
        assert!((market.inventory - 3.5).abs() < 1e-9);
        assert!(market.fees > 0.0);
        assert_eq!(market.counts.taker_short_fills, 0);
        assert_eq!(market.counts.taker_fills, 1);
    }

    #[test]
    fn a_book_too_shallow_to_fill_says_so_rather_than_inventing_depth() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        market.submit_taker(Side::Bid, 99.0, 0, 0, 3.0);
        market.activate(&book, 0);
        assert_eq!(market.counts.taker_short_fills, 1);
        // Five units is all the recording showed on the ask side.
        assert!((market.inventory - 5.0).abs() < 1e-9);
    }

    #[test]
    fn a_reprice_forfeits_queue_position() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        let id = market.submit_maker(Side::Bid, px(99), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        assert!((market.resting()[0].approx_queue_ahead - 3.0).abs() < 1e-9);
        let new_id = market
            .modify(id, px(100), 0.005, 10, 0, 3.0)
            .expect("the order exists");
        assert_ne!(new_id, id);
        market.activate(&book, 10);
        // Behind the two units at 100 now, not still behind three at 99.
        assert!((market.resting()[0].approx_queue_ahead - 2.0).abs() < 1e-9);
    }

    #[test]
    fn shrinking_an_order_keeps_its_place() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        let id = market.submit_maker(Side::Bid, px(100), 0.010, 0, 0, 3.0);
        market.activate(&book, 0);
        let same = market.modify(id, px(100), 0.004, 10, 0, 3.0).unwrap();
        assert_eq!(same, id);
        assert!((market.resting()[0].remaining - 0.004).abs() < 1e-9);
        assert!((market.resting()[0].approx_queue_ahead - 2.0).abs() < 1e-9);
    }

    #[test]
    fn a_cancel_takes_as_long_to_arrive_as_an_order_does() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        let id = market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        market.cancel(id, 10, 5_000_000);
        market.activate(&book, 11);
        assert_eq!(market.resting().len(), 1, "the cancel is still in flight");
        market.activate(&book, 5_000_011);
        assert!(market.resting().is_empty());
        assert_eq!(market.counts.orders_cancelled, 1);
    }

    #[test]
    fn a_snapshot_reseeds_the_queue_rather_than_carrying_it_over() {
        let mut market = Market::new(costs(), tick());
        let mut book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        apply(
            &mut book,
            vec![LevelChange::new(Side::Bid, px(100), qty(9_000))],
            5,
        );
        market.reseed_queues(&book);
        assert!((market.resting()[0].approx_queue_ahead - 9.0).abs() < 1e-9);
        assert!(market.resting()[0].queue_reseeded);
        assert_eq!(market.counts.queue_reseeds, 1);
    }

    #[test]
    fn a_one_sided_book_has_no_mid_and_inventory_cannot_be_marked() {
        let mut one_sided = L2Book::new(sym());
        apply(
            &mut one_sided,
            vec![LevelChange::new(Side::Bid, px(100), qty(1_000))],
            1,
        );
        assert_eq!(mid_of(&one_sided), None);
        let mut market = Market::new(costs(), tick());
        assert_eq!(market.equity(None), Some(0.0), "flat needs no mark");
        market.inventory = 0.005;
        assert_eq!(market.equity(None), None, "a mark with no mid is a fiction");
        assert_eq!(market.equity(Some(100.0)), Some(0.5));
    }

    #[test]
    fn the_kill_switch_cancels_everything_resting_and_in_flight() {
        let mut market = Market::new(costs(), tick());
        let book = book();
        market.submit_maker(Side::Bid, px(100), 0.005, 0, 0, 3.0);
        market.activate(&book, 0);
        market.submit_maker(Side::Ask, px(101), 0.005, 0, 10_000_000, 3.0);
        market.cancel_all(1, 0);
        market.activate(&book, 1);
        assert!(market.resting().is_empty());
        assert!(!market.has_work_outstanding());
        assert_eq!(market.counts.orders_cancelled, 2);
    }
}
