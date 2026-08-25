//! Aggregations computed from the real book.
//!
//! Everything here is derived from the book as it actually stood, not from
//! bars. That is the point: a bar built from a bar has already lost the thing a
//! microstructure question is about.
//!
//! # Volume
//!
//! An OHLCV bar normally carries traded volume, and this archive is a *book*
//! archive. An aggregated feed shows a level shrinking and never says whether
//! it traded or was cancelled, so the volume of a Kraken or Coinbase bar is not
//! something the data contains. It is reported as `None` rather than zero,
//! because zero would claim nothing traded.
//!
//! An order-by-order feed does report executions, so a Bitstamp L3 bar carries
//! a real traded quantity. The same struct, honest in both cases, with the
//! difference visible instead of averaged away.

use crate::Fixed;
use crate::book::L2Book;
use crate::query::{BookCursor, Tick};
use crate::types::Side;

/// Best bid and ask, if either side has anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopOfBook {
    pub bid: Option<(Fixed, Fixed)>,
    pub ask: Option<(Fixed, Fixed)>,
}

impl TopOfBook {
    pub fn of(book: &L2Book) -> Self {
        TopOfBook {
            bid: book.best_bid(),
            ask: book.best_ask(),
        }
    }

    /// Halfway between the touch. `None` when either side is empty, because a
    /// mid with one side missing is not a mid.
    pub fn mid(&self) -> Option<Fixed> {
        Some(self.bid?.0.midpoint(self.ask?.0))
    }

    pub fn spread(&self) -> Option<Fixed> {
        self.ask?.0.checked_sub(self.bid?.0)
    }

    /// Spread as a fraction of the mid, in basis points.
    pub fn spread_bps(&self) -> Option<f64> {
        let mid = self.mid()?;
        if mid.is_zero() {
            return None;
        }
        Some(self.spread()?.to_f64_lossy() / mid.to_f64_lossy() * 10_000.0)
    }
}

/// Resting quantity on each side of the top `depth` levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Depth {
    pub bid_qty: Fixed,
    pub ask_qty: Fixed,
    pub bid_levels: usize,
    pub ask_levels: usize,
}

impl Depth {
    pub fn of(book: &L2Book, depth: usize) -> Self {
        let sum = |side| {
            book.top(side, depth)
                .into_iter()
                .fold(Fixed::ZERO, |acc, (_, qty)| {
                    acc.checked_add(qty).unwrap_or(acc)
                })
        };
        Depth {
            bid_qty: sum(Side::Bid),
            ask_qty: sum(Side::Ask),
            bid_levels: book.level_count(Side::Bid).min(depth),
            ask_levels: book.level_count(Side::Ask).min(depth),
        }
    }

    /// Order book imbalance: `(bid - ask) / (bid + ask)`, in `[-1, 1]`.
    ///
    /// A ratio is a real number rather than a price, so this is the one place a
    /// float is the right type. `None` when both sides are empty, where the
    /// ratio is undefined rather than zero.
    pub fn imbalance(&self) -> Option<f64> {
        let total = self.bid_qty.checked_add(self.ask_qty)?;
        if total.is_zero() {
            return None;
        }
        let diff = self.bid_qty.checked_sub(self.ask_qty)?;
        Some(diff.to_f64_lossy() / total.to_f64_lossy())
    }
}

/// Quantity resting within a price offset of the mid.
///
/// The question "how much can I trade before moving the price by X" answered
/// from the real book. Levels are included whole: a level at exactly the
/// boundary counts, and one beyond it does not, rather than being pro-rated.
pub fn depth_within(book: &L2Book, offset: Fixed) -> Option<Depth> {
    let top = TopOfBook::of(book);
    let mid = top.mid()?;
    let floor = mid.checked_sub(offset)?;
    let ceiling = mid.checked_add(offset)?;

    let mut out = Depth {
        bid_qty: Fixed::ZERO,
        ask_qty: Fixed::ZERO,
        bid_levels: 0,
        ask_levels: 0,
    };
    for (price, qty) in book.top(Side::Bid, usize::MAX) {
        if price < floor {
            break;
        }
        out.bid_qty = out.bid_qty.checked_add(qty).unwrap_or(out.bid_qty);
        out.bid_levels += 1;
    }
    for (price, qty) in book.top(Side::Ask, usize::MAX) {
        if price > ceiling {
            break;
        }
        out.ask_qty = out.ask_qty.checked_add(qty).unwrap_or(out.ask_qty);
        out.ask_levels += 1;
    }
    Some(out)
}

/// One interval's worth of book activity.
#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    /// Inclusive start of the interval, in wall nanoseconds.
    pub start_wall: i64,
    /// Exclusive end.
    pub end_wall: i64,
    /// Open, high, low and close of the **mid price**, sampled from the book
    /// after every message.
    pub open: Fixed,
    pub high: Fixed,
    pub low: Fixed,
    pub close: Fixed,
    /// Messages that landed in the interval.
    pub updates: u64,
    /// Messages the recorder could not vouch for.
    pub suspect_updates: u64,
    /// Quantity traded, where the feed reports executions.
    ///
    /// `None` on an aggregated feed. Not zero: an aggregated feed does not know
    /// whether a level shrank because it traded, and zero would say it did not.
    pub traded_qty: Option<Fixed>,
    /// Mean spread over the interval, in basis points.
    pub mean_spread_bps: Option<f64>,
}

impl Bar {
    /// True when any message in this bar was inside a suspect window.
    pub fn is_suspect(&self) -> bool {
        self.suspect_updates > 0
    }
}

/// Accumulates bars at a fixed interval.
#[derive(Debug, Clone)]
pub struct BarBuilder {
    interval: i64,
    current: Option<Bar>,
    spread_sum: f64,
    spread_count: u64,
    finished: Vec<Bar>,
    /// Volume seen in this bucket before any two-sided book existed.
    ///
    /// A bar needs a mid to carry prices, but a trade does not need a mid to
    /// have happened. On a feed that starts empty and fills in, the first
    /// executions arrive while one side of the book is still unknown, and
    /// dropping the tick outright would take real volume with it.
    pending_traded: Option<Fixed>,
    pending_bucket: i64,
    unattributed_traded: Fixed,
}

impl BarBuilder {
    /// `interval_nanos` may be any width; nothing here assumes a round number.
    pub fn new(interval_nanos: i64) -> Self {
        assert!(interval_nanos > 0, "a bar interval must be positive");
        BarBuilder {
            interval: interval_nanos,
            current: None,
            spread_sum: 0.0,
            spread_count: 0,
            finished: Vec::new(),
            pending_traded: None,
            pending_bucket: i64::MIN,
            unattributed_traded: Fixed::ZERO,
        }
    }

    /// Volume that happened in a bucket which never produced a bar.
    ///
    /// Only reachable when a whole interval passes with trades but without the
    /// book ever having two sides. It is reported rather than discarded so the
    /// sum of the volume column can be checked against it.
    pub fn unattributed_traded(&self) -> Fixed {
        self.unattributed_traded
    }

    fn bucket(&self, at: i64) -> i64 {
        // Floor division, so instants before the epoch bucket downwards rather
        // than towards zero.
        at.div_euclid(self.interval) * self.interval
    }

    /// Fold one message and the book that followed it into the bars.
    pub fn observe(&mut self, tick: &Tick, book: &L2Book) {
        let top = TopOfBook::of(book);
        let start = self.bucket(tick.at_wall);
        if start != self.pending_bucket {
            // Whatever was held for the previous bucket never found a bar.
            if let Some(orphan) = self.pending_traded.take() {
                self.unattributed_traded = self
                    .unattributed_traded
                    .checked_add(orphan)
                    .unwrap_or(self.unattributed_traded);
            }
            self.pending_bucket = start;
        }
        let Some(mid) = top.mid() else {
            // One-sided book: there is no mid to record, and inventing one
            // would put a price in the bar that never existed. The trade still
            // happened, so its size waits here for a bar to attach it to.
            if let Some(traded) = tick.traded_qty {
                let held = self.pending_traded.get_or_insert(Fixed::ZERO);
                *held = held.checked_add(traded).unwrap_or(*held);
            }
            return;
        };

        if self.current.as_ref().is_some_and(|b| b.start_wall != start) {
            self.close_current();
        }
        let carried = self.pending_traded.take();
        let bar = self.current.get_or_insert_with(|| Bar {
            start_wall: start,
            end_wall: start + self.interval,
            open: mid,
            high: mid,
            low: mid,
            close: mid,
            updates: 0,
            suspect_updates: 0,
            traded_qty: tick
                .traded_qty
                .map(|_| Fixed::ZERO)
                .or(carried.map(|_| Fixed::ZERO)),
            mean_spread_bps: None,
        });
        if let (Some(total), Some(held)) = (bar.traded_qty.as_mut(), carried) {
            *total = total.checked_add(held).unwrap_or(*total);
        }

        bar.high = bar.high.max(mid);
        bar.low = bar.low.min(mid);
        bar.close = mid;
        bar.updates += 1;
        if tick.suspect {
            bar.suspect_updates += 1;
        }
        if let (Some(total), Some(traded)) = (bar.traded_qty.as_mut(), tick.traded_qty) {
            *total = total.checked_add(traded).unwrap_or(*total);
        }
        if let Some(bps) = top.spread_bps() {
            self.spread_sum += bps;
            self.spread_count += 1;
        }
    }

    fn close_current(&mut self) {
        if let Some(mut bar) = self.current.take() {
            if self.spread_count > 0 {
                bar.mean_spread_bps = Some(self.spread_sum / self.spread_count as f64);
            }
            self.spread_sum = 0.0;
            self.spread_count = 0;
            self.finished.push(bar);
        }
    }

    /// Finish and return every bar, in time order.
    ///
    /// Intervals with no messages are absent rather than filled in. A bar that
    /// was never observed is not the same as a flat one, and inventing it would
    /// hide exactly the outages this archive exists to publish.
    pub fn finish(mut self) -> Vec<Bar> {
        self.close_current();
        self.finished
    }
}

/// Build bars over a whole query range, streaming.
pub fn bars(cursor: &mut BookCursor, interval_nanos: i64) -> crate::error::Result<Vec<Bar>> {
    let mut builder = BarBuilder::new(interval_nanos);
    while let Some(tick) = cursor.advance()? {
        let book = cursor.book();
        builder.observe(&tick, book);
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::BookSnapshot;
    use crate::clock::{Stamp, Timestamps};
    use crate::store::schema::EventKind;
    use crate::types::Symbol;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    fn book(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> L2Book {
        let symbol = Symbol::new("BTC", "USD");
        let mut b = L2Book::new(symbol.clone());
        b.reset_from_snapshot(&BookSnapshot {
            symbol,
            bids: bids.iter().map(|(p, q)| (f(p), f(q))).collect(),
            asks: asks.iter().map(|(p, q)| (f(p), f(q))).collect(),
            seq: None,
            checksum: None,
            stamps: Timestamps::recv_only(Stamp::ZERO),
        });
        b
    }

    fn tick(at: i64, traded: Option<&str>) -> Tick {
        Tick {
            at_wall: at,
            venue_ts: None,
            kind: EventKind::Delta,
            changes: 1,
            suspect: false,
            traded_qty: traded.map(f),
        }
    }

    #[test]
    fn a_trade_before_the_book_has_two_sides_still_counts() {
        // A feed with no snapshot starts empty and fills in, so the first
        // executions land while one side is still unknown. There is no mid to
        // put in a bar, but the trade happened and its size is real.
        let mut builder = BarBuilder::new(1_000_000_000);
        let one_sided = book(&[("100", "1")], &[]);
        builder.observe(&tick(0, Some("0.001")), &one_sided);

        let two_sided = book(&[("100", "1")], &[("102", "2")]);
        builder.observe(&tick(500_000_000, Some("0.009")), &two_sided);
        let bars = builder.finish();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].traded_qty, Some(f("0.01")));
    }

    #[test]
    fn volume_in_a_bucket_that_never_opened_a_bar_is_reported_not_lost() {
        let mut builder = BarBuilder::new(1_000_000_000);
        let one_sided = book(&[("100", "1")], &[]);
        builder.observe(&tick(0, Some("0.001")), &one_sided);
        // A whole interval later, still one-sided: the first bucket is over.
        builder.observe(&tick(2_000_000_000, None), &one_sided);
        assert_eq!(builder.unattributed_traded(), f("0.001"));
        assert!(builder.clone().finish().is_empty());
    }

    #[test]
    fn an_aggregated_feed_carries_no_pending_volume_at_all() {
        // The pending path must never invent a volume for a venue that cannot
        // report one, or an L2 bar would come out claiming zero traded.
        let mut builder = BarBuilder::new(1_000_000_000);
        let one_sided = book(&[("100", "1")], &[]);
        builder.observe(&tick(0, None), &one_sided);
        let two_sided = book(&[("100", "1")], &[("102", "2")]);
        builder.observe(&tick(1, None), &two_sided);
        assert_eq!(builder.unattributed_traded(), Fixed::ZERO);
        let bars = builder.finish();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].traded_qty, None);
    }

    #[test]
    fn top_of_book_gives_mid_and_spread() {
        let b = book(&[("100", "1")], &[("102", "2")]);
        let top = TopOfBook::of(&b);
        assert_eq!(top.mid(), Some(f("101")));
        assert_eq!(top.spread(), Some(f("2")));
        let bps = top.spread_bps().unwrap();
        assert!((bps - 198.0198).abs() < 0.01, "{bps}");
    }

    #[test]
    fn a_one_sided_book_has_no_mid_rather_than_a_made_up_one() {
        let bids_only = book(&[("100", "1")], &[]);
        let top = TopOfBook::of(&bids_only);
        assert_eq!(top.mid(), None);
        assert_eq!(top.spread(), None);
        assert_eq!(top.spread_bps(), None);
    }

    #[test]
    fn imbalance_is_signed_and_bounded() {
        let heavy_bid = Depth::of(&book(&[("100", "3")], &[("101", "1")]), 10);
        assert!((heavy_bid.imbalance().unwrap() - 0.5).abs() < 1e-12);
        let heavy_ask = Depth::of(&book(&[("100", "1")], &[("101", "3")]), 10);
        assert!((heavy_ask.imbalance().unwrap() + 0.5).abs() < 1e-12);
        let balanced = Depth::of(&book(&[("100", "2")], &[("101", "2")]), 10);
        assert_eq!(balanced.imbalance(), Some(0.0));
    }

    #[test]
    fn an_empty_book_has_no_imbalance_rather_than_zero() {
        let empty = Depth::of(&book(&[], &[]), 10);
        assert_eq!(empty.imbalance(), None, "undefined is not balanced");
    }

    #[test]
    fn depth_counts_only_the_levels_asked_for() {
        let b = book(
            &[("100", "1"), ("99", "1"), ("98", "1")],
            &[("101", "2"), ("102", "2")],
        );
        let two = Depth::of(&b, 2);
        assert_eq!(two.bid_qty, f("2"));
        assert_eq!(two.ask_qty, f("4"));
        assert_eq!(two.bid_levels, 2);
        let all = Depth::of(&b, usize::MAX);
        assert_eq!(all.bid_qty, f("3"));
    }

    #[test]
    fn depth_within_an_offset_includes_levels_whole() {
        // Mid is 100.5; a one-unit offset covers 99.5 to 101.5.
        let b = book(&[("100", "1"), ("99", "5")], &[("101", "2"), ("102", "9")]);
        let near = depth_within(&b, f("1")).unwrap();
        assert_eq!(near.bid_qty, f("1"), "99 is outside the window");
        assert_eq!(near.ask_qty, f("2"), "102 is outside the window");
        // Widen it and the far levels come in whole, not pro-rated.
        let wide = depth_within(&b, f("2")).unwrap();
        assert_eq!(wide.bid_qty, f("6"));
        assert_eq!(wide.ask_qty, f("11"));
    }

    #[test]
    fn depth_within_needs_two_sides() {
        assert!(depth_within(&book(&[("100", "1")], &[]), f("1")).is_none());
    }

    #[test]
    fn bars_take_open_high_low_close_from_the_mid() {
        let mut builder = BarBuilder::new(1_000);
        for (at, bid, ask) in [
            (0i64, "100", "102"), // mid 101
            (100, "104", "106"),  // mid 105, the high
            (200, "98", "100"),   // mid 99, the low
            (900, "101", "103"),  // mid 102, the close
        ] {
            builder.observe(&tick(at, None), &book(&[(bid, "1")], &[(ask, "1")]));
        }
        let bars = builder.finish();
        assert_eq!(bars.len(), 1);
        let bar = &bars[0];
        assert_eq!(bar.open, f("101"));
        assert_eq!(bar.high, f("105"));
        assert_eq!(bar.low, f("99"));
        assert_eq!(bar.close, f("102"));
        assert_eq!(bar.updates, 4);
        assert!(bar.mean_spread_bps.is_some());
    }

    #[test]
    fn bars_split_on_the_interval_and_intervals_may_be_any_width() {
        for interval in [1_000i64, 7_777, 1_000_000_000] {
            let mut builder = BarBuilder::new(interval);
            for i in 0..3 {
                builder.observe(
                    &tick(i * interval + 1, None),
                    &book(&[("100", "1")], &[("102", "1")]),
                );
            }
            let bars = builder.finish();
            assert_eq!(bars.len(), 3, "interval {interval}");
            assert!(bars.windows(2).all(|w| w[0].start_wall < w[1].start_wall));
            assert!(bars.iter().all(|b| b.end_wall - b.start_wall == interval));
        }
    }

    #[test]
    fn an_interval_with_no_messages_is_absent_rather_than_flat() {
        // Filling it in would hide exactly the outages this archive publishes.
        let mut builder = BarBuilder::new(1_000);
        builder.observe(&tick(0, None), &book(&[("100", "1")], &[("102", "1")]));
        builder.observe(&tick(5_000, None), &book(&[("100", "1")], &[("102", "1")]));
        let bars = builder.finish();
        assert_eq!(bars.len(), 2, "the four empty intervals must not appear");
        assert_eq!(bars[0].start_wall, 0);
        assert_eq!(bars[1].start_wall, 5_000);
    }

    #[test]
    fn volume_is_absent_on_a_feed_that_does_not_report_trades() {
        // Zero would claim nothing traded. The feed simply does not say.
        let mut builder = BarBuilder::new(1_000);
        builder.observe(&tick(0, None), &book(&[("100", "1")], &[("102", "1")]));
        assert_eq!(builder.finish()[0].traded_qty, None);
    }

    #[test]
    fn volume_is_summed_on_a_feed_that_does_report_trades() {
        let mut builder = BarBuilder::new(1_000);
        builder.observe(
            &tick(0, Some("1.5")),
            &book(&[("100", "1")], &[("102", "1")]),
        );
        builder.observe(
            &tick(1, Some("0.5")),
            &book(&[("100", "1")], &[("102", "1")]),
        );
        assert_eq!(builder.finish()[0].traded_qty, Some(f("2")));
    }

    #[test]
    fn a_bar_reports_how_much_of_it_was_suspect() {
        let mut builder = BarBuilder::new(1_000);
        let mut bad = tick(0, None);
        bad.suspect = true;
        builder.observe(&bad, &book(&[("100", "1")], &[("102", "1")]));
        builder.observe(&tick(1, None), &book(&[("100", "1")], &[("102", "1")]));
        let bars = builder.finish();
        assert!(bars[0].is_suspect());
        assert_eq!(bars[0].suspect_updates, 1);
        assert_eq!(bars[0].updates, 2);
    }

    #[test]
    fn a_one_sided_book_contributes_nothing_to_a_bar() {
        let mut builder = BarBuilder::new(1_000);
        builder.observe(&tick(0, None), &book(&[("100", "1")], &[]));
        assert!(
            builder.finish().is_empty(),
            "a bar with no mid would be a price that never existed"
        );
    }
}
