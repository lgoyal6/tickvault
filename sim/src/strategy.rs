//! Four small strategies, and the decision-time view they are allowed to see.
//!
//! Small on purpose. The deliverable of this experiment is the simulator and
//! the risk system, not a model, and a strategy anyone can read in a minute is
//! one whose result cannot be explained away by something clever hiding in it.
//!
//! [`Features`] is the only thing a strategy sees, and it carries the two times
//! that make a point-in-time read honest, in the same sense as
//! `tickvault::features`: when the message behind it was received, and when it
//! became visible here after market data latency. A decision may only use a
//! feature whose visible time is at or before the decision time, and
//! [`Features::leaks`] is what the evaluator asserts on every decision. The
//! `leak-future` control exists to make that assertion fail.

use tickvault::Side;
use tickvault::book::L2Book;

use crate::manifest::StrategySpec;
use crate::market::{OwnOrderId, mid_of};
use crate::{SimResult, bail};

/// How deep the imbalance features go.
pub const MAX_DEPTH: usize = 5;

/// What a strategy can see at one instant.
#[derive(Debug, Clone, PartialEq)]
pub struct Features {
    pub decision_time: i64,
    /// `None` when the book is one sided. A book with no ask has no mid, and a
    /// zero there would be a price nobody quoted.
    pub mid: Option<f64>,
    pub best_bid: Option<(f64, f64)>,
    pub best_ask: Option<(f64, f64)>,
    /// Top-k depth imbalance for k of 1 through [`MAX_DEPTH`], as bid quantity
    /// minus ask quantity over their sum. `None` where a side has fewer than k
    /// levels, because an imbalance computed against a side that is not there
    /// is a statement about our book rather than the market's.
    pub imbalance: [Option<f64>; MAX_DEPTH],
    /// Receive time of the latest message behind these numbers.
    pub max_source_recv_wall: i64,
    /// When that message became visible to a strategy, which is its receive
    /// time plus the market data latency drawn for it.
    pub max_source_visible_at: i64,
}

impl Features {
    /// True when a feature was built from something not yet visible.
    ///
    /// The whole point of the delayed book is that this can never happen by
    /// construction, so this is the assertion rather than the mechanism. It
    /// fires under the `leak-future` control and nowhere else.
    pub fn leaks(&self) -> bool {
        self.max_source_visible_at > self.decision_time
    }

    /// Read a book into features.
    pub fn from_book(
        book: &L2Book,
        decision_time: i64,
        max_source_recv_wall: i64,
        max_source_visible_at: i64,
    ) -> Self {
        let bids = book.top(Side::Bid, MAX_DEPTH);
        let asks = book.top(Side::Ask, MAX_DEPTH);
        let mut imbalance = [None; MAX_DEPTH];
        let mut bid_sum = 0.0;
        let mut ask_sum = 0.0;
        for k in 0..MAX_DEPTH {
            if k >= bids.len() || k >= asks.len() {
                break;
            }
            bid_sum += bids[k].1.to_f64_lossy();
            ask_sum += asks[k].1.to_f64_lossy();
            let total = bid_sum + ask_sum;
            if total > 0.0 {
                imbalance[k] = Some((bid_sum - ask_sum) / total);
            }
        }
        Features {
            decision_time,
            mid: mid_of(book),
            best_bid: book
                .best_bid()
                .map(|(p, q)| (p.to_f64_lossy(), q.to_f64_lossy())),
            best_ask: book
                .best_ask()
                .map(|(p, q)| (p.to_f64_lossy(), q.to_f64_lossy())),
            imbalance,
            max_source_recv_wall,
            max_source_visible_at,
        }
    }
}

/// What the strategy is holding and working.
#[derive(Debug, Clone, Default)]
pub struct PortfolioView {
    pub inventory: f64,
    pub resting: Vec<(OwnOrderId, Side, f64, f64)>,
}

/// What a strategy asks for.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Action {
    CancelAll,
    PlaceMaker { side: Side, price: f64, size: f64 },
    PlaceTaker { side: Side, size: f64 },
}

/// One point of a strategy's grid.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Params {
    NoTrade,
    FixedSpreadMm {
        half_spread_bps: f64,
        requote_ms: i64,
        quote_size: f64,
    },
    DepthImbalance {
        depth_k: usize,
        threshold: f64,
        horizon_ms: i64,
        trade_size: f64,
    },
    ImbalanceSkewMm {
        half_spread_bps: f64,
        skew_bps: f64,
        inv_skew_bps: f64,
        depth_k: usize,
        requote_ms: i64,
        quote_size: f64,
    },
}

impl Params {
    pub fn strategy_name(&self) -> &'static str {
        match self {
            Params::NoTrade => "no_trade",
            Params::FixedSpreadMm { .. } => "fixed_spread_mm",
            Params::DepthImbalance { .. } => "depth_imbalance",
            Params::ImbalanceSkewMm { .. } => "imbalance_skew_mm",
        }
    }

    /// A stable, readable name for the JSON and the report.
    pub fn label(&self) -> String {
        match self {
            Params::NoTrade => "none".to_string(),
            Params::FixedSpreadMm {
                half_spread_bps,
                requote_ms,
                ..
            } => format!("half_spread_bps={half_spread_bps} requote_ms={requote_ms}"),
            Params::DepthImbalance {
                depth_k,
                threshold,
                horizon_ms,
                ..
            } => format!("depth_k={depth_k} threshold={threshold} horizon_ms={horizon_ms}"),
            Params::ImbalanceSkewMm {
                half_spread_bps,
                skew_bps,
                inv_skew_bps,
                ..
            } => format!(
                "half_spread_bps={half_spread_bps} skew_bps={skew_bps} inv_skew_bps={inv_skew_bps}"
            ),
        }
    }
}

/// Enumerate a strategy's grid, in the order the manifest declares.
///
/// The order is load bearing: it is the tie break in the fitting procedure, so
/// two grid points with identical training PnL and turnover resolve the same
/// way on every run.
pub fn grid(name: &str, spec: &StrategySpec) -> SimResult<Vec<Params>> {
    Ok(match name {
        "no_trade" => vec![Params::NoTrade],
        "fixed_spread_mm" => {
            let quote_size = spec.fixed_f64("quote_size_base")?;
            let mut out = Vec::new();
            for half_spread_bps in spec.axis_f64("half_spread_bps")? {
                for requote_ms in spec.axis_i64("requote_ms")? {
                    out.push(Params::FixedSpreadMm {
                        half_spread_bps,
                        requote_ms,
                        quote_size,
                    });
                }
            }
            out
        }
        "depth_imbalance" => {
            let trade_size = spec.fixed_f64("trade_size_base")?;
            let mut out = Vec::new();
            for depth_k in spec.axis_i64("depth_k")? {
                for threshold in spec.axis_f64("threshold")? {
                    for horizon_ms in spec.axis_i64("horizon_ms")? {
                        out.push(Params::DepthImbalance {
                            depth_k: depth_k as usize,
                            threshold,
                            horizon_ms,
                            trade_size,
                        });
                    }
                }
            }
            out
        }
        "imbalance_skew_mm" => {
            let quote_size = spec.fixed_f64("quote_size_base")?;
            let depth_k = spec.fixed_i64("depth_k")? as usize;
            let requote_ms = spec.fixed_i64("requote_ms")?;
            let mut out = Vec::new();
            for half_spread_bps in spec.axis_f64("half_spread_bps")? {
                for skew_bps in spec.axis_f64("skew_bps")? {
                    for inv_skew_bps in spec.axis_f64("inv_skew_bps")? {
                        out.push(Params::ImbalanceSkewMm {
                            half_spread_bps,
                            skew_bps,
                            inv_skew_bps,
                            depth_k,
                            requote_ms,
                            quote_size,
                        });
                    }
                }
            }
            out
        }
        other => bail!("no grid is defined for strategy {other}"),
    })
}

/// A strategy and whatever it remembers between decisions.
#[derive(Debug, Clone)]
pub struct Strategy {
    pub params: Params,
    last_requote: Option<i64>,
    /// 1 long, -1 short, 0 flat. What the strategy believes, which is not the
    /// same as what actually filled.
    intent_dir: i32,
    entered_at: Option<i64>,
}

impl Strategy {
    pub fn new(params: Params) -> Self {
        Strategy {
            params,
            last_requote: None,
            intent_dir: 0,
            entered_at: None,
        }
    }

    pub fn name(&self) -> &'static str {
        self.params.strategy_name()
    }

    /// Decide what to do at `features.decision_time`.
    ///
    /// `inventory_limit` is passed in rather than read from a global because
    /// the inventory skew is expressed as a fraction of it, and a strategy that
    /// hard coded the number would drift from the frozen manifest.
    pub fn decide(
        &mut self,
        features: &Features,
        view: &PortfolioView,
        inventory_limit: f64,
    ) -> Vec<Action> {
        match self.params {
            Params::NoTrade => Vec::new(),
            Params::FixedSpreadMm {
                half_spread_bps,
                requote_ms,
                quote_size,
            } => self.quote(features, requote_ms, |mid| {
                let edge = mid * half_spread_bps / 10_000.0;
                (mid - edge, mid + edge, quote_size)
            }),
            Params::ImbalanceSkewMm {
                half_spread_bps,
                skew_bps,
                inv_skew_bps,
                depth_k,
                requote_ms,
                quote_size,
            } => {
                let imbalance = features.imbalance[depth_k.min(MAX_DEPTH) - 1];
                let Some(imbalance) = imbalance else {
                    return Vec::new();
                };
                let inventory_fraction = if inventory_limit > 0.0 {
                    (view.inventory / inventory_limit).clamp(-1.0, 1.0)
                } else {
                    0.0
                };
                self.quote(features, requote_ms, |mid| {
                    // Lean the whole quote toward where the book is heavier,
                    // and away from the inventory already held.
                    let skew =
                        (skew_bps * imbalance - inv_skew_bps * inventory_fraction) / 10_000.0;
                    let centre = mid * (1.0 + skew);
                    let edge = mid * half_spread_bps / 10_000.0;
                    (centre - edge, centre + edge, quote_size)
                })
            }
            Params::DepthImbalance {
                depth_k,
                threshold,
                horizon_ms,
                trade_size,
            } => self.imbalance_trade(features, view, depth_k, threshold, horizon_ms, trade_size),
        }
    }

    /// The requote cadence both quoting strategies share.
    fn quote<F>(&mut self, features: &Features, requote_ms: i64, prices: F) -> Vec<Action>
    where
        F: FnOnce(f64) -> (f64, f64, f64),
    {
        let Some(mid) = features.mid else {
            return Vec::new();
        };
        let due = match self.last_requote {
            Some(last) => features.decision_time - last >= requote_ms * 1_000_000,
            None => true,
        };
        if !due {
            return Vec::new();
        }
        self.last_requote = Some(features.decision_time);
        let (bid, ask, size) = prices(mid);
        vec![
            Action::CancelAll,
            Action::PlaceMaker {
                side: Side::Bid,
                price: bid,
                size,
            },
            Action::PlaceMaker {
                side: Side::Ask,
                price: ask,
                size,
            },
        ]
    }

    fn imbalance_trade(
        &mut self,
        features: &Features,
        view: &PortfolioView,
        depth_k: usize,
        threshold: f64,
        horizon_ms: i64,
        trade_size: f64,
    ) -> Vec<Action> {
        let Some(imbalance) = features.imbalance[depth_k.min(MAX_DEPTH) - 1] else {
            return Vec::new();
        };
        let signal = if imbalance > threshold {
            1
        } else if imbalance < -threshold {
            -1
        } else {
            0
        };

        if self.intent_dir != 0 {
            let expired = self
                .entered_at
                .is_some_and(|at| features.decision_time - at >= horizon_ms * 1_000_000);
            let reversed = signal != 0 && signal != self.intent_dir;
            if expired || reversed {
                let size = view.inventory.abs();
                self.intent_dir = 0;
                self.entered_at = None;
                if size > 0.0 {
                    let side = if view.inventory > 0.0 {
                        Side::Ask
                    } else {
                        Side::Bid
                    };
                    return vec![Action::PlaceTaker { side, size }];
                }
                return Vec::new();
            }
            return Vec::new();
        }

        if signal == 0 {
            return Vec::new();
        }
        self.intent_dir = signal;
        self.entered_at = Some(features.decision_time);
        let side = if signal > 0 { Side::Bid } else { Side::Ask };
        vec![Action::PlaceTaker {
            side,
            size: trade_size,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickvault::Fixed;
    use tickvault::Symbol;
    use tickvault::book::{BookDelta, LevelChange};
    use tickvault::clock::{Stamp, Timestamps};

    fn px(units: i64) -> Fixed {
        Fixed::from_mantissa(units * 1_000_000_000)
    }

    fn qty(thousandths: i64) -> Fixed {
        Fixed::from_mantissa(thousandths * 1_000_000)
    }

    fn book_with(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> L2Book {
        let mut book = L2Book::new(Symbol::new("BTC", "USD"));
        let mut changes = Vec::new();
        for (price, size) in bids {
            changes.push(LevelChange::new(Side::Bid, px(*price), qty(*size)));
        }
        for (price, size) in asks {
            changes.push(LevelChange::new(Side::Ask, px(*price), qty(*size)));
        }
        book.apply_delta(&BookDelta {
            symbol: Symbol::new("BTC", "USD"),
            changes,
            seq: None,
            checksum: None,
            stamps: Timestamps::recv_only(Stamp {
                mono_nanos: 0,
                wall_nanos: 1,
            }),
            prev_seq: None,
            first_seq: None,
        });
        book
    }

    fn features(book: &L2Book, now: i64) -> Features {
        Features::from_book(book, now, now - 2_000_000, now - 1_000_000)
    }

    #[test]
    fn a_balanced_book_has_no_imbalance_and_a_heavier_bid_has_a_positive_one() {
        let balanced = book_with(&[(100, 1_000)], &[(101, 1_000)]);
        assert_eq!(features(&balanced, 0).imbalance[0], Some(0.0));
        let heavy = book_with(&[(100, 3_000)], &[(101, 1_000)]);
        assert_eq!(features(&heavy, 0).imbalance[0], Some(0.5));
    }

    #[test]
    fn an_imbalance_against_a_side_that_is_not_there_is_nothing_rather_than_one() {
        // Three bid levels and one ask level. At depth two there is no second
        // ask, so the answer is that we do not know, not that the book is
        // maximally bid.
        let book = book_with(&[(100, 1_000), (99, 1_000), (98, 1_000)], &[(101, 1_000)]);
        let f = features(&book, 0);
        assert!(f.imbalance[0].is_some());
        assert_eq!(f.imbalance[1], None);
        assert_eq!(f.imbalance[4], None);
    }

    #[test]
    fn a_one_sided_book_has_no_mid() {
        let book = book_with(&[(100, 1_000)], &[]);
        assert_eq!(features(&book, 0).mid, None);
    }

    #[test]
    fn features_built_from_visible_data_do_not_leak() {
        let book = book_with(&[(100, 1_000)], &[(101, 1_000)]);
        let f = Features::from_book(&book, 1_000_000_000, 999_000_000, 999_500_000);
        assert!(!f.leaks());
    }

    #[test]
    fn a_feature_not_yet_visible_is_a_leak() {
        let book = book_with(&[(100, 1_000)], &[(101, 1_000)]);
        let f = Features::from_book(&book, 1_000_000_000, 1_000_000_001, 1_002_000_000);
        assert!(f.leaks());
    }

    #[test]
    fn no_trade_never_asks_for_anything() {
        let mut s = Strategy::new(Params::NoTrade);
        let book = book_with(&[(100, 1_000)], &[(101, 1_000)]);
        assert!(
            s.decide(&features(&book, 0), &PortfolioView::default(), 0.02)
                .is_empty()
        );
    }

    #[test]
    fn a_market_maker_quotes_both_sides_around_the_mid() {
        let mut s = Strategy::new(Params::FixedSpreadMm {
            half_spread_bps: 10.0,
            requote_ms: 500,
            quote_size: 0.005,
        });
        let book = book_with(&[(100, 1_000)], &[(102, 1_000)]);
        let actions = s.decide(&features(&book, 0), &PortfolioView::default(), 0.02);
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0], Action::CancelAll);
        let Action::PlaceMaker { price: bid, .. } = actions[1] else {
            panic!("expected a bid")
        };
        let Action::PlaceMaker { price: ask, .. } = actions[2] else {
            panic!("expected an ask")
        };
        assert!((bid - 100.899).abs() < 1e-6, "{bid}");
        assert!((ask - 101.101).abs() < 1e-6, "{ask}");
    }

    #[test]
    fn a_market_maker_leaves_its_quotes_alone_until_the_cadence_is_due() {
        let mut s = Strategy::new(Params::FixedSpreadMm {
            half_spread_bps: 10.0,
            requote_ms: 500,
            quote_size: 0.005,
        });
        let book = book_with(&[(100, 1_000)], &[(102, 1_000)]);
        assert_eq!(
            s.decide(&features(&book, 0), &PortfolioView::default(), 0.02)
                .len(),
            3
        );
        assert!(
            s.decide(
                &features(&book, 499_000_000),
                &PortfolioView::default(),
                0.02
            )
            .is_empty()
        );
        assert_eq!(
            s.decide(
                &features(&book, 500_000_000),
                &PortfolioView::default(),
                0.02
            )
            .len(),
            3
        );
    }

    #[test]
    fn a_market_maker_with_no_mid_does_nothing_rather_than_quoting_off_nothing() {
        let mut s = Strategy::new(Params::FixedSpreadMm {
            half_spread_bps: 10.0,
            requote_ms: 500,
            quote_size: 0.005,
        });
        let book = book_with(&[(100, 1_000)], &[]);
        assert!(
            s.decide(&features(&book, 0), &PortfolioView::default(), 0.02)
                .is_empty()
        );
    }

    #[test]
    fn the_candidate_leans_its_quotes_toward_the_heavier_side() {
        let params = Params::ImbalanceSkewMm {
            half_spread_bps: 10.0,
            skew_bps: 20.0,
            inv_skew_bps: 0.0,
            depth_k: 1,
            requote_ms: 500,
            quote_size: 0.005,
        };
        let heavy_bid = book_with(&[(100, 3_000)], &[(102, 1_000)]);
        let mut s = Strategy::new(params);
        let leaning = s.decide(&features(&heavy_bid, 0), &PortfolioView::default(), 0.02);
        let Action::PlaceMaker { price: bid, .. } = leaning[1] else {
            panic!()
        };

        let balanced = book_with(&[(100, 1_000)], &[(102, 1_000)]);
        let mut flat = Strategy::new(params);
        let level = flat.decide(&features(&balanced, 0), &PortfolioView::default(), 0.02);
        let Action::PlaceMaker {
            price: flat_bid, ..
        } = level[1]
        else {
            panic!()
        };
        assert!(bid > flat_bid, "a heavier bid should lift the quote");
    }

    #[test]
    fn the_candidate_leans_away_from_the_inventory_it_already_holds() {
        let params = Params::ImbalanceSkewMm {
            half_spread_bps: 10.0,
            skew_bps: 0.0,
            inv_skew_bps: 20.0,
            depth_k: 1,
            requote_ms: 500,
            quote_size: 0.005,
        };
        let book = book_with(&[(100, 1_000)], &[(102, 1_000)]);
        let mut long = Strategy::new(params);
        let view = PortfolioView {
            inventory: 0.02,
            resting: Vec::new(),
        };
        let actions = long.decide(&features(&book, 0), &view, 0.02);
        let Action::PlaceMaker { price: bid, .. } = actions[1] else {
            panic!()
        };
        let mut flat = Strategy::new(params);
        let level = flat.decide(&features(&book, 0), &PortfolioView::default(), 0.02);
        let Action::PlaceMaker {
            price: flat_bid, ..
        } = level[1]
        else {
            panic!()
        };
        assert!(bid < flat_bid, "holding a long should lower the bid");
    }

    #[test]
    fn the_imbalance_strategy_enters_in_the_direction_of_the_book() {
        let mut s = Strategy::new(Params::DepthImbalance {
            depth_k: 1,
            threshold: 0.2,
            horizon_ms: 2_000,
            trade_size: 0.005,
        });
        let quiet = book_with(&[(100, 1_100)], &[(101, 1_000)]);
        assert!(
            s.decide(&features(&quiet, 0), &PortfolioView::default(), 0.02)
                .is_empty(),
            "below the threshold nothing happens"
        );
        let heavy = book_with(&[(100, 3_000)], &[(101, 1_000)]);
        let actions = s.decide(&features(&heavy, 1_000), &PortfolioView::default(), 0.02);
        assert_eq!(
            actions,
            vec![Action::PlaceTaker {
                side: Side::Bid,
                size: 0.005
            }]
        );
    }

    #[test]
    fn the_imbalance_strategy_leaves_after_its_horizon() {
        let mut s = Strategy::new(Params::DepthImbalance {
            depth_k: 1,
            threshold: 0.2,
            horizon_ms: 2_000,
            trade_size: 0.005,
        });
        let heavy = book_with(&[(100, 3_000)], &[(101, 1_000)]);
        s.decide(&features(&heavy, 0), &PortfolioView::default(), 0.02);
        let holding = PortfolioView {
            inventory: 0.005,
            resting: Vec::new(),
        };
        assert!(
            s.decide(&features(&heavy, 1_999_000_000), &holding, 0.02)
                .is_empty()
        );
        let out = s.decide(&features(&heavy, 2_000_000_000), &holding, 0.02);
        assert_eq!(
            out,
            vec![Action::PlaceTaker {
                side: Side::Ask,
                size: 0.005
            }]
        );
    }

    #[test]
    fn the_imbalance_strategy_leaves_when_the_book_turns_against_it() {
        let mut s = Strategy::new(Params::DepthImbalance {
            depth_k: 1,
            threshold: 0.2,
            horizon_ms: 60_000,
            trade_size: 0.005,
        });
        let heavy_bid = book_with(&[(100, 3_000)], &[(101, 1_000)]);
        s.decide(&features(&heavy_bid, 0), &PortfolioView::default(), 0.02);
        let heavy_ask = book_with(&[(100, 1_000)], &[(101, 3_000)]);
        let holding = PortfolioView {
            inventory: 0.005,
            resting: Vec::new(),
        };
        let out = s.decide(&features(&heavy_ask, 1_000_000), &holding, 0.02);
        assert_eq!(
            out,
            vec![Action::PlaceTaker {
                side: Side::Ask,
                size: 0.005
            }]
        );
    }

    #[test]
    fn every_grid_comes_out_of_the_manifest_at_the_size_it_declares() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let loaded = crate::manifest::Loaded::open(&root.join("sim/manifest.json"), &root).unwrap();
        let sizes = [
            ("no_trade", 1),
            ("fixed_spread_mm", 6),
            ("depth_imbalance", 8),
            ("imbalance_skew_mm", 18),
        ];
        for (name, want) in sizes {
            let points = grid(name, loaded.spec(name).unwrap()).unwrap();
            assert_eq!(points.len(), want, "{name}");
            // The enumeration order is the fitting procedure's tie break, so it
            // has to be stable.
            let again = grid(name, loaded.spec(name).unwrap()).unwrap();
            assert_eq!(points, again);
        }
    }
}
