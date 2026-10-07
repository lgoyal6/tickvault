//! Indicators over point-in-time books. Missing or unverified inputs stay
//! missing; neither a gap nor warm-up is a zero return.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use tickvault::{Side, book::L2Book};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verification {
    Verified,
    ObservedUnverifiable,
    Invalid,
    Missing,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    MidPrice,
    Spread,
    DepthWeightedPrice,
    OrderBookImbalance,
    Returns,
    RealizedVolatility,
    TradeImbalance,
}

impl Feature {
    pub const ALL: [Self; 7] = [
        Self::MidPrice,
        Self::Spread,
        Self::DepthWeightedPrice,
        Self::OrderBookImbalance,
        Self::Returns,
        Self::RealizedVolatility,
        Self::TradeImbalance,
    ];
}

#[derive(Debug, Clone, Serialize)]
pub struct Contract {
    pub feature: Feature,
    pub units: &'static str,
    pub input: &'static str,
    pub warmup_samples: usize,
    pub absolute_tolerance: f64,
    pub relative_tolerance: f64,
    pub requires_verified: bool,
    pub missing_behavior: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exclusion {
    Verification(Verification),
    OneSidedBook,
    CrossedBook,
    NonPositivePrice,
    Warmup,
    TradeDataUnavailable,
    NoTrades,
    InvalidTradeVolume,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// Availability time, in signed nanoseconds. Never venue event time.
    pub available_at: i64,
    pub verification: Verification,
    pub values: BTreeMap<Feature, Option<f64>>,
    pub exclusions: BTreeMap<Feature, Exclusion>,
}

impl Point {
    fn excluded(at: i64, verification: Verification, reason: Exclusion) -> Self {
        Self {
            available_at: at,
            verification,
            values: Feature::ALL.into_iter().map(|f| (f, None)).collect(),
            exclusions: Feature::ALL
                .into_iter()
                .map(|f| (f, reason.clone()))
                .collect(),
        }
    }

    fn set(&mut self, feature: Feature, value: f64) {
        self.values.insert(feature, Some(value));
        self.exclusions.remove(&feature);
    }
}

/// Actual classified traded volume over the sample interval, not inferred
/// from resting-book changes, which may be cancellations.
#[derive(Debug, Clone, Copy)]
pub struct TradeVolume {
    pub buy: f64,
    pub sell: f64,
}

/// State belongs to one venue/symbol stream. Sampling intervals must be
/// fixed by the experiment contract; volatility is not annualized.
pub struct Engine {
    depth: usize,
    volatility_returns: usize,
    previous_mid: Option<f64>,
    returns: VecDeque<f64>,
    last_at: Option<i64>,
}

impl Engine {
    pub fn new(depth: usize, volatility_returns: usize) -> Result<Self, &'static str> {
        if depth == 0 || volatility_returns < 2 || volatility_returns == usize::MAX {
            return Err("depth must be positive and volatility needs at least two returns");
        }
        Ok(Self {
            depth,
            volatility_returns,
            previous_mid: None,
            returns: VecDeque::new(),
            last_at: None,
        })
    }

    pub fn contracts(&self) -> Vec<Contract> {
        Feature::ALL.into_iter().map(|feature| Contract {
            feature,
            units: match feature {
                Feature::MidPrice | Feature::Spread | Feature::DepthWeightedPrice => "quote currency",
                Feature::Returns | Feature::RealizedVolatility => "log return",
                _ => "dimensionless",
            },
            input: match feature {
                Feature::Returns | Feature::RealizedVolatility => "consecutive verified mid-prices",
                Feature::TradeImbalance => "classified traded volume",
                _ => "verified bid and ask levels",
            },
            warmup_samples: match feature {
                Feature::Returns => 2,
                Feature::RealizedVolatility => self.volatility_returns + 1,
                _ => 1,
            },
                absolute_tolerance: 1e-9,
                relative_tolerance: 1e-12,
            requires_verified: true,
            missing_behavior: "null with an exclusion reason; temporal state resets after a gap",
        }).collect()
    }

    fn reset_history(&mut self) {
        self.previous_mid = None;
        self.returns.clear();
    }

    pub fn sample(
        &mut self,
        available_at: i64,
        verification: Verification,
        book: &L2Book,
        trades: Option<TradeVolume>,
    ) -> Result<Point, &'static str> {
        if self.last_at.is_some_and(|at| available_at <= at) {
            return Err("sample availability times must increase strictly");
        }
        self.last_at = Some(available_at);
        let unusable = if verification != Verification::Verified {
            Some(Exclusion::Verification(verification))
        } else if book.best_bid().is_none() || book.best_ask().is_none() {
            Some(Exclusion::OneSidedBook)
        } else if book.is_crossed() {
            Some(Exclusion::CrossedBook)
        } else {
            None
        };
        if let Some(reason) = unusable {
            self.reset_history();
            return Ok(Point::excluded(available_at, verification, reason));
        }
        // Both sides were checked above. Fixed::midpoint avoids integer
        // overflow before conversion to the documented floating output.
        let (bid, _) = book.best_bid().expect("checked bid");
        let (ask, _) = book.best_ask().expect("checked ask");
        let mid = bid.midpoint(ask).to_f64_lossy();
        if bid.mantissa() <= 0 || ask.mantissa() <= 0 {
            self.reset_history();
            return Ok(Point::excluded(
                available_at,
                verification,
                Exclusion::NonPositivePrice,
            ));
        }
        let mut point = Point::excluded(available_at, verification, Exclusion::Warmup);
        point.set(Feature::MidPrice, mid);
        point.set(
            Feature::Spread,
            ask.checked_sub(bid)
                .expect("ordered positive prices")
                .to_f64_lossy(),
        );
        let totals = |side| {
            book.top(side, self.depth)
                .iter()
                .fold((0.0, 0.0), |(qty, notional), (p, q)| {
                    let q = q.to_f64_lossy();
                    (qty + q, notional + p.to_f64_lossy() * q)
                })
        };
        let (bq, bn) = totals(Side::Bid);
        let (aq, an) = totals(Side::Ask);
        point.set(Feature::OrderBookImbalance, (bq - aq) / (bq + aq));
        point.set(Feature::DepthWeightedPrice, (bn / bq + an / aq) / 2.0);
        if let Some(previous) = self.previous_mid {
            let ret = (mid / previous).ln();
            point.set(Feature::Returns, ret);
            self.returns.push_back(ret);
            if self.returns.len() > self.volatility_returns {
                self.returns.pop_front();
            }
            if self.returns.len() == self.volatility_returns {
                point.set(
                    Feature::RealizedVolatility,
                    self.returns.iter().map(|r| r * r).sum::<f64>().sqrt(),
                );
            }
        }
        self.previous_mid = Some(mid);
        match trades {
            Some(v)
                if v.buy.is_finite()
                    && v.sell.is_finite()
                    && v.buy >= 0.0
                    && v.sell >= 0.0
                    && v.buy + v.sell > 0.0
                    && (v.buy + v.sell).is_finite() =>
            {
                point.set(Feature::TradeImbalance, (v.buy - v.sell) / (v.buy + v.sell));
            }
            Some(v) if v.buy == 0.0 && v.sell == 0.0 => {
                point
                    .exclusions
                    .insert(Feature::TradeImbalance, Exclusion::NoTrades);
            }
            Some(_) => {
                point
                    .exclusions
                    .insert(Feature::TradeImbalance, Exclusion::InvalidTradeVolume);
            }
            None => {
                point
                    .exclusions
                    .insert(Feature::TradeImbalance, Exclusion::TradeDataUnavailable);
            }
        }
        Ok(point)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickvault::{
        Fixed, Symbol,
        book::{ApplyOutcome, LevelChange},
    };

    fn book(bid: i64, ask: i64, bid_qty: i64, ask_qty: i64) -> L2Book {
        let mut b = L2Book::new(Symbol::new("BTC", "USD"));
        let mut outcome = ApplyOutcome::default();
        for (side, price, qty) in [(Side::Bid, bid, bid_qty), (Side::Ask, ask, ask_qty)] {
            b.apply_change(
                LevelChange::new(
                    side,
                    Fixed::from_units(price).unwrap(),
                    Fixed::from_units(qty).unwrap(),
                ),
                &mut outcome,
            );
        }
        b
    }

    #[test]
    fn numerical_values_and_trade_support_are_explicit() {
        let mut e = Engine::new(1, 2).unwrap();
        let p = e
            .sample(
                -5,
                Verification::Verified,
                &book(99, 101, 3, 1),
                Some(TradeVolume {
                    buy: 3.0,
                    sell: 1.0,
                }),
            )
            .unwrap();
        for (f, v) in [
            (Feature::MidPrice, 100.0),
            (Feature::Spread, 2.0),
            (Feature::OrderBookImbalance, 0.5),
            (Feature::DepthWeightedPrice, 100.0),
            (Feature::TradeImbalance, 0.5),
        ] {
            assert!((p.values[&f].unwrap() - v).abs() < 1e-9);
        }
        assert_eq!(p.values[&Feature::Returns], None);
        assert!(
            e.sample(-5, Verification::Verified, &book(99, 101, 3, 1), None)
                .is_err()
        );
    }

    #[test]
    fn gaps_reset_returns_and_volatility_instead_of_interpolating() {
        let mut e = Engine::new(1, 2).unwrap();
        let b = book(99, 101, 1, 1);
        e.sample(1, Verification::Verified, &b, None).unwrap();
        e.sample(2, Verification::Verified, &b, None).unwrap();
        let warm = e.sample(3, Verification::Verified, &b, None).unwrap();
        assert_eq!(warm.values[&Feature::RealizedVolatility], Some(0.0));
        for state in [
            Verification::Unknown,
            Verification::Missing,
            Verification::Invalid,
            Verification::ObservedUnverifiable,
        ] {
            let p = e.sample(e.last_at.unwrap() + 1, state, &b, None).unwrap();
            assert!(p.values.values().all(Option::is_none));
        }
        let p = e.sample(8, Verification::Verified, &b, None).unwrap();
        assert_eq!(p.values[&Feature::Returns], None);
        assert_eq!(p.values[&Feature::RealizedVolatility], None);
        assert_eq!(
            p.exclusions[&Feature::TradeImbalance],
            Exclusion::TradeDataUnavailable
        );
    }

    #[test]
    fn invalid_books_are_not_numeric_observations() {
        let mut e = Engine::new(1, 2).unwrap();
        for (at, b, reason) in [
            (1, book(101, 99, 1, 1), Exclusion::CrossedBook),
            (2, book(99, 101, 0, 1), Exclusion::OneSidedBook),
            (3, book(0, 1, 1, 1), Exclusion::NonPositivePrice),
        ] {
            let p = e.sample(at, Verification::Verified, &b, None).unwrap();
            assert!(p.values.values().all(Option::is_none));
            assert_eq!(p.exclusions[&Feature::MidPrice], reason);
        }
    }

    #[test]
    fn imbalance_is_bounded_and_increases_with_bid_volume() {
        for qty in 1..30 {
            let mut e = Engine::new(1, 2).unwrap();
            let a = e
                .sample(1, Verification::Verified, &book(99, 101, qty, 3), None)
                .unwrap()
                .values[&Feature::OrderBookImbalance]
                .unwrap();
            let b = e
                .sample(2, Verification::Verified, &book(99, 101, qty + 1, 3), None)
                .unwrap()
                .values[&Feature::OrderBookImbalance]
                .unwrap();
            assert!((-1.0..=1.0).contains(&a));
            assert!(b > a);
        }
    }

    #[test]
    fn spread_preserves_small_tick_at_large_price() {
        let mut b = L2Book::new(Symbol::new("BTC", "USD"));
        let mut outcome = ApplyOutcome::default();
        for (side, price) in [(Side::Bid, i64::MAX - 1), (Side::Ask, i64::MAX)] {
            b.apply_change(
                LevelChange::new(
                    side,
                    Fixed::from_mantissa(price),
                    Fixed::from_units(1).unwrap(),
                ),
                &mut outcome,
            );
        }
        let p = Engine::new(1, 2)
            .unwrap()
            .sample(0, Verification::Verified, &b, None)
            .unwrap();
        assert_eq!(p.values[&Feature::Spread], Some(1e-9));
    }
}
