//! Limits, and the switch that stops trading when one is breached.
//!
//! Two different jobs, kept apart on purpose.
//!
//! **Pre-trade checks** refuse an order before it is submitted, and every
//! refusal is counted under a named reason rather than dropped. A strategy that
//! wants to quote a size the position limit will not allow should show up in the
//! output as a rejection, not as a strategy that quoted less than it asked to.
//!
//! **The kill switch** reacts to a breach that has already happened, because
//! inventory and PnL limits can only be breached by a fill, and a fill is not
//! something we get to refuse. On a breach it cancels everything resting and in
//! flight and blocks new orders for the rest of the window. Activations are
//! counted, and the evaluator's gate requires that every breach observed was
//! followed by one.

use std::collections::BTreeMap;

use tickvault::Side;

use crate::manifest::RiskLimits;
use crate::market::Market;

/// Why an order was not submitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rejection {
    KillSwitchActive,
    PositionLimit,
    InventoryLimit,
    GrossExposureLimit,
    NoTwoSidedBook,
    NonPositiveSize,
    WouldCrossBook,
}

impl Rejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Rejection::KillSwitchActive => "kill_switch_active",
            Rejection::PositionLimit => "position_limit",
            Rejection::InventoryLimit => "inventory_limit",
            Rejection::GrossExposureLimit => "gross_exposure_limit",
            Rejection::NoTwoSidedBook => "no_two_sided_book",
            Rejection::NonPositiveSize => "non_positive_size",
            Rejection::WouldCrossBook => "would_cross_book",
        }
    }

    pub const ALL: &'static [Rejection] = &[
        Rejection::KillSwitchActive,
        Rejection::PositionLimit,
        Rejection::InventoryLimit,
        Rejection::GrossExposureLimit,
        Rejection::NoTwoSidedBook,
        Rejection::NonPositiveSize,
        Rejection::WouldCrossBook,
    ];
}

/// Which limit a breach was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Breach {
    Inventory,
    GrossExposure,
    Loss,
}

impl Breach {
    pub fn as_str(self) -> &'static str {
        match self {
            Breach::Inventory => "inventory",
            Breach::GrossExposure => "gross_exposure",
            Breach::Loss => "loss",
        }
    }
}

/// What an order wants to do, before the checks see it.
#[derive(Debug, Clone, Copy)]
pub struct Intent {
    pub side: Side,
    pub size: f64,
    /// `None` for a marketable order.
    pub price: Option<f64>,
}

/// The risk state of one run.
#[derive(Debug, Clone)]
pub struct Risk {
    limits: RiskLimits,
    killed: bool,
    activations: u64,
    breaches: BTreeMap<Breach, u64>,
    rejections: BTreeMap<Rejection, u64>,
    /// The worst absolute inventory the run ever held, and the mean of it.
    max_abs_inventory: f64,
    inventory_sum: f64,
    inventory_samples: u64,
    max_gross_exposure: f64,
    worst_pnl: f64,
}

impl Risk {
    pub fn new(limits: RiskLimits) -> Self {
        Risk {
            limits,
            killed: false,
            activations: 0,
            breaches: BTreeMap::new(),
            rejections: BTreeMap::new(),
            max_abs_inventory: 0.0,
            inventory_sum: 0.0,
            inventory_samples: 0,
            max_gross_exposure: 0.0,
            worst_pnl: 0.0,
        }
    }

    pub fn is_killed(&self) -> bool {
        self.killed
    }

    pub fn activations(&self) -> u64 {
        self.activations
    }

    pub fn breaches(&self) -> &BTreeMap<Breach, u64> {
        &self.breaches
    }

    pub fn rejections(&self) -> &BTreeMap<Rejection, u64> {
        &self.rejections
    }

    pub fn total_rejections(&self) -> u64 {
        self.rejections.values().sum()
    }

    pub fn max_abs_inventory(&self) -> f64 {
        self.max_abs_inventory
    }

    pub fn max_gross_exposure(&self) -> f64 {
        self.max_gross_exposure
    }

    pub fn worst_pnl(&self) -> f64 {
        self.worst_pnl
    }

    /// Mean absolute inventory over every message the run saw.
    ///
    /// `None` when nothing was ever sampled. Zero would say the run held no
    /// inventory, which is a different statement from having no measurement.
    pub fn mean_abs_inventory(&self) -> Option<f64> {
        (self.inventory_samples > 0).then(|| self.inventory_sum / self.inventory_samples as f64)
    }

    fn reject(&mut self, reason: Rejection) -> Rejection {
        *self.rejections.entry(reason).or_insert(0) += 1;
        reason
    }

    /// Decide whether an order may be submitted.
    ///
    /// The position limit is the interesting one: it is not the inventory we
    /// hold, it is the inventory we would hold if everything currently working
    /// on that side filled. A quoting strategy that ignores its own working
    /// orders can breach an inventory limit without ever having breached it at
    /// the moment it quoted.
    pub fn admit(
        &mut self,
        intent: Intent,
        market: &Market,
        mid: Option<f64>,
    ) -> Result<(), Rejection> {
        if self.killed {
            return Err(self.reject(Rejection::KillSwitchActive));
        }
        if !intent.size.is_finite() || intent.size <= 0.0 {
            return Err(self.reject(Rejection::NonPositiveSize));
        }
        let Some(mid) = mid else {
            // No two-sided book means no mid, and no mid means the exposure
            // check has nothing to measure against.
            return Err(self.reject(Rejection::NoTwoSidedBook));
        };

        let signed = match intent.side {
            Side::Bid => intent.size,
            Side::Ask => -intent.size,
        };
        let inventory = market.inventory;
        if (inventory + signed).abs() > self.limits.inventory_limit_base + 1e-12 {
            return Err(self.reject(Rejection::InventoryLimit));
        }

        // Everything already working on this side would extend the position the
        // same way.
        let working = market.resting_size(intent.side) + market.pending_size(intent.side);
        let worst_case = if signed > 0.0 {
            inventory + working + intent.size
        } else {
            inventory - working - intent.size
        };
        if worst_case.abs() > self.limits.position_limit_base + 1e-12 {
            return Err(self.reject(Rejection::PositionLimit));
        }

        let exposure = (inventory + signed).abs() * mid;
        if exposure > self.limits.gross_exposure_limit_quote + 1e-9 {
            return Err(self.reject(Rejection::GrossExposureLimit));
        }
        Ok(())
    }

    /// Refuse a quote that would take liquidity instead of providing it.
    pub fn admit_maker_price(
        &mut self,
        side: Side,
        price: f64,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
    ) -> Result<(), Rejection> {
        let crosses = match side {
            Side::Bid => best_ask.is_some_and(|ask| price >= ask),
            Side::Ask => best_bid.is_some_and(|bid| price <= bid),
        };
        if crosses {
            return Err(self.reject(Rejection::WouldCrossBook));
        }
        Ok(())
    }

    /// Sample the run's state and fire the kill switch if a limit is breached.
    ///
    /// Returns the breaches seen at this instant. The caller cancels, because
    /// cancelling needs the clock and the latency draw.
    pub fn observe(&mut self, market: &Market, mid: Option<f64>, pnl: Option<f64>) -> Vec<Breach> {
        let inventory = market.inventory.abs();
        self.max_abs_inventory = self.max_abs_inventory.max(inventory);
        self.inventory_sum += inventory;
        self.inventory_samples += 1;

        let mut breached = Vec::new();
        if inventory > self.limits.inventory_limit_base + 1e-12 {
            breached.push(Breach::Inventory);
        }
        if let Some(mid) = mid {
            let exposure = inventory * mid;
            self.max_gross_exposure = self.max_gross_exposure.max(exposure);
            if exposure > self.limits.gross_exposure_limit_quote + 1e-9 {
                breached.push(Breach::GrossExposure);
            }
        }
        // A PnL that cannot be marked is not a PnL of zero, so a run holding
        // inventory against a one-sided book is not tested against the loss
        // floor at that instant.
        if let Some(pnl) = pnl {
            self.worst_pnl = self.worst_pnl.min(pnl);
            if pnl < self.limits.loss_limit_quote {
                breached.push(Breach::Loss);
            }
        }
        for breach in &breached {
            *self.breaches.entry(*breach).or_insert(0) += 1;
        }
        if !breached.is_empty() && !self.killed {
            self.killed = true;
            self.activations += 1;
        }
        breached
    }

    /// True when every breach the run saw was answered by the switch firing.
    ///
    /// This is one of the promotion gate's clauses. A run that breached a limit
    /// and carried on quoting has no risk system, whatever its PnL says.
    pub fn controls_held(&self) -> bool {
        let breaches: u64 = self.breaches.values().sum();
        if breaches == 0 {
            return self.activations == 0;
        }
        self.activations >= 1 && self.killed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::costs::Costs;
    use tickvault::Fixed;

    fn limits() -> RiskLimits {
        RiskLimits {
            inventory_limit_base: 0.02,
            position_limit_base: 0.04,
            gross_exposure_limit_quote: 3_000.0,
            loss_limit_quote: -40.0,
        }
    }

    fn market() -> Market {
        Market::new(Costs::zeroed(), Fixed::from_mantissa(10_000_000))
    }

    fn buy(size: f64) -> Intent {
        Intent {
            side: Side::Bid,
            size,
            price: None,
        }
    }

    #[test]
    fn an_order_inside_every_limit_is_admitted() {
        let mut risk = Risk::new(limits());
        assert!(risk.admit(buy(0.005), &market(), Some(100_000.0)).is_ok());
        assert_eq!(risk.total_rejections(), 0);
    }

    #[test]
    fn an_order_that_would_breach_the_inventory_limit_is_refused_by_name() {
        let mut risk = Risk::new(limits());
        let mut market = market();
        market.inventory = 0.018;
        let err = risk
            .admit(buy(0.005), &market, Some(100_000.0))
            .expect_err("0.023 is past the limit");
        assert_eq!(err, Rejection::InventoryLimit);
        assert_eq!(risk.rejections()[&Rejection::InventoryLimit], 1);
    }

    #[test]
    fn the_position_limit_counts_orders_already_working() {
        // Flat, and each order is inside the inventory limit on its own. What
        // is not inside anything is the position they would add up to.
        let mut risk = Risk::new(limits());
        let mut market = market();
        let book = tickvault::book::L2Book::new(tickvault::Symbol::new("BTC", "USD"));
        for _ in 0..8 {
            market.submit_maker(
                Side::Bid,
                Fixed::from_mantissa(100_000_000_000),
                0.005,
                0,
                0,
                3.0,
            );
        }
        market.activate(&book, 0);
        let err = risk
            .admit(buy(0.005), &market, Some(100_000.0))
            .expect_err("0.045 of working buys is past the position limit");
        assert_eq!(err, Rejection::PositionLimit);
    }

    #[test]
    fn a_zero_or_negative_size_is_refused_rather_than_silently_dropped() {
        let mut risk = Risk::new(limits());
        assert_eq!(
            risk.admit(buy(0.0), &market(), Some(100.0)),
            Err(Rejection::NonPositiveSize)
        );
        assert_eq!(
            risk.admit(buy(-1.0), &market(), Some(100.0)),
            Err(Rejection::NonPositiveSize)
        );
        assert_eq!(risk.rejections()[&Rejection::NonPositiveSize], 2);
    }

    #[test]
    fn no_mid_means_no_order_rather_than_an_order_priced_off_nothing() {
        let mut risk = Risk::new(limits());
        assert_eq!(
            risk.admit(buy(0.005), &market(), None),
            Err(Rejection::NoTwoSidedBook)
        );
    }

    #[test]
    fn a_quote_that_would_take_liquidity_is_refused() {
        let mut risk = Risk::new(limits());
        assert_eq!(
            risk.admit_maker_price(Side::Bid, 101.0, Some(100.0), Some(101.0)),
            Err(Rejection::WouldCrossBook)
        );
        assert!(
            risk.admit_maker_price(Side::Bid, 100.5, Some(100.0), Some(101.0))
                .is_ok()
        );
        assert_eq!(
            risk.admit_maker_price(Side::Ask, 100.0, Some(100.0), Some(101.0)),
            Err(Rejection::WouldCrossBook)
        );
    }

    #[test]
    fn the_gross_exposure_limit_is_measured_in_quote_units() {
        let mut risk = Risk::new(limits());
        let mut market = market();
        market.inventory = 0.015;
        // 0.02 of BTC at 200,000 is 4,000 of exposure, past the 3,000 limit,
        // while 0.02 itself is inside the inventory limit.
        let err = risk
            .admit(buy(0.005), &market, Some(200_000.0))
            .expect_err("exposure, not size, is what breaches here");
        assert_eq!(err, Rejection::GrossExposureLimit);
    }

    #[test]
    fn breaching_inventory_fires_the_switch_once_and_blocks_everything_after() {
        let mut risk = Risk::new(limits());
        let mut market = market();
        market.inventory = 0.03;
        let breaches = risk.observe(&market, Some(100_000.0), Some(0.0));
        assert_eq!(breaches, vec![Breach::Inventory]);
        assert!(risk.is_killed());
        assert_eq!(risk.activations(), 1);
        // A second sample of the same breach does not count as a second
        // activation: the switch is already down.
        risk.observe(&market, Some(100_000.0), Some(0.0));
        assert_eq!(risk.activations(), 1);
        assert_eq!(
            risk.admit(buy(0.001), &market, Some(100_000.0)),
            Err(Rejection::KillSwitchActive)
        );
    }

    #[test]
    fn breaching_the_loss_floor_fires_the_switch() {
        let mut risk = Risk::new(limits());
        let market = market();
        let breaches = risk.observe(&market, Some(100_000.0), Some(-41.0));
        assert_eq!(breaches, vec![Breach::Loss]);
        assert!(risk.is_killed());
    }

    #[test]
    fn a_pnl_that_cannot_be_marked_is_not_tested_against_the_floor() {
        // Null is not zero, and it is not a loss either.
        let mut risk = Risk::new(limits());
        let mut market = market();
        market.inventory = 0.001;
        let breaches = risk.observe(&market, None, None);
        assert!(breaches.is_empty());
        assert!(!risk.is_killed());
    }

    #[test]
    fn mean_inventory_is_nothing_until_something_is_sampled() {
        let mut risk = Risk::new(limits());
        assert_eq!(risk.mean_abs_inventory(), None);
        let mut market = market();
        market.inventory = 0.01;
        risk.observe(&market, Some(1.0), Some(0.0));
        market.inventory = -0.02;
        risk.observe(&market, Some(1.0), Some(0.0));
        assert_eq!(risk.mean_abs_inventory(), Some(0.015));
        assert_eq!(risk.max_abs_inventory(), 0.02);
    }

    #[test]
    fn controls_held_means_every_breach_was_answered() {
        let mut clean = Risk::new(limits());
        assert!(clean.controls_held());
        let mut market = market();
        market.inventory = 0.0;
        clean.observe(&market, Some(1.0), Some(0.0));
        assert!(clean.controls_held());

        let mut breached = Risk::new(limits());
        market.inventory = 0.5;
        breached.observe(&market, Some(1.0), Some(0.0));
        assert!(breached.controls_held(), "the switch fired, so they held");
    }
}
