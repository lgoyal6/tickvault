//! Fees, slippage, and a latency model that is the same every run.
//!
//! Two things here are worth being explicit about, because both flatter a
//! quoting strategy and both are frozen in the manifest rather than chosen
//! while looking at a result.
//!
//! The maker rebate is zero, which means a maker fill pays nothing and receives
//! nothing. Real spot maker fees are usually a positive cost, so every quoting
//! number produced here is an upper bound rather than an estimate.
//!
//! The latency model is a shifted exponential sampled from a seeded
//! [`SplitMix64`], not a constant. A constant delay is the wrong shape: it is
//! the tail that decides whether a quote is still there when the market moves,
//! and a model with no tail cannot show that. The seed is derived from the run's
//! own identity, so the same window with the same parameters draws the same
//! delays whichever order the runs happen in, which is what makes the
//! determinism check in the gate mean something.

use crate::manifest::{Costs as CostSpec, LatencyLeg, LatencyModel as LatencySpec};

/// A small deterministic generator.
///
/// SplitMix64 rather than anything from `rand`: this needs to produce the same
/// stream on every machine and every build, and an implementation in twenty
/// lines that can be read in full is worth more here than a distribution
/// library whose defaults could change under us.
#[derive(Debug, Clone, Copy)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in the open interval (0, 1). Never zero, because the latency
    /// model takes its logarithm.
    pub fn next_unit(&mut self) -> f64 {
        // 53 bits of mantissa, shifted off zero by half a step.
        let bits = self.next_u64() >> 11;
        (bits as f64 + 0.5) / (1u64 << 53) as f64
    }

    /// A uniform index below `n`.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next_u64() % n as u64) as usize
    }
}

/// Mix a run's identity into a seed, so runs are independent but reproducible.
pub fn derive_seed(base: u64, venue: &str, window: usize, strategy: &str, params: usize) -> u64 {
    let mut mix = SplitMix64::new(base);
    for part in [venue, strategy] {
        for byte in part.as_bytes() {
            mix.state ^= *byte as u64;
            mix.next_u64();
        }
    }
    mix.state ^= (window as u64) << 32;
    mix.next_u64();
    mix.state ^= params as u64;
    mix.next_u64()
}

/// Sampled delays, and what they came out as.
#[derive(Debug, Clone)]
pub struct Latency {
    entry: LatencyLeg,
    market_data: LatencyLeg,
    rng: SplitMix64,
    entry_samples: Vec<f64>,
    market_data_samples: Vec<f64>,
}

fn shifted_exponential(leg: &LatencyLeg, rng: &mut SplitMix64) -> f64 {
    let u = rng.next_unit();
    let sample = leg.shift + leg.mean_excess * -u.ln();
    sample.clamp(leg.clamp_ms[0], leg.clamp_ms[1])
}

impl Latency {
    pub fn new(spec: &LatencySpec, seed: u64) -> Self {
        Latency {
            entry: spec.order_entry_ms,
            market_data: spec.market_data_ms,
            rng: SplitMix64::new(seed),
            entry_samples: Vec::new(),
            market_data_samples: Vec::new(),
        }
    }

    /// How long an order takes to reach the venue, in nanoseconds.
    pub fn order_entry_nanos(&mut self) -> i64 {
        let ms = shifted_exponential(&self.entry, &mut self.rng);
        self.entry_samples.push(ms);
        (ms * 1_000_000.0) as i64
    }

    /// How long a message takes to reach the strategy, in nanoseconds.
    pub fn market_data_nanos(&mut self) -> i64 {
        let ms = shifted_exponential(&self.market_data, &mut self.rng);
        self.market_data_samples.push(ms);
        (ms * 1_000_000.0) as i64
    }

    /// The most recent order-entry delay, for stamping on a fill.
    pub fn last_entry_ms(&self) -> f64 {
        self.entry_samples.last().copied().unwrap_or(0.0)
    }

    pub fn entry_samples(&self) -> &[f64] {
        &self.entry_samples
    }

    pub fn market_data_samples(&self) -> &[f64] {
        &self.market_data_samples
    }

    /// Every delay drawn, both legs together.
    pub fn all_samples(&self) -> Vec<f64> {
        let mut all = self.entry_samples.clone();
        all.extend_from_slice(&self.market_data_samples);
        all
    }
}

/// The quantile of a sample, by nearest rank on the sorted values.
///
/// Not interpolated: a p99 that is a weighted average of two observations is
/// not one of the delays that actually happened.
pub fn quantile(samples: &[f64], q: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("latency samples are finite"));
    let rank = (q * (sorted.len() - 1) as f64).round() as usize;
    Some(sorted[rank.min(sorted.len() - 1)])
}

/// The frozen cost block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Costs {
    pub taker_fee_bps: f64,
    pub maker_rebate_bps: f64,
    pub extra_slippage_bps: f64,
}

impl Costs {
    pub fn from_spec(spec: &CostSpec) -> Self {
        Costs {
            taker_fee_bps: spec.taker_fee_bps,
            maker_rebate_bps: spec.maker_rebate_bps,
            extra_slippage_bps: spec.extra_slippage_bps,
        }
    }

    /// Every cost set to nothing.
    ///
    /// Only the `no-costs` control builds this, and the evaluator refuses to
    /// report its output as comparable to a run that paid.
    pub fn zeroed() -> Self {
        Costs {
            taker_fee_bps: 0.0,
            maker_rebate_bps: 0.0,
            extra_slippage_bps: 0.0,
        }
    }

    pub fn is_zeroed(&self) -> bool {
        self.taker_fee_bps == 0.0 && self.maker_rebate_bps == 0.0 && self.extra_slippage_bps == 0.0
    }

    /// What a marketable fill costs. Positive is money leaving.
    pub fn taker_fee(&self, notional: f64) -> f64 {
        notional.abs() * self.taker_fee_bps / 10_000.0
    }

    /// What a resting fill costs. Negative would be a rebate received.
    pub fn maker_fee(&self, notional: f64) -> f64 {
        -notional.abs() * self.maker_rebate_bps / 10_000.0
    }

    /// The price a marketable order actually pays at a book level, after the
    /// slippage the book itself does not show.
    pub fn slipped(&self, buying: bool, level_price: f64) -> f64 {
        let shift = level_price * self.extra_slippage_bps / 10_000.0;
        if buying {
            level_price + shift
        } else {
            level_price - shift
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> LatencySpec {
        LatencySpec {
            order_entry_ms: LatencyLeg {
                shift: 3.0,
                mean_excess: 4.0,
                clamp_ms: [3.0, 60.0],
            },
            market_data_ms: LatencyLeg {
                shift: 1.0,
                mean_excess: 2.0,
                clamp_ms: [1.0, 40.0],
            },
            base_seed: 20260825,
        }
    }

    #[test]
    fn the_same_seed_draws_the_same_delays() {
        let mut a = Latency::new(&spec(), 7);
        let mut b = Latency::new(&spec(), 7);
        for _ in 0..64 {
            assert_eq!(a.order_entry_nanos(), b.order_entry_nanos());
            assert_eq!(a.market_data_nanos(), b.market_data_nanos());
        }
    }

    #[test]
    fn a_different_run_draws_different_delays() {
        let one = derive_seed(20260825, "kraken", 1, "fixed_spread_mm", 0);
        let two = derive_seed(20260825, "kraken", 2, "fixed_spread_mm", 0);
        let three = derive_seed(20260825, "okx", 1, "fixed_spread_mm", 0);
        let four = derive_seed(20260825, "kraken", 1, "fixed_spread_mm", 1);
        assert_ne!(one, two);
        assert_ne!(one, three);
        assert_ne!(one, four);
    }

    #[test]
    fn every_delay_respects_its_clamp_and_its_floor() {
        let mut latency = Latency::new(&spec(), 99);
        for _ in 0..20_000 {
            let entry = latency.order_entry_nanos() as f64 / 1_000_000.0;
            assert!((3.0..=60.0).contains(&entry), "{entry}");
            let md = latency.market_data_nanos() as f64 / 1_000_000.0;
            assert!((1.0..=40.0).contains(&md), "{md}");
        }
    }

    #[test]
    fn the_delay_distribution_has_a_tail() {
        // A constant delay cannot show whether a quote survives a move, so the
        // model must actually spread out.
        let mut latency = Latency::new(&spec(), 4);
        for _ in 0..20_000 {
            latency.order_entry_nanos();
        }
        let samples = latency.entry_samples();
        let median = quantile(samples, 0.5).unwrap();
        let p99 = quantile(samples, 0.99).unwrap();
        assert!(median > 3.0, "median {median}");
        assert!(p99 > median * 2.0, "p99 {p99} is not a tail over {median}");
    }

    #[test]
    fn a_quantile_is_an_observation_not_an_average_of_two() {
        let samples = [1.0, 2.0, 3.0, 4.0];
        let q = quantile(&samples, 0.5).unwrap();
        assert!(samples.contains(&q), "{q} is not one of the samples");
        assert_eq!(quantile(&samples, 0.0), Some(1.0));
        assert_eq!(quantile(&samples, 1.0), Some(4.0));
        assert_eq!(quantile(&[], 0.5), None);
    }

    #[test]
    fn a_taker_pays_and_a_maker_with_no_rebate_pays_nothing() {
        let costs = Costs {
            taker_fee_bps: 5.0,
            maker_rebate_bps: 0.0,
            extra_slippage_bps: 0.5,
        };
        assert_eq!(costs.taker_fee(10_000.0), 5.0);
        assert_eq!(costs.maker_fee(10_000.0), 0.0);
    }

    #[test]
    fn slippage_always_moves_against_the_side_that_crossed() {
        let costs = Costs {
            taker_fee_bps: 5.0,
            maker_rebate_bps: 0.0,
            extra_slippage_bps: 1.0,
        };
        assert!(costs.slipped(true, 100.0) > 100.0);
        assert!(costs.slipped(false, 100.0) < 100.0);
        let zero = Costs::zeroed();
        assert_eq!(zero.slipped(true, 100.0), 100.0);
        assert!(zero.is_zeroed());
    }
}
