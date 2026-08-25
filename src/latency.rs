//! Turning latency that is recorded into latency that is known.
//!
//! Every archived row already carries `skew_ns`, the gap between when a venue
//! stamped a message and when it reached us. Nothing ever summarised it, so the
//! dataset could tell you the skew of any single message and nothing at all
//! about the distribution, which is the thing that says how far to trust the
//! timestamps.
//!
//! A running recorder cannot keep every observation, so this is a histogram
//! rather than a list: bounded memory, and percentiles good to about twelve
//! percent. Exact where exactness is free, which is the count, the sum, and the
//! extremes.
//!
//! # Negative skew is ordinary, not exceptional
//!
//! A message can arrive stamped *later* than we received it, because the two
//! clocks disagree. Measured against Kraken from a laptop: 1,174 of 1,255
//! messages. So negatives are not an edge case to count and set aside, they can
//! be the bulk of the distribution, and a percentile computed over the positive
//! values alone would have reported a median of 31ms for a feed whose real
//! median was below zero.
//!
//! They are therefore bucketed by magnitude on their own side and ordered
//! before the non-negatives, so a quantile walks the whole distribution.

use serde::{Deserialize, Serialize};

/// Sub-buckets per power of two. Eight gives roughly 12% resolution, which is
/// finer than the question "how late is this feed" is ever asked.
const SUB_BITS: u32 = 3;
const SUB: usize = 1 << SUB_BITS;
/// Nanosecond magnitudes fit in 63 bits, and each gets `SUB` buckets.
const BUCKETS: usize = 64 * SUB;

/// A bounded summary of a stream of nanosecond durations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Histogram {
    /// Log-spaced counts over non-negative observations.
    buckets: Vec<u64>,
    /// The same, over the magnitudes of the negative ones. Ordered before the
    /// bucket above when walking a quantile, largest magnitude first.
    below: Vec<u64>,
    count: u64,
    negative: u64,
    sum: i128,
    min: Option<i64>,
    max: Option<i64>,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            buckets: vec![0; BUCKETS],
            below: vec![0; BUCKETS],
            count: 0,
            negative: 0,
            sum: 0,
            min: None,
            max: None,
        }
    }
}

fn index_of(nanos: u64) -> usize {
    if nanos < SUB as u64 {
        return nanos as usize;
    }
    // Position of the leading bit, then the next SUB_BITS bits beneath it.
    let exponent = 63 - nanos.leading_zeros();
    let shift = exponent - SUB_BITS;
    let sub = (nanos >> shift) as usize & (SUB - 1);
    ((exponent - SUB_BITS + 1) as usize) * SUB + sub
}

/// The smallest value that lands in a bucket, used to report a percentile.
fn floor_of(index: usize) -> u64 {
    if index < SUB {
        return index as u64;
    }
    let group = index / SUB;
    let sub = (index % SUB) as u64;
    let shift = group as u32 - 1;
    ((SUB as u64) + sub) << shift
}

impl Histogram {
    pub fn record(&mut self, nanos: i64) {
        self.count += 1;
        self.sum += nanos as i128;
        self.min = Some(self.min.map_or(nanos, |m| m.min(nanos)));
        self.max = Some(self.max.map_or(nanos, |m| m.max(nanos)));
        if nanos < 0 {
            self.negative += 1;
            let i = index_of(nanos.unsigned_abs()).min(BUCKETS - 1);
            self.below[i] += 1;
            return;
        }
        let i = index_of(nanos as u64).min(BUCKETS - 1);
        self.buckets[i] += 1;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// How many observations had the venue's clock ahead of ours.
    pub fn negative(&self) -> u64 {
        self.negative
    }

    pub fn min(&self) -> Option<i64> {
        self.min
    }

    pub fn max(&self) -> Option<i64> {
        self.max
    }

    pub fn mean(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum as f64 / self.count as f64)
    }

    /// The value at `q`, in nanoseconds, over **every** observation.
    ///
    /// Signed, and the sign matters: a feed whose clock runs ahead of ours has
    /// a genuinely negative median, and reporting a quantile over the positive
    /// values alone would put that median at tens of milliseconds.
    ///
    /// Reported as a bucket edge nearest zero, so the magnitude understates
    /// rather than overstates. A latency figure that rounds up alarms people
    /// with a number the data does not support.
    pub fn quantile(&self, q: f64) -> Option<i64> {
        if self.count == 0 {
            return None;
        }
        let target = (q.clamp(0.0, 1.0) * self.count as f64).ceil().max(1.0) as u64;
        // A bucket edge is an approximation, and on the negative side negating
        // a magnitude floor moves the answer *towards* zero, which can put a
        // high quantile above the true maximum. Seen for real on Binance.US:
        // every observation negative, and p99 reported above max. Clamping to
        // the exact extremes keeps every quantile a value the data supports.
        let clamp = |v: i64| match (self.min, self.max) {
            (Some(lo), Some(hi)) => v.clamp(lo, hi),
            _ => v,
        };
        let mut seen = 0u64;
        // Most negative first: that is the low end of the distribution.
        for i in (0..BUCKETS).rev() {
            seen += self.below[i];
            if seen >= target {
                return Some(clamp(-(floor_of(i) as i64)));
            }
        }
        for (i, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= target {
                return Some(clamp(floor_of(i) as i64));
            }
        }
        None
    }

    pub fn p50(&self) -> Option<i64> {
        self.quantile(0.50)
    }

    pub fn p99(&self) -> Option<i64> {
        self.quantile(0.99)
    }

    /// Fold another histogram in, for merging per-connection runs.
    pub fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += b;
        }
        for (a, b) in self.below.iter_mut().zip(other.below.iter()) {
            *a += b;
        }
        self.count += other.count;
        self.negative += other.negative;
        self.sum += other.sum;
        self.min = match (self.min, other.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max = match (self.max, other.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }

    /// `p50 / p99 / max`, in milliseconds, for a report column.
    pub fn summary_millis(&self) -> String {
        let ms = |n: Option<i64>| match n {
            Some(v) => format!("{:.2}", v as f64 / 1e6),
            None => "-".to_string(),
        };
        format!(
            "{} / {} / {}",
            ms(self.p50()),
            ms(self.p99()),
            ms(self.max())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_histogram_reports_nothing_rather_than_zero() {
        let h = Histogram::default();
        assert!(h.is_empty());
        assert_eq!(h.p50(), None);
        assert_eq!(h.mean(), None);
        assert_eq!(h.summary_millis(), "- / - / -");
    }

    #[test]
    fn small_values_are_recorded_exactly() {
        // Below the first octave every bucket holds one value, so there is no
        // approximation to argue about.
        let mut h = Histogram::default();
        for n in 0..8 {
            h.record(n);
        }
        assert_eq!(h.quantile(0.0), Some(0));
        assert_eq!(h.max(), Some(7));
    }

    #[test]
    fn percentiles_land_within_the_stated_resolution() {
        let mut h = Histogram::default();
        for n in 1..=100_000i64 {
            h.record(n * 1_000); // 1us to 100ms
        }
        let p50 = h.p50().unwrap();
        let expected = 50_000_000u64;
        let error = (p50 as f64 - expected as f64).abs() / expected as f64;
        assert!(error < 0.13, "p50 {p50} is {error:.3} off {expected}");

        let p99 = h.p99().unwrap();
        let expected99 = 99_000_000u64;
        let error99 = (p99 as f64 - expected99 as f64).abs() / expected99 as f64;
        assert!(error99 < 0.13, "p99 {p99} is {error99:.3} off {expected99}");
    }

    #[test]
    fn a_percentile_never_overstates() {
        // Reporting a bucket floor means the number is always one the data
        // supports, rather than one the bucket boundary invented.
        let mut h = Histogram::default();
        for n in [1_000_000i64, 2_000_000, 3_000_000] {
            h.record(n);
        }
        assert!(h.p99().unwrap() <= 3_000_000);
    }

    #[test]
    fn a_venue_clock_running_ahead_is_ordered_below_zero() {
        let mut h = Histogram::default();
        h.record(-5_000_000);
        h.record(10_000_000);
        assert_eq!(h.negative(), 1);
        assert_eq!(h.count(), 2);
        assert_eq!(h.min(), Some(-5_000_000));
        // Half the observations are below zero, so the low end of the
        // distribution is too.
        assert!(h.quantile(0.0).unwrap() < 0);
        assert!(h.quantile(1.0).unwrap() > 0);
    }

    #[test]
    fn a_mostly_negative_feed_has_a_negative_median() {
        // Measured against Kraken from a laptop: 1,174 of 1,255 messages
        // arrived stamped ahead of our clock. A quantile computed over the
        // positive values alone reported a median of 31ms for that feed. The
        // real median is below zero, and saying otherwise invents latency that
        // the venue is not responsible for.
        let mut h = Histogram::default();
        for _ in 0..1_174 {
            h.record(-20_000_000);
        }
        for _ in 0..81 {
            h.record(30_000_000);
        }
        let p50 = h.p50().unwrap();
        assert!(p50 < 0, "median came out at {p50}, above zero");
        // The tail is still positive, because some messages really were late.
        assert!(h.p99().unwrap() > 0);
    }

    #[test]
    fn no_quantile_ever_falls_outside_the_observed_range() {
        // The invariant that caught a real bug: on a feed where every
        // observation was negative, p99 came out above the maximum.
        for values in [
            vec![-9_590_000i64, -9_500_000, -9_440_000, -80_000_000],
            vec![1, 2, 3, 4, 5],
            vec![-1_000_000_000, 1_000_000_000],
            (0..200).map(|n| n * 7 - 500).collect(),
        ] {
            let mut h = Histogram::default();
            for v in &values {
                h.record(*v);
            }
            let (lo, hi) = (h.min().unwrap(), h.max().unwrap());
            for q in [0.0, 0.01, 0.25, 0.5, 0.9, 0.99, 1.0] {
                let v = h.quantile(q).unwrap();
                assert!(v >= lo && v <= hi, "q{q} gave {v}, outside [{lo}, {hi}]");
            }
        }
    }

    #[test]
    fn quantiles_move_monotonically_across_the_sign_boundary() {
        let mut h = Histogram::default();
        for n in -50..50i64 {
            h.record(n * 1_000_000);
        }
        let mut last = i64::MIN;
        for q in [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0] {
            let v = h.quantile(q).unwrap();
            assert!(v >= last, "quantile {q} gave {v} after {last}");
            last = v;
        }
    }

    #[test]
    fn extremes_and_totals_stay_exact() {
        let mut h = Histogram::default();
        for n in [3i64, 1_000_000, 999, 250_000_000] {
            h.record(n);
        }
        assert_eq!(h.min(), Some(3));
        assert_eq!(h.max(), Some(250_000_000));
        assert_eq!(h.count(), 4);
        let mean = h.mean().unwrap();
        assert!((mean - (3.0 + 1_000_000.0 + 999.0 + 250_000_000.0) / 4.0).abs() < 1.0);
    }

    #[test]
    fn merging_is_the_same_as_recording_into_one() {
        let values: Vec<i64> = (1..500).map(|n| n * 12_345).collect();
        let mut whole = Histogram::default();
        for v in &values {
            whole.record(*v);
        }
        let (a, b) = values.split_at(200);
        let mut left = Histogram::default();
        for v in a {
            left.record(*v);
        }
        let mut right = Histogram::default();
        for v in b {
            right.record(*v);
        }
        left.merge(&right);
        assert_eq!(left, whole);
    }

    #[test]
    fn every_bucket_index_round_trips_below_its_own_value() {
        // The invariant the percentile rests on: a bucket's floor belongs to
        // that bucket, and never overstates the values inside it.
        for shift in 0..62 {
            for extra in [0u64, 1, 7, 100] {
                let v = (1u64 << shift).saturating_add(extra);
                let i = index_of(v);
                assert!(i < BUCKETS, "{v} landed outside the histogram");
                assert!(floor_of(i) <= v, "floor of {v} was {} ", floor_of(i));
                assert_eq!(index_of(floor_of(i)), i, "{v} did not round trip");
            }
        }
    }
}
