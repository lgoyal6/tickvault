//! Replaying a range at wall-clock speed, or as fast as it reads.
//!
//! A backtest wants the archive as fast as the disk gives it. Testing whether a
//! strategy can *keep up* wants the gaps between messages preserved, so it sees
//! the burst that would have arrived in one millisecond as one millisecond of
//! work rather than as a comfortable trickle.
//!
//! Pacing is against the recorded receipt gaps, so a replay reproduces the
//! feed's own rhythm rather than a smoothed version of it.

use std::time::Duration;

#[cfg(feature = "record")]
use crate::error::Result;
#[cfg(feature = "record")]
use crate::query::{BookCursor, Tick};

/// How fast to replay.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Speed {
    /// As fast as the archive reads.
    Unpaced,
    /// Preserve the recorded gaps, scaled. A multiplier of one is real time;
    /// ten is ten times faster; a half is half speed.
    Scaled(f64),
}

impl Speed {
    pub fn real_time() -> Self {
        Speed::Scaled(1.0)
    }

    /// How long to wait before delivering a message `gap_nanos` after the last.
    ///
    /// Returns zero for an unpaced replay and for a gap that has already
    /// elapsed, so a slow consumer never accrues a debt of sleeps it can never
    /// pay off.
    pub fn delay_for(&self, gap_nanos: i64, already_elapsed: Duration) -> Duration {
        let Speed::Scaled(multiplier) = self else {
            return Duration::ZERO;
        };
        if gap_nanos <= 0 || *multiplier <= 0.0 {
            return Duration::ZERO;
        }
        let target = Duration::from_nanos((gap_nanos as f64 / multiplier) as u64);
        target.saturating_sub(already_elapsed)
    }
}

/// What a replay did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReplayStats {
    pub messages: u64,
    pub suspect_messages: u64,
    /// Wall-clock span of the replayed range, from the archive's own stamps.
    pub archive_span_nanos: i64,
    /// How long the replay actually took.
    pub elapsed_nanos: u64,
}

impl ReplayStats {
    /// How much faster than real time the replay ran.
    pub fn speedup(&self) -> Option<f64> {
        if self.elapsed_nanos == 0 || self.archive_span_nanos <= 0 {
            return None;
        }
        Some(self.archive_span_nanos as f64 / self.elapsed_nanos as f64)
    }
}

/// Replay a range, calling `f` after each message.
///
/// Async because pacing means sleeping, and sleeping on the runtime is the only
/// way to do that without blocking everything else on it.
///
/// Behind `record` for that reason. A caller without a runtime, the browser
/// being the one that matters here, drives [`BookCursor`] directly and paces
/// itself against its own clock.
#[cfg(feature = "record")]
pub async fn replay<F>(cursor: &mut BookCursor, speed: Speed, mut f: F) -> Result<ReplayStats>
where
    F: FnMut(&Tick, &mut BookCursor) -> Result<()>,
{
    let started = std::time::Instant::now();
    let mut stats = ReplayStats::default();
    let mut previous_wall: Option<i64> = None;
    let mut first_wall: Option<i64> = None;
    let mut last_wall = 0i64;

    loop {
        let Some(tick) = cursor.advance()? else {
            break;
        };
        if let Some(previous) = previous_wall {
            // Measured from the replay's own start, so a consumer that took
            // longer than the gap simply proceeds rather than falling further
            // behind on every message.
            let due = tick.at_wall - first_wall.unwrap_or(tick.at_wall);
            let delay = speed.delay_for(
                tick.at_wall - previous,
                started
                    .elapsed()
                    .saturating_sub(Duration::from_nanos(due.max(0) as u64)),
            );
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
        previous_wall = Some(tick.at_wall);
        first_wall.get_or_insert(tick.at_wall);
        last_wall = tick.at_wall;

        stats.messages += 1;
        if tick.suspect {
            stats.suspect_messages += 1;
        }
        f(&tick, cursor)?;
    }

    stats.archive_span_nanos = last_wall - first_wall.unwrap_or(last_wall);
    stats.elapsed_nanos = started.elapsed().as_nanos() as u64;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unpaced_replay_never_waits() {
        assert_eq!(
            Speed::Unpaced.delay_for(1_000_000_000, Duration::ZERO),
            Duration::ZERO
        );
    }

    #[test]
    fn real_time_preserves_the_recorded_gap() {
        let delay = Speed::real_time().delay_for(250_000_000, Duration::ZERO);
        assert_eq!(delay, Duration::from_millis(250));
    }

    #[test]
    fn a_multiplier_scales_the_gap() {
        assert_eq!(
            Speed::Scaled(10.0).delay_for(1_000_000_000, Duration::ZERO),
            Duration::from_millis(100)
        );
        assert_eq!(
            Speed::Scaled(0.5).delay_for(1_000_000_000, Duration::ZERO),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn a_slow_consumer_does_not_accrue_a_debt_of_sleeps() {
        // It already took longer than the gap, so there is nothing left to wait
        // for. Waiting anyway would make every later message later still.
        let delay = Speed::real_time().delay_for(100_000_000, Duration::from_millis(500));
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn a_backwards_or_zero_gap_never_waits() {
        // Wall clocks step. Sleeping on a negative gap would hang the replay.
        assert_eq!(
            Speed::real_time().delay_for(-1_000_000, Duration::ZERO),
            Duration::ZERO
        );
        assert_eq!(
            Speed::real_time().delay_for(0, Duration::ZERO),
            Duration::ZERO
        );
    }

    #[test]
    fn speedup_is_reported_only_when_it_means_something() {
        let stats = ReplayStats {
            messages: 10,
            suspect_messages: 0,
            archive_span_nanos: 10_000_000_000,
            elapsed_nanos: 1_000_000_000,
        };
        assert_eq!(stats.speedup(), Some(10.0));
        assert_eq!(ReplayStats::default().speedup(), None);
    }
}
