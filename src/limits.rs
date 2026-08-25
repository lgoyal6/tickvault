//! Rate limiting and subscription budgeting.
//!
//! Two jobs that are usually treated as one and should not be.
//!
//! **Rate limiting** keeps us inside what a venue permits per second. Exceeding
//! it does not fail loudly; it gets the connection dropped or the IP banned,
//! which shows up in the dataset as a gap the venue did not actually cause.
//!
//! **Subscription budgeting** decides how many symbols share a socket, and that
//! is not purely an efficiency question. On a venue whose sequence numbers
//! belong to the connection rather than the instrument, every symbol on a
//! socket is invalidated by any one gap on it. Packing forty symbols onto one
//! Coinbase connection makes a single dropped message cost forty suspect
//! windows instead of one. The planner reads that from the capability matrix.
//!
//! The limits encoded per venue are deliberately conservative rather than the
//! documented maxima. Being throttled corrupts the dataset; being slow to
//! subscribe does not.

use std::time::Duration;

use crate::clock::Clock;
use crate::types::Symbol;

/// What a venue tolerates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueBudget {
    /// Symbols we are willing to put on one socket.
    pub subscriptions_per_connection: usize,
    /// Sockets we are willing to open to this venue.
    pub max_connections: usize,
    /// Minimum sustained gap between websocket connection attempts.
    ///
    /// An interval rather than a rate so the whole budget stays comparable and
    /// hashable, and so it reads the way the venue documents it.
    pub connect_interval: Duration,
    /// Connection attempts allowed back to back before the interval bites.
    pub connect_burst: u32,
    /// Minimum sustained gap between REST calls.
    pub rest_interval: Duration,
    /// REST calls allowed back to back.
    pub rest_burst: u32,
}

impl VenueBudget {
    /// Deliberately cautious defaults for a venue we have not tuned.
    pub const fn conservative() -> Self {
        VenueBudget {
            subscriptions_per_connection: 10,
            max_connections: 4,
            connect_interval: Duration::from_secs(2),
            connect_burst: 2,
            rest_interval: Duration::from_secs(1),
            rest_burst: 2,
        }
    }

    /// The most symbols this venue can carry at once under the budget.
    pub fn capacity(&self) -> usize {
        self.subscriptions_per_connection
            .saturating_mul(self.max_connections)
    }
}

impl Default for VenueBudget {
    fn default() -> Self {
        Self::conservative()
    }
}

/// Why a set of symbols cannot be recorded from one venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetError {
    /// More symbols than the connection budget can carry.
    OverCapacity {
        requested: usize,
        capacity: usize,
        max_connections: usize,
    },
    /// Nothing to subscribe to.
    Empty,
}

impl std::fmt::Display for BudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BudgetError::OverCapacity {
                requested,
                capacity,
                max_connections,
            } => write!(
                f,
                "{requested} symbols exceed this venue's budget of {capacity} \
                 ({max_connections} connections); raise the budget deliberately \
                 rather than silently dropping symbols"
            ),
            BudgetError::Empty => write!(f, "no symbols requested"),
        }
    }
}

impl std::error::Error for BudgetError {}

/// How symbols are distributed across sockets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionPlan {
    pub connections: Vec<Vec<Symbol>>,
    /// Why this shape was chosen, for the log line that explains a wide outage.
    pub rationale: &'static str,
}

impl SubscriptionPlan {
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn symbol_count(&self) -> usize {
        self.connections.iter().map(Vec::len).sum()
    }

    /// The number of symbols a single gap would invalidate, at worst.
    pub fn worst_case_blast_radius(&self, gap_is_connection_wide: bool) -> usize {
        if gap_is_connection_wide {
            self.connections.iter().map(Vec::len).max().unwrap_or(0)
        } else {
            1
        }
    }
}

/// Lay symbols out across connections.
///
/// `spread` should be true when a gap on the connection invalidates every
/// symbol on it, which is read from [`crate::venue::VenueCapabilities`]. In that
/// case symbols are spread as thinly as the connection budget allows, trading
/// more sockets for a smaller blast radius. Otherwise they are packed, which
/// uses fewer sockets and is strictly better when a gap only affects one
/// instrument.
pub fn plan(
    symbols: &[Symbol],
    budget: &VenueBudget,
    spread: bool,
) -> Result<SubscriptionPlan, BudgetError> {
    if symbols.is_empty() {
        return Err(BudgetError::Empty);
    }
    let capacity = budget.capacity();
    if symbols.len() > capacity {
        return Err(BudgetError::OverCapacity {
            requested: symbols.len(),
            capacity,
            max_connections: budget.max_connections,
        });
    }

    let (buckets, rationale) = if spread {
        // Use as many sockets as allowed, so no one gap takes down more
        // symbols than it has to.
        let wanted = budget.max_connections.min(symbols.len()).max(1);
        (
            wanted,
            "sequence numbers belong to the connection, so symbols are spread to \
             limit how many a single gap invalidates",
        )
    } else {
        let needed = symbols.len().div_ceil(budget.subscriptions_per_connection);
        (
            needed.max(1),
            "sequence numbers are per symbol, so symbols are packed onto as few \
             sockets as the budget allows",
        )
    };

    let mut connections: Vec<Vec<Symbol>> = vec![Vec::new(); buckets];
    for (i, symbol) in symbols.iter().enumerate() {
        connections[i % buckets].push(symbol.clone());
    }
    connections.retain(|c| !c.is_empty());

    debug_assert!(
        connections
            .iter()
            .all(|c| c.len() <= budget.subscriptions_per_connection),
        "planner produced a connection over the per-socket limit"
    );

    Ok(SubscriptionPlan {
        connections,
        rationale,
    })
}

/// A token bucket, driven by an injected clock so it is testable without
/// sleeping and deterministic under replay.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    capacity: f64,
    tokens: f64,
    per_second: f64,
    last_nanos: Option<u64>,
}

impl RateLimiter {
    /// One token every `interval`, up to `burst` in hand.
    pub fn new(interval: Duration, burst: u32) -> Self {
        assert!(
            !interval.is_zero(),
            "a zero interval would make the limiter meaningless"
        );
        let per_second = 1.0 / interval.as_secs_f64();
        let capacity = burst.max(1) as f64;
        RateLimiter {
            capacity,
            tokens: capacity,
            per_second,
            last_nanos: None,
        }
    }

    /// Tokens currently available, for tests and logging.
    pub fn available(&self) -> f64 {
        self.tokens
    }

    fn refill(&mut self, now_nanos: u64) {
        if let Some(last) = self.last_nanos {
            let elapsed = now_nanos.saturating_sub(last) as f64 / 1e9;
            self.tokens = (self.tokens + elapsed * self.per_second).min(self.capacity);
        }
        self.last_nanos = Some(now_nanos);
    }

    /// How long to wait before the next call is permitted, taking a token if it
    /// is available now.
    ///
    /// Returns `Duration::ZERO` when the call may proceed immediately.
    pub fn acquire(&mut self, clock: &dyn Clock) -> Duration {
        let now = clock.stamp().mono_nanos;
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Duration::ZERO;
        }
        let deficit = 1.0 - self.tokens;
        let seconds = deficit / self.per_second;
        // The token is spent on the caller's behalf; they are expected to wait.
        self.tokens = 0.0;
        Duration::from_secs_f64(seconds)
    }
}

/// Rate limiters for one venue.
#[derive(Debug, Clone)]
pub struct VenueLimiter {
    pub connects: RateLimiter,
    pub rest: RateLimiter,
}

impl VenueLimiter {
    pub fn new(budget: &VenueBudget) -> Self {
        VenueLimiter {
            connects: RateLimiter::new(budget.connect_interval, budget.connect_burst),
            rest: RateLimiter::new(budget.rest_interval, budget.rest_burst),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    fn symbols(n: usize) -> Vec<Symbol> {
        (0..n)
            .map(|i| Symbol::new(&format!("A{i}"), "USD"))
            .collect()
    }

    fn budget(per_conn: usize, conns: usize) -> VenueBudget {
        VenueBudget {
            subscriptions_per_connection: per_conn,
            max_connections: conns,
            ..VenueBudget::conservative()
        }
    }

    #[test]
    fn packing_uses_as_few_sockets_as_possible() {
        let packed = plan(&symbols(25), &budget(10, 8), false).unwrap();
        assert_eq!(packed.connection_count(), 3);
        assert_eq!(packed.symbol_count(), 25);
        assert!(packed.connections.iter().all(|c| c.len() <= 10));
    }

    #[test]
    fn spreading_uses_every_socket_the_budget_allows() {
        // The Coinbase case: a gap invalidates the whole socket, so four
        // symbols go on four sockets rather than all on one.
        let spread_plan = plan(&symbols(4), &budget(10, 8), true).unwrap();
        assert_eq!(spread_plan.connection_count(), 4);
        assert_eq!(spread_plan.worst_case_blast_radius(true), 1);
        // Packed, the same symbols would all fall together.
        let packed = plan(&symbols(4), &budget(10, 8), false).unwrap();
        assert_eq!(packed.connection_count(), 1);
        assert_eq!(packed.worst_case_blast_radius(true), 4);
    }

    #[test]
    fn spreading_still_respects_the_per_socket_limit() {
        let laid_out = plan(&symbols(16), &budget(4, 8), true).unwrap();
        assert!(
            laid_out.connections.iter().all(|c| c.len() <= 4),
            "{:?}",
            laid_out
                .connections
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>()
        );
        assert_eq!(laid_out.symbol_count(), 16);
    }

    #[test]
    fn every_symbol_appears_exactly_once_in_a_plan() {
        for spread in [true, false] {
            let all = symbols(23);
            let laid_out = plan(&all, &budget(6, 8), spread).unwrap();
            let mut flat: Vec<&Symbol> = laid_out.connections.iter().flatten().collect();
            flat.sort();
            flat.dedup();
            assert_eq!(flat.len(), 23, "spread={spread}");
        }
    }

    #[test]
    fn exceeding_the_budget_is_an_error_not_a_silent_truncation() {
        let err = plan(&symbols(50), &budget(10, 4), false).unwrap_err();
        assert_eq!(
            err,
            BudgetError::OverCapacity {
                requested: 50,
                capacity: 40,
                max_connections: 4
            }
        );
        assert!(err.to_string().contains("rather than silently dropping"));
    }

    #[test]
    fn an_empty_request_is_rejected() {
        assert_eq!(plan(&[], &budget(10, 4), false), Err(BudgetError::Empty));
    }

    #[test]
    fn a_full_bucket_lets_a_burst_through_then_makes_the_caller_wait() {
        let clock = ManualClock::new(0, 0);
        let mut limiter = RateLimiter::new(Duration::from_millis(500), 3);
        for i in 0..3 {
            assert_eq!(limiter.acquire(&clock), Duration::ZERO, "burst slot {i}");
        }
        // Fourth call must wait half a second at two per second.
        let wait = limiter.acquire(&clock);
        assert!(
            (wait.as_secs_f64() - 0.5).abs() < 1e-6,
            "expected ~0.5s, got {wait:?}"
        );
    }

    #[test]
    fn tokens_refill_as_the_clock_advances() {
        let clock = ManualClock::new(0, 0);
        let mut limiter = RateLimiter::new(Duration::from_millis(100), 2);
        limiter.acquire(&clock);
        limiter.acquire(&clock);
        assert!(limiter.acquire(&clock) > Duration::ZERO);
        // A full second at ten per second refills well past the burst cap.
        clock.advance(1_000_000_000);
        assert_eq!(limiter.acquire(&clock), Duration::ZERO);
        assert!(
            limiter.available() <= 2.0,
            "refill must not exceed the burst capacity"
        );
    }

    #[test]
    fn a_limiter_is_deterministic_for_a_given_clock() {
        let run = || {
            let clock = ManualClock::new(0, 1_000_000);
            let mut limiter = RateLimiter::new(Duration::from_millis(250), 2);
            (0..6)
                .map(|_| limiter.acquire(&clock))
                .collect::<Vec<Duration>>()
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn capacity_is_the_product_of_the_two_limits() {
        assert_eq!(budget(10, 4).capacity(), 40);
        assert_eq!(VenueBudget::conservative().capacity(), 40);
    }
}
