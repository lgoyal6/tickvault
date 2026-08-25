//! Receipt timestamps.
//!
//! Two clocks, deliberately. The monotonic reading is what we order the archive
//! by, because a wall clock can step backwards under NTP and turn a clean feed
//! into a fake sequence violation. The wall reading is what makes a recording
//! comparable to anything recorded on another machine.
//!
//! We keep the venue's own timestamp separately and never reconcile it with
//! ours. The difference between the two is one of the more useful columns in
//! the dataset: it is the only view a downstream user gets of how stale the
//! book was by the time it reached them.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A single instant, read from both clocks at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Stamp {
    /// Nanoseconds on a monotonic clock, measured from the recorder's start.
    pub mono_nanos: u64,
    /// Nanoseconds since the Unix epoch.
    pub wall_nanos: i64,
}

impl Stamp {
    pub const ZERO: Stamp = Stamp {
        mono_nanos: 0,
        wall_nanos: 0,
    };

    /// Monotonic nanoseconds elapsed since an earlier stamp; zero if `earlier`
    /// is not actually earlier.
    pub fn since(self, earlier: Stamp) -> u64 {
        self.mono_nanos.saturating_sub(earlier.mono_nanos)
    }
}

impl fmt::Display for Stamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "+{}ns (wall {})", self.mono_nanos, self.wall_nanos)
    }
}

/// A receipt stamp paired with whatever the venue claimed the time was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timestamps {
    /// When we read the bytes off the socket.
    pub recv: Stamp,
    /// The venue's own timestamp, in nanoseconds since the Unix epoch, if it
    /// sent one at all.
    pub venue_nanos: Option<i64>,
}

impl Timestamps {
    pub fn new(recv: Stamp, venue_nanos: Option<i64>) -> Self {
        Timestamps { recv, venue_nanos }
    }

    pub fn recv_only(recv: Stamp) -> Self {
        Timestamps {
            recv,
            venue_nanos: None,
        }
    }

    /// Receipt minus venue time, in nanoseconds. Positive means the message
    /// reached us after the venue stamped it, which is the normal case; a
    /// negative value means the two clocks disagree and is worth recording
    /// rather than clamping.
    pub fn skew_nanos(&self) -> Option<i64> {
        self.venue_nanos.map(|v| self.recv.wall_nanos - v)
    }
}

/// Source of receipt stamps. A trait so replay and tests get determinism
/// without a second code path through the ingest loop.
pub trait Clock: Send + Sync + fmt::Debug {
    fn stamp(&self) -> Stamp;
}

/// The real clock: monotonic elapsed time anchored to one wall reading.
#[derive(Debug)]
pub struct MonotonicClock {
    anchor_instant: Instant,
    anchor_wall_nanos: i64,
}

impl MonotonicClock {
    pub fn new() -> Self {
        let anchor_wall_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        MonotonicClock {
            anchor_instant: Instant::now(),
            anchor_wall_nanos,
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn stamp(&self) -> Stamp {
        let mono_nanos = self.anchor_instant.elapsed().as_nanos() as u64;
        Stamp {
            mono_nanos,
            wall_nanos: self.anchor_wall_nanos + mono_nanos as i64,
        }
    }
}

/// A clock that only moves when told to, so a replay of the same tape produces
/// the same stamps every time.
#[derive(Debug, Clone)]
pub struct ManualClock {
    mono: Arc<AtomicU64>,
    wall_epoch: i64,
    step: u64,
}

impl ManualClock {
    /// `step` nanoseconds are added on every read, so successive frames get
    /// strictly increasing stamps without any test having to advance by hand.
    pub fn new(wall_epoch: i64, step: u64) -> Self {
        ManualClock {
            mono: Arc::new(AtomicU64::new(0)),
            wall_epoch,
            step,
        }
    }

    pub fn advance(&self, nanos: u64) {
        self.mono.fetch_add(nanos, Ordering::Relaxed);
    }

    pub fn set(&self, nanos: u64) {
        self.mono.store(nanos, Ordering::Relaxed);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        ManualClock::new(1_700_000_000_000_000_000, 1_000_000)
    }
}

impl Clock for ManualClock {
    fn stamp(&self) -> Stamp {
        let mono_nanos = self.mono.fetch_add(self.step, Ordering::Relaxed);
        Stamp {
            mono_nanos,
            wall_nanos: self.wall_epoch + mono_nanos as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_is_deterministic_and_strictly_increasing() {
        let a = ManualClock::new(1_000, 5);
        let b = ManualClock::new(1_000, 5);
        let sa: Vec<Stamp> = (0..4).map(|_| a.stamp()).collect();
        let sb: Vec<Stamp> = (0..4).map(|_| b.stamp()).collect();
        assert_eq!(sa, sb);
        assert!(sa.windows(2).all(|w| w[1].mono_nanos > w[0].mono_nanos));
        assert_eq!(sa[0].wall_nanos, 1_000);
        assert_eq!(sa[3].mono_nanos, 15);
    }

    #[test]
    fn monotonic_clock_never_goes_backwards() {
        let c = MonotonicClock::new();
        let mut last = c.stamp();
        for _ in 0..1_000 {
            let now = c.stamp();
            assert!(now.mono_nanos >= last.mono_nanos);
            last = now;
        }
    }

    #[test]
    fn skew_is_signed_and_kept_not_clamped() {
        let recv = Stamp {
            mono_nanos: 10,
            wall_nanos: 1_000,
        };
        assert_eq!(Timestamps::new(recv, Some(400)).skew_nanos(), Some(600));
        // Venue clock ahead of ours. Recorded as negative, not hidden.
        assert_eq!(Timestamps::new(recv, Some(1_500)).skew_nanos(), Some(-500));
        assert_eq!(Timestamps::recv_only(recv).skew_nanos(), None);
    }
}

/// Parse an RFC 3339 timestamp in UTC into nanoseconds since the Unix epoch.
///
/// Both venues send `2026-08-24T22:22:17.566544332Z`, with anywhere from zero
/// to nine fractional digits. This handles exactly that shape and returns
/// `None` for anything else, rather than pulling in a date library to be
/// approximately as strict.
///
/// A venue timestamp we cannot read is recorded as absent, never as zero: a
/// zero would silently become a skew of fifty-six years in the dataset.
pub fn parse_rfc3339_nanos(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    if !s.ends_with('Z') {
        return None;
    }
    let num = |a: usize, b: usize| -> Option<i64> { s.get(a..b)?.parse().ok() };
    let (year, month, day) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (hour, minute, second) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let nanos: i64 = match bytes[19] {
        b'Z' => 0,
        b'.' => {
            let frac = &s[20..s.len() - 1];
            if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            let scaled: i64 = frac.parse().ok()?;
            scaled * 10i64.pow(9 - frac.len() as u32)
        }
        _ => return None,
    };

    // Days from the civil calendar, shifting the year to start in March so leap
    // days land at the end of the cycle.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(test)]
mod rfc3339_tests {
    use super::parse_rfc3339_nanos;

    #[test]
    fn parses_the_epoch_and_known_instants() {
        assert_eq!(parse_rfc3339_nanos("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_nanos("2000-01-01T00:00:00Z"),
            Some(946_684_800_000_000_000)
        );
        // Leap day, to catch an off-by-one in the civil-days arithmetic.
        assert_eq!(
            parse_rfc3339_nanos("2024-02-29T00:00:00Z"),
            Some(1_709_164_800_000_000_000)
        );
    }

    #[test]
    fn fractional_digits_are_scaled_by_their_length() {
        let base = parse_rfc3339_nanos("2026-08-24T22:22:17Z").unwrap();
        assert_eq!(
            parse_rfc3339_nanos("2026-08-24T22:22:17.566544332Z"),
            Some(base + 566_544_332)
        );
        // Three digits are milliseconds, not nanoseconds.
        assert_eq!(
            parse_rfc3339_nanos("2026-08-24T22:22:17.5Z"),
            Some(base + 500_000_000)
        );
        assert_eq!(
            parse_rfc3339_nanos("2026-08-24T22:22:17.001Z"),
            Some(base + 1_000_000)
        );
    }

    #[test]
    fn real_venue_timestamps_parse() {
        // Captured verbatim from the two live feeds.
        assert!(parse_rfc3339_nanos("2026-08-24T22:22:17.566544332Z").is_some());
        assert!(parse_rfc3339_nanos("2026-08-24T22:14:26.614281Z").is_some());
    }

    #[test]
    fn unreadable_timestamps_are_none_rather_than_zero() {
        for bad in [
            "",
            "not a time",
            "2026-08-24 22:22:17Z",
            "2026-08-24T22:22:17+01:00",
            "2026-13-01T00:00:00Z",
            "2026-08-24T25:00:00Z",
            "2026-08-24T22:22:17.1234567890Z",
        ] {
            assert_eq!(parse_rfc3339_nanos(bad), None, "{bad:?} should not parse");
        }
    }
}

/// Format nanoseconds since the Unix epoch as a UTC `YYYY-MM-DD` date.
///
/// The archive partitions on this, so it has to be the inverse of
/// [`parse_rfc3339_nanos`] exactly. A day boundary that disagreed with the
/// parser by one would put a day's data in two partitions and make both look
/// like they had gaps.
pub fn format_utc_date(nanos: i64) -> String {
    let (y, m, d) = civil_from_nanos(nanos);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Format nanoseconds since the Unix epoch as a full RFC 3339 instant.
///
/// All nine digits, because feeds routinely put several messages inside one
/// microsecond and the point of recording receipt time is to tell them apart.
/// This is the inverse of [`parse_rfc3339_nanos`].
pub fn format_rfc3339_nanos(nanos: i64) -> String {
    let (y, m, d) = civil_from_nanos(nanos);
    let within_day = nanos.rem_euclid(86_400 * 1_000_000_000);
    let secs = within_day / 1_000_000_000;
    let sub = within_day % 1_000_000_000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{sub:09}Z",
        secs / 3_600,
        (secs / 60) % 60,
        secs % 60,
    )
}

/// Split nanoseconds since the epoch into a UTC civil date.
pub fn civil_from_nanos(nanos: i64) -> (i64, u32, u32) {
    // Floor division, so instants before 1970 land on the right day rather
    // than truncating towards zero into the next one.
    let secs = nanos.div_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod date_tests {
    use super::{format_utc_date, parse_rfc3339_nanos};

    #[test]
    fn dates_round_trip_against_the_parser() {
        for date in [
            "1970-01-01",
            "1999-12-31",
            "2000-01-01",
            "2024-02-29",
            "2026-08-24",
            "2100-03-01",
        ] {
            let nanos = parse_rfc3339_nanos(&format!("{date}T00:00:00Z")).unwrap();
            assert_eq!(format_utc_date(nanos), date);
            // And anywhere inside the day maps to the same partition.
            let end = parse_rfc3339_nanos(&format!("{date}T23:59:59.999999999Z")).unwrap();
            assert_eq!(format_utc_date(end), date);
        }
    }

    #[test]
    fn a_day_boundary_lands_on_exactly_one_partition() {
        let midnight = parse_rfc3339_nanos("2026-08-25T00:00:00Z").unwrap();
        assert_eq!(format_utc_date(midnight - 1), "2026-08-24");
        assert_eq!(format_utc_date(midnight), "2026-08-25");
    }

    #[test]
    fn instants_before_the_epoch_do_not_truncate_into_the_next_day() {
        assert_eq!(format_utc_date(-1), "1969-12-31");
        assert_eq!(format_utc_date(-86_400_000_000_000), "1969-12-31");
        assert_eq!(format_utc_date(-86_400_000_000_001), "1969-12-30");
    }
}

#[cfg(test)]
mod rfc3339_format_tests {
    use super::*;

    #[test]
    fn formatting_round_trips_through_the_parser() {
        // These have to be exact inverses. The archive partitions on the date
        // half of this, and a disagreement of one nanosecond at a day boundary
        // would split a day across two partitions and make both look gappy.
        for nanos in [
            0,
            1,
            1_787_626_160_284_980_917,
            1_577_836_800_000_000_000,
            -1_000_000_000,
            86_399_999_999_999,
        ] {
            let text = format_rfc3339_nanos(nanos);
            assert_eq!(
                parse_rfc3339_nanos(&text),
                Some(nanos),
                "{nanos} formatted as {text}"
            );
        }
    }

    #[test]
    fn it_keeps_every_digit() {
        assert_eq!(
            format_rfc3339_nanos(1_787_626_160_284_980_917),
            "2026-08-25T02:49:20.284980917Z"
        );
        assert_eq!(format_rfc3339_nanos(0), "1970-01-01T00:00:00.000000000Z");
    }

    #[test]
    fn it_agrees_with_the_date_formatter() {
        for nanos in [0, 1_787_626_160_284_980_917, -1] {
            assert!(format_rfc3339_nanos(nanos).starts_with(&format_utc_date(nanos)));
        }
    }
}
