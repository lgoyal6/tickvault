//! Suspect windows and the gap report.
//!
//! This is the module the whole project points at. The recorder's claim is not
//! that it captures everything, it is that it knows precisely which stretches
//! of time it cannot vouch for. A window opens the moment we lose confidence
//! and closes when a fresh snapshot restores it; a consumer excludes those
//! windows and trusts the rest.
//!
//! The counters here become the published front page in phase 8. They are kept
//! from the first phase so that "we have no gap statistics yet" is never an
//! available excuse.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::book::BookAnomaly;
use crate::clock::Stamp;
use crate::sequence::{LossSize, SeqVerdict, Unverifiable};
use crate::types::{Symbol, VenueId};

/// Why a stretch of time cannot be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SuspectCause {
    /// The feed skipped something. How much is expressed in whatever unit the
    /// venue's scheme can actually prove.
    SequenceGap { size: LossSize },
    /// A checksum-based feed disagreed with our copy of the book.
    ChecksumDivergence { expected: u32, computed: u32 },
    /// The venue renumbered and our baseline is void.
    SequenceReset,
    /// A message arrived stamped earlier than one already applied.
    OutOfOrder { previous: i64, observed: i64 },
    /// The book entered a state that should not occur.
    Anomaly(BookAnomaly),
    /// The socket dropped.
    Disconnect,
    /// Connected, but no snapshot has arrived yet.
    AwaitingSnapshot,
    /// The archive writer could not keep up and rows were discarded. Ours, not
    /// the venue's, and reported as such.
    WriterFellBehind { rows: u64 },
}

impl fmt::Display for SuspectCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SuspectCause::SequenceGap { size } => write!(f, "sequence gap ({size} missing)"),
            SuspectCause::ChecksumDivergence { expected, computed } => {
                write!(f, "checksum divergence (venue {expected}, ours {computed})")
            }
            SuspectCause::SequenceReset => write!(f, "sequence reset"),
            SuspectCause::OutOfOrder { previous, observed } => {
                write!(f, "out of order (stamp {observed} after {previous})")
            }
            SuspectCause::Anomaly(a) => write!(f, "{a}"),
            SuspectCause::Disconnect => write!(f, "disconnect"),
            SuspectCause::AwaitingSnapshot => write!(f, "awaiting snapshot"),
            SuspectCause::WriterFellBehind { rows } => {
                write!(f, "writer fell behind, {rows} row(s) dropped")
            }
        }
    }
}

/// A bounded stretch of time whose data should be excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspectWindow {
    pub venue: VenueId,
    pub symbol: Symbol,
    pub cause: SuspectCause,
    pub opened: Stamp,
    /// `None` while still open.
    pub closed: Option<Stamp>,
    /// Further causes that fired before the window closed. A gap followed by a
    /// crossed book is one bad stretch, not two.
    pub also: Vec<SuspectCause>,
}

impl SuspectWindow {
    pub fn is_open(&self) -> bool {
        self.closed.is_none()
    }

    /// Duration in nanoseconds, measuring an open window up to `now`.
    pub fn duration_nanos(&self, now: Stamp) -> u64 {
        self.closed.unwrap_or(now).since(self.opened)
    }
}

/// A class of loss this venue cannot detect at all, stated up front.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectionLimit {
    pub venue: VenueId,
    /// The condition under which the blind spot applies.
    pub scope: String,
    /// What goes unnoticed, in plain terms.
    pub consequence: String,
}

/// Counters for one instrument on one venue.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolStats {
    pub messages: u64,
    pub applied: u64,
    /// How far behind the venue's own timestamp each message arrived.
    ///
    /// Recorded per row already; summarised here so the dataset can say what
    /// the distribution is rather than only what one message was. This is what
    /// tells a consumer how far to trust the timestamps.
    pub skew: crate::latency::Histogram,
    /// Messages that arrived but could not be validated either way.
    pub unverifiable: u64,
    pub sequence_gaps: u64,
    /// Messages lost, where the scheme can count messages.
    pub messages_missing: u64,
    /// Update ids lost, where the scheme counts update ids instead. Not the
    /// same quantity as `messages_missing` and deliberately not added to it.
    pub updates_missing: u64,
    /// Gaps that were proven to exist but whose size is unknowable, which is
    /// every gap on a chained-identifier feed.
    pub gaps_of_unknown_size: u64,
    /// Messages that arrived stamped earlier than one already applied.
    pub out_of_order: u64,
    /// Order-by-order events for orders we never saw added.
    ///
    /// Normal at the start of an L3 feed that has no snapshot, and it should
    /// fall to zero once the pre-existing orders have all traded or been
    /// cancelled. Staying high means the book is not converging.
    pub l3_unknown_orders: u64,
    /// Orders the venue added twice without removing in between.
    pub l3_duplicate_adds: u64,
    /// Orders from a seeding snapshot that had to be removed because they
    /// crossed the book, meaning the snapshot held orders the stream never
    /// retracted.
    pub l3_stale_seed_orders: u64,
    /// Orders that changed price, which is a cancel and an add in one event.
    pub l3_price_moves: u64,
    /// Rows the writer could not keep up with, discarded under the `drop`
    /// backpressure policy.
    ///
    /// Counted here rather than in a separate log on purpose. A hole we caused
    /// costs a backtest exactly as much as one the venue caused, so it belongs
    /// on the same front page.
    pub rows_dropped_by_backpressure: u64,
    pub checksum_divergences: u64,
    pub sequence_resets: u64,
    pub duplicates: u64,
    pub stale: u64,
    pub resnapshots: u64,
    pub disconnects: u64,
    pub crossed_books: u64,
    pub locked_books: u64,
    pub deletes_of_absent_levels: u64,
    pub negative_quantities: u64,
    pub first_seen: Option<Stamp>,
    pub last_seen: Option<Stamp>,
    /// Nanoseconds inside closed suspect windows.
    pub suspect_nanos: u64,
}

impl SymbolStats {
    /// Total observed span in nanoseconds.
    pub fn observed_nanos(&self) -> u64 {
        match (self.first_seen, self.last_seen) {
            (Some(a), Some(b)) => b.since(a),
            _ => 0,
        }
    }

    /// Fraction of the observed span we are prepared to vouch for.
    ///
    /// Returns `None` when nothing was observed, rather than a flattering 1.0.
    pub fn clean_fraction(&self) -> Option<f64> {
        let total = self.observed_nanos();
        if total == 0 {
            return None;
        }
        let clean = total.saturating_sub(self.suspect_nanos);
        Some(clean as f64 / total as f64)
    }

    /// Messages that actually contributed to the book.
    ///
    /// Stale and duplicate messages are excluded: a diff the snapshot already
    /// contains was superseded, not examined, and counting it either way would
    /// misdescribe the run. On a venue whose REST book lags its stream, the
    /// discarded prefix can be most of the first second's traffic.
    pub fn considered(&self) -> u64 {
        self.messages
            .saturating_sub(self.stale)
            .saturating_sub(self.duplicates)
    }

    /// Fraction of the messages we kept that we were able to validate.
    ///
    /// Deliberately not `1 - unverifiable/messages`. That formula counts a
    /// discarded stale message as verified, which let a feed carrying no
    /// sequence information at all report two thirds of its messages as
    /// validated.
    pub fn verified_fraction(&self) -> Option<f64> {
        let considered = self.considered();
        if considered == 0 {
            return None;
        }
        let verified = considered.saturating_sub(self.unverifiable);
        Some(verified as f64 / considered as f64)
    }
}

type Key = (VenueId, Symbol);

/// Accumulates suspect windows and counters across a recording run.
#[derive(Debug, Clone, Default)]
pub struct GapLog {
    windows: Vec<SuspectWindow>,
    open: BTreeMap<Key, usize>,
    stats: BTreeMap<Key, SymbolStats>,
    limits: Vec<DetectionLimit>,
}

impl GapLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a blind spot for a venue. Recorded once, published always.
    pub fn declare_limit(&mut self, limit: DetectionLimit) {
        if !self.limits.contains(&limit) {
            self.limits.push(limit);
        }
    }

    pub fn limits(&self) -> &[DetectionLimit] {
        &self.limits
    }

    pub fn windows(&self) -> &[SuspectWindow] {
        &self.windows
    }

    pub fn stats(&self, venue: VenueId, symbol: &Symbol) -> Option<&SymbolStats> {
        self.stats.get(&(venue, symbol.clone()))
    }

    fn entry(&mut self, venue: VenueId, symbol: &Symbol) -> &mut SymbolStats {
        self.stats.entry((venue, symbol.clone())).or_default()
    }

    /// Register a symbol we intend to record, before any message arrives.
    ///
    /// Without this, a symbol that never delivered a single message is simply
    /// absent from the report, and an empty table reads as "nothing to say"
    /// rather than "we captured nothing". Absence is exactly what this report
    /// exists to make visible.
    pub fn ensure_symbol(&mut self, venue: VenueId, symbol: &Symbol) {
        self.entry(venue, symbol);
    }

    /// Note that a message arrived, before any judgement about it.
    pub fn record_message(&mut self, venue: VenueId, symbol: &Symbol, at: Stamp) {
        let s = self.entry(venue, symbol);
        s.messages += 1;
        s.first_seen.get_or_insert(at);
        s.last_seen = Some(at);
    }

    /// Fold a validation verdict into the counters, opening a window when the
    /// verdict means we have lost confidence.
    pub fn record_verdict(
        &mut self,
        venue: VenueId,
        symbol: &Symbol,
        verdict: &SeqVerdict,
        at: Stamp,
    ) {
        match *verdict {
            SeqVerdict::InOrder => {}
            SeqVerdict::Duplicate { .. } => self.entry(venue, symbol).duplicates += 1,
            SeqVerdict::Stale { .. } => self.entry(venue, symbol).stale += 1,
            SeqVerdict::Unverifiable(reason) => {
                self.entry(venue, symbol).unverifiable += 1;
                if matches!(reason, Unverifiable::AwaitingSnapshot) {
                    self.open_window(venue, symbol, SuspectCause::AwaitingSnapshot, at);
                }
            }
            SeqVerdict::Gap(evidence) => {
                {
                    let s = self.entry(venue, symbol);
                    s.sequence_gaps += 1;
                    s.messages_missing += evidence.size.messages();
                    s.updates_missing += evidence.size.updates();
                    if evidence.size.is_unknown() {
                        s.gaps_of_unknown_size += 1;
                    }
                }
                self.open_window(
                    venue,
                    symbol,
                    SuspectCause::SequenceGap {
                        size: evidence.size,
                    },
                    at,
                );
            }
            SeqVerdict::OutOfOrder { previous, observed } => {
                self.entry(venue, symbol).out_of_order += 1;
                self.open_window(
                    venue,
                    symbol,
                    SuspectCause::OutOfOrder { previous, observed },
                    at,
                );
            }
            SeqVerdict::Divergence { expected, computed } => {
                self.entry(venue, symbol).checksum_divergences += 1;
                self.open_window(
                    venue,
                    symbol,
                    SuspectCause::ChecksumDivergence { expected, computed },
                    at,
                );
            }
            SeqVerdict::Reset { .. } => {
                self.entry(venue, symbol).sequence_resets += 1;
                self.open_window(venue, symbol, SuspectCause::SequenceReset, at);
            }
        }
    }

    /// Fold a book anomaly into the counters, opening a window when it means
    /// the book is wrong.
    ///
    /// `corrupting` is the caller's call, not the anomaly's. Whether a delete
    /// for an absent level indicates loss depends on the venue that sent it,
    /// and the log has no way to know which venue that is beyond its id.
    pub fn record_anomaly(
        &mut self,
        venue: VenueId,
        symbol: &Symbol,
        anomaly: BookAnomaly,
        at: Stamp,
        corrupting: bool,
    ) {
        {
            let s = self.entry(venue, symbol);
            match anomaly {
                BookAnomaly::Crossed { .. } => s.crossed_books += 1,
                BookAnomaly::Locked { .. } => s.locked_books += 1,
                BookAnomaly::RemovedMissingLevel { .. } => s.deletes_of_absent_levels += 1,
                BookAnomaly::NegativeQty { .. } => s.negative_quantities += 1,
            }
        }
        if corrupting {
            self.open_window(venue, symbol, SuspectCause::Anomaly(anomaly), at);
        }
    }

    pub fn record_disconnect(&mut self, venue: VenueId, symbol: &Symbol, at: Stamp) {
        self.entry(venue, symbol).disconnects += 1;
        self.open_window(venue, symbol, SuspectCause::Disconnect, at);
    }

    /// A fresh snapshot restores confidence: close the window and count it.
    pub fn record_resnapshot(&mut self, venue: VenueId, symbol: &Symbol, at: Stamp) {
        self.entry(venue, symbol).resnapshots += 1;
        self.close_window(venue, symbol, at);
    }

    /// Record rows lost because the writer fell behind the socket.
    pub fn record_backpressure_drop(
        &mut self,
        venue: VenueId,
        symbol: &Symbol,
        rows: u64,
        at: Stamp,
    ) {
        self.entry(venue, symbol).rows_dropped_by_backpressure += rows;
        self.open_window(venue, symbol, SuspectCause::WriterFellBehind { rows }, at);
    }

    /// Fold an order-by-order anomaly into the counters.
    ///
    /// `corrupting` is the caller's call for the same reason it is for L2: an
    /// event for an unknown order is expected on a feed with no snapshot and is
    /// evidence of a real problem on one that has been seeded.
    pub fn record_l3_anomaly(
        &mut self,
        venue: VenueId,
        symbol: &Symbol,
        anomaly: &crate::book::l3::L3Anomaly,
        at: Stamp,
        corrupting: bool,
    ) {
        use crate::book::l3::L3Anomaly as A;
        {
            let s = self.entry(venue, symbol);
            match anomaly {
                A::UnknownOrder { .. } => s.l3_unknown_orders += 1,
                A::DuplicateAdd { .. } => s.l3_duplicate_adds += 1,
                A::PriceMoved { .. } => s.l3_price_moves += 1,
                A::NegativeQty { .. } => s.negative_quantities += 1,
                A::Crossed { .. } => s.crossed_books += 1,
            }
        }
        if corrupting {
            let cause = match anomaly {
                A::Crossed { best_bid, best_ask } => {
                    SuspectCause::Anomaly(crate::book::BookAnomaly::Crossed {
                        best_bid: *best_bid,
                        best_ask: *best_ask,
                    })
                }
                _ => SuspectCause::AwaitingSnapshot,
            };
            self.open_window(venue, symbol, cause, at);
        }
    }

    /// Note orders dropped from a seeding snapshot because they crossed.
    pub fn record_stale_seed_orders(&mut self, venue: VenueId, symbol: &Symbol, count: u64) {
        self.entry(venue, symbol).l3_stale_seed_orders += count;
    }

    pub fn record_applied(&mut self, venue: VenueId, symbol: &Symbol) {
        self.entry(venue, symbol).applied += 1;
    }

    /// Note how late one message was against the venue's own clock.
    ///
    /// Only where the venue stamps its messages at all. A venue that does not
    /// gets no distribution rather than a distribution of zeros.
    pub fn record_skew(&mut self, venue: VenueId, symbol: &Symbol, skew_nanos: Option<i64>) {
        if let Some(skew) = skew_nanos {
            self.entry(venue, symbol).skew.record(skew);
        }
    }

    /// Open a suspect window, or extend the one already open.
    pub fn open_window(&mut self, venue: VenueId, symbol: &Symbol, cause: SuspectCause, at: Stamp) {
        let key = (venue, symbol.clone());
        if let Some(&idx) = self.open.get(&key) {
            // Already suspect. Record the additional cause rather than starting
            // a second window, which would double count the same bad stretch.
            let w = &mut self.windows[idx];
            if w.cause != cause && !w.also.contains(&cause) {
                w.also.push(cause);
            }
            return;
        }
        self.windows.push(SuspectWindow {
            venue,
            symbol: symbol.clone(),
            cause,
            opened: at,
            closed: None,
            also: Vec::new(),
        });
        self.open.insert(key, self.windows.len() - 1);
    }

    /// Close the open window for this symbol, if any.
    pub fn close_window(&mut self, venue: VenueId, symbol: &Symbol, at: Stamp) {
        let key = (venue, symbol.clone());
        if let Some(idx) = self.open.remove(&key) {
            let duration = {
                let w = &mut self.windows[idx];
                w.closed = Some(at);
                w.duration_nanos(at)
            };
            self.entry(venue, symbol).suspect_nanos += duration;
        }
    }

    /// Is this symbol currently inside a suspect window?
    pub fn is_suspect(&self, venue: VenueId, symbol: &Symbol) -> bool {
        self.open.contains_key(&(venue, symbol.clone()))
    }

    /// Close every open window, as at end of run.
    pub fn seal(&mut self, at: Stamp) {
        let keys: Vec<Key> = self.open.keys().cloned().collect();
        for (venue, symbol) in keys {
            self.close_window(venue, &symbol, at);
        }
    }

    /// Build the report. Seal first if the run is over.
    pub fn report(&self) -> GapReport {
        let rows = self
            .stats
            .iter()
            .map(|((venue, symbol), stats)| SymbolReport {
                venue: *venue,
                symbol: symbol.clone(),
                stats: stats.clone(),
                windows: self
                    .windows
                    .iter()
                    .filter(|w| w.venue == *venue && w.symbol == *symbol)
                    .cloned()
                    .collect(),
            })
            .collect();
        GapReport {
            rows,
            limits: self.limits.clone(),
        }
    }
}

/// One row of the published gap report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolReport {
    pub venue: VenueId,
    pub symbol: Symbol,
    pub stats: SymbolStats,
    pub windows: Vec<SuspectWindow>,
}

/// The front page: what we captured, and where we could not vouch for it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapReport {
    pub rows: Vec<SymbolReport>,
    pub limits: Vec<DetectionLimit>,
}

impl GapReport {
    pub fn total_missing(&self) -> u64 {
        self.rows.iter().map(|r| r.stats.messages_missing).sum()
    }

    /// Rows the archive lost because our own writer could not keep up.
    pub fn total_dropped_by_backpressure(&self) -> u64 {
        self.rows
            .iter()
            .map(|r| r.stats.rows_dropped_by_backpressure)
            .sum()
    }

    /// Gaps we proved happened but could not size. Reported separately so a
    /// zero in the "missing" column never reads as "nothing was lost".
    pub fn total_unsized_gaps(&self) -> u64 {
        self.rows.iter().map(|r| r.stats.gaps_of_unknown_size).sum()
    }

    pub fn total_gaps(&self) -> u64 {
        self.rows
            .iter()
            .map(|r| r.stats.sequence_gaps + r.stats.checksum_divergences)
            .sum()
    }

    /// Combine two reports covering disjoint symbols.
    ///
    /// Used when a venue's symbols are spread over several connections: each
    /// runs its own session, and the published report has to be one table
    /// rather than one per socket.
    pub fn merge(mut self, other: GapReport) -> GapReport {
        self.rows.extend(other.rows);
        self.rows
            .sort_by(|a, b| (a.venue, a.symbol.as_str()).cmp(&(b.venue, b.symbol.as_str())));
        for limit in other.limits {
            if !self.limits.contains(&limit) {
                self.limits.push(limit);
            }
        }
        self
    }

    /// True when every row is clean and everything was verifiable.
    ///
    /// Deliberately strict on two counts. Unverifiable is not clean: a message
    /// we could not check is not a message we checked. And a run that opened
    /// any suspect window at all is not clean even if no counter here names the
    /// reason, which is what makes this hold for anomalies that are added
    /// later without anyone remembering to update this function.
    pub fn is_spotless(&self) -> bool {
        self.rows.iter().all(|r| {
            r.windows.is_empty()
                && r.stats.sequence_gaps == 0
                && r.stats.checksum_divergences == 0
                && r.stats.crossed_books == 0
                && r.stats.out_of_order == 0
                && r.stats.rows_dropped_by_backpressure == 0
                && r.stats.unverifiable == 0
        })
    }
}

impl fmt::Display for GapReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:<11} {:<10} {:>9} {:>6} {:>8} {:>8} {:>7} {:>8} {:>7} {:>7} {:>9} {:>9}",
            "venue",
            "symbol",
            "msgs",
            "gaps",
            "missing",
            "unsized",
            "orphan",
            "dropped",
            "crossed",
            "resnap",
            "verified%",
            "clean%"
        )?;
        for row in &self.rows {
            let s = &row.stats;
            let pct = |v: Option<f64>| {
                v.map(|x| format!("{:.4}", x * 100.0))
                    .unwrap_or_else(|| "n/a".to_string())
            };
            let missing = if s.updates_missing > 0 {
                format!("{}u", s.updates_missing)
            } else {
                s.messages_missing.to_string()
            };
            writeln!(
                f,
                "{:<11} {:<10} {:>9} {:>6} {:>8} {:>8} {:>7} {:>8} {:>7} {:>7} {:>9} {:>9}",
                row.venue.as_str(),
                row.symbol.as_str(),
                s.messages,
                s.sequence_gaps + s.checksum_divergences + s.out_of_order,
                missing,
                s.gaps_of_unknown_size,
                // Deletes for levels we never held. On venues that emit these
                // routinely it is noise; on Bitstamp it is the only health
                // signal the feed leaves, which is why it is always shown.
                s.deletes_of_absent_levels,
                s.rows_dropped_by_backpressure,
                s.crossed_books,
                s.resnapshots,
                pct(s.verified_fraction()),
                pct(s.clean_fraction())
            )?;
        }
        let stamped: Vec<&SymbolReport> = self
            .rows
            .iter()
            .filter(|r| !r.stats.skew.is_empty())
            .collect();
        if !stamped.is_empty() {
            writeln!(
                f,
                "\nvenue timestamp to receipt, milliseconds (p50 / p99 / max):"
            )?;
            writeln!(
                f,
                "  a difference between two clocks, which is latency only as far as ours is right"
            )?;
            for row in stamped {
                let h = &row.stats.skew;
                // A venue whose clock runs ahead of ours produces negative
                // skew. Saying so is more useful than a distribution that
                // quietly excludes it.
                let ahead = if h.negative() > 0 {
                    format!(", {} arrived stamped ahead of our clock", h.negative())
                } else {
                    String::new()
                };
                writeln!(
                    f,
                    "  {:<11} {:<10} {:>22}   over {} stamped{}",
                    row.venue.as_str(),
                    row.symbol.as_str(),
                    h.summary_millis(),
                    h.count(),
                    ahead
                )?;
            }
            // Venues do not conspire. If they all read early, the common term
            // is this host, and saying so turns a confusing number into an
            // actionable one. Measured while writing this: every venue's median
            // was tens of milliseconds negative, and `sntp` put the local clock
            // 130ms behind.
            let medians: Vec<i64> = self
                .rows
                .iter()
                .filter_map(|r| r.stats.skew.p50())
                .collect();
            if medians.len() > 1 && medians.iter().all(|m| *m < 0) {
                writeln!(
                    f,
                    "  every venue reads early, so the clock that is wrong is most likely this one"
                )?;
            }
        }
        let unstamped: Vec<&SymbolReport> = self
            .rows
            .iter()
            .filter(|r| r.stats.skew.is_empty() && r.stats.messages > 0)
            .collect();
        if !unstamped.is_empty() {
            // No distribution rather than a distribution of zeros.
            writeln!(f, "\nno venue timestamp to compare against:")?;
            for row in unstamped {
                writeln!(f, "  {} {}", row.venue.as_str(), row.symbol.as_str())?;
            }
        }
        if !self.limits.is_empty() {
            writeln!(f, "\nknown blind spots:")?;
            for limit in &self.limits {
                writeln!(
                    f,
                    "  {} when {}: {}",
                    limit.venue.as_str(),
                    limit.scope,
                    limit.consequence
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::Fixed;
    use crate::sequence::GapEvidence;
    use crate::types::Side;

    fn at(n: u64) -> Stamp {
        Stamp {
            mono_nanos: n,
            wall_nanos: n as i64,
        }
    }

    fn sym() -> Symbol {
        Symbol::new("BTC", "USD")
    }

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    #[test]
    fn a_gap_opens_a_window_and_a_resnapshot_closes_it() {
        let mut log = GapLog::new();
        let v = VenueId::Coinbase;
        log.record_message(v, &sym(), at(0));
        log.record_verdict(
            v,
            &sym(),
            &SeqVerdict::Gap(GapEvidence {
                expected: 2,
                observed: 5,
                size: LossSize::Messages(3),
            }),
            at(100),
        );
        assert!(log.is_suspect(v, &sym()));
        log.record_message(v, &sym(), at(400));
        log.record_resnapshot(v, &sym(), at(400));
        assert!(!log.is_suspect(v, &sym()));

        let stats = log.stats(v, &sym()).unwrap();
        assert_eq!(stats.sequence_gaps, 1);
        assert_eq!(stats.messages_missing, 3);
        assert_eq!(stats.resnapshots, 1);
        assert_eq!(stats.suspect_nanos, 300);
    }

    #[test]
    fn overlapping_causes_extend_one_window_rather_than_stacking() {
        let mut log = GapLog::new();
        let v = VenueId::Kraken;
        log.record_message(v, &sym(), at(0));
        log.record_verdict(
            v,
            &sym(),
            &SeqVerdict::Divergence {
                expected: 1,
                computed: 2,
            },
            at(10),
        );
        log.record_anomaly(
            v,
            &sym(),
            BookAnomaly::Crossed {
                best_bid: f("2"),
                best_ask: f("1"),
            },
            at(20),
            true,
        );
        assert_eq!(log.windows().len(), 1, "one bad stretch, one window");
        assert_eq!(log.windows()[0].also.len(), 1);
        // Both are still counted individually.
        let stats = log.stats(v, &sym()).unwrap();
        assert_eq!(stats.checksum_divergences, 1);
        assert_eq!(stats.crossed_books, 1);
    }

    #[test]
    fn suspect_time_accumulates_across_separate_windows() {
        let mut log = GapLog::new();
        let v = VenueId::Coinbase;
        log.record_message(v, &sym(), at(0));
        for (open, close) in [(10u64, 30u64), (50, 90)] {
            log.record_verdict(
                v,
                &sym(),
                &SeqVerdict::Gap(GapEvidence {
                    expected: 1,
                    observed: 2,
                    size: LossSize::Messages(1),
                }),
                at(open),
            );
            log.record_resnapshot(v, &sym(), at(close));
        }
        log.record_message(v, &sym(), at(100));
        let stats = log.stats(v, &sym()).unwrap();
        assert_eq!(stats.suspect_nanos, 20 + 40);
        assert_eq!(stats.observed_nanos(), 100);
        assert!((stats.clean_fraction().unwrap() - 0.4).abs() < 1e-9);
    }

    #[test]
    fn a_locked_book_is_counted_but_does_not_open_a_window() {
        let mut log = GapLog::new();
        let v = VenueId::Kraken;
        log.record_anomaly(
            v,
            &sym(),
            BookAnomaly::Locked { price: f("100") },
            at(5),
            false,
        );
        assert!(!log.is_suspect(v, &sym()));
        assert_eq!(log.stats(v, &sym()).unwrap().locked_books, 1);
        assert!(log.windows().is_empty());
    }

    #[test]
    fn a_delete_of_an_absent_level_is_treated_as_loss_evidence() {
        let mut log = GapLog::new();
        let v = VenueId::Kraken;
        log.record_anomaly(
            v,
            &sym(),
            BookAnomaly::RemovedMissingLevel {
                side: Side::Bid,
                price: f("100"),
            },
            at(5),
            // As it would be on Kraken, which never deletes what it did not
            // publish. The same anomaly on Coinbase is passed as `false`.
            true,
        );
        assert!(log.is_suspect(v, &sym()));
    }

    #[test]
    fn sealing_closes_open_windows_so_the_report_is_not_understated() {
        let mut log = GapLog::new();
        let v = VenueId::Coinbase;
        log.record_message(v, &sym(), at(0));
        log.record_disconnect(v, &sym(), at(10));
        log.record_message(v, &sym(), at(200));
        assert_eq!(log.stats(v, &sym()).unwrap().suspect_nanos, 0);
        log.seal(at(200));
        assert_eq!(log.stats(v, &sym()).unwrap().suspect_nanos, 190);
        assert!(!log.is_suspect(v, &sym()));
    }

    #[test]
    fn a_symbol_that_never_delivered_a_message_still_appears_in_the_report() {
        let mut log = GapLog::new();
        log.ensure_symbol(VenueId::Coinbase, &sym());
        let report = log.report();
        assert_eq!(report.rows.len(), 1, "an empty table would hide the outage");
        assert_eq!(report.rows[0].stats.messages, 0);
        // Not a flattering 100%.
        assert_eq!(report.rows[0].stats.clean_fraction(), None);
        assert!(log.report().to_string().contains("n/a"));
    }

    #[test]
    fn discarded_messages_do_not_count_towards_being_verified() {
        // The bug this replaced let a feed with no sequence numbers whatsoever
        // report 67% of its messages as validated, purely because the ones it
        // had discarded as stale were counted in the numerator.
        let mut stats = SymbolStats {
            messages: 12,
            stale: 8,
            unverifiable: 4,
            ..SymbolStats::default()
        };
        assert_eq!(stats.considered(), 4);
        assert_eq!(stats.verified_fraction(), Some(0.0));

        stats.unverifiable = 0;
        assert_eq!(stats.verified_fraction(), Some(1.0));

        // A run that was entirely superseded has nothing to report either way.
        let all_stale = SymbolStats {
            messages: 5,
            stale: 5,
            ..SymbolStats::default()
        };
        assert_eq!(all_stale.verified_fraction(), None);
    }

    #[test]
    fn a_run_with_no_observed_time_reports_no_coverage_rather_than_perfect() {
        let stats = SymbolStats::default();
        assert_eq!(stats.clean_fraction(), None);
        assert_eq!(stats.verified_fraction(), None);
    }

    #[test]
    fn unverifiable_messages_keep_a_report_from_claiming_to_be_spotless() {
        let mut log = GapLog::new();
        let v = VenueId::Kraken;
        log.record_message(v, &sym(), at(0));
        log.record_verdict(
            v,
            &sym(),
            &SeqVerdict::Unverifiable(Unverifiable::RenderingWasLossy { levels: 1 }),
            at(1),
        );
        let report = log.report();
        assert!(!report.is_spotless());
        assert_eq!(report.total_gaps(), 0);
    }

    #[test]
    fn declared_limits_are_deduplicated_and_rendered() {
        let mut log = GapLog::new();
        let limit = DetectionLimit {
            venue: VenueId::Kraken,
            scope: "depth > 10".to_string(),
            consequence: "changes below the tenth level are invisible".to_string(),
        };
        log.declare_limit(limit.clone());
        log.declare_limit(limit);
        assert_eq!(log.limits().len(), 1);
        let rendered = log.report().to_string();
        assert!(rendered.contains("known blind spots"));
        assert!(rendered.contains("invisible"));
    }

    #[test]
    fn a_suspect_window_alone_is_enough_to_stop_a_report_being_spotless() {
        // Without this, an anomaly that opens a window but has no dedicated
        // counter here would produce a report that invalidated the book and
        // called itself clean in the same breath.
        let mut log = GapLog::new();
        let v = VenueId::Kraken;
        log.record_message(v, &sym(), at(0));
        log.record_anomaly(
            v,
            &sym(),
            BookAnomaly::RemovedMissingLevel {
                side: Side::Bid,
                price: f("100"),
            },
            at(1),
            true,
        );
        let report = log.report();
        assert_eq!(report.total_gaps(), 0, "no gap counter fires for this");
        assert!(!report.is_spotless(), "but the run is not clean");
    }

    #[test]
    fn reports_from_separate_connections_merge_into_one_table() {
        // A venue whose symbols are spread over several sockets runs a session
        // per socket. The published report has to be one table, not one per
        // connection, and its declared blind spots must not be duplicated.
        let limit = DetectionLimit {
            venue: VenueId::Coinbase,
            scope: "always".to_string(),
            consequence: "shared".to_string(),
        };
        let mut a = GapLog::new();
        a.declare_limit(limit.clone());
        a.record_message(VenueId::Coinbase, &Symbol::new("SOL", "USD"), at(0));
        let mut b = GapLog::new();
        b.declare_limit(limit);
        b.record_message(VenueId::Coinbase, &sym(), at(0));

        let merged = a.report().merge(b.report());
        assert_eq!(merged.rows.len(), 2);
        assert_eq!(merged.limits.len(), 1, "blind spots must not duplicate");
        // Rows come out ordered, so the table is stable across runs.
        let names: Vec<&str> = merged.rows.iter().map(|r| r.symbol.as_str()).collect();
        assert_eq!(names, vec!["BTC-USD", "SOL-USD"]);
    }

    #[test]
    fn report_rows_carry_their_own_windows() {
        let mut log = GapLog::new();
        log.record_message(VenueId::Coinbase, &sym(), at(0));
        log.record_disconnect(VenueId::Coinbase, &sym(), at(1));
        log.record_message(VenueId::Kraken, &sym(), at(2));
        let report = log.report();
        assert_eq!(report.rows.len(), 2);
        let coinbase = report
            .rows
            .iter()
            .find(|r| r.venue == VenueId::Coinbase)
            .unwrap();
        assert_eq!(coinbase.windows.len(), 1);
        let kraken = report
            .rows
            .iter()
            .find(|r| r.venue == VenueId::Kraken)
            .unwrap();
        assert!(kraken.windows.is_empty());
    }
}

/// What one bucket of a coverage grid is allowed to claim.
///
/// Three states rather than two, because collapsing them is exactly the
/// dishonesty this project exists to avoid. "No data" and "data we cannot
/// check" and "data we checked" are different claims, and a grid that painted
/// the middle one green would be making the strongest claim about the weakest
/// evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageState {
    /// Nothing was recorded here.
    Absent,
    /// Recorded, and the recorder marked some of it as not vouched for.
    Suspect,
    /// Recorded and checked against what the venue publishes to check with.
    Clean,
    /// Recorded, nothing looks wrong, and the venue publishes nothing that
    /// could tell us if it were. Never upgraded to clean.
    Unverifiable,
}

impl CoverageState {
    /// Classify one bucket.
    ///
    /// `verifiable` comes from the venue's capability matrix, not from whether
    /// this particular window happened to look fine.
    pub fn classify(messages: u64, suspect_rows: u64, verifiable: bool) -> Self {
        if messages == 0 {
            CoverageState::Absent
        } else if suspect_rows > 0 {
            CoverageState::Suspect
        } else if verifiable {
            CoverageState::Clean
        } else {
            CoverageState::Unverifiable
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            CoverageState::Absent => "absent",
            CoverageState::Suspect => "suspect",
            CoverageState::Clean => "clean",
            CoverageState::Unverifiable => "unverifiable",
        }
    }
}

#[cfg(test)]
mod coverage_state_tests {
    use super::CoverageState;

    #[test]
    fn a_venue_that_cannot_detect_loss_is_never_reported_clean() {
        // The whole headline. Bitstamp's aggregated feed carries no sequence
        // number and no checksum, so a quiet window there is not evidence of
        // anything, and a grid that painted it the same green as Kraken would
        // be the exact lie the dataset is meant to avoid.
        for messages in [1, 1_000, u64::MAX] {
            assert_eq!(
                CoverageState::classify(messages, 0, false),
                CoverageState::Unverifiable
            );
        }
    }

    #[test]
    fn a_marked_window_outranks_everything() {
        // Suspect wins even where the venue could otherwise prove cleanliness,
        // and even where it could not prove anything at all.
        assert_eq!(CoverageState::classify(10, 1, true), CoverageState::Suspect);
        assert_eq!(
            CoverageState::classify(10, 1, false),
            CoverageState::Suspect
        );
    }

    #[test]
    fn nothing_recorded_is_absent_rather_than_clean() {
        assert_eq!(CoverageState::classify(0, 0, true), CoverageState::Absent);
        assert_eq!(CoverageState::classify(0, 0, false), CoverageState::Absent);
    }

    #[test]
    fn only_a_checked_window_on_a_checkable_venue_is_clean() {
        assert_eq!(CoverageState::classify(1, 0, true), CoverageState::Clean);
    }
}
