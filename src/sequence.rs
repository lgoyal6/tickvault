//! Loss detection.
//!
//! Feeds drop messages and do not say so. Every venue offers some way to notice
//! and no two offer the same one, so this module models the *schemes* rather
//! than the venues:
//!
//! - a monotonic counter per message, which pinpoints exactly how many messages
//!   went missing (Coinbase Advanced Trade's `sequence_num`)
//! - a checksum of the book after each message, which proves divergence but
//!   cannot say how much was lost, and only sees as far down the book as the
//!   venue hashes (Kraken v2)
//!
//! The distinction is not academic. A counter detects every loss. A checksum
//! detects only losses that changed the hashed region, which is why
//! [`Unverifiable`] exists: a message we could not check is recorded as
//! unchecked, never as clean.

use std::fmt;

use crate::book::checksum::{ChecksumResult, KRAKEN_CHECKSUM_DEPTH};

/// What we concluded about one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqVerdict {
    /// Continuous with the previous message.
    InOrder,
    /// The same message twice. Harmless, but do not apply it again.
    Duplicate { seq: u128 },
    /// Older than what we already have; predates the snapshot. Discard quietly.
    Stale { seq: u128, floor: u128 },
    /// Messages were lost.
    Gap(GapEvidence),
    /// Our book no longer matches the venue's own hash of it. Something was
    /// lost, reordered, or misapplied; the checksum cannot say which.
    Divergence { expected: u32, computed: u32 },
    /// The venue restarted its numbering. Requires a fresh snapshot, but is not
    /// data loss on their side.
    Reset { previous: u128, observed: u128 },
    /// A message arrived stamped earlier than one we already applied.
    ///
    /// Nothing was necessarily lost, but the book was built in the wrong order,
    /// so it may be wrong. This is the only fault a timestamp-only feed can
    /// detect, which is precisely why it is not the same thing as a gap.
    OutOfOrder { previous: i64, observed: i64 },
    /// We could not judge this message. Counted separately from clean.
    Unverifiable(Unverifiable),
}

impl SeqVerdict {
    /// True when the book is no longer a faithful copy and needs a re-snapshot.
    pub fn requires_resnapshot(&self) -> bool {
        matches!(
            self,
            SeqVerdict::Gap(_)
                | SeqVerdict::Divergence { .. }
                | SeqVerdict::Reset { .. }
                | SeqVerdict::OutOfOrder { .. }
        )
    }

    /// True when the message should be applied to the book.
    pub fn should_apply(&self) -> bool {
        matches!(self, SeqVerdict::InOrder | SeqVerdict::Unverifiable(_))
    }

    /// True when this is positive evidence of loss, as opposed to an absence of
    /// evidence either way.
    pub fn is_loss(&self) -> bool {
        matches!(
            self,
            SeqVerdict::Gap(_) | SeqVerdict::Divergence { .. } | SeqVerdict::OutOfOrder { .. }
        )
    }
}

/// How much a gap swallowed, in the only unit the scheme can honestly report.
///
/// Three different things, and conflating them would put made-up numbers on the
/// front page of the dataset:
///
/// - A per-message counter (Coinbase, Bybit) knows the message count exactly.
/// - An update-id range (Binance) knows how many *update ids* were skipped,
///   which is not the same as the number of messages, since one message can
///   carry a whole range of them.
/// - A chained id (OKX) proves a break exactly and says nothing about size: its
///   `seqId` is an internal identifier, not a counter, and jumps by tens
///   between consecutive messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LossSize {
    /// Exactly this many messages went missing.
    Messages(u64),
    /// Exactly this many update ids went missing, which may be fewer messages.
    Updates(u64),
    /// Something was lost. The scheme cannot say how much, and neither can we.
    Unknown,
}

impl LossSize {
    pub fn messages(self) -> u64 {
        match self {
            LossSize::Messages(n) => n,
            _ => 0,
        }
    }

    pub fn updates(self) -> u64 {
        match self {
            LossSize::Updates(n) => n,
            _ => 0,
        }
    }

    pub fn is_unknown(self) -> bool {
        matches!(self, LossSize::Unknown)
    }
}

impl fmt::Display for LossSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LossSize::Messages(n) => write!(f, "{n} message(s)"),
            LossSize::Updates(n) => write!(f, "{n} update id(s)"),
            LossSize::Unknown => write!(f, "an unknown amount"),
        }
    }
}

/// A detected discontinuity.
///
/// Identifiers are held as `u128` because they are not all counters. Bitstamp's
/// L3 feed chains on a 128-bit token, and truncating it to compare would risk
/// two different messages looking like the same one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapEvidence {
    /// The identifier we were expecting next.
    pub expected: u128,
    /// The identifier that actually arrived.
    pub observed: u128,
    /// How much went missing, in whatever unit this scheme can prove.
    pub size: LossSize,
}

/// Why a message could not be judged.
///
/// Every variant here is a hole in our own coverage, not the venue's, and the
/// published gap report names them rather than folding them into the clean
/// count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unverifiable {
    /// No snapshot yet, so there is no baseline to compare against.
    AwaitingSnapshot,
    /// The venue sends neither a sequence number nor a checksum. Loss on such a
    /// feed is undetectable by construction.
    VenuePublishesNoSequence,
    /// The book has fewer levels than the venue's checksum covers, so a
    /// matching hash would prove less than it appears to.
    BookShallowerThanChecksum { have: usize, need: usize },
    /// Our own decimal rendering lost digits, so a mismatch would be our bug
    /// and blaming the venue for it would corrupt the gap statistics.
    RenderingWasLossy { levels: usize },
    /// The venue's checksum is computed at the instrument's own price and
    /// quantity precision, and we do not have that metadata for this symbol.
    /// Guessing it would make every message look corrupt.
    MissingInstrumentMetadata,
    /// The message simply carried no sequence number or checksum, on a venue
    /// that normally sends one.
    MessageCarriedNoCheck,
}

impl fmt::Display for Unverifiable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Unverifiable::AwaitingSnapshot => write!(f, "no snapshot yet"),
            Unverifiable::VenuePublishesNoSequence => {
                write!(f, "venue publishes neither sequence nor checksum")
            }
            Unverifiable::BookShallowerThanChecksum { have, need } => {
                write!(f, "book has {have} levels, checksum covers {need}")
            }
            Unverifiable::RenderingWasLossy { levels } => {
                write!(f, "{levels} level(s) lost digits in our own rendering")
            }
            Unverifiable::MissingInstrumentMetadata => {
                write!(f, "instrument precision unknown")
            }
            Unverifiable::MessageCarriedNoCheck => {
                write!(f, "message carried no sequence or checksum")
            }
        }
    }
}

/// A monotonic per-message counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterState {
    last: Option<u64>,
    /// How much the counter advances per message. One, everywhere we have seen.
    step: u64,
    /// A counter at or below this value, arriving after a higher one, is the
    /// venue restarting rather than a stale message. Coinbase Advanced Trade
    /// numbers each connection from zero.
    restart_floor: u64,
    seen: u64,
}

impl CounterState {
    pub fn new(step: u64, restart_floor: u64) -> Self {
        assert!(step > 0, "a counter that never advances cannot detect loss");
        CounterState {
            last: None,
            step,
            restart_floor,
            seen: 0,
        }
    }

    pub fn last(&self) -> Option<u64> {
        self.last
    }

    /// Anchor to a snapshot's sequence number, discarding prior history.
    pub fn anchor(&mut self, seq: u64) {
        self.last = Some(seq);
    }

    pub fn reset(&mut self) {
        self.last = None;
    }

    /// Judge one message and advance.
    pub fn observe(&mut self, seq: u64) -> SeqVerdict {
        self.seen += 1;
        let Some(last) = self.last else {
            self.last = Some(seq);
            return SeqVerdict::InOrder;
        };

        if seq == last {
            return SeqVerdict::Duplicate { seq: seq as u128 };
        }

        if seq < last {
            // Numbering went backwards. Either the venue restarted its counter
            // or this message predates our snapshot.
            return if seq <= self.restart_floor {
                self.last = Some(seq);
                SeqVerdict::Reset {
                    previous: last as u128,
                    observed: seq as u128,
                }
            } else {
                SeqVerdict::Stale {
                    seq: seq as u128,
                    floor: last as u128,
                }
            };
        }

        let expected = last + self.step;
        self.last = Some(seq);
        if seq == expected {
            SeqVerdict::InOrder
        } else {
            SeqVerdict::Gap(GapEvidence {
                expected: expected as u128,
                observed: seq as u128,
                size: LossSize::Messages((seq - expected) / self.step),
            })
        }
    }
}

/// A per-message checksum over the book.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChecksumState {
    last_matched: Option<u32>,
    verified: u64,
    unverifiable: u64,
}

impl ChecksumState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Messages we were actually able to check.
    pub fn verified(&self) -> u64 {
        self.verified
    }

    /// Messages that arrived but could not be checked.
    pub fn unverifiable(&self) -> u64 {
        self.unverifiable
    }

    pub fn reset(&mut self) {
        self.last_matched = None;
    }

    /// Compare the venue's checksum against one we recomputed.
    ///
    /// `computed` carries its own trustworthiness. A mismatch produced from a
    /// shallow book or a lossy rendering is reported as unverifiable, because
    /// calling it a gap would attribute our defect to the venue and inflate the
    /// dataset's own honesty numbers in the wrong direction.
    pub fn observe(&mut self, expected: u32, computed: ChecksumResult) -> SeqVerdict {
        if computed.lossy_levels > 0 {
            self.unverifiable += 1;
            return SeqVerdict::Unverifiable(Unverifiable::RenderingWasLossy {
                levels: computed.lossy_levels,
            });
        }
        let shallow = computed.bid_levels.min(computed.ask_levels);
        if shallow < KRAKEN_CHECKSUM_DEPTH && computed.value != expected {
            self.unverifiable += 1;
            return SeqVerdict::Unverifiable(Unverifiable::BookShallowerThanChecksum {
                have: shallow,
                need: KRAKEN_CHECKSUM_DEPTH,
            });
        }
        if computed.value == expected {
            self.last_matched = Some(expected);
            self.verified += 1;
            SeqVerdict::InOrder
        } else {
            SeqVerdict::Divergence {
                expected,
                computed: computed.value,
            }
        }
    }
}

/// An explicitly chained identifier, where each message names its predecessor.
///
/// OKX works this way: every book message carries `seqId` and `prevSeqId`, and
/// the snapshot carries `prevSeqId: -1`. This is the strongest continuity proof
/// of any scheme here, because it needs no assumption about step size at all.
/// It is also the least informative about magnitude: consecutive `seqId`s
/// observed live jumped by anywhere from 7 to 93, so the identifier is not a
/// message count and the size of a loss is genuinely unknowable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainState {
    last: Option<u128>,
}

impl ChainState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last(&self) -> Option<u128> {
        self.last
    }

    pub fn reset(&mut self) {
        self.last = None;
    }

    /// Judge a message that claims to follow `prev`.
    ///
    /// `prev` is `None` for a message that starts a chain, which is how OKX's
    /// `prevSeqId: -1` arrives after parsing.
    ///
    /// Works equally for OKX's numeric `seqId` and Bitstamp's 128-bit L3 event
    /// token, because the check is identity rather than arithmetic. Neither
    /// venue's identifier is a counter, so neither can size a gap.
    pub fn observe(&mut self, seq: u128, prev: Option<u128>) -> SeqVerdict {
        let Some(last) = self.last else {
            // Nothing to chain to yet; this message defines the start.
            self.last = Some(seq);
            return SeqVerdict::InOrder;
        };
        let Some(prev) = prev else {
            // The venue says this begins a new chain. Trust it and rebase.
            self.last = Some(seq);
            return SeqVerdict::Reset {
                previous: last,
                observed: seq,
            };
        };
        if prev == last {
            self.last = Some(seq);
            return SeqVerdict::InOrder;
        }
        if seq == last {
            return SeqVerdict::Duplicate { seq };
        }
        if seq < last {
            return SeqVerdict::Stale { seq, floor: last };
        }
        self.last = Some(seq);
        SeqVerdict::Gap(GapEvidence {
            expected: last,
            observed: prev,
            // The chain is broken and by exactly how much is not recoverable.
            size: LossSize::Unknown,
        })
    }
}

/// A range of update identifiers per message, anchored to a REST snapshot.
///
/// Binance works this way. Each diff carries a first and last update id, and
/// the stream is contiguous when each message's first id is one past the
/// previous message's last. Syncing to a snapshot is its own small protocol:
/// discard everything at or below the snapshot's `lastUpdateId`, then require
/// the first surviving message to straddle `lastUpdateId + 1`. A snapshot that
/// is too old or too new fails that test, and the only correct response is to
/// fetch another one rather than start from a book that does not line up.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeState {
    snapshot_id: Option<u64>,
    last_id: Option<u64>,
    synced: bool,
}

impl RangeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_synced(&self) -> bool {
        self.synced
    }

    pub fn last_id(&self) -> Option<u64> {
        self.last_id
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Anchor to a REST snapshot's `lastUpdateId`.
    pub fn anchor_snapshot(&mut self, last_update_id: u64) {
        self.snapshot_id = Some(last_update_id);
        self.last_id = None;
        self.synced = false;
    }

    /// Judge a diff spanning update ids `first..=last`.
    pub fn observe(&mut self, first: u64, last: u64) -> SeqVerdict {
        if !self.synced {
            let Some(snapshot_id) = self.snapshot_id else {
                return SeqVerdict::Unverifiable(Unverifiable::AwaitingSnapshot);
            };
            if last <= snapshot_id {
                // Predates the snapshot entirely; the snapshot already has it.
                return SeqVerdict::Stale {
                    seq: last as u128,
                    floor: snapshot_id as u128,
                };
            }
            // Written as the venue documents the rule, `U <= lastUpdateId+1
            // <= u`, because that is what a reader will be checking it against.
            #[allow(clippy::int_plus_one)]
            let straddles = first <= snapshot_id + 1 && snapshot_id + 1 <= last;
            if straddles {
                self.synced = true;
                self.last_id = Some(last);
                return SeqVerdict::InOrder;
            }
            // The stream has already moved past where the snapshot ends, so
            // whatever happened in between is lost to us.
            self.last_id = Some(last);
            self.synced = true;
            return SeqVerdict::Gap(GapEvidence {
                expected: (snapshot_id + 1) as u128,
                observed: first as u128,
                size: LossSize::Updates(first.saturating_sub(snapshot_id + 1)),
            });
        }

        let last_id = self.last_id.expect("synced implies an anchor");
        if last <= last_id {
            return SeqVerdict::Duplicate { seq: last as u128 };
        }
        let expected = last_id + 1;
        self.last_id = Some(last);
        if first == expected {
            SeqVerdict::InOrder
        } else if first < expected {
            // Overlapping ranges: some of this we already have, but it does
            // carry new ids, so it still has to be applied.
            SeqVerdict::InOrder
        } else {
            SeqVerdict::Gap(GapEvidence {
                expected: expected as u128,
                observed: first as u128,
                size: LossSize::Updates(first - expected),
            })
        }
    }
}

/// A timestamp and nothing else.
///
/// Bitstamp's diff feed carries a microsecond timestamp and no identifier of
/// any kind. This state exists to be explicit about what that buys, which is
/// less than it looks: it detects a message arriving out of order, and it
/// detects an exact repeat. **It cannot detect loss at all.** A perfectly
/// ordered stream with a hole in it is indistinguishable from a complete one.
///
/// So a well-ordered message is reported [`Unverifiable`], never `InOrder`.
/// Calling it in-order would let a feed with no loss detection whatsoever
/// report itself as fully verified.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimestampState {
    last: Option<i64>,
    /// The snapshot's own timestamp. Anything at or below it is already
    /// contained in that snapshot.
    floor: Option<i64>,
}

impl TimestampState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last(&self) -> Option<i64> {
        self.last
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Anchor to a snapshot taken at `nanos`.
    ///
    /// Without this the feed cannot be spliced to a snapshot at all. Fetching a
    /// REST book takes long enough that diffs pile up in the socket while it is
    /// in flight, and every one of them predates the snapshot. Judged against
    /// the snapshot's clock they look like messages arriving out of order,
    /// which forces a re-snapshot, during which more diffs pile up, which
    /// forces another. Observed live as roughly three re-snapshots a second on
    /// a feed that was entirely healthy.
    ///
    /// This is the timestamp counterpart of discarding update ids at or below a
    /// Binance `lastUpdateId`, and it exists for exactly the same reason.
    pub fn anchor_snapshot(&mut self, nanos: i64) {
        self.floor = Some(nanos);
        self.last = Some(nanos);
    }

    pub fn observe(&mut self, stamp: i64) -> SeqVerdict {
        if let Some(floor) = self.floor
            && stamp <= floor
        {
            // Already contained in the snapshot. Discard without alarm.
            return SeqVerdict::Stale {
                seq: stamp.max(0) as u128,
                floor: floor.max(0) as u128,
            };
        }
        match self.last {
            Some(last) if stamp == last => SeqVerdict::Duplicate {
                seq: stamp.max(0) as u128,
            },
            Some(last) if stamp < last => SeqVerdict::OutOfOrder {
                previous: last,
                observed: stamp,
            },
            _ => {
                self.last = Some(stamp);
                SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence)
            }
        }
    }
}

/// Per-symbol validation state, in whichever scheme the venue uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeqState {
    Counter(CounterState),
    Checksum(ChecksumState),
    Chain(ChainState),
    Range(RangeState),
    Timestamp(TimestampState),
    /// The venue gives us nothing to validate against.
    Unverifiable,
}

impl SeqState {
    pub fn counter(step: u64, restart_floor: u64) -> Self {
        SeqState::Counter(CounterState::new(step, restart_floor))
    }

    pub fn checksum() -> Self {
        SeqState::Checksum(ChecksumState::new())
    }

    pub fn chain() -> Self {
        SeqState::Chain(ChainState::new())
    }

    pub fn range() -> Self {
        SeqState::Range(RangeState::new())
    }

    pub fn timestamp() -> Self {
        SeqState::Timestamp(TimestampState::new())
    }

    /// Forget history, as after a reconnect or a re-snapshot.
    pub fn reset(&mut self) {
        match self {
            SeqState::Counter(c) => c.reset(),
            SeqState::Checksum(c) => c.reset(),
            SeqState::Chain(c) => c.reset(),
            SeqState::Range(c) => c.reset(),
            SeqState::Timestamp(c) => c.reset(),
            SeqState::Unverifiable => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counter() -> CounterState {
        CounterState::new(1, 0)
    }

    #[test]
    fn a_continuous_run_is_all_in_order() {
        let mut c = counter();
        for seq in 100..110 {
            assert_eq!(c.observe(seq), SeqVerdict::InOrder, "at {seq}");
        }
    }

    #[test]
    fn a_single_missing_message_is_detected_and_counted() {
        let mut c = counter();
        c.observe(100);
        // 101 never arrives.
        assert_eq!(
            c.observe(102),
            SeqVerdict::Gap(GapEvidence {
                expected: 101,
                observed: 102,
                size: LossSize::Messages(1)
            })
        );
    }

    #[test]
    fn a_burst_of_missing_messages_reports_the_exact_count() {
        let mut c = counter();
        c.observe(5);
        let SeqVerdict::Gap(evidence) = c.observe(1_005) else {
            panic!("expected a gap")
        };
        assert_eq!(evidence.size, LossSize::Messages(999));
        assert_eq!(evidence.expected, 6);
    }

    #[test]
    fn after_a_gap_the_stream_resynchronises_rather_than_gapping_forever() {
        let mut c = counter();
        c.observe(1);
        assert!(matches!(c.observe(4), SeqVerdict::Gap(_)));
        // The next message follows 4, so it is clean. A validator that kept
        // comparing against 1 would report a gap on every message after the
        // first loss and make the venue look far worse than it is.
        assert_eq!(c.observe(5), SeqVerdict::InOrder);
    }

    #[test]
    fn duplicates_and_stale_messages_are_distinguished_from_loss() {
        let mut c = counter();
        c.observe(10);
        assert_eq!(c.observe(10), SeqVerdict::Duplicate { seq: 10 });
        assert_eq!(c.observe(7), SeqVerdict::Stale { seq: 7, floor: 10 });
        assert!(!SeqVerdict::Duplicate { seq: 10 }.is_loss());
        assert!(!SeqVerdict::Duplicate { seq: 10 }.should_apply());
    }

    #[test]
    fn a_restart_at_the_floor_is_a_reset_not_a_stale_message() {
        let mut c = counter();
        c.observe(900);
        assert_eq!(
            c.observe(0),
            SeqVerdict::Reset {
                previous: 900,
                observed: 0
            }
        );
        // And the validator now tracks the new numbering.
        assert_eq!(c.observe(1), SeqVerdict::InOrder);
    }

    #[test]
    fn the_first_message_can_never_be_a_gap() {
        let mut c = counter();
        assert_eq!(c.observe(999_999), SeqVerdict::InOrder);
    }

    #[test]
    fn anchoring_to_a_snapshot_makes_the_next_message_checkable() {
        let mut c = counter();
        c.anchor(500);
        assert_eq!(c.observe(501), SeqVerdict::InOrder);
        let mut d = counter();
        d.anchor(500);
        assert!(matches!(d.observe(503), SeqVerdict::Gap(_)));
    }

    fn result(value: u32, bids: usize, asks: usize, lossy: usize) -> ChecksumResult {
        ChecksumResult {
            value,
            lossy_levels: lossy,
            bid_levels: bids,
            ask_levels: asks,
        }
    }

    #[test]
    fn a_matching_checksum_is_in_order() {
        let mut s = ChecksumState::new();
        assert_eq!(s.observe(42, result(42, 10, 10, 0)), SeqVerdict::InOrder);
        assert_eq!(s.verified(), 1);
    }

    #[test]
    fn a_mismatch_on_a_full_book_is_divergence() {
        let mut s = ChecksumState::new();
        assert_eq!(
            s.observe(42, result(43, 10, 10, 0)),
            SeqVerdict::Divergence {
                expected: 42,
                computed: 43
            }
        );
        assert!(
            SeqVerdict::Divergence {
                expected: 42,
                computed: 43
            }
            .requires_resnapshot()
        );
    }

    #[test]
    fn our_own_lossy_rendering_is_never_reported_as_the_venue_losing_data() {
        let mut s = ChecksumState::new();
        assert_eq!(
            s.observe(42, result(43, 10, 10, 2)),
            SeqVerdict::Unverifiable(Unverifiable::RenderingWasLossy { levels: 2 })
        );
        assert!(!s.observe(42, result(43, 10, 10, 2)).is_loss());
    }

    #[test]
    fn a_shallow_book_yields_unverifiable_rather_than_a_false_gap() {
        let mut s = ChecksumState::new();
        assert_eq!(
            s.observe(42, result(43, 3, 10, 0)),
            SeqVerdict::Unverifiable(Unverifiable::BookShallowerThanChecksum { have: 3, need: 10 })
        );
        assert_eq!(s.unverifiable(), 1);
    }

    #[test]
    fn a_shallow_book_that_still_matches_is_accepted() {
        // A thin instrument genuinely has fewer than ten levels. If the hash
        // agrees anyway there is nothing to be suspicious about.
        let mut s = ChecksumState::new();
        assert_eq!(s.observe(42, result(42, 3, 3, 0)), SeqVerdict::InOrder);
    }

    #[test]
    fn unverifiable_messages_are_still_applied() {
        // We cannot check them, but refusing to apply them would guarantee the
        // book is wrong instead of merely unproven.
        assert!(SeqVerdict::Unverifiable(Unverifiable::AwaitingSnapshot).should_apply());
        assert!(!SeqVerdict::Unverifiable(Unverifiable::AwaitingSnapshot).is_loss());
    }

    // ------------------------------------------------------- chained ids --

    #[test]
    fn a_chain_holds_when_each_message_names_its_predecessor() {
        let mut c = ChainState::new();
        // Real OKX ids, which jump by varying amounts.
        assert_eq!(c.observe(80_274_832_416, None), SeqVerdict::InOrder);
        assert_eq!(
            c.observe(80_274_832_423, Some(80_274_832_416)),
            SeqVerdict::InOrder
        );
        assert_eq!(
            c.observe(80_274_832_435, Some(80_274_832_423)),
            SeqVerdict::InOrder
        );
    }

    #[test]
    fn a_broken_chain_is_a_gap_of_unknowable_size() {
        let mut c = ChainState::new();
        c.observe(100, None);
        // The next message claims to follow 150, which we never saw.
        let SeqVerdict::Gap(evidence) = c.observe(160, Some(150)) else {
            panic!("expected a gap")
        };
        // The identifier is not a counter, so 150 - 100 is not a message count
        // and must not be reported as one.
        assert_eq!(evidence.size, LossSize::Unknown);
        assert_eq!(evidence.size.messages(), 0);
        assert!(evidence.size.is_unknown());
    }

    #[test]
    fn a_chain_resynchronises_after_a_break() {
        let mut c = ChainState::new();
        c.observe(10, None);
        assert!(matches!(c.observe(30, Some(25)), SeqVerdict::Gap(_)));
        // The break moved us to 30; the message following it is clean.
        assert_eq!(c.observe(40, Some(30)), SeqVerdict::InOrder);
    }

    #[test]
    fn a_chain_start_marker_mid_stream_is_a_reset_not_a_gap() {
        // OKX sends prevSeqId -1 on a snapshot, which is the venue telling us
        // to rebase rather than the venue losing anything.
        let mut c = ChainState::new();
        c.observe(500, None);
        assert_eq!(
            c.observe(900, None),
            SeqVerdict::Reset {
                previous: 500,
                observed: 900
            }
        );
        assert_eq!(c.observe(905, Some(900)), SeqVerdict::InOrder);
    }

    #[test]
    fn a_repeated_chain_message_is_a_duplicate() {
        let mut c = ChainState::new();
        c.observe(10, None);
        c.observe(20, Some(10));
        assert_eq!(c.observe(20, Some(10)), SeqVerdict::Duplicate { seq: 20 });
    }

    // -------------------------------------------------- update id ranges --

    #[test]
    fn a_range_feed_syncs_to_a_snapshot_that_straddles_it() {
        // The real numbers from a captured Binance.US fixture.
        let mut r = RangeState::new();
        r.anchor_snapshot(3_297_892_542);
        // Predates the snapshot entirely.
        assert!(matches!(
            r.observe(3_297_892_529, 3_297_892_535),
            SeqVerdict::Stale { .. }
        ));
        assert!(!r.is_synced());
        // Straddles lastUpdateId + 1, so this is the message to start from.
        assert_eq!(r.observe(3_297_892_543, 3_297_892_543), SeqVerdict::InOrder);
        assert!(r.is_synced());
    }

    #[test]
    fn a_range_feed_counts_missing_update_ids_not_missing_messages() {
        let mut r = RangeState::new();
        r.anchor_snapshot(100);
        assert_eq!(r.observe(101, 105), SeqVerdict::InOrder);
        // 106..=109 never arrived. That is four update ids, carried by an
        // unknown number of messages, so it is reported in the right unit.
        let SeqVerdict::Gap(evidence) = r.observe(110, 112) else {
            panic!("expected a gap")
        };
        assert_eq!(evidence.size, LossSize::Updates(4));
        assert_eq!(evidence.size.messages(), 0, "these are not messages");
    }

    #[test]
    fn a_snapshot_the_stream_has_already_passed_is_a_gap_not_a_clean_start() {
        let mut r = RangeState::new();
        r.anchor_snapshot(100);
        // The first live message starts at 200, so 101..199 happened while we
        // were fetching and are gone.
        let SeqVerdict::Gap(evidence) = r.observe(200, 210) else {
            panic!("expected a gap")
        };
        assert_eq!(evidence.size, LossSize::Updates(99));
        assert!(
            r.is_synced(),
            "we are now tracking, just with a hole behind us"
        );
    }

    #[test]
    fn a_range_feed_without_a_snapshot_cannot_judge_anything() {
        let mut r = RangeState::new();
        assert_eq!(
            r.observe(1, 2),
            SeqVerdict::Unverifiable(Unverifiable::AwaitingSnapshot)
        );
    }

    #[test]
    fn an_overlapping_range_is_applied_rather_than_flagged() {
        let mut r = RangeState::new();
        r.anchor_snapshot(100);
        r.observe(101, 110);
        // Starts before where we are but carries new ids too.
        assert_eq!(r.observe(108, 115), SeqVerdict::InOrder);
        // Entirely behind us.
        assert!(matches!(r.observe(109, 112), SeqVerdict::Duplicate { .. }));
    }

    // ------------------------------------------------- timestamps only --

    #[test]
    fn a_timestamp_feed_never_claims_a_message_was_verified() {
        // The whole point. An ordered stream with a hole in it is
        // indistinguishable from a complete one, so "in order" would be a lie.
        let mut t = TimestampState::new();
        for stamp in [1_000i64, 1_001, 5_000, 9_999] {
            assert_eq!(
                t.observe(stamp),
                SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence),
                "stamp {stamp}"
            );
        }
        // And crucially, none of that counts as detected loss.
        assert!(!t.observe(10_000).is_loss());
    }

    #[test]
    fn a_timestamp_feed_does_detect_reordering() {
        let mut t = TimestampState::new();
        t.observe(5_000);
        assert_eq!(
            t.observe(4_000),
            SeqVerdict::OutOfOrder {
                previous: 5_000,
                observed: 4_000
            }
        );
        // Which is a real fault: the book was built in the wrong order.
        assert!(
            SeqVerdict::OutOfOrder {
                previous: 5_000,
                observed: 4_000
            }
            .requires_resnapshot()
        );
    }

    #[test]
    fn a_repeated_timestamp_is_a_duplicate_not_a_reorder() {
        let mut t = TimestampState::new();
        t.observe(7_000);
        assert_eq!(t.observe(7_000), SeqVerdict::Duplicate { seq: 7_000 });
    }

    #[test]
    fn diffs_predating_a_snapshot_are_discarded_rather_than_called_reordered() {
        // The bug this exists to prevent: a REST fetch takes long enough that
        // diffs queue up behind it, and every one predates the snapshot. Read
        // as reordering they force a re-snapshot, which queues more diffs,
        // which forces another. Live, that was three re-snapshots a second on
        // a perfectly healthy feed.
        let mut t = TimestampState::new();
        t.anchor_snapshot(1_000);
        assert!(matches!(t.observe(900), SeqVerdict::Stale { .. }));
        assert!(matches!(t.observe(1_000), SeqVerdict::Stale { .. }));
        assert!(!t.observe(900).requires_resnapshot());
        // The first diff genuinely after the snapshot resumes normally.
        assert_eq!(
            t.observe(2_000),
            SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence)
        );
        // Reordering above the floor is still caught, so discarding stale
        // diffs has not quietly disabled the one check this venue has.
        assert_eq!(
            t.observe(1_500),
            SeqVerdict::OutOfOrder {
                previous: 2_000,
                observed: 1_500
            }
        );
    }

    #[test]
    fn out_of_order_does_not_advance_the_timestamp_baseline() {
        // Otherwise one stray early message would make every later one look
        // out of order too.
        let mut t = TimestampState::new();
        t.observe(5_000);
        t.observe(4_000);
        assert_eq!(t.last(), Some(5_000));
        assert!(!t.observe(6_000).is_loss());
    }
}
