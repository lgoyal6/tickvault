//! The venue abstraction.
//!
//! Every venue differs in all five of connect, subscribe, snapshot, parse, and
//! validate, and the differences are not cosmetic. The two implemented here
//! disagree about nearly everything that matters:
//!
//! | | Coinbase Advanced Trade | Kraken v2 |
//! |---|---|---|
//! | loss detection | per-message counter | CRC32 of the book |
//! | counter scope | one per **connection** | none |
//! | validation runs | **before** applying | **after** applying |
//! | decimals arrive as | strings | JSON numbers |
//! | feed depth | full book | client truncates to N |
//! | detection coverage | total | top ten levels only |
//!
//! Two rows there are what shape this module. Because Coinbase numbers a whole
//! socket rather than an instrument, one gap makes *every* symbol on that
//! socket suspect. Because Kraken's checksum describes the book *after* the
//! update, its check cannot run until the update has been applied. Both facts
//! are declared in [`VenueCapabilities`] and read once by the session loop, so
//! neither becomes an `if venue == ...` at the call site.


use std::fmt;
use std::time::Duration;

use async_trait::async_trait;

use crate::book::l3::{OrderEvent, OrderId};
use crate::book::{BookDelta, BookSnapshot, L2Book};
use crate::clock::Stamp;
use crate::error::Result;
use crate::gap::DetectionLimit;
use crate::limits::{BudgetError, SubscriptionPlan, VenueBudget};
use crate::sequence::{SeqState, SeqVerdict};
use crate::types::{BookLevel, Side, Symbol, VenueId, VenueSymbol};

/// Bytes as they came off the socket, stamped on receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub venue: VenueId,
    pub payload: Vec<u8>,
    pub stamp: Stamp,
}

impl RawFrame {
    pub fn new(venue: VenueId, payload: impl Into<Vec<u8>>, stamp: Stamp) -> Self {
        RawFrame {
            venue,
            payload: payload.into(),
            stamp,
        }
    }

    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.payload)
    }
}

/// A book stated as individual orders rather than aggregated levels.
///
/// Listed in the venue's own order, which for a price-time venue is taken to be
/// queue order. That is an assumption, and it is why every seeded order is
/// marked [`crate::book::l3::QueueCertainty::Seeded`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderSnapshot {
    pub symbol: Symbol,
    /// `(id, side, price, quantity)`, in the venue's listing order.
    pub orders: Vec<(OrderId, Side, crate::Fixed, crate::Fixed)>,
    pub stamps: crate::clock::Timestamps,
}

/// Something a venue told us, once parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedEvent {
    Snapshot(BookSnapshot),
    Delta(BookDelta),
    /// One order added, changed, cancelled, or executed.
    Order(Box<OrderEvent>),
    /// Acks, heartbeats, status. Carried rather than discarded: a subscription
    /// error that is silently dropped becomes a symbol that is simply absent
    /// from the dataset with no explanation.
    Control(ControlEvent),
}

impl FeedEvent {
    pub fn symbol(&self) -> Option<&Symbol> {
        match self {
            FeedEvent::Snapshot(s) => Some(&s.symbol),
            FeedEvent::Delta(d) => Some(&d.symbol),
            FeedEvent::Order(o) => Some(&o.symbol),
            FeedEvent::Control(_) => None,
        }
    }

    pub fn stamp(&self) -> Stamp {
        match self {
            FeedEvent::Snapshot(s) => s.stamps.recv,
            FeedEvent::Delta(d) => d.stamps.recv,
            FeedEvent::Order(o) => o.stamps.recv,
            FeedEvent::Control(c) => c.stamp,
        }
    }

    /// True for events that carry book content.
    pub fn is_book_event(&self) -> bool {
        !matches!(self, FeedEvent::Control(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlEvent {
    pub kind: ControlKind,
    pub detail: String,
    pub stamp: Stamp,
    /// The connection's sequence number for this frame, where the venue numbers
    /// control frames from the same counter as book frames.
    ///
    /// Coinbase does. A subscription acknowledgement arrives numbered between
    /// two book messages, so a validator that skipped control frames would
    /// report a phantom gap every time the venue acknowledged something.
    pub seq: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlKind {
    SubscriptionAck,
    SubscriptionError,
    Heartbeat,
    Status,
    /// The venue is telling us to reconnect, ahead of dropping the socket.
    /// Bitstamp does this before maintenance; honouring it turns an outage into
    /// a controlled resync.
    ReconnectRequested,
    /// Understood, and deliberately carries no book content.
    Ignored,
}

/// How a venue lets us detect loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationScheme {
    /// A monotonic counter on every message.
    Counter { step: u64, restart_floor: u64 },
    /// A hash of the book after each message, covering `depth` levels a side.
    Checksum { depth: usize },
    /// Each message names its own predecessor, so continuity is proven exactly
    /// without assuming anything about step size. The identifier is not a
    /// counter, so the size of a loss is never recoverable.
    Chained,
    /// Each message spans a range of update ids and the stream is anchored to a
    /// REST snapshot's last id. Loss is measured in update ids, not messages.
    UpdateIdRange,
    /// A timestamp and nothing else. Detects reordering and repeats; **cannot
    /// detect loss**, which is a property of the venue, not of this recorder.
    MonotonicTimestamp,
    /// Nothing. Loss on such a feed cannot be detected at all, and any dataset
    /// built from one should say so on its front page.
    None,
}

/// Whether a validator's state belongs to a socket or to an instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceScope {
    /// One counter shared by every symbol on the connection. A single gap
    /// invalidates all of them at once.
    PerConnection,
    /// Independent per instrument.
    PerSymbol,
}

/// Whether the check can run before the update is applied, or only after.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationTiming {
    /// The message carries its own identity, so it can be judged on arrival and
    /// a bad one never reaches the book.
    BeforeApply,
    /// The message carries a hash of the resulting book, so the update must be
    /// applied before it can be checked. A failure means the book is already
    /// contaminated and has to be rebuilt from a snapshot.
    AfterApply,
}

/// Where the authoritative initial book comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSource {
    /// Pushed on the websocket when subscribing, so a re-snapshot means
    /// resubscribing.
    InBand,
    /// Fetched over REST, independently of the socket.
    Rest,
    /// There is no usable snapshot at all.
    ///
    /// The book starts empty and fills in as orders churn. Not a gap in the
    /// implementation: Bitstamp's order-by-order feed carries no snapshot, and
    /// its REST book holds orders the stream never retracts, so seeding from it
    /// is measurably worse than starting empty. A book in this state is
    /// incomplete rather than wrong, and says which of its levels are affected.
    None,
}

/// How many levels the feed itself carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedDepth {
    /// The whole book.
    Full,
    /// A fixed window that the client is required to truncate to.
    Limited(usize),
}

/// How decimal values are encoded on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalEncoding {
    /// Quoted, so the venue's exact digits survive.
    Strings,
    /// Bare JSON numbers, which must pass through a float on the way in.
    JsonNumbers,
}

/// Whether the venue deletes levels it never told us about.
///
/// This decides whether a delete for an absent price is evidence of loss or
/// ordinary noise, and the two venues genuinely differ. Measured over live
/// captures rather than assumed: in one 30-second recording Coinbase sent 210
/// deletes for prices absent from its own 44,000-level snapshot, while Kraken
/// sent none in 958 updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedundantDeletes {
    /// The venue only deletes levels it previously published. A delete for a
    /// level we are not holding therefore means we missed the add, and is real
    /// evidence that something was lost.
    Never,
    /// The venue emits deletes idempotently. Such a delete says nothing about
    /// data loss, and treating it as corruption would re-snapshot every few
    /// seconds while reporting a feed that is actually fine as almost entirely
    /// suspect.
    Expected,
}

/// What we must send to stop the venue hanging up on us.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keepalive {
    /// The websocket protocol's own ping and pong suffice.
    ProtocolPings,
    /// The venue wants an application-level heartbeat on this interval, and
    /// closes the socket without one. A missed heartbeat looks exactly like an
    /// outage in the dataset, so this is not optional detail.
    Text {
        payload: &'static str,
        every: Duration,
    },
}

/// What the venue's own clock tells us.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueTimestamps {
    None,
    PerMessage,
    /// Per message and additionally per level, which lets us see batching.
    PerMessageAndPerLevel,
}

/// Everything the session needs to know about a venue without asking which one
/// it is. This is the phase 2 capability matrix, in code first so the docs
/// cannot drift from the implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueCapabilities {
    pub id: VenueId,
    pub book_level: BookLevel,
    pub validation: ValidationScheme,
    pub scope: SequenceScope,
    pub timing: ValidationTiming,
    pub snapshot_source: SnapshotSource,
    pub feed_depth: FeedDepth,
    pub decimals: DecimalEncoding,
    pub redundant_deletes: RedundantDeletes,
    pub timestamps: VenueTimestamps,
    pub keepalive: Keepalive,
    /// Conservative rate and connection limits. See [`crate::limits`].
    pub budget: VenueBudget,
    /// Whether an API key is needed. Everything here is keyless by design; a
    /// venue that changes its mind should fail loudly rather than degrade.
    pub requires_auth: bool,
    /// Loss this venue cannot detect, whatever we do.
    pub detection_limits: Vec<DetectionLimit>,
}

impl VenueCapabilities {
    /// A fresh validator matching this venue's scheme.
    pub fn new_seq_state(&self) -> SeqState {
        match self.validation {
            ValidationScheme::Counter {
                step,
                restart_floor,
            } => SeqState::counter(step, restart_floor),
            ValidationScheme::Checksum { .. } => SeqState::checksum(),
            ValidationScheme::Chained => SeqState::chain(),
            ValidationScheme::UpdateIdRange => SeqState::range(),
            ValidationScheme::MonotonicTimestamp => SeqState::timestamp(),
            ValidationScheme::None => SeqState::Unverifiable,
        }
    }

    /// A fresh book honouring the feed's depth semantics.
    pub fn new_book(&self, symbol: &Symbol) -> L2Book {
        match self.feed_depth {
            FeedDepth::Full => L2Book::new(symbol.clone()),
            FeedDepth::Limited(n) => L2Book::with_depth_limit(symbol.clone(), n),
        }
    }

    /// True when a gap on one symbol invalidates every other symbol we are
    /// recording from the same socket.
    pub fn gap_is_connection_wide(&self) -> bool {
        self.scope == SequenceScope::PerConnection
    }

    /// Lay these symbols out across connections under this venue's budget.
    ///
    /// The spread-or-pack decision is read straight from the sequence scope: a
    /// venue whose counter belongs to the connection gets its symbols spread,
    /// because there a single gap invalidates every symbol sharing the socket.
    pub fn plan_subscriptions(
        &self,
        symbols: &[crate::types::Symbol],
    ) -> std::result::Result<SubscriptionPlan, BudgetError> {
        crate::limits::plan(symbols, &self.budget, self.gap_is_connection_wide())
    }

    /// True when this venue offers no way at all to detect a lost message.
    pub fn can_detect_loss(&self) -> bool {
        !matches!(
            self.validation,
            ValidationScheme::None | ValidationScheme::MonotonicTimestamp
        )
    }

    /// Whether this anomaly means the book must be rebuilt *on this venue*.
    ///
    /// Only one anomaly is venue-dependent, and it is the one that decides
    /// whether the recorder runs quietly or reconnects every two seconds.
    pub fn corrupts_book(&self, anomaly: &crate::book::BookAnomaly) -> bool {
        match anomaly {
            crate::book::BookAnomaly::RemovedMissingLevel { .. } => {
                self.redundant_deletes == RedundantDeletes::Never
            }
            other => other.corrupts_book(),
        }
    }
}

/// A source of raw frames. Implemented by a live websocket and by a replay of
/// recorded bytes, so the ingest path under test is the ingest path that runs.
#[async_trait]
pub trait FeedTransport: Send {
    /// Next frame, or `None` when the source is exhausted.
    async fn recv(&mut self) -> Result<Option<RawFrame>>;
    async fn send_text(&mut self, text: &str) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

/// Minimal HTTP surface, so REST snapshots can be canned in tests without a
/// network or a mock server.
#[async_trait]
pub trait HttpFetch: Send + Sync + fmt::Debug {
    async fn get(&self, url: &str) -> Result<Vec<u8>>;
}

/// One venue's protocol.
///
/// The five methods the recorder actually calls are [`Venue::connect`],
/// [`Venue::subscribe`], [`Venue::snapshot`], [`Venue::parse_delta`], and
/// [`Venue::validate_sequence`]. Parsing and validation are synchronous and
/// free of IO on purpose: the gate test drives them over recorded bytes with
/// no network at all, and it exercises the same code the live recorder runs.
#[async_trait]
pub trait Venue: Send + Sync {
    fn capabilities(&self) -> &VenueCapabilities;

    fn id(&self) -> VenueId {
        self.capabilities().id
    }

    /// This venue's spelling of a canonical symbol.
    fn venue_symbol(&self, symbol: &Symbol) -> VenueSymbol;

    /// The canonical symbol behind this venue's spelling.
    fn canonical_symbol(&self, raw: &str) -> Result<Symbol>;

    /// Open a feed.
    async fn connect(&self) -> Result<Box<dyn FeedTransport>>;

    /// The frames to send to start receiving these symbols.
    fn subscribe(&self, symbols: &[Symbol]) -> Result<Vec<String>>;

    /// Fetch an order-level snapshot, for venues that expose L3.
    ///
    /// `None` means this venue has no order-by-order book to fetch, which is
    /// the case for five of the six recorded here. It is the same fact as
    /// `capabilities().book_level`, and the conformance suite checks the two
    /// agree rather than trusting either alone.
    async fn order_snapshot(&self, _symbol: &Symbol) -> Result<Option<OrderSnapshot>> {
        Ok(None)
    }

    /// Fetch an out-of-band book snapshot over REST.
    ///
    /// For an [`SnapshotSource::InBand`] venue this is a cross-check rather
    /// than the primary path: comparing it against the socket-maintained book
    /// catches drift that neither a sequence number nor a checksum would.
    async fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot>;

    /// Parse one raw frame. A frame may hold several events, or none.
    fn parse_delta(&self, frame: &RawFrame) -> Result<Vec<FeedEvent>>;

    /// Judge one event against the validator state.
    ///
    /// `book` is the book *before* the update for a [`ValidationTiming::BeforeApply`]
    /// venue and *after* it for [`ValidationTiming::AfterApply`]. The session
    /// reads the timing from capabilities and calls at the right moment.
    fn validate_sequence(
        &self,
        state: &mut SeqState,
        event: &FeedEvent,
        book: &L2Book,
    ) -> SeqVerdict;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequence::CounterState;

    fn caps(scheme: ValidationScheme, depth: FeedDepth) -> VenueCapabilities {
        VenueCapabilities {
            id: VenueId::Coinbase,
            book_level: BookLevel::L2,
            validation: scheme,
            scope: SequenceScope::PerConnection,
            timing: ValidationTiming::BeforeApply,
            snapshot_source: SnapshotSource::InBand,
            feed_depth: depth,
            decimals: DecimalEncoding::Strings,
            redundant_deletes: RedundantDeletes::Never,
            timestamps: VenueTimestamps::PerMessage,
            keepalive: Keepalive::ProtocolPings,
            budget: VenueBudget::conservative(),
            requires_auth: false,
            detection_limits: Vec::new(),
        }
    }

    #[test]
    fn capabilities_build_the_matching_validator() {
        let counter = caps(
            ValidationScheme::Counter {
                step: 1,
                restart_floor: 0,
            },
            FeedDepth::Full,
        );
        assert_eq!(
            counter.new_seq_state(),
            SeqState::Counter(CounterState::new(1, 0))
        );
        let checksum = caps(ValidationScheme::Checksum { depth: 10 }, FeedDepth::Full);
        assert!(matches!(checksum.new_seq_state(), SeqState::Checksum(_)));
        let none = caps(ValidationScheme::None, FeedDepth::Full);
        assert_eq!(none.new_seq_state(), SeqState::Unverifiable);
    }

    #[test]
    fn a_depth_limited_feed_produces_a_truncating_book() {
        let symbol = Symbol::new("BTC", "USD");
        let full = caps(ValidationScheme::None, FeedDepth::Full);
        assert_eq!(full.new_book(&symbol).depth_limit(), None);
        let limited = caps(ValidationScheme::None, FeedDepth::Limited(25));
        assert_eq!(limited.new_book(&symbol).depth_limit(), Some(25));
    }

    #[test]
    fn a_timestamp_only_venue_admits_it_cannot_detect_loss() {
        let mut c = caps(ValidationScheme::MonotonicTimestamp, FeedDepth::Full);
        assert!(!c.can_detect_loss());
        c.validation = ValidationScheme::None;
        assert!(!c.can_detect_loss());
        c.validation = ValidationScheme::Chained;
        assert!(c.can_detect_loss());
    }

    #[test]
    fn each_new_scheme_builds_its_matching_validator() {
        use crate::sequence::SeqState;
        for (scheme, matches) in [
            (
                ValidationScheme::Chained,
                matches!(
                    caps(ValidationScheme::Chained, FeedDepth::Full).new_seq_state(),
                    SeqState::Chain(_)
                ),
            ),
            (
                ValidationScheme::UpdateIdRange,
                matches!(
                    caps(ValidationScheme::UpdateIdRange, FeedDepth::Full).new_seq_state(),
                    SeqState::Range(_)
                ),
            ),
            (
                ValidationScheme::MonotonicTimestamp,
                matches!(
                    caps(ValidationScheme::MonotonicTimestamp, FeedDepth::Full).new_seq_state(),
                    SeqState::Timestamp(_)
                ),
            ),
        ] {
            assert!(matches, "{scheme:?} built the wrong validator");
        }
    }

    #[test]
    fn the_subscription_plan_follows_the_sequence_scope() {
        let symbols: Vec<crate::types::Symbol> = (0..4)
            .map(|i| crate::types::Symbol::new(&format!("A{i}"), "USD"))
            .collect();

        // Connection-scoped: spread, so one gap costs one symbol.
        let mut c = caps(ValidationScheme::None, FeedDepth::Full);
        c.scope = SequenceScope::PerConnection;
        let spread = c.plan_subscriptions(&symbols).unwrap();
        assert_eq!(spread.worst_case_blast_radius(true), 1);

        // Symbol-scoped: pack, because a gap only ever affects one instrument.
        c.scope = SequenceScope::PerSymbol;
        let packed = c.plan_subscriptions(&symbols).unwrap();
        assert_eq!(packed.connection_count(), 1);
    }

    #[test]
    fn connection_scope_is_readable_without_naming_a_venue() {
        let mut c = caps(ValidationScheme::None, FeedDepth::Full);
        assert!(c.gap_is_connection_wide());
        c.scope = SequenceScope::PerSymbol;
        assert!(!c.gap_is_connection_wide());
    }
}
