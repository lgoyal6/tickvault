//! Bitstamp.
//!
//! The honest case, and the reason it is in the set at all.
//!
//! Bitstamp's `diff_order_book` channel carries a microsecond timestamp and
//! **nothing else**. No sequence number, no update id, no checksum. That is not
//! a limitation of this recorder, it is a property of the feed, and it means
//! that a lost message on Bitstamp is *undetectable by any means*. A stream
//! with a hole in it is bit-for-bit indistinguishable from a complete one.
//!
//! Every book message is therefore reported unverifiable rather than clean. A
//! Bitstamp day in this dataset reads "100% clean, 0% verified", which is the
//! literal truth: we have no reason to think it is broken and no way to know.
//! Any free dataset that shows Bitstamp as fully validated is not measuring
//! something we failed to measure; it is asserting something the wire cannot
//! support.
//!
//! Two weaker checks are still available and are used:
//!
//! - **Ordering.** The microtimestamp must increase. A message arriving out of
//!   order means the book was built wrongly, and that *is* detectable.
//! - **Deletes for levels we never held.** Since nothing else can indicate
//!   loss, this is treated as evidence here, unlike on Coinbase and Binance
//!   where the venue emits such deletes constantly. Measured over 327 live
//!   messages against a complete 3000-level snapshot, it fired 3 times, all at
//!   prices well inside the covered range. Those three cannot be distinguished
//!   from the venue reporting a level that was added and cancelled inside one
//!   aggregation window, so Bitstamp's suspect time is an upper bound rather
//!   than an estimate, and the capability matrix says so.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::book::l3::{OrderAction, OrderEvent, OrderId};
use crate::book::{BookDelta, BookSnapshot, L2Book, LevelChange};
use crate::clock::{Clock, Timestamps};
use crate::error::{Error, Result};
use crate::fixed::Fixed;
use crate::gap::DetectionLimit;
use crate::limits::VenueBudget;
use crate::sequence::{SeqState, SeqVerdict, Unverifiable};
use crate::symbols::{SymbolMapper, mappers};
use crate::types::{BookLevel, Side, Symbol, VenueId, VenueSymbol};
use crate::venue::{
    ControlEvent, ControlKind, DecimalEncoding, FeedDepth, FeedEvent, FeedTransport, HttpFetch,
    Keepalive, OrderSnapshot, RawFrame, RedundantDeletes, SequenceScope, SnapshotSource,
    ValidationScheme, ValidationTiming, Venue, VenueCapabilities, VenueTimestamps,
};

const WS_URL: &str = "wss://ws.bitstamp.net";
const REST_BOOK: &str = "https://www.bitstamp.net/api/v2/order_book";

pub struct Bitstamp {
    caps: VenueCapabilities,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
    ws_url: String,
    symbols: SymbolMapper,
    book_level: BookLevel,
    seed_from_rest: bool,
}

impl Bitstamp {
    pub fn new(http: Arc<dyn HttpFetch>, clock: Arc<dyn Clock>) -> Self {
        Bitstamp {
            caps: Self::capabilities(),
            http,
            clock,
            ws_url: WS_URL.to_string(),
            symbols: mappers::bitstamp(),
            book_level: BookLevel::L2,
            seed_from_rest: false,
        }
    }

    /// Record order by order instead of by price level.
    ///
    /// Worth doing on this venue specifically. Its aggregated feed carries no
    /// sequence number, update id, or checksum, so loss on it is undetectable.
    /// Its order-by-order feed chains every event to its predecessor, so loss
    /// is caught exactly. The same venue is unverifiable at L2 and fully
    /// chained at L3.
    pub fn with_book_level(mut self, level: BookLevel) -> Self {
        self.book_level = level;
        self.caps = Self::capabilities_for(level);
        self
    }

    pub fn book_level(&self) -> BookLevel {
        self.book_level
    }

    /// Seed the order book from the REST snapshot. **Off by default, and the
    /// measurements are why.**
    ///
    /// Bitstamp's REST book contains orders its stream never retracts, the same
    /// defect its aggregated book has. Seeding a 25 second L3 recording from it
    /// left the book crossed on 6,097 of 6,596 events; starting from empty gave
    /// a single transient crossing in 500. Re-seeding on each crossing made it
    /// worse, because every new snapshot plants the phantoms again.
    ///
    /// The cost of not seeding is that orders predating the recording are
    /// invisible. That is counted, and the levels they rest on are marked so no
    /// queue position there claims to be observed. An incomplete book that says
    /// so beats a complete-looking one that is wrong.
    pub fn with_order_seed(mut self, seed: bool) -> Self {
        self.seed_from_rest = seed;
        self
    }

    fn live_orders_channel(&self, symbol: &Symbol) -> String {
        format!("live_orders_{}", self.venue_symbol(symbol))
    }

    /// Parse a Bitstamp event id into the token the chain is checked on.
    ///
    /// `000659d4-cba2-b200-0000-000101000020` is 32 hex digits once the dashes
    /// come out, which is exactly a `u128`. Truncating it to something narrower
    /// would risk two different events comparing equal.
    fn parse_event_token(raw: &str) -> Option<u128> {
        let hex: String = raw.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        (hex.len() == 32).then(|| u128::from_str_radix(&hex, 16).ok())?
    }

    /// Decide what an event did to an order.
    ///
    /// Bitstamp only says created, changed, or deleted. Whether a deletion was
    /// an execution or a cancellation has to be read out of `amount_traded`
    /// against `amount_at_create`, and that inference is recorded rather than
    /// hidden: see `size_change_explained` on the event.
    fn classify(event: &str, order: &LiveOrder) -> Option<OrderAction> {
        match event {
            "order_created" => Some(OrderAction::Add),
            "order_changed" => Some(OrderAction::Modify),
            "order_deleted" => {
                let fully_traded = order.amount_at_create.is_positive()
                    && order.amount_traded >= order.amount_at_create;
                Some(if fully_traded {
                    OrderAction::Execute
                } else {
                    OrderAction::Cancel
                })
            }
            _ => None,
        }
    }

    pub fn with_symbols(mut self, symbols: &[Symbol]) -> Self {
        self.symbols.register_all(symbols);
        self
    }

    pub fn with_ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = url.into();
        self
    }

    fn channel(&self, symbol: &Symbol) -> String {
        format!("diff_order_book_{}", self.venue_symbol(symbol))
    }

    /// Recover the symbol from a channel name like `diff_order_book_btcusd`.
    fn symbol_from_channel(&self, channel: &str) -> Result<Symbol> {
        let raw = channel
            .strip_prefix("diff_order_book_")
            .or_else(|| channel.strip_prefix("live_orders_"))
            .or_else(|| channel.strip_prefix("order_book_"))
            .ok_or_else(|| Error::UnknownSymbol {
                venue: VenueId::Bitstamp,
                symbol: channel.to_string(),
            })?;
        self.canonical_symbol(raw)
    }

    fn capabilities() -> VenueCapabilities {
        Self::capabilities_for(BookLevel::L2)
    }

    fn capabilities_for(level: BookLevel) -> VenueCapabilities {
        if level == BookLevel::L3 {
            return Self::l3_capabilities();
        }
        VenueCapabilities {
            id: VenueId::Bitstamp,
            book_level: BookLevel::L2,
            validation: ValidationScheme::MonotonicTimestamp,
            scope: SequenceScope::PerSymbol,
            timing: ValidationTiming::BeforeApply,
            snapshot_source: SnapshotSource::Rest,
            feed_depth: FeedDepth::Full,
            decimals: DecimalEncoding::Strings,
            // Counted and published, but not acted on.
            //
            // This was the other way round first, on the reasoning that an
            // orphan delete is the only loss signal Bitstamp leaves us and so
            // ought to be worth acting on. Measurement said otherwise. From a
            // single snapshot the book stays consistent across 327 diffs with
            // three orphan deletes, but tearing the book down on the first one
            // means splicing a REST snapshot that runs a second behind the
            // stream, and each splice is another chance to go wrong. Acting on
            // the signal took runs from 98% clean to 0% clean, with up to 58
            // re-snapshots in twenty seconds.
            //
            // So the count is published as the weak health signal it is, and
            // the book is left alone.
            redundant_deletes: RedundantDeletes::Expected,
            timestamps: VenueTimestamps::PerMessage,
            keepalive: Keepalive::ProtocolPings,
            budget: VenueBudget {
                subscriptions_per_connection: 20,
                max_connections: 4,
                connect_interval: std::time::Duration::from_secs(1),
                connect_burst: 2,
                rest_interval: std::time::Duration::from_millis(500),
                rest_burst: 2,
            },
            requires_auth: false,
            detection_limits: vec![
                DetectionLimit {
                    venue: VenueId::Bitstamp,
                    scope: "always".to_string(),
                    consequence:
                        "the feed carries no sequence number, update id, or checksum, so a lost \
                         message is undetectable by any means; every book message is reported \
                         unverifiable and no Bitstamp window is ever claimed as validated"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::Bitstamp,
                    scope: "a book built from one REST snapshot".to_string(),
                    consequence:
                        "the REST book contains levels the diff stream never retracts. Measured: \
                         starting from one snapshot and applying its own diffs, the book crosses \
                         by the fifteenth message and never heals. The crossed-book check \
                         catches it and rebuilds, which is why this venue re-snapshots a couple \
                         of times a minute; a consumer that trusted a single snapshot would hold \
                         a crossed book all day without noticing"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::Bitstamp,
                    scope: "deletes for levels we never held".to_string(),
                    consequence:
                        "counted and published as this venue's only weak health signal, but not \
                         acted on: rebuilding the book on each one measurably degraded the \
                         capture, because the REST snapshot runs about a second behind the \
                         stream and every splice is another chance to go wrong"
                            .to_string(),
                },
            ],
        }
    }

    /// What the order-by-order feed can prove, which is a great deal more than
    /// the aggregated one.
    fn l3_capabilities() -> VenueCapabilities {
        VenueCapabilities {
            id: VenueId::Bitstamp,
            book_level: BookLevel::L3,
            // Every event names its predecessor. Measured over 3,999 live
            // events: not one break.
            validation: ValidationScheme::Chained,
            scope: SequenceScope::PerSymbol,
            timing: ValidationTiming::BeforeApply,
            // No usable snapshot: see `with_order_seed` for the measurements.
            snapshot_source: SnapshotSource::None,
            feed_depth: FeedDepth::Full,
            decimals: DecimalEncoding::Strings,
            redundant_deletes: RedundantDeletes::Expected,
            timestamps: VenueTimestamps::PerMessage,
            keepalive: Keepalive::ProtocolPings,
            budget: VenueBudget {
                subscriptions_per_connection: 10,
                max_connections: 4,
                connect_interval: std::time::Duration::from_secs(1),
                connect_burst: 2,
                rest_interval: std::time::Duration::from_millis(500),
                rest_burst: 2,
            },
            requires_auth: false,
            detection_limits: vec![
                DetectionLimit {
                    venue: VenueId::Bitstamp,
                    scope: "every gap".to_string(),
                    consequence:
                        "the event token chains exactly, so a break proves loss occurred, but \
                         the token is an identifier rather than a counter and can never say how \
                         many events it swallowed"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::Bitstamp,
                    scope: "orders that predate the recording".to_string(),
                    consequence:
                        "the feed carries no snapshot and the REST book holds orders the stream \
                         never retracts, so the book starts empty and fills in as orders churn. \
                         Levels known to hold an order we never watched arrive are marked, and \
                         no queue position on one of them is reported as observed"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::Bitstamp,
                    scope: "queue position for orders seeded from a snapshot".to_string(),
                    consequence:
                        "orders seeded from the REST book inherit the venue's listing order as \
                         their queue order. Measured: across 591 price levels holding more than \
                         one order, every one was listed in ascending order id, and order ids \
                         increase with time. Well evidenced is still not observed, so those \
                         positions are marked seeded rather than counted as known"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::Bitstamp,
                    scope: "execution versus cancellation".to_string(),
                    consequence:
                        "the feed reports created, changed, and deleted, so which of the last \
                         two a deletion was has to be inferred from amount_traded. On 19 of \
                         3,999 measured events the resting quantity moved by more than the \
                         reported trade explains, and those are flagged rather than guessed"
                            .to_string(),
                },
            ],
        }
    }

    fn levels(entries: &[Level], side: Side) -> Vec<LevelChange> {
        entries
            .iter()
            .map(|l| LevelChange::new(side, l.0, l.1))
            .collect()
    }

    /// Bitstamp sends microseconds since the epoch, as a string.
    fn micros_to_nanos(raw: Option<&String>) -> Option<i64> {
        raw?.parse::<i64>().ok()?.checked_mul(1_000)
    }
}

/// `["78709.26", "0.00000000"]`
#[derive(Debug, Deserialize)]
struct Level(Fixed, Fixed);

#[derive(Debug, Deserialize)]
struct Frame {
    event: Option<String>,
    channel: Option<String>,
    data: Option<serde_json::Value>,
    /// The chain, on the order-by-order feed.
    event_id: Option<String>,
    pre_event_id: Option<String>,
}

/// One order on the `live_orders` feed.
///
/// The string forms are used throughout. Bitstamp sends both a JSON number and
/// a string for price and amount, and only the string is exact.
#[derive(Debug, Deserialize)]
struct LiveOrder {
    id_str: String,
    /// 0 is a bid, 1 is an ask.
    order_type: u8,
    microtimestamp: Option<String>,
    amount_str: Fixed,
    #[serde(default)]
    amount_traded: Fixed,
    #[serde(default)]
    amount_at_create: Fixed,
    price_str: Fixed,
}

impl LiveOrder {
    fn side(&self) -> Side {
        if self.order_type == 0 {
            Side::Bid
        } else {
            Side::Ask
        }
    }

    /// Whether the venue's own numbers account for the resting quantity.
    ///
    /// Measured false on 19 of 3,999 events: the order shrank by more than the
    /// reported trade explains. When this is false, execution and cancellation
    /// are not distinguishable from the feed.
    fn size_change_explained(&self) -> bool {
        match self.amount_at_create.checked_sub(self.amount_traded) {
            Some(expected) => expected == self.amount_str,
            None => false,
        }
    }
}

/// `["79626.92", "0.49948900", "2043022972653571"]` from the group=2 book.
#[derive(Debug, Deserialize)]
struct SnapshotOrder(Fixed, Fixed, String);

#[derive(Debug, Deserialize)]
struct OrderBookL3 {
    microtimestamp: Option<String>,
    #[serde(default)]
    bids: Vec<SnapshotOrder>,
    #[serde(default)]
    asks: Vec<SnapshotOrder>,
}

#[derive(Debug, Deserialize)]
struct BookData {
    #[allow(dead_code)]
    timestamp: Option<String>,
    microtimestamp: Option<String>,
    #[serde(default)]
    bids: Vec<Level>,
    #[serde(default)]
    asks: Vec<Level>,
}

#[async_trait]
impl Venue for Bitstamp {
    fn capabilities(&self) -> &VenueCapabilities {
        &self.caps
    }

    fn venue_symbol(&self, symbol: &Symbol) -> VenueSymbol {
        self.symbols.to_venue(symbol)
    }

    fn canonical_symbol(&self, raw: &str) -> Result<Symbol> {
        self.symbols
            .to_canonical(raw)
            .map_err(|e| Error::Other(e.to_string()))
    }

    async fn connect(&self) -> Result<Box<dyn FeedTransport>> {
        Ok(Box::new(
            crate::transport::WsTransport::connect(
                VenueId::Bitstamp,
                &self.ws_url,
                Arc::clone(&self.clock),
            )
            .await?,
        ))
    }

    fn subscribe(&self, symbols: &[Symbol]) -> Result<Vec<String>> {
        if symbols.is_empty() {
            return Err(Error::Subscription {
                venue: VenueId::Bitstamp,
                reason: "no symbols requested".to_string(),
            });
        }
        // One frame per channel: Bitstamp takes a single channel per request.
        Ok(symbols
            .iter()
            .map(|s| {
                let channel = match self.book_level {
                    BookLevel::L3 => self.live_orders_channel(s),
                    BookLevel::L2 => self.channel(s),
                };
                serde_json::json!({
                    "event": "bts:subscribe",
                    "data": {"channel": channel},
                })
                .to_string()
            })
            .collect())
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot> {
        let url = format!("{REST_BOOK}/{}/", self.venue_symbol(symbol));
        let body = self.http.get(&url).await?;
        let parsed: BookData = serde_json::from_slice(&body)
            .map_err(|e| Error::protocol(VenueId::Bitstamp, format!("order_book: {e}"), &body))?;
        let micros = Bitstamp::micros_to_nanos(parsed.microtimestamp.as_ref());
        Ok(BookSnapshot {
            symbol: symbol.clone(),
            bids: parsed.bids.iter().map(|l| (l.0, l.1)).collect(),
            asks: parsed.asks.iter().map(|l| (l.0, l.1)).collect(),
            // There is no sequence number to anchor to; the timestamp is the
            // only thing tying the snapshot to the stream.
            seq: None,
            checksum: None,
            stamps: Timestamps::new(self.clock.stamp(), micros),
        })
    }

    async fn order_snapshot(&self, symbol: &Symbol) -> Result<Option<OrderSnapshot>> {
        if self.book_level != BookLevel::L3 || !self.seed_from_rest {
            // See `with_order_seed`: this venue's REST book holds orders its
            // stream never retracts, so seeding from it is worse than starting
            // empty.
            return Ok(None);
        }
        // `group=2` returns individual orders with their ids rather than
        // aggregated levels. Their order within a price level is taken to be
        // queue order; see the capability matrix for the evidence and the
        // caveat.
        let url = format!("{REST_BOOK}/{}/?group=2", self.venue_symbol(symbol));
        let body = self.http.get(&url).await?;
        let parsed: OrderBookL3 = serde_json::from_slice(&body).map_err(|e| {
            Error::protocol(VenueId::Bitstamp, format!("order_book group=2: {e}"), &body)
        })?;
        let mut orders = Vec::with_capacity(parsed.bids.len() + parsed.asks.len());
        for (side, entries) in [(Side::Bid, &parsed.bids), (Side::Ask, &parsed.asks)] {
            for entry in entries {
                orders.push((OrderId::new(entry.2.clone()), side, entry.0, entry.1));
            }
        }
        Ok(Some(OrderSnapshot {
            symbol: symbol.clone(),
            orders,
            stamps: Timestamps::new(
                self.clock.stamp(),
                Bitstamp::micros_to_nanos(parsed.microtimestamp.as_ref()),
            ),
        }))
    }

    fn parse_delta(&self, frame: &RawFrame) -> Result<Vec<FeedEvent>> {
        let parsed: Frame = serde_json::from_slice(&frame.payload)
            .map_err(|e| Error::protocol(VenueId::Bitstamp, e.to_string(), &frame.payload))?;

        let event = parsed.event.as_deref().unwrap_or("");

        // The order-by-order feed names its event types directly rather than
        // calling everything "data".
        if let Some(action) = ["order_created", "order_changed", "order_deleted"]
            .contains(&event)
            .then_some(event)
        {
            let channel = parsed.channel.as_deref().unwrap_or_default();
            let symbol = self.symbol_from_channel(channel)?;
            let value = parsed.data.ok_or_else(|| {
                Error::protocol(
                    VenueId::Bitstamp,
                    "order event without data",
                    &frame.payload,
                )
            })?;
            let order: LiveOrder = serde_json::from_value(value).map_err(|e| {
                Error::protocol(
                    VenueId::Bitstamp,
                    format!("live order: {e}"),
                    &frame.payload,
                )
            })?;
            let Some(kind) = Bitstamp::classify(action, &order) else {
                return Err(Error::protocol(
                    VenueId::Bitstamp,
                    format!("unknown order event {action:?}"),
                    &frame.payload,
                ));
            };
            let micros = Bitstamp::micros_to_nanos(order.microtimestamp.as_ref());
            return Ok(vec![FeedEvent::Order(Box::new(OrderEvent {
                symbol,
                order_id: OrderId::new(order.id_str.clone()),
                action: kind,
                side: order.side(),
                price: order.price_str,
                qty: if kind.removes() {
                    // A removed order rests for nothing, whatever the last
                    // reported amount was.
                    Fixed::ZERO
                } else {
                    order.amount_str
                },
                original_qty: Some(order.amount_at_create),
                executed_qty: Some(order.amount_traded),
                stamps: Timestamps::new(frame.stamp, micros),
                event_token: parsed
                    .event_id
                    .as_deref()
                    .and_then(Bitstamp::parse_event_token),
                prev_event_token: parsed
                    .pre_event_id
                    .as_deref()
                    .and_then(Bitstamp::parse_event_token),
                size_change_explained: order.size_change_explained(),
            }))]);
        }

        if event != "data" {
            let kind = match event {
                "bts:subscription_succeeded" => ControlKind::SubscriptionAck,
                "bts:error" => ControlKind::SubscriptionError,
                // The venue asking us to move before it drops the socket.
                // Honouring this turns an outage into a controlled resync.
                "bts:request_reconnect" => ControlKind::ReconnectRequested,
                _ => ControlKind::Status,
            };
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind,
                detail: event.to_string(),
                stamp: frame.stamp,
                seq: None,
            })]);
        }

        let Some(channel) = parsed.channel.as_deref() else {
            return Err(Error::protocol(
                VenueId::Bitstamp,
                "data frame without a channel",
                &frame.payload,
            ));
        };
        let symbol = self.symbol_from_channel(channel)?;
        let value = parsed.data.ok_or_else(|| {
            Error::protocol(VenueId::Bitstamp, "data frame without data", &frame.payload)
        })?;
        let data: BookData = serde_json::from_value(value).map_err(|e| {
            Error::protocol(VenueId::Bitstamp, format!("book data: {e}"), &frame.payload)
        })?;

        let micros = Bitstamp::micros_to_nanos(data.microtimestamp.as_ref());
        let mut changes = Bitstamp::levels(&data.bids, Side::Bid);
        changes.extend(Bitstamp::levels(&data.asks, Side::Ask));
        Ok(vec![FeedEvent::Delta(BookDelta {
            symbol,
            changes,
            seq: None,
            checksum: None,
            stamps: Timestamps::new(frame.stamp, micros),
            prev_seq: None,
            first_seq: None,
        })])
    }

    fn validate_sequence(
        &self,
        state: &mut SeqState,
        event: &FeedEvent,
        _book: &L2Book,
    ) -> SeqVerdict {
        // The order-by-order feed chains; the aggregated one carries nothing.
        // Same venue, opposite situations.
        if let SeqState::Chain(chain) = state {
            return match event {
                FeedEvent::Order(order) => match order.event_token {
                    Some(token) => chain.observe(token, order.prev_event_token),
                    None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
                },
                _ => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            };
        }
        let SeqState::Timestamp(timestamps) = state else {
            return SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence);
        };
        match event {
            // A snapshot sets the floor: everything stamped at or before it is
            // already inside the snapshot and must be discarded rather than
            // applied on top.
            FeedEvent::Snapshot(s) => match s.stamps.venue_nanos {
                Some(nanos) => {
                    timestamps.anchor_snapshot(nanos);
                    SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence)
                }
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            FeedEvent::Delta(d) => match d.stamps.venue_nanos {
                // Note this returns Unverifiable for a well-ordered message,
                // never InOrder. Ordering is not evidence of completeness.
                Some(nanos) => timestamps.observe(nanos),
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            // This venue publishes no order-by-order feed, so it never
            // produces one of these.
            FeedEvent::Order(_) | FeedEvent::Control(_) => {
                SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck)
            }
        }
    }
}
