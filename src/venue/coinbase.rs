//! Coinbase, via the Advanced Trade market data feed.
//!
//! Which Coinbase API this targets is a decision, not an accident. Coinbase
//! Exchange (`ws-feed.exchange.coinbase.com`) now answers a subscription to
//! `full`, `level2`, or `level3` with:
//!
//! ```text
//! {"type":"error","message":"Failed to subscribe",
//!  "reason":"level2, level3, and full channels now require authentication."}
//! ```
//!
//! and its one remaining keyless book channel, `level2_batch`, sends neither a
//! sequence number nor a checksum. A recorder built on it could not detect loss
//! at all, and a dataset built from it could not honestly publish a gap report.
//!
//! `advanced-trade-ws.coinbase.com` is keyless, sends the full book, and stamps
//! every message with a monotonic `sequence_num`. The catch, and it shapes the
//! session loop: that counter belongs to the **connection**, not the product.
//! One gap invalidates every symbol on the socket at once.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::book::{BookDelta, BookSnapshot, L2Book, LevelChange};
use crate::clock::{Clock, Timestamps, parse_rfc3339_nanos};
use crate::error::{Error, Result};
use crate::fixed::Fixed;
use crate::gap::DetectionLimit;
use crate::limits::VenueBudget;
use crate::sequence::{SeqState, SeqVerdict, Unverifiable};
use crate::symbols::{SymbolMapper, mappers};
use crate::types::{BookLevel, Side, Symbol, VenueId, VenueSymbol};
use crate::venue::{
    ControlEvent, ControlKind, DecimalEncoding, FeedDepth, FeedEvent, FeedTransport, HttpFetch,
    Keepalive, RawFrame, RedundantDeletes, SequenceScope, SnapshotSource, ValidationScheme,
    ValidationTiming, Venue, VenueCapabilities, VenueTimestamps,
};

const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const REST_BOOK: &str = "https://api.coinbase.com/api/v3/brokerage/market/product_book";

/// The Coinbase Advanced Trade level2 feed.
pub struct Coinbase {
    caps: VenueCapabilities,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
    ws_url: String,
    rest_depth: usize,
    symbols: SymbolMapper,
}

impl Coinbase {
    pub fn new(http: Arc<dyn HttpFetch>, clock: Arc<dyn Clock>) -> Self {
        Coinbase {
            caps: Self::capabilities(),
            http,
            clock,
            ws_url: WS_URL.to_string(),
            rest_depth: 500,
            symbols: mappers::coinbase(),
        }
    }

    /// Teach the symbol mapper the exact pairs we will record.
    pub fn with_symbols(mut self, symbols: &[Symbol]) -> Self {
        self.symbols.register_all(symbols);
        self
    }

    /// Point at a different websocket, for tests against a local server.
    pub fn with_ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = url.into();
        self
    }

    pub fn with_rest_depth(mut self, depth: usize) -> Self {
        self.rest_depth = depth;
        self
    }

    fn capabilities() -> VenueCapabilities {
        VenueCapabilities {
            id: VenueId::Coinbase,
            book_level: BookLevel::L2,
            // Every message is numbered, so every loss is detectable and its
            // size is exactly known.
            validation: ValidationScheme::Counter {
                step: 1,
                restart_floor: 0,
            },
            scope: SequenceScope::PerConnection,
            timing: ValidationTiming::BeforeApply,
            snapshot_source: SnapshotSource::InBand,
            feed_depth: FeedDepth::Full,
            decimals: DecimalEncoding::Strings,
            // Measured, not assumed. In a 30-second capture Coinbase sent 210
            // deletes for prices that appeared neither in its own 44,067-level
            // snapshot nor in any earlier update. Treating those as corruption
            // made the recorder re-snapshot every two seconds, pulling a 4.8 MB
            // book each time and reporting a healthy feed as 2% clean.
            redundant_deletes: RedundantDeletes::Expected,
            timestamps: VenueTimestamps::PerMessageAndPerLevel,
            keepalive: Keepalive::ProtocolPings,
            budget: VenueBudget {
                // Coinbase throttles subscribe messages per IP, and its counter
                // is connection-wide, so a small per-socket count is both
                // polite and a smaller blast radius.
                subscriptions_per_connection: 8,
                max_connections: 6,
                connect_interval: std::time::Duration::from_secs(1),
                connect_burst: 2,
                rest_interval: std::time::Duration::from_millis(200),
                rest_burst: 5,
            },
            requires_auth: false,
            detection_limits: vec![
                DetectionLimit {
                    venue: VenueId::Coinbase,
                    scope: "more than one product on a connection".to_string(),
                    consequence:
                        "sequence_num numbers the connection, not the product, so a gap cannot \
                         be attributed to one symbol and every symbol on that socket is marked \
                         suspect"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::Coinbase,
                    scope: "always".to_string(),
                    consequence:
                        "the venue deletes price levels it never published, so an absent-level \
                         delete carries no information about loss here and is counted rather \
                         than acted on"
                            .to_string(),
                },
            ],
        }
    }

    fn side_from(raw: &str, payload: &[u8]) -> Result<Side> {
        match raw {
            "bid" => Ok(Side::Bid),
            // Coinbase says "offer", not "ask" or "sell".
            "offer" => Ok(Side::Ask),
            other => Err(Error::protocol(
                VenueId::Coinbase,
                format!("unknown side {other:?}"),
                payload,
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Envelope {
    channel: Option<String>,
    timestamp: Option<String>,
    sequence_num: Option<u64>,
    /// Left unparsed until the channel is known. The `subscriptions` channel
    /// puts a completely different object in here, and a struct that insisted
    /// on the `l2_data` shape would reject every acknowledgement the venue
    /// sends.
    #[serde(default)]
    events: Vec<serde_json::Value>,
    #[serde(rename = "type")]
    kind: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Event {
    #[serde(rename = "type")]
    kind: String,
    product_id: Option<String>,
    #[serde(default)]
    updates: Vec<Update>,
}

#[derive(Debug, Deserialize)]
struct Update {
    side: String,
    event_time: Option<String>,
    price_level: Fixed,
    new_quantity: Fixed,
}

#[derive(Debug, Deserialize)]
struct PriceBookResponse {
    pricebook: PriceBook,
}

#[derive(Debug, Deserialize)]
struct PriceBook {
    product_id: String,
    #[serde(default)]
    bids: Vec<PriceBookLevel>,
    #[serde(default)]
    asks: Vec<PriceBookLevel>,
    time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PriceBookLevel {
    price: Fixed,
    size: Fixed,
}

#[async_trait]
impl Venue for Coinbase {
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
        let transport = crate::transport::WsTransport::connect(
            VenueId::Coinbase,
            &self.ws_url,
            Arc::clone(&self.clock),
        )
        .await?;
        Ok(Box::new(transport))
    }

    fn subscribe(&self, symbols: &[Symbol]) -> Result<Vec<String>> {
        if symbols.is_empty() {
            return Err(Error::Subscription {
                venue: VenueId::Coinbase,
                reason: "no symbols requested".to_string(),
            });
        }
        let products: Vec<String> = symbols.iter().map(|s| self.venue_symbol(s).0).collect();
        Ok(vec![
            serde_json::json!({
                "type": "subscribe",
                "product_ids": products,
                "channel": "level2",
            })
            .to_string(),
        ])
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot> {
        let url = format!(
            "{REST_BOOK}?product_id={}&limit={}",
            self.venue_symbol(symbol),
            self.rest_depth
        );
        let body = self.http.get(&url).await?;
        let parsed: PriceBookResponse = serde_json::from_slice(&body)
            .map_err(|e| Error::protocol(VenueId::Coinbase, format!("product_book: {e}"), &body))?;

        let returned = self.canonical_symbol(&parsed.pricebook.product_id)?;
        if returned != *symbol {
            return Err(Error::protocol(
                VenueId::Coinbase,
                format!("asked for {symbol}, got {returned}"),
                &body,
            ));
        }

        let venue_nanos = parsed
            .pricebook
            .time
            .as_deref()
            .and_then(parse_rfc3339_nanos);
        Ok(BookSnapshot {
            symbol: symbol.clone(),
            bids: parsed
                .pricebook
                .bids
                .iter()
                .map(|l| (l.price, l.size))
                .collect(),
            asks: parsed
                .pricebook
                .asks
                .iter()
                .map(|l| (l.price, l.size))
                .collect(),
            // The REST book carries no sequence number, so it cannot be spliced
            // into the socket stream. It is a cross-check, not a resync point.
            seq: None,
            checksum: None,
            stamps: Timestamps::new(self.clock.stamp(), venue_nanos),
        })
    }

    fn parse_delta(&self, frame: &RawFrame) -> Result<Vec<FeedEvent>> {
        let envelope: Envelope = serde_json::from_slice(&frame.payload)
            .map_err(|e| Error::protocol(VenueId::Coinbase, e.to_string(), &frame.payload))?;

        if envelope.kind.as_deref() == Some("error") {
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind: ControlKind::SubscriptionError,
                detail: envelope
                    .message
                    .unwrap_or_else(|| "unspecified".to_string()),
                stamp: frame.stamp,
                seq: envelope.sequence_num,
            })]);
        }

        let channel = envelope.channel.as_deref().unwrap_or("");
        if channel != "l2_data" {
            let kind = match channel {
                "subscriptions" => ControlKind::SubscriptionAck,
                "heartbeats" => ControlKind::Heartbeat,
                "" => ControlKind::Ignored,
                _ => ControlKind::Status,
            };
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind,
                detail: channel.to_string(),
                stamp: frame.stamp,
                seq: envelope.sequence_num,
            })]);
        }

        let envelope_nanos = envelope.timestamp.as_deref().and_then(parse_rfc3339_nanos);
        let stamps = Timestamps::new(frame.stamp, envelope_nanos);
        let mut out = Vec::with_capacity(envelope.events.len());

        for value in envelope.events {
            let event: Event = serde_json::from_value(value).map_err(|e| {
                Error::protocol(
                    VenueId::Coinbase,
                    format!("l2_data event: {e}"),
                    &frame.payload,
                )
            })?;
            let Some(product) = event.product_id.as_deref() else {
                return Err(Error::protocol(
                    VenueId::Coinbase,
                    "l2_data event without product_id",
                    &frame.payload,
                ));
            };
            let symbol = self.canonical_symbol(product)?;

            match event.kind.as_str() {
                "snapshot" => {
                    let mut bids = Vec::new();
                    let mut asks = Vec::new();
                    for u in &event.updates {
                        match Coinbase::side_from(&u.side, &frame.payload)? {
                            Side::Bid => bids.push((u.price_level, u.new_quantity)),
                            Side::Ask => asks.push((u.price_level, u.new_quantity)),
                        }
                    }
                    out.push(FeedEvent::Snapshot(BookSnapshot {
                        symbol,
                        bids,
                        asks,
                        seq: envelope.sequence_num,
                        checksum: None,
                        stamps,
                    }));
                }
                "update" => {
                    let mut changes = Vec::with_capacity(event.updates.len());
                    for u in &event.updates {
                        changes.push(LevelChange::new(
                            Coinbase::side_from(&u.side, &frame.payload)?,
                            u.price_level,
                            u.new_quantity,
                        ));
                    }
                    // Per-level event_time is finer than the envelope's, and the
                    // gap between them is how much the venue batched.
                    let level_nanos = event
                        .updates
                        .first()
                        .and_then(|u| u.event_time.as_deref())
                        .and_then(parse_rfc3339_nanos);
                    out.push(FeedEvent::Delta(BookDelta {
                        symbol,
                        changes,
                        seq: envelope.sequence_num,
                        checksum: None,
                        stamps: Timestamps::new(frame.stamp, level_nanos.or(envelope_nanos)),
                        prev_seq: None,
                        first_seq: None,
                    }));
                }
                other => {
                    return Err(Error::protocol(
                        VenueId::Coinbase,
                        format!("unknown l2_data event type {other:?}"),
                        &frame.payload,
                    ));
                }
            }
        }
        Ok(out)
    }

    fn validate_sequence(
        &self,
        state: &mut SeqState,
        event: &FeedEvent,
        _book: &L2Book,
    ) -> SeqVerdict {
        let SeqState::Counter(counter) = state else {
            return SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence);
        };
        match event {
            // A snapshot is authoritative: it defines the new baseline rather
            // than being measured against the old one.
            FeedEvent::Snapshot(s) => match s.seq {
                Some(seq) => {
                    counter.anchor(seq);
                    SeqVerdict::InOrder
                }
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            FeedEvent::Delta(d) => match d.seq {
                Some(seq) => counter.observe(seq),
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            // Control frames share the connection's counter, so they must be
            // observed too. Skipping them is indistinguishable from losing them.
            FeedEvent::Control(c) => match c.seq {
                Some(seq) => counter.observe(seq),
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            // Coinbase's order-by-order channel requires authentication, so
            // this venue never produces one.
            FeedEvent::Order(_) => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
        }
    }
}
