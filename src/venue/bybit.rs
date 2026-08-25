//! Bybit v5 spot.
//!
//! A per-symbol counter, which after Coinbase's connection-wide one is a useful
//! contrast: here a gap really does belong to a single instrument, so symbols
//! can be packed onto a socket without widening the blast radius.
//!
//! One Bybit-specific rule shapes the validator. Its `u` normally increments by
//! one per message for a topic, but the venue resets it to `1` when the service
//! restarts, and that message is a fresh snapshot rather than a lost-message
//! event. A validator that only knew "numbers should go up" would report a
//! catastrophic backwards jump every time Bybit restarted.
//!
//! Its REST book is unavailable from some regions. From a US address it answers
//! `The Amazon CloudFront distribution is configured to block access from your
//! country` while the websocket connects and streams normally. Recording is
//! therefore unaffected, because the snapshot arrives in band, but the REST
//! cross-check is not available everywhere and the capability matrix says so.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

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
    Keepalive, RawFrame, RedundantDeletes, SequenceScope, SnapshotSource, ValidationScheme,
    ValidationTiming, Venue, VenueCapabilities, VenueTimestamps,
};

const WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const REST_BOOK: &str = "https://api.bybit.com/v5/market/orderbook";

/// Depths the spot orderbook topic offers.
pub const VALID_DEPTHS: &[usize] = &[1, 50, 200];

/// Bybit renumbers from here when its service restarts, so a `u` of one after a
/// larger one is a restart rather than a backwards jump.
const RESTART_FLOOR: u64 = 1;

pub struct Bybit {
    caps: VenueCapabilities,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
    ws_url: String,
    depth: usize,
    symbols: SymbolMapper,
}

impl Bybit {
    pub fn new(http: Arc<dyn HttpFetch>, clock: Arc<dyn Clock>, depth: usize) -> Result<Self> {
        if !VALID_DEPTHS.contains(&depth) {
            return Err(Error::Other(format!(
                "bybit spot depth must be one of {VALID_DEPTHS:?}, got {depth}"
            )));
        }
        Ok(Bybit {
            caps: Self::capabilities(depth),
            http,
            clock,
            ws_url: WS_URL.to_string(),
            depth,
            symbols: mappers::bybit(),
        })
    }

    pub fn with_symbols(mut self, symbols: &[Symbol]) -> Self {
        self.symbols.register_all(symbols);
        self
    }

    pub fn with_ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = url.into();
        self
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    fn topic(&self, symbol: &Symbol) -> String {
        format!("orderbook.{}.{}", self.depth, self.venue_symbol(symbol))
    }

    fn capabilities(depth: usize) -> VenueCapabilities {
        VenueCapabilities {
            id: VenueId::Bybit,
            book_level: BookLevel::L2,
            validation: ValidationScheme::Counter {
                step: 1,
                restart_floor: RESTART_FLOOR,
            },
            scope: SequenceScope::PerSymbol,
            timing: ValidationTiming::BeforeApply,
            snapshot_source: SnapshotSource::InBand,
            feed_depth: FeedDepth::Limited(depth),
            decimals: DecimalEncoding::Strings,
            // Measured over 398 live updates at depth 50 with client-side
            // truncation: not one delete referred to a level we were not
            // holding, so an orphan delete here is real evidence.
            redundant_deletes: RedundantDeletes::Never,
            timestamps: VenueTimestamps::PerMessage,
            keepalive: Keepalive::Text {
                payload: r#"{"op":"ping"}"#,
                every: std::time::Duration::from_secs(20),
            },
            budget: VenueBudget {
                // Bybit caps the args in one subscribe request; several
                // requests per socket are fine, so this is per request.
                subscriptions_per_connection: 10,
                max_connections: 6,
                connect_interval: std::time::Duration::from_secs(1),
                connect_burst: 3,
                rest_interval: std::time::Duration::from_millis(200),
                rest_burst: 5,
            },
            requires_auth: false,
            detection_limits: vec![DetectionLimit {
                venue: VenueId::Bybit,
                scope: "the REST cross-check, in some regions".to_string(),
                consequence:
                    "the REST book is geo-blocked from a US address while the websocket streams \
                     normally, so recording works but the out-of-band snapshot comparison is \
                     unavailable there"
                        .to_string(),
            }],
        }
    }

    fn levels(entries: &[Level], side: Side) -> Vec<LevelChange> {
        entries
            .iter()
            .map(|l| LevelChange::new(side, l.0, l.1))
            .collect()
    }
}

/// `["78759.70", "0.083247"]`
#[derive(Debug, Deserialize)]
struct Level(Fixed, Fixed);

#[derive(Debug, Deserialize)]
struct Frame {
    topic: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    ts: Option<i64>,
    data: Option<BookData>,
    // Subscription and ping replies use a different shape.
    op: Option<String>,
    success: Option<bool>,
    ret_msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BookData {
    s: String,
    #[serde(default)]
    b: Vec<Level>,
    #[serde(default)]
    a: Vec<Level>,
    u: Option<u64>,
    #[allow(dead_code)]
    seq: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RestResponse {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: Option<String>,
    result: Option<RestBook>,
}

#[derive(Debug, Deserialize)]
struct RestBook {
    #[serde(default)]
    b: Vec<Level>,
    #[serde(default)]
    a: Vec<Level>,
    ts: Option<i64>,
    u: Option<u64>,
}

#[async_trait]
impl Venue for Bybit {
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
                VenueId::Bybit,
                &self.ws_url,
                Arc::clone(&self.clock),
            )
            .await?,
        ))
    }

    fn subscribe(&self, symbols: &[Symbol]) -> Result<Vec<String>> {
        if symbols.is_empty() {
            return Err(Error::Subscription {
                venue: VenueId::Bybit,
                reason: "no symbols requested".to_string(),
            });
        }
        let args: Vec<String> = symbols.iter().map(|s| self.topic(s)).collect();
        Ok(vec![
            serde_json::json!({"op": "subscribe", "args": args}).to_string(),
        ])
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot> {
        let url = format!(
            "{REST_BOOK}?category=spot&symbol={}&limit={}",
            self.venue_symbol(symbol),
            self.depth
        );
        let body = self.http.get(&url).await?;
        let parsed: RestResponse = serde_json::from_slice(&body).map_err(|e| {
            Error::protocol(
                VenueId::Bybit,
                format!("orderbook: {e} (this endpoint is geo-blocked in some regions)"),
                &body,
            )
        })?;
        if parsed.ret_code != 0 {
            return Err(Error::protocol(
                VenueId::Bybit,
                format!(
                    "orderbook returned {} ({})",
                    parsed.ret_code,
                    parsed.ret_msg.unwrap_or_default()
                ),
                &body,
            ));
        }
        let book = parsed
            .result
            .ok_or_else(|| Error::protocol(VenueId::Bybit, "orderbook had no result", &body))?;
        Ok(BookSnapshot {
            symbol: symbol.clone(),
            bids: book.b.iter().map(|l| (l.0, l.1)).collect(),
            asks: book.a.iter().map(|l| (l.0, l.1)).collect(),
            seq: book.u,
            checksum: None,
            stamps: Timestamps::new(
                self.clock.stamp(),
                book.ts.and_then(|t| t.checked_mul(1_000_000)),
            ),
        })
    }

    fn parse_delta(&self, frame: &RawFrame) -> Result<Vec<FeedEvent>> {
        let parsed: Frame = serde_json::from_slice(&frame.payload)
            .map_err(|e| Error::protocol(VenueId::Bybit, e.to_string(), &frame.payload))?;

        if let Some(op) = parsed.op.as_deref() {
            let ok = parsed.success.unwrap_or(false);
            let kind = match (op, ok) {
                ("ping" | "pong", _) => ControlKind::Heartbeat,
                (_, true) => ControlKind::SubscriptionAck,
                (_, false) => ControlKind::SubscriptionError,
            };
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind,
                detail: parsed.ret_msg.unwrap_or_else(|| op.to_string()),
                stamp: frame.stamp,
                seq: None,
            })]);
        }

        let (Some(topic), Some(data)) = (parsed.topic.as_deref(), parsed.data.as_ref()) else {
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind: ControlKind::Ignored,
                detail: "frame without a book topic".to_string(),
                stamp: frame.stamp,
                seq: None,
            })]);
        };
        let _ = topic;

        let symbol = self.canonical_symbol(&data.s)?;
        let stamps = Timestamps::new(
            frame.stamp,
            parsed.ts.and_then(|t| t.checked_mul(1_000_000)),
        );

        let event = match parsed.kind.as_deref() {
            Some("snapshot") => FeedEvent::Snapshot(BookSnapshot {
                symbol,
                bids: data.b.iter().map(|l| (l.0, l.1)).collect(),
                asks: data.a.iter().map(|l| (l.0, l.1)).collect(),
                seq: data.u,
                checksum: None,
                stamps,
            }),
            Some("delta") => {
                let mut changes = Bybit::levels(&data.b, Side::Bid);
                changes.extend(Bybit::levels(&data.a, Side::Ask));
                FeedEvent::Delta(BookDelta {
                    symbol,
                    changes,
                    seq: data.u,
                    checksum: None,
                    stamps,
                    prev_seq: None,
                    first_seq: None,
                })
            }
            other => {
                return Err(Error::protocol(
                    VenueId::Bybit,
                    format!("unknown orderbook message type {other:?}"),
                    &frame.payload,
                ));
            }
        };
        Ok(vec![event])
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
            // Bybit numbers only its book topics, so acks and pongs carry
            // nothing the counter should see.
            // This venue publishes no order-by-order feed, so it never
            // produces one of these.
            FeedEvent::Order(_) | FeedEvent::Control(_) => {
                SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck)
            }
        }
    }
}
