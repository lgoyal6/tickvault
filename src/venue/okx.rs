//! OKX v5 spot.
//!
//! The strongest continuity proof of the six venues, and the least informative
//! about magnitude. Every book message carries both `seqId` and `prevSeqId`, so
//! continuity is checked by *identity* rather than by arithmetic: a message is
//! in order when it names the one we last applied. Nothing has to be assumed
//! about step size, which matters because there is no step size to assume.
//! Consecutive ids observed live jumped by 7, 12, 13, 20, 45, and 93.
//!
//! The flip side is that a broken chain proves loss occurred and can say
//! nothing whatever about how much, so gaps here are recorded with
//! [`crate::sequence::LossSize::Unknown`] rather than a fabricated count.
//!
//! The `books` channel also publishes a `checksum` field, and it is **zero on
//! every frame**. Measured across 400 messages on BTC-USDT and again on
//! ETH-USDT: snapshot and update alike, always zero. It is therefore treated as
//! absent rather than as a check that always fails.

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

const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const REST_BOOKS: &str = "https://www.okx.com/api/v5/market/books";

/// The `books` channel carries 400 levels a side and maintains that depth
/// itself, sending the removal for any level leaving the window.
pub const CHANNEL_DEPTH: usize = 400;

pub struct Okx {
    caps: VenueCapabilities,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
    ws_url: String,
    symbols: SymbolMapper,
}

impl Okx {
    pub fn new(http: Arc<dyn HttpFetch>, clock: Arc<dyn Clock>) -> Self {
        Okx {
            caps: Self::capabilities(),
            http,
            clock,
            ws_url: WS_URL.to_string(),
            symbols: mappers::okx(),
        }
    }

    /// Teach the symbol mapper the exact pairs we will record, so inbound
    /// `instId` values resolve by lookup rather than by suffix guessing.
    pub fn with_symbols(mut self, symbols: &[Symbol]) -> Self {
        self.symbols.register_all(symbols);
        self
    }

    pub fn with_ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = url.into();
        self
    }

    fn capabilities() -> VenueCapabilities {
        VenueCapabilities {
            id: VenueId::Okx,
            book_level: BookLevel::L2,
            validation: ValidationScheme::Chained,
            scope: SequenceScope::PerSymbol,
            timing: ValidationTiming::BeforeApply,
            snapshot_source: SnapshotSource::InBand,
            feed_depth: FeedDepth::Limited(CHANNEL_DEPTH),
            decimals: DecimalEncoding::Strings,
            // Measured over 398 live updates: the venue sends the removal for
            // every level leaving the 400-deep window, so the book never
            // overflows and no delete ever orphans.
            redundant_deletes: RedundantDeletes::Never,
            timestamps: VenueTimestamps::PerMessage,
            // OKX closes an idle socket after 30 seconds, and a closed socket
            // is a gap in the dataset that the venue did not cause.
            keepalive: Keepalive::Text {
                payload: "ping",
                every: std::time::Duration::from_secs(20),
            },
            budget: VenueBudget {
                subscriptions_per_connection: 40,
                max_connections: 4,
                connect_interval: std::time::Duration::from_secs(1),
                connect_burst: 3,
                rest_interval: std::time::Duration::from_millis(100),
                rest_burst: 5,
            },
            requires_auth: false,
            detection_limits: vec![
                DetectionLimit {
                    venue: VenueId::Okx,
                    scope: "every gap".to_string(),
                    consequence:
                        "seqId is an identifier rather than a counter, so a broken chain proves \
                         loss occurred and can never say how many messages it swallowed"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::Okx,
                    scope: "the books channel checksum".to_string(),
                    consequence:
                        "the venue publishes a checksum field that is zero on every frame \
                         observed, so it is treated as absent; the chain is the only check"
                            .to_string(),
                },
            ],
        }
    }

    fn levels(entries: &[Level], side: Side) -> Vec<LevelChange> {
        entries
            .iter()
            .map(|l| LevelChange::new(side, l.price, l.qty))
            .collect()
    }

    /// OKX stamps messages in milliseconds, as a string.
    fn millis_to_nanos(raw: Option<&String>) -> Option<i64> {
        raw?.parse::<i64>().ok()?.checked_mul(1_000_000)
    }
}

/// `["78774.8", "0.49445787", "0", "12"]`: price, size, a deprecated field, and
/// the number of orders resting at the level. The order count is genuine L3
/// adjacent information and is parsed so the shape is validated, though nothing
/// consumes it until phase 4.
#[derive(Debug, Deserialize)]
struct Level {
    price: Fixed,
    qty: Fixed,
    #[allow(dead_code)]
    deprecated: String,
    #[allow(dead_code)]
    orders: String,
}

#[derive(Debug, Deserialize)]
struct Frame {
    event: Option<String>,
    msg: Option<String>,
    code: Option<String>,
    arg: Option<Arg>,
    action: Option<String>,
    #[serde(default)]
    data: Vec<BookData>,
}

#[derive(Debug, Deserialize)]
struct Arg {
    #[allow(dead_code)]
    channel: Option<String>,
    #[serde(rename = "instId")]
    inst_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BookData {
    #[serde(default)]
    bids: Vec<Level>,
    #[serde(default)]
    asks: Vec<Level>,
    ts: Option<String>,
    #[serde(rename = "seqId")]
    seq_id: Option<i64>,
    #[serde(rename = "prevSeqId")]
    prev_seq_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RestResponse {
    code: String,
    msg: Option<String>,
    #[serde(default)]
    data: Vec<BookData>,
}

#[async_trait]
impl Venue for Okx {
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
                VenueId::Okx,
                &self.ws_url,
                Arc::clone(&self.clock),
            )
            .await?,
        ))
    }

    fn subscribe(&self, symbols: &[Symbol]) -> Result<Vec<String>> {
        if symbols.is_empty() {
            return Err(Error::Subscription {
                venue: VenueId::Okx,
                reason: "no symbols requested".to_string(),
            });
        }
        let args: Vec<serde_json::Value> = symbols
            .iter()
            .map(|s| serde_json::json!({"channel": "books", "instId": self.venue_symbol(s).0}))
            .collect();
        Ok(vec![
            serde_json::json!({"op": "subscribe", "args": args}).to_string(),
        ])
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot> {
        let url = format!(
            "{REST_BOOKS}?instId={}&sz={CHANNEL_DEPTH}",
            self.venue_symbol(symbol)
        );
        let body = self.http.get(&url).await?;
        let parsed: RestResponse = serde_json::from_slice(&body)
            .map_err(|e| Error::protocol(VenueId::Okx, format!("books: {e}"), &body))?;
        if parsed.code != "0" {
            return Err(Error::protocol(
                VenueId::Okx,
                format!(
                    "books returned code {} ({})",
                    parsed.code,
                    parsed.msg.unwrap_or_default()
                ),
                &body,
            ));
        }
        let data = parsed
            .data
            .first()
            .ok_or_else(|| Error::protocol(VenueId::Okx, "books returned no levels", &body))?;
        Ok(BookSnapshot {
            symbol: symbol.clone(),
            bids: data.bids.iter().map(|l| (l.price, l.qty)).collect(),
            asks: data.asks.iter().map(|l| (l.price, l.qty)).collect(),
            seq: None,
            checksum: None,
            stamps: Timestamps::new(self.clock.stamp(), Okx::millis_to_nanos(data.ts.as_ref())),
        })
    }

    fn parse_delta(&self, frame: &RawFrame) -> Result<Vec<FeedEvent>> {
        // OKX answers our keepalive with a bare, unquoted `pong`, which is not
        // JSON and would otherwise be logged as an unparseable frame forever.
        let text = frame.text();
        if text.trim() == "pong" {
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind: ControlKind::Heartbeat,
                detail: "pong".to_string(),
                stamp: frame.stamp,
                seq: None,
            })]);
        }

        let parsed: Frame = serde_json::from_slice(&frame.payload)
            .map_err(|e| Error::protocol(VenueId::Okx, e.to_string(), &frame.payload))?;

        if let Some(event) = parsed.event.as_deref() {
            let kind = match event {
                "subscribe" => ControlKind::SubscriptionAck,
                "error" => ControlKind::SubscriptionError,
                _ => ControlKind::Status,
            };
            let detail = match (parsed.code.as_deref(), parsed.msg.as_deref()) {
                (Some(code), Some(msg)) => format!("{code}: {msg}"),
                _ => event.to_string(),
            };
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind,
                detail,
                stamp: frame.stamp,
                seq: None,
            })]);
        }

        let Some(inst_id) = parsed.arg.as_ref().and_then(|a| a.inst_id.as_deref()) else {
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind: ControlKind::Ignored,
                detail: "frame without instId".to_string(),
                stamp: frame.stamp,
                seq: None,
            })]);
        };
        let symbol = self.canonical_symbol(inst_id)?;

        let is_snapshot = match parsed.action.as_deref() {
            Some("snapshot") => true,
            Some("update") => false,
            other => {
                return Err(Error::protocol(
                    VenueId::Okx,
                    format!("unknown books action {other:?}"),
                    &frame.payload,
                ));
            }
        };

        let mut out = Vec::with_capacity(parsed.data.len());
        for data in &parsed.data {
            let stamps = Timestamps::new(frame.stamp, Okx::millis_to_nanos(data.ts.as_ref()));
            // A negative seqId is not an identifier; -1 in prevSeqId is the
            // venue saying "this begins a chain".
            let seq = data.seq_id.filter(|v| *v >= 0).map(|v| v as u64);
            if is_snapshot {
                out.push(FeedEvent::Snapshot(BookSnapshot {
                    symbol: symbol.clone(),
                    bids: data.bids.iter().map(|l| (l.price, l.qty)).collect(),
                    asks: data.asks.iter().map(|l| (l.price, l.qty)).collect(),
                    seq,
                    checksum: None,
                    stamps,
                }));
            } else {
                let mut changes = Okx::levels(&data.bids, Side::Bid);
                changes.extend(Okx::levels(&data.asks, Side::Ask));
                out.push(FeedEvent::Delta(BookDelta {
                    symbol: symbol.clone(),
                    changes,
                    seq,
                    // The chain's predecessor rides in `checksum` would be
                    // wrong; it is carried explicitly below instead.
                    checksum: None,
                    stamps,
                    prev_seq: data.prev_seq_id.filter(|v| *v >= 0).map(|v| v as u64),
                    first_seq: None,
                }));
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
        let SeqState::Chain(chain) = state else {
            return SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence);
        };
        match event {
            FeedEvent::Snapshot(s) => match s.seq {
                Some(seq) => {
                    // A snapshot always starts a chain, whatever came before.
                    chain.reset();
                    chain.observe(seq as u128, None)
                }
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            FeedEvent::Delta(d) => match d.seq {
                Some(seq) => chain.observe(seq as u128, d.prev_seq.map(u128::from)),
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
