//! Binance.US spot.
//!
//! The only venue of the six with **no in-band snapshot at all**. The websocket
//! carries nothing but diffs, and the starting book must be fetched over REST
//! and spliced into the stream. That splice is a small protocol in itself, and
//! getting it subtly wrong is the classic way to end up with a book that is
//! plausible and wrong from the first second:
//!
//! 1. Start buffering diffs before fetching anything.
//! 2. Fetch `/api/v3/depth`, which returns a `lastUpdateId`.
//! 3. Discard every buffered diff whose final id is at or below it.
//! 4. Require the first surviving diff to satisfy
//!    `U <= lastUpdateId + 1 <= u`. If it does not, the snapshot and the stream
//!    do not meet, and the only correct move is to fetch another snapshot
//!    rather than start from a book with a hole behind it.
//!
//! The second thing that makes this venue different: each message spans a
//! *range* of update ids rather than carrying one. A gap is therefore measured
//! in update ids, not messages, and those are not the same quantity. One
//! observed message covered five ids at once.

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

const WS_URL: &str = "wss://stream.binance.us:9443/ws";
const REST_DEPTH: &str = "https://api.binance.us/api/v3/depth";

/// Update cadence of the diff stream. The venue also offers 1000ms; 100ms is
/// the finer of the two and what a microstructure user would want.
const STREAM_INTERVAL: &str = "100ms";

pub struct BinanceUs {
    caps: VenueCapabilities,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
    ws_url: String,
    rest_depth: usize,
    symbols: SymbolMapper,
}

impl BinanceUs {
    pub fn new(http: Arc<dyn HttpFetch>, clock: Arc<dyn Clock>) -> Self {
        BinanceUs {
            caps: Self::capabilities(),
            http,
            clock,
            ws_url: WS_URL.to_string(),
            rest_depth: 1000,
            symbols: mappers::binance_us(),
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

    pub fn with_rest_depth(mut self, depth: usize) -> Self {
        self.rest_depth = depth;
        self
    }

    fn stream(&self, symbol: &Symbol) -> String {
        format!(
            "{}@depth@{STREAM_INTERVAL}",
            self.venue_symbol(symbol).0.to_ascii_lowercase()
        )
    }

    fn capabilities() -> VenueCapabilities {
        VenueCapabilities {
            id: VenueId::BinanceUs,
            book_level: BookLevel::L2,
            validation: ValidationScheme::UpdateIdRange,
            scope: SequenceScope::PerSymbol,
            timing: ValidationTiming::BeforeApply,
            // The one venue here whose book cannot start without a REST call.
            snapshot_source: SnapshotSource::Rest,
            feed_depth: FeedDepth::Full,
            decimals: DecimalEncoding::Strings,
            // Measured: 156 orphan deletes across 211 live diffs. The stream
            // reports the net state of each price level over a 100ms window, so
            // a level added and cancelled inside one window arrives as a delete
            // for something that never existed as far as we ever saw.
            redundant_deletes: RedundantDeletes::Expected,
            timestamps: VenueTimestamps::PerMessage,
            keepalive: Keepalive::ProtocolPings,
            budget: VenueBudget {
                subscriptions_per_connection: 20,
                max_connections: 4,
                connect_interval: std::time::Duration::from_secs(1),
                connect_burst: 2,
                // The REST depth endpoint is weighted heavily at this size.
                rest_interval: std::time::Duration::from_secs(1),
                rest_burst: 2,
            },
            requires_auth: false,
            detection_limits: vec![
                DetectionLimit {
                    venue: VenueId::BinanceUs,
                    scope: "every gap".to_string(),
                    consequence:
                        "a message spans a range of update ids, so a gap is measured in update \
                         ids and the number of messages lost is not recoverable"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::BinanceUs,
                    scope: "the moments either side of a snapshot".to_string(),
                    consequence:
                        "the book has no in-band snapshot, so every resync spends a REST round \
                         trip during which the stream keeps moving; the window is bounded and \
                         reported rather than hidden"
                            .to_string(),
                },
                DetectionLimit {
                    venue: VenueId::BinanceUs,
                    scope: "always".to_string(),
                    consequence:
                        "the diff stream reports the net state of a price level per window, so \
                         a level added and cancelled within one window arrives as a delete for \
                         a level that was never published"
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
}

/// `["78784.68000000", "0.03173000"]`
#[derive(Debug, Deserialize)]
struct Level(Fixed, Fixed);

#[derive(Debug, Deserialize)]
struct Frame {
    #[serde(rename = "e")]
    event: Option<String>,
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "U")]
    first_update_id: Option<u64>,
    #[serde(rename = "u")]
    last_update_id: Option<u64>,
    #[serde(default, rename = "b")]
    bids: Vec<Level>,
    #[serde(default, rename = "a")]
    asks: Vec<Level>,
    /// Subscribe replies are `{"result":null,"id":1}`.
    id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct DepthResponse {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(default)]
    bids: Vec<Level>,
    #[serde(default)]
    asks: Vec<Level>,
}

#[async_trait]
impl Venue for BinanceUs {
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
                VenueId::BinanceUs,
                &self.ws_url,
                Arc::clone(&self.clock),
            )
            .await?,
        ))
    }

    fn subscribe(&self, symbols: &[Symbol]) -> Result<Vec<String>> {
        if symbols.is_empty() {
            return Err(Error::Subscription {
                venue: VenueId::BinanceUs,
                reason: "no symbols requested".to_string(),
            });
        }
        let params: Vec<String> = symbols.iter().map(|s| self.stream(s)).collect();
        Ok(vec![
            serde_json::json!({"method": "SUBSCRIBE", "params": params, "id": 1}).to_string(),
        ])
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot> {
        let url = format!(
            "{REST_DEPTH}?symbol={}&limit={}",
            self.venue_symbol(symbol),
            self.rest_depth
        );
        let body = self.http.get(&url).await?;
        let parsed: DepthResponse = serde_json::from_slice(&body)
            .map_err(|e| Error::protocol(VenueId::BinanceUs, format!("depth: {e}"), &body))?;
        Ok(BookSnapshot {
            symbol: symbol.clone(),
            bids: parsed.bids.iter().map(|l| (l.0, l.1)).collect(),
            asks: parsed.asks.iter().map(|l| (l.0, l.1)).collect(),
            // This is the value the whole splice hinges on.
            seq: Some(parsed.last_update_id),
            checksum: None,
            stamps: Timestamps::recv_only(self.clock.stamp()),
        })
    }

    fn parse_delta(&self, frame: &RawFrame) -> Result<Vec<FeedEvent>> {
        let parsed: Frame = serde_json::from_slice(&frame.payload)
            .map_err(|e| Error::protocol(VenueId::BinanceUs, e.to_string(), &frame.payload))?;

        if parsed.event.as_deref() != Some("depthUpdate") {
            let kind = if parsed.id.is_some() {
                ControlKind::SubscriptionAck
            } else {
                ControlKind::Ignored
            };
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind,
                detail: parsed
                    .event
                    .unwrap_or_else(|| "subscribe reply".to_string()),
                stamp: frame.stamp,
                seq: None,
            })]);
        }

        let Some(raw_symbol) = parsed.symbol.as_deref() else {
            return Err(Error::protocol(
                VenueId::BinanceUs,
                "depthUpdate without a symbol",
                &frame.payload,
            ));
        };
        let symbol = self.canonical_symbol(raw_symbol)?;

        let mut changes = BinanceUs::levels(&parsed.bids, Side::Bid);
        changes.extend(BinanceUs::levels(&parsed.asks, Side::Ask));
        Ok(vec![FeedEvent::Delta(BookDelta {
            symbol,
            changes,
            seq: parsed.last_update_id,
            checksum: None,
            stamps: Timestamps::new(
                frame.stamp,
                parsed.event_time.and_then(|t| t.checked_mul(1_000_000)),
            ),
            prev_seq: None,
            first_seq: parsed.first_update_id,
        })])
    }

    fn validate_sequence(
        &self,
        state: &mut SeqState,
        event: &FeedEvent,
        _book: &L2Book,
    ) -> SeqVerdict {
        let SeqState::Range(range) = state else {
            return SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence);
        };
        match event {
            FeedEvent::Snapshot(s) => match s.seq {
                Some(last_update_id) => {
                    range.anchor_snapshot(last_update_id);
                    SeqVerdict::InOrder
                }
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            FeedEvent::Delta(d) => match (d.first_seq, d.seq) {
                (Some(first), Some(last)) => range.observe(first, last),
                _ => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            // This venue publishes no order-by-order feed, so it never
            // produces one of these.
            FeedEvent::Order(_) | FeedEvent::Control(_) => {
                SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck)
            }
        }
    }
}
