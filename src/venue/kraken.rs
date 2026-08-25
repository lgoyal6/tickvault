//! Kraken Spot, WebSocket API v2.
//!
//! Kraken is the deliberate opposite of Coinbase, and that is why it is the
//! second venue rather than an easier third counter-based feed. It sends **no
//! sequence numbers at all**. Every book message instead carries a CRC32 over
//! the ten best levels a side, rendered at the instrument's own precision. Loss
//! is detected by recomputing that hash and finding it does not match.
//!
//! Three consequences run through this file:
//!
//! 1. The check can only run *after* the update is applied, because the hash
//!    describes the resulting book. A mismatch therefore means the book is
//!    already contaminated and must be rebuilt, not merely that one message was
//!    suspect.
//! 2. It needs the pair's price and quantity precision. Without that metadata
//!    the hash cannot be computed at all, so those messages are reported as
//!    unverifiable rather than guessed at.
//! 3. **It only sees the top ten levels.** Subscribed deeper than that, a lost
//!    message touching level four hundred changes the book and not the hash.
//!    That blind spot is declared in the capabilities and published in the gap
//!    report; it is not something the depth default can paper over.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::book::checksum::{KRAKEN_CHECKSUM_DEPTH, kraken_checksum};
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

const WS_URL: &str = "wss://ws.kraken.com/v2";
const REST_ASSET_PAIRS: &str = "https://api.kraken.com/0/public/AssetPairs";
const REST_DEPTH: &str = "https://api.kraken.com/0/public/Depth";

/// Depths Kraken's book channel accepts. Anything else is rejected outright,
/// which is better found here than in a subscription error at 3am.
pub const VALID_DEPTHS: &[usize] = &[10, 25, 100, 500, 1000];

/// An instrument's decimal precision, as Kraken defines it.
///
/// These are not cosmetic. The checksum is computed over price and quantity
/// strings rendered at exactly these widths, so a wrong value here makes every
/// message look corrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Precision {
    pub price: u32,
    pub qty: u32,
}

/// The Kraken v2 book feed.
pub struct Kraken {
    caps: VenueCapabilities,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
    ws_url: String,
    depth: usize,
    precisions: HashMap<Symbol, Precision>,
    /// The v2 websocket spelling, `BTC/USD`.
    symbols: SymbolMapper,
    /// The REST spelling, `XBTUSD`, which uses the legacy asset names.
    rest_symbols: SymbolMapper,
}

impl Kraken {
    /// Build at a given book depth.
    ///
    /// The default of ten is chosen so that gap detection is *complete*: at
    /// depth ten every level the feed can touch is inside the checksum window.
    /// Deeper is more useful data and strictly weaker validation. That is a
    /// real trade, and the caller should make it knowingly.
    pub fn new(http: Arc<dyn HttpFetch>, clock: Arc<dyn Clock>, depth: usize) -> Result<Self> {
        if !VALID_DEPTHS.contains(&depth) {
            return Err(Error::Other(format!(
                "kraken book depth must be one of {VALID_DEPTHS:?}, got {depth}"
            )));
        }
        Ok(Kraken {
            caps: Self::capabilities(depth),
            http,
            clock,
            ws_url: WS_URL.to_string(),
            depth,
            precisions: HashMap::new(),
            symbols: mappers::kraken(),
            rest_symbols: mappers::kraken_rest(),
        })
    }

    /// Teach both mappers the exact pairs we will record. Registration is what
    /// makes `XBTUSD` resolve, since it is otherwise ambiguous with `XB/TUSD`.
    pub fn with_symbols(mut self, symbols: &[Symbol]) -> Self {
        self.symbols.register_all(symbols);
        self.rest_symbols.register_all(symbols);
        self
    }

    pub fn with_ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = url.into();
        self
    }

    /// Supply an instrument's precision directly, bypassing the REST lookup.
    pub fn with_precision(mut self, symbol: Symbol, precision: Precision) -> Self {
        self.precisions.insert(symbol, precision);
        self
    }

    pub fn precision(&self, symbol: &Symbol) -> Option<Precision> {
        self.precisions.get(symbol).copied()
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    fn capabilities(depth: usize) -> VenueCapabilities {
        let mut detection_limits = vec![DetectionLimit {
            venue: VenueId::Kraken,
            scope: "always".to_string(),
            consequence:
                "the feed carries no sequence numbers, so the size of a loss is never known, \
                 only that one occurred"
                    .to_string(),
        }];
        if depth > KRAKEN_CHECKSUM_DEPTH {
            detection_limits.push(DetectionLimit {
                venue: VenueId::Kraken,
                scope: format!("book depth {depth}"),
                consequence: format!(
                    "the venue checksum covers only the {KRAKEN_CHECKSUM_DEPTH} best levels a \
                     side, so a lost message touching levels {} to {depth} changes the book \
                     without changing the checksum and is undetectable",
                    KRAKEN_CHECKSUM_DEPTH + 1
                ),
            });
        }
        VenueCapabilities {
            id: VenueId::Kraken,
            book_level: BookLevel::L2,
            validation: ValidationScheme::Checksum {
                depth: KRAKEN_CHECKSUM_DEPTH,
            },
            scope: SequenceScope::PerSymbol,
            // The hash describes the book after the update, so it cannot be
            // checked until the update has been applied.
            timing: ValidationTiming::AfterApply,
            snapshot_source: SnapshotSource::InBand,
            feed_depth: FeedDepth::Limited(depth),
            // Prices arrive as bare JSON numbers, not quoted strings.
            decimals: DecimalEncoding::JsonNumbers,
            // Measured over 958 live updates at depth 10: not one delete
            // referred to a level we were not already holding. So on Kraken
            // such a delete really is evidence that something went missing.
            redundant_deletes: RedundantDeletes::Never,
            timestamps: VenueTimestamps::PerMessage,
            keepalive: Keepalive::ProtocolPings,
            budget: VenueBudget {
                // Kraken carries many symbols per socket happily, and its
                // per-symbol checksums mean packing costs nothing in blast
                // radius. Its public REST tier is the tight constraint.
                subscriptions_per_connection: 50,
                max_connections: 4,
                connect_interval: std::time::Duration::from_secs(1),
                connect_burst: 2,
                rest_interval: std::time::Duration::from_secs(1),
                rest_burst: 2,
            },
            requires_auth: false,
            detection_limits,
        }
    }

    /// Kraken's own name for an asset. `XBT` is Bitcoin and `XDG` is Dogecoin
    /// in its REST metadata, while the v2 websocket says `BTC` and `DOGE`. The
    /// same instrument therefore has two spellings inside one venue.
    ///
    /// The outbound direction lives in [`crate::symbols::mappers::kraken_rest`];
    /// this is the inbound one, used when reading Kraken's own metadata back.
    fn from_kraken_asset(asset: &str) -> &str {
        match asset {
            "XBT" => "BTC",
            "XDG" => "DOGE",
            other => other,
        }
    }

    /// The pair name Kraken's REST API expects, e.g. `XBTUSD`.
    fn rest_pair(&self, symbol: &Symbol) -> String {
        self.rest_symbols.to_venue(symbol).0
    }

    /// Fetch price and quantity precision for these symbols.
    ///
    /// Call before recording. A symbol whose metadata is missing still records,
    /// but every one of its messages is reported unverifiable rather than
    /// silently trusted.
    pub async fn load_precisions(&mut self, symbols: &[Symbol]) -> Result<()> {
        let body = self.http.get(REST_ASSET_PAIRS).await?;
        let parsed: AssetPairsResponse = serde_json::from_slice(&body)
            .map_err(|e| Error::protocol(VenueId::Kraken, format!("AssetPairs: {e}"), &body))?;
        if let Some(first) = parsed.error.first() {
            return Err(Error::protocol(
                VenueId::Kraken,
                format!("AssetPairs returned {first}"),
                &body,
            ));
        }

        let mut by_symbol: HashMap<Symbol, Precision> = HashMap::new();
        for pair in parsed.result.values() {
            let Some(wsname) = &pair.wsname else { continue };
            let Ok(symbol) = Kraken::canonical_from_slash(wsname) else {
                continue;
            };
            by_symbol.insert(
                symbol,
                Precision {
                    price: pair.pair_decimals,
                    qty: pair.lot_decimals,
                },
            );
        }

        for symbol in symbols {
            match by_symbol.get(symbol) {
                Some(p) => {
                    self.precisions.insert(symbol.clone(), *p);
                }
                None => {
                    return Err(Error::UnknownSymbol {
                        venue: VenueId::Kraken,
                        symbol: symbol.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn canonical_from_slash(raw: &str) -> Result<Symbol> {
        let symbol = Symbol::parse(raw).map_err(|_| Error::UnknownSymbol {
            venue: VenueId::Kraken,
            symbol: raw.to_string(),
        })?;
        Ok(Symbol::new(
            Kraken::from_kraken_asset(symbol.base()),
            Kraken::from_kraken_asset(symbol.quote()),
        ))
    }

    fn levels(entries: &[BookLevelJson], side: Side) -> Vec<LevelChange> {
        entries
            .iter()
            .map(|l| LevelChange::new(side, l.price, l.qty))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct AssetPairsResponse {
    #[serde(default)]
    error: Vec<String>,
    #[serde(default)]
    result: HashMap<String, AssetPair>,
}

#[derive(Debug, Deserialize)]
struct AssetPair {
    wsname: Option<String>,
    pair_decimals: u32,
    lot_decimals: u32,
}

#[derive(Debug, Deserialize)]
struct DepthResponse {
    #[serde(default)]
    error: Vec<String>,
    #[serde(default)]
    result: HashMap<String, DepthBook>,
}

#[derive(Debug, Deserialize)]
struct DepthBook {
    #[serde(default)]
    asks: Vec<DepthLevel>,
    #[serde(default)]
    bids: Vec<DepthLevel>,
}

/// `["78858.30000", "4.520", 1787609576]`. The third element is Kraken's own
/// level timestamp; it is deserialized so the tuple arity matches the wire
/// format, and ignored because the REST book is only a cross-check.
#[derive(Debug, Deserialize)]
struct DepthLevel(Fixed, Fixed, #[allow(dead_code)] serde_json::Value);

#[derive(Debug, Deserialize)]
struct Frame {
    channel: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    /// Left unparsed until the channel is known: the `status` channel puts
    /// connection metadata in `data`, which has nothing in common with a book.
    #[serde(default)]
    data: Vec<serde_json::Value>,
    // Subscription replies use a different shape entirely.
    method: Option<String>,
    success: Option<bool>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BookData {
    symbol: String,
    #[serde(default)]
    bids: Vec<BookLevelJson>,
    #[serde(default)]
    asks: Vec<BookLevelJson>,
    checksum: Option<u32>,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BookLevelJson {
    price: Fixed,
    qty: Fixed,
}

#[async_trait]
impl Venue for Kraken {
    fn capabilities(&self) -> &VenueCapabilities {
        &self.caps
    }

    fn venue_symbol(&self, symbol: &Symbol) -> VenueSymbol {
        self.symbols.to_venue(symbol)
    }

    fn canonical_symbol(&self, raw: &str) -> Result<Symbol> {
        // `XBT/USD` still has to resolve, since Kraken's REST metadata uses it
        // while the v2 websocket says `BTC/USD`.
        Kraken::canonical_from_slash(raw)
    }

    async fn connect(&self) -> Result<Box<dyn FeedTransport>> {
        let transport = crate::transport::WsTransport::connect(
            VenueId::Kraken,
            &self.ws_url,
            Arc::clone(&self.clock),
        )
        .await?;
        Ok(Box::new(transport))
    }

    fn subscribe(&self, symbols: &[Symbol]) -> Result<Vec<String>> {
        if symbols.is_empty() {
            return Err(Error::Subscription {
                venue: VenueId::Kraken,
                reason: "no symbols requested".to_string(),
            });
        }
        let names: Vec<String> = symbols.iter().map(|s| self.venue_symbol(s).0).collect();
        Ok(vec![
            serde_json::json!({
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": names,
                    "depth": self.depth,
                    "snapshot": true,
                },
            })
            .to_string(),
        ])
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<BookSnapshot> {
        let url = format!(
            "{REST_DEPTH}?pair={}&count={}",
            self.rest_pair(symbol),
            self.depth
        );
        let body = self.http.get(&url).await?;
        let parsed: DepthResponse = serde_json::from_slice(&body)
            .map_err(|e| Error::protocol(VenueId::Kraken, format!("Depth: {e}"), &body))?;
        if let Some(first) = parsed.error.first() {
            return Err(Error::protocol(
                VenueId::Kraken,
                format!("Depth returned {first}"),
                &body,
            ));
        }
        // Kraken keys the result by its own internal pair name, which is a
        // third spelling again (`XXBTZUSD`). With one pair requested there is
        // exactly one entry, so take it rather than trying to predict the key.
        let book = parsed
            .result
            .values()
            .next()
            .ok_or_else(|| Error::protocol(VenueId::Kraken, "Depth returned no pair", &body))?;

        Ok(BookSnapshot {
            symbol: symbol.clone(),
            bids: book.bids.iter().map(|l| (l.0, l.1)).collect(),
            asks: book.asks.iter().map(|l| (l.0, l.1)).collect(),
            seq: None,
            // The REST book publishes no checksum, so it cannot resync the
            // socket's validation state on its own.
            checksum: None,
            stamps: Timestamps::recv_only(self.clock.stamp()),
        })
    }

    fn parse_delta(&self, frame: &RawFrame) -> Result<Vec<FeedEvent>> {
        let parsed: Frame = serde_json::from_slice(&frame.payload)
            .map_err(|e| Error::protocol(VenueId::Kraken, e.to_string(), &frame.payload))?;

        if parsed.method.is_some() {
            let ok = parsed.success.unwrap_or(false);
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind: if ok {
                    ControlKind::SubscriptionAck
                } else {
                    ControlKind::SubscriptionError
                },
                detail: parsed
                    .error
                    .unwrap_or_else(|| parsed.method.unwrap_or_default()),
                stamp: frame.stamp,
                // Kraken numbers nothing, so there is no counter to keep in step.
                seq: None,
            })]);
        }

        let channel = parsed.channel.as_deref().unwrap_or("");
        if channel != "book" {
            let kind = match channel {
                "heartbeat" => ControlKind::Heartbeat,
                "status" => ControlKind::Status,
                "" => ControlKind::Ignored,
                _ => ControlKind::Status,
            };
            return Ok(vec![FeedEvent::Control(ControlEvent {
                kind,
                detail: channel.to_string(),
                stamp: frame.stamp,
                seq: None,
            })]);
        }

        let is_snapshot = match parsed.kind.as_deref() {
            Some("snapshot") => true,
            Some("update") => false,
            other => {
                return Err(Error::protocol(
                    VenueId::Kraken,
                    format!("unknown book message type {other:?}"),
                    &frame.payload,
                ));
            }
        };

        let mut out = Vec::with_capacity(parsed.data.len());
        for value in parsed.data {
            let data: BookData = serde_json::from_value(value).map_err(|e| {
                Error::protocol(VenueId::Kraken, format!("book data: {e}"), &frame.payload)
            })?;
            let symbol = self.canonical_symbol(&data.symbol)?;
            let stamps = Timestamps::new(
                frame.stamp,
                data.timestamp.as_deref().and_then(parse_rfc3339_nanos),
            );
            if is_snapshot {
                out.push(FeedEvent::Snapshot(BookSnapshot {
                    symbol,
                    bids: data.bids.iter().map(|l| (l.price, l.qty)).collect(),
                    asks: data.asks.iter().map(|l| (l.price, l.qty)).collect(),
                    seq: None,
                    checksum: data.checksum,
                    stamps,
                }));
            } else {
                let mut changes = Kraken::levels(&data.bids, Side::Bid);
                changes.extend(Kraken::levels(&data.asks, Side::Ask));
                out.push(FeedEvent::Delta(BookDelta {
                    symbol,
                    changes,
                    seq: None,
                    checksum: data.checksum,
                    stamps,
                    // Kraken names no predecessor and batches no id range; its
                    // checksum is the whole check.
                    prev_seq: None,
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
        book: &L2Book,
    ) -> SeqVerdict {
        let SeqState::Checksum(checksum_state) = state else {
            return SeqVerdict::Unverifiable(Unverifiable::VenuePublishesNoSequence);
        };
        let (symbol, expected) = match event {
            FeedEvent::Snapshot(s) => (&s.symbol, s.checksum),
            FeedEvent::Delta(d) => (&d.symbol, d.checksum),
            FeedEvent::Order(_) | FeedEvent::Control(_) => {
                return SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck);
            }
        };
        let Some(expected) = expected else {
            return SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck);
        };
        let Some(precision) = self.precision(symbol) else {
            return SeqVerdict::Unverifiable(Unverifiable::MissingInstrumentMetadata);
        };
        let computed = kraken_checksum(book, precision.price, precision.qty);
        checksum_state.observe(expected, computed)
    }
}
