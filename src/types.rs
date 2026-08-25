//! Identifiers shared by every venue.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A venue we record from.
///
/// An enum rather than a string: the archive partitions by venue, and a typo in
/// a partition path is a silently split dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VenueId {
    /// Coinbase Exchange (the `ws-feed.exchange.coinbase.com` feed).
    Coinbase,
    /// Kraken Spot, WebSocket API v2.
    Kraken,
    /// OKX v5 spot.
    Okx,
    /// Bybit v5 spot.
    Bybit,
    /// Binance.US spot.
    #[serde(rename = "binance-us")]
    BinanceUs,
    /// Bitstamp.
    Bitstamp,
}

impl VenueId {
    pub const ALL: &'static [VenueId] = &[
        VenueId::Coinbase,
        VenueId::Kraken,
        VenueId::Okx,
        VenueId::Bybit,
        VenueId::BinanceUs,
        VenueId::Bitstamp,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            VenueId::Coinbase => "coinbase",
            VenueId::Kraken => "kraken",
            VenueId::Okx => "okx",
            VenueId::Bybit => "bybit",
            VenueId::BinanceUs => "binance-us",
            VenueId::Bitstamp => "bitstamp",
        }
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for VenueId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "coinbase" => Ok(VenueId::Coinbase),
            "kraken" => Ok(VenueId::Kraken),
            "okx" => Ok(VenueId::Okx),
            "bybit" => Ok(VenueId::Bybit),
            "binance-us" | "binanceus" | "binance" => Ok(VenueId::BinanceUs),
            "bitstamp" => Ok(VenueId::Bitstamp),
            other => Err(format!("unknown venue {other:?}")),
        }
    }
}

/// A canonical instrument name, `BASE-QUOTE` in upper case.
///
/// Phase 2 grows a real normalization layer. What matters now is that the
/// canonical form is a distinct type from whatever a venue happens to call the
/// same pair, so the two can never be swapped by accident.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(base: &str, quote: &str) -> Self {
        Symbol(format!(
            "{}-{}",
            base.to_ascii_uppercase(),
            quote.to_ascii_uppercase()
        ))
    }

    /// Parse `BASE-QUOTE`, `BASE/QUOTE`, or `BASE_QUOTE`.
    pub fn parse(s: &str) -> Result<Self, String> {
        let cleaned = s.trim().to_ascii_uppercase();
        let parts: Vec<&str> = cleaned.split(['-', '/', '_']).collect();
        match parts.as_slice() {
            [base, quote] if !base.is_empty() && !quote.is_empty() => Ok(Symbol::new(base, quote)),
            _ => Err(format!(
                "cannot read {s:?} as a canonical symbol; expected BASE-QUOTE"
            )),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn base(&self) -> &str {
        self.0.split('-').next().unwrap_or_default()
    }

    pub fn quote(&self) -> &str {
        self.0.split('-').nth(1).unwrap_or_default()
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A venue's own spelling of an instrument. Never used as an archive key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VenueSymbol(pub String);

impl VenueSymbol {
    pub fn new(s: impl Into<String>) -> Self {
        VenueSymbol(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VenueSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which side of the book a level sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub const fn as_str(self) -> &'static str {
        match self {
            Side::Bid => "bid",
            Side::Ask => "ask",
        }
    }

    pub const fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How much of the book a venue lets us capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookLevel {
    /// Aggregated price levels.
    L2,
    /// Individual resting orders.
    L3,
}

impl fmt::Display for BookLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BookLevel::L2 => "L2",
            BookLevel::L3 => "L3",
        })
    }
}
