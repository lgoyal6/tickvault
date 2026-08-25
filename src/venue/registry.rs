//! Building any venue by id.
//!
//! One place that knows how to construct all six, so the CLI, the conformance
//! suite, and the tests do not each grow their own copy of the wiring. Adding a
//! seventh venue should mean writing its module and adding one arm here.

use std::sync::Arc;

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::recorder::RawTape;
use crate::types::{Symbol, VenueId};
use crate::venue::kraken::Precision;
use crate::venue::{FeedEvent, HttpFetch, Venue};

/// Knobs that differ per venue but are not part of the protocol.
#[derive(Debug, Clone)]
pub struct VenueConfig {
    /// Symbols to record. Registered with each venue's symbol mapper so
    /// inbound names resolve by lookup rather than by suffix guessing.
    pub symbols: Vec<Symbol>,
    /// Kraken book depth. Ten is the only depth at which its checksum covers
    /// every level the feed can touch.
    pub kraken_depth: usize,
    /// Supply Kraken's precision directly to skip the REST metadata fetch,
    /// which is what makes an offline build possible.
    pub kraken_precision: Option<Precision>,
    pub bybit_depth: usize,
    pub binance_rest_depth: usize,
    /// Record Bitstamp order by order rather than by price level.
    ///
    /// Worth choosing deliberately: its aggregated feed carries nothing to
    /// validate against, while its order-by-order feed chains every event.
    pub bitstamp_book_level: crate::types::BookLevel,
}

impl Default for VenueConfig {
    fn default() -> Self {
        VenueConfig {
            symbols: vec![Symbol::new("BTC", "USD")],
            kraken_depth: 10,
            kraken_precision: None,
            bybit_depth: 50,
            binance_rest_depth: 1000,
            bitstamp_book_level: crate::types::BookLevel::L2,
        }
    }
}

impl VenueConfig {
    pub fn with_symbols(mut self, symbols: Vec<Symbol>) -> Self {
        self.symbols = symbols;
        self
    }

    /// The pair this venue actually lists against the dollar.
    ///
    /// Coinbase, Kraken, and Bitstamp quote real dollars; OKX, Bybit, and
    /// Binance quote Tether. They are different instruments at different
    /// prices, so a caller asking for "Bitcoin against the dollar" has to be
    /// told which one it will get rather than silently handed the other.
    pub fn default_symbol(venue: VenueId) -> Symbol {
        match venue {
            VenueId::Coinbase | VenueId::Kraken | VenueId::Bitstamp => Symbol::new("BTC", "USD"),
            VenueId::Okx | VenueId::Bybit | VenueId::BinanceUs => Symbol::new("BTC", "USDT"),
        }
    }

    /// True when this venue lists the symbol's quote asset at all.
    pub fn quotes_supported(venue: VenueId, symbol: &Symbol) -> bool {
        let quote = symbol.quote();
        match venue {
            VenueId::Coinbase | VenueId::Kraken | VenueId::Bitstamp => quote != "USDT",
            VenueId::Okx | VenueId::Bybit | VenueId::BinanceUs => quote != "USD",
        }
    }
}

/// Construct a venue, performing any metadata fetch it needs.
pub async fn build(
    id: VenueId,
    config: &VenueConfig,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
) -> Result<Arc<dyn Venue>> {
    use crate::venue::{
        binance_us::BinanceUs, bitstamp::Bitstamp, bybit::Bybit, coinbase::Coinbase,
        kraken::Kraken, okx::Okx,
    };
    let symbols = &config.symbols;
    Ok(match id {
        VenueId::Coinbase => Arc::new(Coinbase::new(http, clock).with_symbols(symbols)),
        VenueId::Kraken => {
            let mut kraken = Kraken::new(http, clock, config.kraken_depth)?.with_symbols(symbols);
            match config.kraken_precision {
                Some(precision) => {
                    for symbol in symbols {
                        kraken = kraken.with_precision(symbol.clone(), precision);
                    }
                }
                None => kraken.load_precisions(symbols).await?,
            }
            Arc::new(kraken)
        }
        VenueId::Okx => Arc::new(Okx::new(http, clock).with_symbols(symbols)),
        VenueId::Bybit => {
            Arc::new(Bybit::new(http, clock, config.bybit_depth)?.with_symbols(symbols))
        }
        VenueId::BinanceUs => Arc::new(
            BinanceUs::new(http, clock)
                .with_rest_depth(config.binance_rest_depth)
                .with_symbols(symbols),
        ),
        VenueId::Bitstamp => Arc::new(
            Bitstamp::new(http, clock)
                .with_book_level(config.bitstamp_book_level)
                .with_symbols(symbols),
        ),
    })
}

/// Construct a venue without touching the network.
///
/// Every venue must be buildable this way, which is what lets the conformance
/// suite exercise all six offline. Kraken is the only one needing help, since
/// its checksum cannot be computed without instrument precision.
pub fn build_offline(
    id: VenueId,
    config: &VenueConfig,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn Clock>,
) -> Result<Arc<dyn Venue>> {
    if id == VenueId::Kraken && config.kraken_precision.is_none() {
        return Err(Error::Other(
            "an offline Kraken needs kraken_precision; its checksum cannot be computed without \
             the pair's price and quantity precision"
                .to_string(),
        ));
    }
    // No venue's constructor performs IO, so this cannot block. The await is
    // driven to completion immediately.
    futures_util::future::FutureExt::now_or_never(build(id, config, http, clock))
        .ok_or_else(|| Error::Other(format!("{id} needed the network to build offline")))?
}

/// Indices of tape frames that parse into at least one book delta.
///
/// Used wherever a test or the CLI wants to remove *book* messages rather than
/// acknowledgements and heartbeats. Determined by parsing with the venue rather
/// than by matching a substring, because the substrings are venue-specific and
/// several of them collide: Kraken's connection banner is also
/// `"type":"update"`.
pub fn delta_frame_indices(venue: &dyn Venue, tape: &RawTape) -> Vec<usize> {
    tape.frames
        .iter()
        .enumerate()
        .filter(|(_, recorded)| {
            venue
                .parse_delta(&recorded.to_raw())
                .map(|events| events.iter().any(|e| matches!(e, FeedEvent::Delta(_))))
                .unwrap_or(false)
        })
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::transport::CannedFetch;

    fn offline(id: VenueId) -> Arc<dyn Venue> {
        let config = VenueConfig::default()
            .with_symbols(vec![VenueConfig::default_symbol(id)])
            .tap_precision();
        build_offline(
            id,
            &config,
            CannedFetch::new().shared(),
            Arc::new(ManualClock::default()),
        )
        .unwrap_or_else(|e| panic!("{id} failed to build offline: {e}"))
    }

    impl VenueConfig {
        fn tap_precision(mut self) -> Self {
            self.kraken_precision = Some(Precision { price: 1, qty: 8 });
            self
        }
    }

    #[test]
    fn every_venue_builds_offline_and_reports_its_own_id() {
        for id in VenueId::ALL {
            assert_eq!(offline(*id).id(), *id);
        }
    }

    #[test]
    fn kraken_refuses_to_build_offline_without_its_precision() {
        // Building it anyway would produce a venue whose every message looks
        // corrupt, which is worse than refusing.
        let err = match build_offline(
            VenueId::Kraken,
            &VenueConfig::default(),
            CannedFetch::new().shared(),
            Arc::new(ManualClock::default()),
        ) {
            Ok(_) => panic!("kraken built without the precision it needs"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("kraken_precision"), "{err}");
    }

    #[test]
    fn the_dollar_pair_a_venue_lists_is_stated_not_assumed() {
        assert_eq!(
            VenueConfig::default_symbol(VenueId::Coinbase),
            Symbol::new("BTC", "USD")
        );
        assert_eq!(
            VenueConfig::default_symbol(VenueId::Bybit),
            Symbol::new("BTC", "USDT")
        );
        assert!(VenueConfig::quotes_supported(
            VenueId::Kraken,
            &Symbol::new("BTC", "USD")
        ));
        assert!(!VenueConfig::quotes_supported(
            VenueId::Kraken,
            &Symbol::new("BTC", "USDT")
        ));
        assert!(!VenueConfig::quotes_supported(
            VenueId::Okx,
            &Symbol::new("BTC", "USD")
        ));
    }
}
