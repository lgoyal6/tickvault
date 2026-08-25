//! Symbol normalization.
//!
//! The same pair is spelled differently on every venue, and the differences are
//! not just punctuation:
//!
//! | venue | BTC against the dollar |
//! |---|---|
//! | Coinbase | `BTC-USD` |
//! | Kraken (websocket) | `BTC/USD` |
//! | Kraken (REST metadata) | `XBT/USD` |
//! | Kraken (REST result key) | `XXBTZUSD` |
//! | OKX | `BTC-USDT` |
//! | Bybit | `BTCUSDT` |
//! | Binance.US | `BTCUSDT` |
//! | Bitstamp | `btcusd` |
//!
//! Two traps live in that table.
//!
//! **`USD` and `USDT` are different assets.** Coinbase, Kraken, and Bitstamp
//! list Bitcoin against actual dollars; OKX, Bybit, and Binance list it against
//! Tether. Folding them together would silently merge two instruments that
//! trade at different prices into one column of the archive. The canonical form
//! keeps them distinct and the archive partitions on it.
//!
//! **A concatenated name cannot be split without knowing the quote assets.**
//! `BTCUSDT` is `BTC` + `USDT` only because `USDT` is a known quote. So the
//! reverse direction takes the exact spellings we subscribed to wherever it
//! can, and falls back to a longest-suffix match that fails loudly rather than
//! guessing.

use std::collections::HashMap;
use std::fmt;

use crate::types::{Symbol, VenueSymbol};

/// Quote assets, longest first so `USDT` is tried before `USD`.
///
/// Order is the whole algorithm here: matched shortest-first, `BTCUSDT` would
/// split as `BTCUS` + `DT` the moment a two-letter quote entered the list.
const DEFAULT_QUOTES: &[&str] = &[
    "FDUSD", "TUSD", "BUSD", "USDC", "USDT", "USDE", "DAI", "USD", "EUR", "GBP", "AUD", "CAD",
    "CHF", "JPY", "BTC", "ETH", "BNB", "SOL", "XBT",
];

/// How a venue punctuates a pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Separator {
    Dash,
    Slash,
    Underscore,
    /// No separator at all, which is what makes the reverse direction hard.
    None,
}

impl Separator {
    fn as_str(self) -> &'static str {
        match self {
            Separator::Dash => "-",
            Separator::Slash => "/",
            Separator::Underscore => "_",
            Separator::None => "",
        }
    }
}

/// Letter case a venue uses on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Casing {
    Upper,
    Lower,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolError {
    /// The venue spelling could not be split into a base and a quote.
    Unsplittable { raw: String, venue: &'static str },
    /// The venue spelling can be split in more than one way and we were not
    /// told which pair it is.
    ///
    /// `XBTUSD` is the real example: it is `XBT` + `USD`, but it is equally
    /// `XB` + `TUSD` once TrueUSD is a known quote asset. Picking the longer
    /// suffix would quietly file Bitcoin under an asset called `XB`.
    Ambiguous {
        raw: String,
        venue: &'static str,
        candidates: Vec<(String, String)>,
    },
    /// The canonical form was malformed.
    NotCanonical(String),
}

impl fmt::Display for SymbolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SymbolError::Unsplittable { raw, venue } => write!(
                f,
                "cannot split {venue} symbol {raw:?} into base and quote; \
                 its quote asset is not in the known list"
            ),
            SymbolError::Ambiguous {
                raw,
                venue,
                candidates,
            } => {
                let shown: Vec<String> =
                    candidates.iter().map(|(b, q)| format!("{b}/{q}")).collect();
                write!(
                    f,
                    "{venue} symbol {raw:?} splits {} ways ({}); register the pair to \
                     resolve it rather than guessing",
                    candidates.len(),
                    shown.join(" or ")
                )
            }
            SymbolError::NotCanonical(s) => write!(f, "{s:?} is not a canonical BASE-QUOTE symbol"),
        }
    }
}

impl std::error::Error for SymbolError {}

/// Translates between the canonical form and one venue's spelling.
#[derive(Debug, Clone)]
pub struct SymbolMapper {
    venue: &'static str,
    separator: Separator,
    casing: Casing,
    /// Canonical asset name to this venue's name, e.g. `BTC` to `XBT`.
    to_venue_asset: HashMap<String, String>,
    from_venue_asset: HashMap<String, String>,
    quotes: Vec<String>,
    /// Exact spellings we have registered. Consulted before any splitting, so
    /// for the symbols we actually subscribed to the reverse direction is a
    /// lookup rather than a guess.
    known: HashMap<String, Symbol>,
}

impl SymbolMapper {
    pub fn new(venue: &'static str, separator: Separator, casing: Casing) -> Self {
        SymbolMapper {
            venue,
            separator,
            casing,
            to_venue_asset: HashMap::new(),
            from_venue_asset: HashMap::new(),
            quotes: DEFAULT_QUOTES.iter().map(|s| s.to_string()).collect(),
            known: HashMap::new(),
        }
    }

    /// Rename an asset for this venue, e.g. Kraken's `BTC` to `XBT`.
    pub fn alias(mut self, canonical: &str, venue_name: &str) -> Self {
        self.to_venue_asset.insert(
            canonical.to_ascii_uppercase(),
            venue_name.to_ascii_uppercase(),
        );
        self.from_venue_asset.insert(
            venue_name.to_ascii_uppercase(),
            canonical.to_ascii_uppercase(),
        );
        self
    }

    /// Add a quote asset this venue uses that is not in the default list.
    pub fn quote(mut self, asset: &str) -> Self {
        self.quotes.push(asset.to_ascii_uppercase());
        // Longest first, so the greedy suffix match stays correct.
        self.quotes.sort_by_key(|q| std::cmp::Reverse(q.len()));
        self
    }

    /// Teach the mapper an exact pair, making the reverse direction exact for it.
    pub fn register(&mut self, symbol: &Symbol) {
        let spelled = self.to_venue(symbol);
        self.known.insert(spelled.0.clone(), symbol.clone());
        // Venues are not always consistent about case in their own replies.
        self.known
            .insert(spelled.0.to_ascii_uppercase(), symbol.clone());
        self.known
            .insert(spelled.0.to_ascii_lowercase(), symbol.clone());
    }

    pub fn register_all(&mut self, symbols: &[Symbol]) {
        for symbol in symbols {
            self.register(symbol);
        }
    }

    fn cased(&self, s: &str) -> String {
        match self.casing {
            Casing::Upper => s.to_ascii_uppercase(),
            Casing::Lower => s.to_ascii_lowercase(),
        }
    }

    fn venue_asset(&self, canonical: &str) -> String {
        self.to_venue_asset
            .get(canonical)
            .cloned()
            .unwrap_or_else(|| canonical.to_string())
    }

    fn canonical_asset(&self, venue_name: &str) -> String {
        self.from_venue_asset
            .get(venue_name)
            .cloned()
            .unwrap_or_else(|| venue_name.to_string())
    }

    /// Canonical to this venue's spelling. Always unambiguous.
    pub fn to_venue(&self, symbol: &Symbol) -> VenueSymbol {
        VenueSymbol::new(self.cased(&format!(
            "{}{}{}",
            self.venue_asset(symbol.base()),
            self.separator.as_str(),
            self.venue_asset(symbol.quote())
        )))
    }

    /// This venue's spelling back to canonical.
    pub fn to_canonical(&self, raw: &str) -> Result<Symbol, SymbolError> {
        let trimmed = raw.trim();
        if let Some(known) = self
            .known
            .get(trimmed)
            .or_else(|| self.known.get(&trimmed.to_ascii_uppercase()))
        {
            return Ok(known.clone());
        }

        let upper = trimmed.to_ascii_uppercase();
        let (base, quote) = match self.separator {
            Separator::None => self.split_concatenated(&upper)?,
            sep => {
                let mut parts = upper.split(sep.as_str());
                match (parts.next(), parts.next(), parts.next()) {
                    (Some(b), Some(q), None) if !b.is_empty() && !q.is_empty() => {
                        (b.to_string(), q.to_string())
                    }
                    _ => {
                        return Err(SymbolError::Unsplittable {
                            raw: raw.to_string(),
                            venue: self.venue,
                        });
                    }
                }
            }
        };

        Ok(Symbol::new(
            &self.canonical_asset(&base),
            &self.canonical_asset(&quote),
        ))
    }

    /// Split `BTCUSDT` by matching a known quote asset as a suffix.
    ///
    /// Every match is collected rather than the first or longest taken. One
    /// match is an answer; several is an ambiguity the caller has to resolve by
    /// registering the pair, because there is no information in the string
    /// itself that picks between them.
    fn split_concatenated(&self, upper: &str) -> Result<(String, String), SymbolError> {
        let mut matches: Vec<(String, String)> = Vec::new();
        for quote in &self.quotes {
            if upper.len() > quote.len() && upper.ends_with(quote.as_str()) {
                let base = upper[..upper.len() - quote.len()].to_string();
                if !base.is_empty() && !matches.iter().any(|(b, q)| *b == base && q == quote) {
                    matches.push((base, quote.clone()));
                }
            }
        }
        match matches.len() {
            0 => Err(SymbolError::Unsplittable {
                raw: upper.to_string(),
                venue: self.venue,
            }),
            1 => Ok(matches.remove(0)),
            _ => Err(SymbolError::Ambiguous {
                raw: upper.to_string(),
                venue: self.venue,
                candidates: matches,
            }),
        }
    }
}

/// Ready-made mappers, one per venue we record.
pub mod mappers {
    use super::*;

    pub fn coinbase() -> SymbolMapper {
        SymbolMapper::new("coinbase", Separator::Dash, Casing::Upper)
    }

    /// Kraken renames Bitcoin and Dogecoin in its REST metadata while its v2
    /// websocket uses the modern names, so both spellings map home.
    pub fn kraken() -> SymbolMapper {
        SymbolMapper::new("kraken", Separator::Slash, Casing::Upper)
    }

    /// The spelling Kraken's REST endpoints want: no separator, legacy assets.
    pub fn kraken_rest() -> SymbolMapper {
        SymbolMapper::new("kraken-rest", Separator::None, Casing::Upper)
            .alias("BTC", "XBT")
            .alias("DOGE", "XDG")
    }

    pub fn okx() -> SymbolMapper {
        SymbolMapper::new("okx", Separator::Dash, Casing::Upper)
    }

    pub fn bybit() -> SymbolMapper {
        SymbolMapper::new("bybit", Separator::None, Casing::Upper)
    }

    pub fn binance_us() -> SymbolMapper {
        SymbolMapper::new("binance.us", Separator::None, Casing::Upper)
    }

    pub fn bitstamp() -> SymbolMapper {
        SymbolMapper::new("bitstamp", Separator::None, Casing::Lower)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym(base: &str, quote: &str) -> Symbol {
        Symbol::new(base, quote)
    }

    #[test]
    fn each_venue_spells_the_same_pair_its_own_way() {
        let btc_usd = sym("BTC", "USD");
        assert_eq!(mappers::coinbase().to_venue(&btc_usd).0, "BTC-USD");
        assert_eq!(mappers::kraken().to_venue(&btc_usd).0, "BTC/USD");
        assert_eq!(mappers::kraken_rest().to_venue(&btc_usd).0, "XBTUSD");
        assert_eq!(mappers::bitstamp().to_venue(&btc_usd).0, "btcusd");

        let btc_usdt = sym("BTC", "USDT");
        assert_eq!(mappers::okx().to_venue(&btc_usdt).0, "BTC-USDT");
        assert_eq!(mappers::bybit().to_venue(&btc_usdt).0, "BTCUSDT");
        assert_eq!(mappers::binance_us().to_venue(&btc_usdt).0, "BTCUSDT");
    }

    #[test]
    fn every_mapper_round_trips_a_pair_it_was_told_about() {
        let pairs = [sym("BTC", "USD"), sym("ETH", "USDT"), sym("SOL", "USDC")];
        for build in [
            mappers::coinbase as fn() -> SymbolMapper,
            mappers::kraken,
            mappers::kraken_rest,
            mappers::okx,
            mappers::bybit,
            mappers::binance_us,
            mappers::bitstamp,
        ] {
            let mut m = build();
            m.register_all(&pairs);
            for pair in &pairs {
                let spelled = m.to_venue(pair);
                assert_eq!(
                    m.to_canonical(spelled.as_str()).unwrap(),
                    *pair,
                    "round trip failed for {spelled}"
                );
            }
        }
    }

    #[test]
    fn usd_and_usdt_are_never_folded_together() {
        // They are different assets trading at different prices. Merging them
        // would put two instruments in one column of the archive.
        let usd = sym("BTC", "USD");
        let usdt = sym("BTC", "USDT");
        assert_ne!(usd, usdt);
        let m = mappers::bybit();
        assert_eq!(m.to_venue(&usd).0, "BTCUSD");
        assert_eq!(m.to_venue(&usdt).0, "BTCUSDT");
        assert_eq!(m.to_canonical("BTCUSDT").unwrap(), usdt);
        assert_eq!(m.to_canonical("BTCUSD").unwrap(), usd);
    }

    #[test]
    fn concatenated_names_split_on_the_longest_matching_quote() {
        let m = mappers::binance_us();
        assert_eq!(m.to_canonical("BTCUSDT").unwrap(), sym("BTC", "USDT"));
        assert_eq!(m.to_canonical("ETHUSDC").unwrap(), sym("ETH", "USDC"));
        assert_eq!(m.to_canonical("BNBBTC").unwrap(), sym("BNB", "BTC"));
        // A quote asset that is itself a prefix of another one.
        assert_eq!(m.to_canonical("TUSDUSDT").unwrap(), sym("TUSD", "USDT"));
    }

    #[test]
    fn an_unknown_quote_asset_fails_loudly_instead_of_guessing() {
        let m = mappers::bybit();
        let err = m.to_canonical("BTCZZZZ").unwrap_err();
        assert!(matches!(err, SymbolError::Unsplittable { .. }), "{err:?}");
        // Registering it makes the same string resolve exactly.
        let mut m = mappers::bybit().quote("ZZZZ");
        m.register(&sym("BTC", "ZZZZ"));
        assert_eq!(m.to_canonical("BTCZZZZ").unwrap(), sym("BTC", "ZZZZ"));
    }

    #[test]
    fn registration_beats_the_heuristic_even_when_the_heuristic_would_be_wrong() {
        // A pair whose base ends in a quote asset name is exactly the case the
        // suffix match gets wrong, and exactly why registration exists.
        let mut m = mappers::bybit();
        let odd = sym("MYUSD", "USDT");
        m.register(&odd);
        assert_eq!(m.to_canonical("MYUSDUSDT").unwrap(), odd);
    }

    #[test]
    fn kraken_legacy_asset_names_map_home() {
        let m = mappers::kraken_rest();
        assert_eq!(m.to_venue(&sym("BTC", "USD")).0, "XBTUSD");
        assert_eq!(m.to_venue(&sym("DOGE", "EUR")).0, "XDGEUR");
        // A separator-free name with an unambiguous quote resolves.
        assert_eq!(m.to_canonical("XDGEUR").unwrap(), sym("DOGE", "EUR"));
    }

    #[test]
    fn an_ambiguous_concatenation_is_reported_rather_than_guessed() {
        // `XBTUSD` is `XBT` + `USD`, and equally `XB` + `TUSD`. Taking the
        // longest suffix would file Bitcoin under an asset called `XB`.
        let m = mappers::kraken_rest();
        let err = m.to_canonical("XBTUSD").unwrap_err();
        let SymbolError::Ambiguous { candidates, .. } = &err else {
            panic!("expected an ambiguity, got {err:?}")
        };
        assert_eq!(candidates.len(), 2);
        assert!(err.to_string().contains("register the pair"));

        // Registering the pair settles it, which is what the recorder does for
        // every symbol it subscribes to.
        let mut m = mappers::kraken_rest();
        m.register(&sym("BTC", "USD"));
        assert_eq!(m.to_canonical("XBTUSD").unwrap(), sym("BTC", "USD"));
    }

    #[test]
    fn case_differences_in_a_venue_reply_still_resolve() {
        let mut m = mappers::bitstamp();
        m.register(&sym("BTC", "USD"));
        assert_eq!(m.to_canonical("BTCUSD").unwrap(), sym("BTC", "USD"));
        assert_eq!(m.to_canonical("btcusd").unwrap(), sym("BTC", "USD"));
        assert_eq!(m.to_canonical(" btcusd ").unwrap(), sym("BTC", "USD"));
    }

    #[test]
    fn a_separated_name_with_too_many_parts_is_rejected() {
        let m = mappers::okx();
        // OKX uses BTC-USDT-SWAP for perpetuals, which is not a spot pair and
        // must not be silently truncated into one.
        assert!(m.to_canonical("BTC-USDT-SWAP").is_err());
    }

    #[test]
    fn quote_ordering_survives_adding_a_shorter_quote() {
        let m = mappers::bybit().quote("US");
        assert_eq!(m.to_canonical("BTCUSDT").unwrap(), sym("BTC", "USDT"));
    }
}
