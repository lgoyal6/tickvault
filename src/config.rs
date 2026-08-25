//! The deployment's own description of itself.
//!
//! Recording six venues used to mean six invocations with six sets of flags,
//! six log files and six archives, supervised by whatever shell loop the
//! operator wrote. That is how three feeds went silent for an hour without
//! anyone noticing. A file that says what should be running is the first thing
//! needed to notice that something is not.
//!
//! ```toml
//! archive = "./archive"
//! backpressure = "block"
//!
//! [status]
//! listen = "127.0.0.1:8080"
//!
//! [[venue]]
//! name = "kraken"
//! symbols = ["BTC-USD", "ETH-USD"]
//!
//! [[venue]]
//! name = "bitstamp"
//! symbols = ["BTC-USD"]
//! book_level = 3          # its aggregated feed can prove nothing
//! ```

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::pipeline::BackpressurePolicy;
use crate::types::{BookLevel, Symbol, VenueId};
use crate::venue::registry::VenueConfig;

/// Everything one `tickvault serve` needs to know.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Root the per-venue archives live under.
    pub archive: PathBuf,

    /// Block the reader when the writer falls behind, or shed rows and record
    /// the drop. There is no default that is right for everyone, which is why
    /// this is stated rather than assumed.
    #[serde(default)]
    pub backpressure: BackpressurePolicy,

    /// Rotate an archive file this often. This is the bound on what a crash
    /// costs per partition, not a tuning knob.
    #[serde(default = "default_rotate_secs")]
    pub rotate_secs: u64,

    /// Batches the ingest-to-writer channel will hold.
    #[serde(default = "default_queue")]
    pub queue: usize,

    /// How long to wait before restarting a venue whose run ended in an error.
    #[serde(default = "default_restart_secs")]
    pub restart_secs: u64,

    #[serde(default)]
    pub status: Option<StatusConfig>,

    /// One entry per venue. Order does not matter; duplicates are rejected.
    #[serde(rename = "venue", default)]
    pub venues: Vec<VenueEntry>,
}

/// The status service, which is off unless asked for.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusConfig {
    /// Address to serve on. Loopback by default: this exposes what a recorder
    /// is doing, and that is not something to put on a public interface by
    /// accident.
    #[serde(default = "default_listen")]
    pub listen: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueEntry {
    pub name: VenueId,

    /// Canonical `BASE-QUOTE` symbols. Empty means the pair this venue
    /// actually lists against the dollar, which is USDT on half of them.
    #[serde(default)]
    pub symbols: Vec<String>,

    /// Kraken only. Ten is the only depth at which its checksum covers every
    /// level the feed can touch, so raising it buys depth and loses proof.
    pub kraken_depth: Option<usize>,

    pub bybit_depth: Option<usize>,

    /// 2 for aggregated price levels, 3 for order by order. Only Bitstamp
    /// offers order by order without a key, and doing so takes it from
    /// unverifiable to fully chained.
    pub book_level: Option<u8>,
}

fn default_rotate_secs() -> u64 {
    60
}

fn default_queue() -> usize {
    1024
}

fn default_restart_secs() -> u64 {
    5
}

fn default_listen() -> String {
    "127.0.0.1:8080".to_string()
}

impl Config {
    pub fn from_toml(text: &str) -> Result<Self> {
        let config: Config =
            toml::from_str(text).map_err(|e| Error::Other(format!("config: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;
        Self::from_toml(&text)
    }

    /// Reject a configuration that would run but not do what it says.
    fn validate(&self) -> Result<()> {
        if self.venues.is_empty() {
            return Err(Error::Other(
                "config lists no venues, so there is nothing to record".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for entry in &self.venues {
            if !seen.insert(entry.name) {
                // Two entries for one venue would race for the same archive
                // directory and interleave into one manifest.
                return Err(Error::Other(format!(
                    "{} is configured twice; put all of its symbols in one entry",
                    entry.name
                )));
            }
            entry.resolve()?;
        }
        if self.rotate_secs == 0 {
            return Err(Error::Other("rotate_secs must be at least 1".into()));
        }
        if self.queue == 0 {
            return Err(Error::Other("queue must be at least 1".into()));
        }
        Ok(())
    }

    /// Where one venue's archive lives.
    ///
    /// Per venue, not one shared directory, because the manifest is an
    /// append-and-fsync record of what is safe to publish and two writers
    /// appending to one would interleave into something neither of them meant.
    pub fn archive_for(&self, venue: VenueId) -> PathBuf {
        self.archive.join(venue.as_str())
    }

    pub fn rotate(&self) -> Duration {
        Duration::from_secs(self.rotate_secs)
    }

    pub fn restart_delay(&self) -> Duration {
        Duration::from_secs(self.restart_secs)
    }
}

impl VenueEntry {
    /// Turn one entry into the venue configuration the registry expects.
    pub fn resolve(&self) -> Result<VenueConfig> {
        let symbols = if self.symbols.is_empty() {
            vec![VenueConfig::default_symbol(self.name)]
        } else {
            self.symbols
                .iter()
                .map(|s| {
                    Symbol::parse(s)
                        .map_err(|e| Error::Other(format!("{}: symbol {s:?}: {e}", self.name)))
                })
                .collect::<Result<Vec<_>>>()?
        };
        let mut config = VenueConfig {
            symbols,
            ..VenueConfig::default()
        };
        if let Some(depth) = self.kraken_depth {
            config.kraken_depth = depth;
        }
        if let Some(depth) = self.bybit_depth {
            config.bybit_depth = depth;
        }
        config.bitstamp_book_level = match self.book_level {
            Some(3) => BookLevel::L3,
            Some(2) | None => BookLevel::L2,
            Some(other) => {
                return Err(Error::Other(format!(
                    "{}: book_level {other} is neither 2 nor 3",
                    self.name
                )));
            }
        };
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        archive = "./archive"
        [[venue]]
        name = "kraken"
    "#;

    #[test]
    fn a_minimal_config_gets_sensible_defaults() {
        let c = Config::from_toml(MINIMAL).unwrap();
        assert_eq!(c.venues.len(), 1);
        assert_eq!(c.rotate_secs, 60);
        assert_eq!(c.queue, 1024);
        assert!(c.status.is_none(), "the status service is opt in");
        // An unlisted symbol means the pair the venue actually quotes, which
        // is not the same pair on every venue.
        assert_eq!(
            c.venues[0].resolve().unwrap().symbols,
            vec![Symbol::new("BTC", "USD")]
        );
    }

    #[test]
    fn each_venue_gets_its_own_archive_directory() {
        let c = Config::from_toml(MINIMAL).unwrap();
        assert_eq!(
            c.archive_for(VenueId::Kraken),
            PathBuf::from("./archive/kraken")
        );
        assert_ne!(
            c.archive_for(VenueId::Kraken),
            c.archive_for(VenueId::Okx),
            "two writers sharing a manifest would interleave into it"
        );
    }

    #[test]
    fn a_venue_listed_twice_is_rejected() {
        let err = Config::from_toml(
            r#"
            archive = "./a"
            [[venue]]
            name = "kraken"
            symbols = ["BTC-USD"]
            [[venue]]
            name = "kraken"
            symbols = ["ETH-USD"]
        "#,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("configured twice"), "{err}");
    }

    #[test]
    fn a_config_with_no_venues_is_rejected() {
        let err = Config::from_toml(r#"archive = "./a""#).unwrap_err();
        assert!(format!("{err}").contains("no venues"), "{err}");
    }

    #[test]
    fn a_misspelled_key_is_rejected_rather_than_ignored() {
        // The failure this prevents is a deployment that silently records
        // something other than what its file says.
        let err = Config::from_toml(
            r#"
            archive = "./a"
            rotate_seconds = 30
            [[venue]]
            name = "kraken"
        "#,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("rotate_seconds"), "{err}");
    }

    #[test]
    fn an_unparseable_symbol_names_itself() {
        let err = Config::from_toml(
            r#"
            archive = "./a"
            [[venue]]
            name = "kraken"
            symbols = ["not a pair"]
        "#,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("not a pair"), "{err}");
    }

    #[test]
    fn book_level_three_reaches_the_venue_config() {
        let c = Config::from_toml(
            r#"
            archive = "./a"
            [[venue]]
            name = "bitstamp"
            book_level = 3
        "#,
        )
        .unwrap();
        assert_eq!(
            c.venues[0].resolve().unwrap().bitstamp_book_level,
            BookLevel::L3
        );
    }

    #[test]
    fn a_book_level_that_does_not_exist_is_rejected() {
        let c = Config::from_toml(
            r#"
            archive = "./a"
            [[venue]]
            name = "bitstamp"
            book_level = 5
        "#,
        );
        assert!(c.is_err());
    }

    #[test]
    fn the_status_service_binds_loopback_unless_told_otherwise() {
        let c = Config::from_toml(
            r#"
            archive = "./a"
            [status]
            [[venue]]
            name = "kraken"
        "#,
        )
        .unwrap();
        assert_eq!(c.status.unwrap().listen, "127.0.0.1:8080");
    }
}
