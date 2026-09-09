//! The frozen experiment.
//!
//! The manifest is committed before any result exists and is never rewritten
//! afterwards. That is the only thing separating a walk-forward evaluation from
//! a search over thresholds, so the loader is deliberately strict: a missing
//! field is an error rather than a default, and an input whose bytes do not
//! hash to the value the manifest declares stops the run.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{SimResult, bail};

/// One recorded archive the experiment is allowed to read.
#[derive(Debug, Clone, Deserialize)]
pub struct Input {
    pub venue: String,
    pub symbol: String,
    pub archive_root: String,
    pub parquet: String,
    pub parquet_sha256: String,
    pub manifest_jsonl: String,
    pub manifest_jsonl_sha256: String,
    pub rows: u64,
    pub feed_depth: Option<usize>,
    pub first_recv_wall_ns: i64,
    pub last_recv_wall_ns: i64,
    /// Which loss detector this venue's identifiers feed.
    pub sequence_scheme: String,
    /// False when the feed publishes neither a sequence number nor a checksum,
    /// so loss on it cannot be detected at all. Such a venue is reported and
    /// kept out of every gate aggregate: unverifiable is not clean.
    pub verifiable: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HashedFile {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Precision {
    pub price: u32,
    pub qty: u32,
}

/// One chronological window, in receive time.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Window {
    pub index: usize,
    pub from_recv_wall_ns: i64,
    pub to_recv_wall_ns: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueWindows {
    pub venue: String,
    pub windows: Vec<Window>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Windows {
    pub warmup_nanos: i64,
    pub window_nanos: i64,
    pub windows_per_venue: usize,
    pub held_out_indices: Vec<usize>,
    pub per_venue: Vec<VenueWindows>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Costs {
    pub taker_fee_bps: f64,
    pub maker_rebate_bps: f64,
    pub extra_slippage_bps: f64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct LatencyLeg {
    pub shift: f64,
    pub mean_excess: f64,
    pub clamp_ms: [f64; 2],
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct LatencyModel {
    pub order_entry_ms: LatencyLeg,
    pub market_data_ms: LatencyLeg,
    pub base_seed: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct RiskLimits {
    pub inventory_limit_base: f64,
    pub position_limit_base: f64,
    pub gross_exposure_limit_quote: f64,
    pub loss_limit_quote: f64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Bootstrap {
    pub block_length: usize,
    pub resamples: usize,
    pub seed: u64,
}

/// A strategy's declared grid and its fixed, unfitted constants.
#[derive(Debug, Clone, Deserialize)]
pub struct StrategySpec {
    pub role: String,
    #[serde(default)]
    pub grid_order: Vec<String>,
    #[serde(default)]
    pub grid: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub fixed: BTreeMap<String, serde_json::Value>,
}

impl StrategySpec {
    /// A grid axis as floats, in the order the manifest lists them.
    pub fn axis_f64(&self, key: &str) -> SimResult<Vec<f64>> {
        let Some(values) = self.grid.get(key) else {
            bail!("manifest grid is missing axis {key}");
        };
        values
            .iter()
            .map(|v| {
                v.as_f64()
                    .ok_or_else(|| crate::SimError(format!("grid axis {key} holds a non-number")))
            })
            .collect()
    }

    /// A grid axis as integers.
    pub fn axis_i64(&self, key: &str) -> SimResult<Vec<i64>> {
        let Some(values) = self.grid.get(key) else {
            bail!("manifest grid is missing axis {key}");
        };
        values
            .iter()
            .map(|v| {
                v.as_i64()
                    .ok_or_else(|| crate::SimError(format!("grid axis {key} holds a non-integer")))
            })
            .collect()
    }

    pub fn fixed_f64(&self, key: &str) -> SimResult<f64> {
        match self.fixed.get(key).and_then(|v| v.as_f64()) {
            Some(v) => Ok(v),
            None => bail!("manifest fixed block is missing {key}"),
        }
    }

    pub fn fixed_i64(&self, key: &str) -> SimResult<i64> {
        match self.fixed.get(key).and_then(|v| v.as_i64()) {
            Some(v) => Ok(v),
            None => bail!("manifest fixed block is missing {key}"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    pub manifest_version: u32,
    pub data_boundary: String,
    pub inputs: Vec<Input>,
    pub input_index: HashedFile,
    pub kraken_checksum_precision: Precision,
    pub windows: Windows,
    pub costs: Costs,
    pub latency_model: LatencyModel,
    pub risk_limits: RiskLimits,
    pub order_size_base: f64,
    pub strategies: BTreeMap<String, StrategySpec>,
    pub baselines: Vec<String>,
    pub candidate: String,
    pub bootstrap: Bootstrap,
}

/// A loaded manifest, its own hash, and the repository root its paths resolve
/// against.
#[derive(Debug, Clone)]
pub struct Loaded {
    pub manifest: Manifest,
    pub sha256: String,
    pub repo_root: PathBuf,
}

/// Hash a file the way `shasum -a 256` does.
pub fn sha256_file(path: &Path) -> SimResult<String> {
    let bytes = std::fs::read(path)
        .map_err(|e| crate::SimError(format!("cannot read {}: {e}", path.display())))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

impl Loaded {
    pub fn open(path: &Path, repo_root: &Path) -> SimResult<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| crate::SimError(format!("cannot read {}: {e}", path.display())))?;
        let manifest: Manifest = serde_json::from_str(&text).map_err(|e| {
            crate::SimError(format!("{} is not a valid manifest: {e}", path.display()))
        })?;
        if manifest.manifest_version != 1 {
            bail!(
                "manifest version {} is not one this build understands",
                manifest.manifest_version
            );
        }
        let sha256 = sha256_file(path)?;
        Ok(Loaded {
            manifest,
            sha256,
            repo_root: repo_root.to_path_buf(),
        })
    }

    /// Check every declared input against the bytes on disk.
    ///
    /// The gate script does this with `shasum` as well. Twice is deliberate: a
    /// result that names a file it did not actually read is the failure this
    /// exists to make impossible, and the evaluator is the half that reads.
    pub fn verify_inputs(&self) -> SimResult<Vec<(String, String)>> {
        let mut checked = Vec::new();
        let mut declared: Vec<(String, String)> = vec![(
            self.manifest.input_index.path.clone(),
            self.manifest.input_index.sha256.clone(),
        )];
        for input in &self.manifest.inputs {
            declared.push((input.parquet.clone(), input.parquet_sha256.clone()));
            declared.push((
                input.manifest_jsonl.clone(),
                input.manifest_jsonl_sha256.clone(),
            ));
        }
        for (relative, expected) in declared {
            let path = self.repo_root.join(&relative);
            let actual = sha256_file(&path)?;
            if actual != expected {
                bail!(
                    "{relative} hashes to {actual}, the manifest declares {expected}. \
                     A result from bytes the manifest does not name is not reproducible."
                );
            }
            checked.push((relative, actual));
        }
        Ok(checked)
    }

    pub fn input(&self, venue: &str) -> SimResult<&Input> {
        match self.manifest.inputs.iter().find(|i| i.venue == venue) {
            Some(i) => Ok(i),
            None => bail!("manifest has no input for venue {venue}"),
        }
    }

    pub fn windows_for(&self, venue: &str) -> SimResult<&[Window]> {
        match self
            .manifest
            .windows
            .per_venue
            .iter()
            .find(|w| w.venue == venue)
        {
            Some(w) => Ok(&w.windows),
            None => bail!("manifest has no windows for venue {venue}"),
        }
    }

    pub fn spec(&self, strategy: &str) -> SimResult<&StrategySpec> {
        match self.manifest.strategies.get(strategy) {
            Some(s) => Ok(s),
            None => bail!("manifest has no strategy named {strategy}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        // The crate sits one level below the repository root.
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("sim/ has a parent")
            .to_path_buf()
    }

    fn loaded() -> Loaded {
        let root = repo_root();
        Loaded::open(&root.join("sim/manifest.json"), &root).expect("the frozen manifest loads")
    }

    #[test]
    fn sha256_matches_a_known_vector() {
        // The empty string, so a wrong implementation cannot pass by accident.
        let dir = std::env::temp_dir().join("tickvault-sim-sha-vector");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty");
        std::fs::write(&path, b"").unwrap();
        assert_eq!(
            sha256_file(&path).unwrap(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(
            sha256_file(&path).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn the_frozen_manifest_names_six_verifiable_or_unverifiable_venues() {
        let loaded = loaded();
        assert_eq!(loaded.manifest.inputs.len(), 6);
        let unverifiable: Vec<&str> = loaded
            .manifest
            .inputs
            .iter()
            .filter(|i| !i.verifiable)
            .map(|i| i.venue.as_str())
            .collect();
        // Bitstamp's diff feed carries neither a sequence number nor a
        // checksum. If that ever silently becomes verifiable, the gate has
        // started averaging in a feed that cannot detect its own loss.
        assert_eq!(unverifiable, vec!["bitstamp"]);
    }

    #[test]
    fn every_declared_input_hashes_to_what_the_manifest_says() {
        let checked = loaded().verify_inputs().expect("inputs verify");
        // Six parquet files, six per-venue manifests, one index.
        assert_eq!(checked.len(), 13);
    }

    #[test]
    fn every_window_lies_inside_the_recording_it_names() {
        let loaded = loaded();
        for input in &loaded.manifest.inputs {
            let windows = loaded.windows_for(&input.venue).unwrap();
            assert_eq!(windows.len(), loaded.manifest.windows.windows_per_venue);
            for w in windows {
                assert!(
                    w.from_recv_wall_ns >= input.first_recv_wall_ns,
                    "{} window {} starts before the recording",
                    input.venue,
                    w.index
                );
                assert!(
                    w.to_recv_wall_ns <= input.last_recv_wall_ns,
                    "{} window {} ends after the recording",
                    input.venue,
                    w.index
                );
                assert_eq!(
                    w.to_recv_wall_ns - w.from_recv_wall_ns,
                    loaded.manifest.windows.window_nanos
                );
            }
        }
    }

    #[test]
    fn the_windows_are_contiguous_and_chronological() {
        let loaded = loaded();
        for input in &loaded.manifest.inputs {
            let windows = loaded.windows_for(&input.venue).unwrap();
            for pair in windows.windows(2) {
                assert_eq!(
                    pair[0].to_recv_wall_ns, pair[1].from_recv_wall_ns,
                    "{} has a hole between windows",
                    input.venue
                );
                assert!(pair[0].index < pair[1].index);
            }
        }
    }

    #[test]
    fn the_first_window_is_never_held_out() {
        let loaded = loaded();
        assert!(!loaded.manifest.windows.held_out_indices.contains(&0));
        assert_eq!(loaded.manifest.windows.held_out_indices, vec![1, 2, 3, 4]);
    }

    #[test]
    fn a_tampered_hash_is_refused() {
        let mut loaded = loaded();
        loaded.manifest.inputs[0].parquet_sha256 = "0".repeat(64);
        let err = loaded.verify_inputs().expect_err("a wrong hash must fail");
        assert!(err.0.contains("the manifest declares"), "{}", err.0);
    }
}
