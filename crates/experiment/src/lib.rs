//! Reproducible experiment inputs and leakage-safe specifications.
pub mod coverage;
pub mod dataset;
pub mod executor;
pub mod research;
pub mod spec;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeIdentity {
    pub git_commit: String,
    pub source_sha256: String,
    pub lock_sha256: String,
    pub dirty: bool,
}
impl CodeIdentity {
    /// These values describe the compiled executable, not a later working tree.
    pub fn compiled() -> Self {
        Self {
            git_commit: env!("TICKVAULT_GIT_SHA").into(),
            source_sha256: env!("TICKVAULT_SOURCE_HASH").into(),
            lock_sha256: digest(include_bytes!("../../../Cargo.lock")),
            dirty: env!("TICKVAULT_DIRTY") == "true",
        }
    }
}
