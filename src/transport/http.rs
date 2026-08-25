//! HTTP, for REST snapshots and instrument metadata.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::venue::HttpFetch;

/// The real client.
#[derive(Debug, Clone)]
pub struct ReqwestFetch {
    client: reqwest::Client,
}

impl ReqwestFetch {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("tickvault/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(ReqwestFetch { client })
    }

    /// Shared handle, since venues hold this behind a trait object.
    pub fn shared() -> Result<Arc<dyn HttpFetch>> {
        Ok(Arc::new(ReqwestFetch::new()?))
    }
}

#[async_trait]
impl HttpFetch for ReqwestFetch {
    async fn get(&self, url: &str) -> Result<Vec<u8>> {
        let response = self.client.get(url).send().await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(Error::Other(format!(
                "GET {url} returned {status}: {}",
                String::from_utf8_lossy(&body[..body.len().min(200)])
            )));
        }
        Ok(body.to_vec())
    }
}

/// Canned responses, so snapshot parsing is tested against real captured
/// payloads without a network or a mock server.
#[derive(Debug, Clone, Default)]
pub struct CannedFetch {
    responses: HashMap<String, Vec<u8>>,
}

impl CannedFetch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Respond to any URL containing `needle` with `body`.
    pub fn on(mut self, needle: &str, body: &str) -> Self {
        self.responses.insert(needle.to_string(), body.into());
        self
    }

    pub fn shared(self) -> Arc<dyn HttpFetch> {
        Arc::new(self)
    }
}

#[async_trait]
impl HttpFetch for CannedFetch {
    async fn get(&self, url: &str) -> Result<Vec<u8>> {
        self.responses
            .iter()
            .find(|(needle, _)| url.contains(needle.as_str()))
            .map(|(_, body)| body.clone())
            .ok_or_else(|| Error::Other(format!("no canned response matches {url}")))
    }
}
