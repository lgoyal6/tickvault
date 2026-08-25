//! Error type.
//!
//! Protocol failures carry the venue and a snippet of the offending payload.
//! When a venue quietly changes its schema at 3am, the log line has to be
//! enough to diagnose it without a reproduction.

use crate::types::VenueId;

/// Longest payload excerpt attached to a protocol error.
const SNIPPET_LIMIT: usize = 240;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("decimal error: {0}")]
    Decimal(#[from] crate::fixed::ParseFixedError),

    #[cfg(feature = "record")]
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[cfg(feature = "record")]
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("{venue}: cannot parse frame ({reason}); payload began {snippet:?}")]
    Protocol {
        venue: VenueId,
        reason: String,
        snippet: String,
    },

    #[error("{venue}: subscription rejected ({reason})")]
    Subscription { venue: VenueId, reason: String },

    #[error("{venue}: does not list symbol {symbol}")]
    UnknownSymbol { venue: VenueId, symbol: String },

    #[error("{venue}: feed closed by peer")]
    FeedClosed { venue: VenueId },

    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Build a protocol error, truncating the payload to something loggable.
    pub fn protocol(venue: VenueId, reason: impl Into<String>, payload: &[u8]) -> Self {
        let text = String::from_utf8_lossy(payload);
        let snippet = if text.len() > SNIPPET_LIMIT {
            // Truncate on a char boundary so the log line stays valid UTF-8.
            let mut end = SNIPPET_LIMIT;
            while end > 0 && !text.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}...", &text[..end])
        } else {
            text.into_owned()
        };
        Error::Protocol {
            venue,
            reason: reason.into(),
            snippet,
        }
    }

    /// True when reconnecting is a plausible response. A schema mismatch is
    /// not: reconnecting into the same bad parse just spins.
    pub fn is_transient(&self) -> bool {
        #[cfg(feature = "record")]
        {
            matches!(
                self,
                Error::Io(_) | Error::WebSocket(_) | Error::Http(_) | Error::FeedClosed { .. }
            )
        }
        // Without the capture half there is no socket to reconnect, so the
        // transport-shaped variants do not exist to match on.
        #[cfg(not(feature = "record"))]
        {
            matches!(self, Error::Io(_) | Error::FeedClosed { .. })
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_snippet_is_truncated_on_a_char_boundary() {
        let payload = "é".repeat(400);
        let err = Error::protocol(VenueId::Kraken, "unterminated", payload.as_bytes());
        let Error::Protocol { snippet, .. } = err else {
            panic!("wrong variant")
        };
        assert!(snippet.ends_with("..."));
        assert!(snippet.len() <= SNIPPET_LIMIT + 3);
    }

    #[test]
    fn schema_failures_are_not_transient() {
        assert!(!Error::protocol(VenueId::Coinbase, "no such field", b"{}").is_transient());
        assert!(
            Error::FeedClosed {
                venue: VenueId::Coinbase
            }
            .is_transient()
        );
    }
}
