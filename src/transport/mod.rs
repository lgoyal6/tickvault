//! Sources of raw frames.
//!
//! A live socket and a replayed recording implement the same
//! [`crate::venue::FeedTransport`], so the gate exercises the identical ingest
//! path the recorder runs. A test harness that reimplemented the loop would
//! only prove the harness works.

pub mod http;
pub mod replay;
pub mod ws;

pub use http::{CannedFetch, ReqwestFetch};
pub use replay::ReplayTransport;
pub use ws::WsTransport;
