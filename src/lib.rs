//! tickvault: a full-depth crypto order book recorder.
//!
//! The recorder exists to produce a dataset, and the dataset's only real
//! advantage over the free alternatives is that it is honest about its own
//! holes. Every layer here is built so a gap can be *detected*, *bounded in
//! time*, and *reported*, rather than smoothed over into a book that looks
//! continuous and is not.
//!
//! Reading order, roughly bottom up:
//!
//! - [`fixed`] exact decimals, because checksum validation cannot survive floats
//! - [`book`] the L2 book, its crossed-book invariant, and Kraken's CRC32
//! - [`sequence`] five per-venue loss detectors, and what each cannot prove
//! - [`venue`] the [`venue::Venue`] trait, six implementations, the capabilities
//! - [`gap`] suspect windows and the report they feed
//! - [`session`] the ingest loop that ties them together
//! - [`store`] the Parquet archive, its manifest, and crash recovery
//! - [`reconstruct`] rebuilding a book at an arbitrary instant
//! - [`query`] streaming reads, aggregations, and paced replay
//! - [`features`] point-in-time features: availability time, not event time
//!
//! Reading an archive needs none of the capture half, so `venue`, `transport`,
//! `session`, `recorder` and `pipeline` sit behind the default `record`
//! feature. Turning it off leaves the book, the archive, reconstruction and
//! queries, which is exactly the subset that compiles to wasm for the viewer.

pub mod book;
pub mod clock;
#[cfg(feature = "record")]
pub mod config;
pub mod error;
pub mod features;
pub mod fixed;
pub mod gap;
pub mod latency;
pub mod limits;
#[cfg(feature = "record")]
pub mod pipeline;
pub mod query;
pub mod reconstruct;
#[cfg(feature = "record")]
pub mod recorder;
pub mod sequence;
#[cfg(feature = "record")]
pub mod session;
#[cfg(feature = "record")]
pub mod status;
pub mod store;
#[cfg(feature = "record")]
pub mod supervise;
pub mod symbols;
#[cfg(feature = "record")]
pub mod transport;
pub mod types;
#[cfg(feature = "record")]
pub mod venue;

pub use clock::{Clock, ManualClock, MonotonicClock, Stamp, Timestamps};
pub use error::{Error, Result};
pub use fixed::Fixed;
pub use symbols::SymbolMapper;
pub use types::{BookLevel, Side, Symbol, VenueId, VenueSymbol};
