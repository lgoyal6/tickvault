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

pub mod clock;
pub mod error;
pub mod fixed;
pub mod types;

pub use clock::{Clock, ManualClock, MonotonicClock, Stamp, Timestamps};
pub use error::{Error, Result};
pub use fixed::Fixed;
pub use types::{BookLevel, Side, Symbol, VenueId, VenueSymbol};
