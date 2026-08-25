//! The Parquet archive.
//!
//! Split so each piece has one job: [`schema`] decides what a row is,
//! [`writer`] gets rows onto disk and rotates, [`manifest`] records what is
//! safe to publish, [`recovery`] reconciles that record with reality after a
//! crash, [`reader`] reads it back, and [`compact`] merges the many small
//! files that durability requires into the few large ones that reading wants.
#[cfg(feature = "record")]
pub mod compact;
pub mod manifest;
pub mod reader;
#[cfg(feature = "record")]
pub mod recovery;
#[cfg(feature = "record")]
pub mod retention;
pub mod rows;
pub mod schema;
pub mod writer;
