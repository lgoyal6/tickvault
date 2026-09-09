//! A walk-forward strategy evaluator over a recorded tickvault archive.
//!
//! Everything here replays files that are already on disk. Nothing connects to
//! a venue, nothing places an order anywhere, and no money moves. Execution is
//! simulated: fills come from observed level changes and observed crossings of
//! the recorded book, not from a matching engine, and queue position is an
//! approximation throughout, which is why every queue field is named with an
//! `approx_` prefix.
//!
//! The crate is a consumer of the archive rather than part of the recorder, in
//! the same sense as `research/` and `streaming/`. It depends on the library to
//! read rows, rebuild books, judge sequences and hold exact prices; it
//! reimplements none of that.
//!
//! Reading order:
//!
//! - [`manifest`] the frozen experiment. Written and committed before any
//!   result existed, hashes included, because a threshold that can move after
//!   the fact is not a threshold

pub mod manifest;

/// What went wrong. One flat error: this is a batch job that either produces a
/// result or says why it did not.
#[derive(Debug)]
pub struct SimError(pub String);

impl std::fmt::Display for SimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SimError {}

impl From<tickvault::Error> for SimError {
    fn from(e: tickvault::Error) -> Self {
        SimError(e.to_string())
    }
}

impl From<std::io::Error> for SimError {
    fn from(e: std::io::Error) -> Self {
        SimError(e.to_string())
    }
}

impl From<serde_json::Error> for SimError {
    fn from(e: serde_json::Error) -> Self {
        SimError(e.to_string())
    }
}

pub type SimResult<T> = std::result::Result<T, SimError>;

/// Fail with a formatted message.
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::SimError(format!($($arg)*)))
    };
}
