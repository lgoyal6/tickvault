//! Keeping the archive to a stated size, and saying so.
//!
//! Measured across six venues: about 31 MB an hour, which is 270 GB a year.
//! A recorder meant to run continuously and to publish a dataset cannot simply
//! grow until the disk fills, and a disk that fills is not a graceful failure:
//! the writer starts erroring, the gap report fills with holes, and the cause
//! is a decision nobody made.
//!
//! # Removed is not lost
//!
//! The whole project rests on being able to tell those apart. A file removed by
//! retention is recorded as such in the manifest, with the policy that removed
//! it and when. A consumer looking at an archive that begins on the third of
//! the month can then tell the difference between "that is the window" and
//! "something went wrong before then", which is exactly the distinction the gap
//! report exists to preserve.
//!
//! # What removal costs
//!
//! Dropping the oldest file of a partition can drop the snapshot the deltas
//! after it were applied to. Reconstruction degrades honestly rather than
//! silently: it reports its origin as the first archived row rather than a
//! venue snapshot, so a caller can see the book was built from a partial
//! stream. Retention is not free, and this says what it costs.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::store::manifest::{FileRecord, Manifest, RetentionRecord};

/// How much history to keep. Both bounds are optional and both are applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Drop files whose newest row is older than this many days.
    pub max_age_days: Option<u32>,
    /// Drop the oldest files until the archive fits in this many bytes.
    pub max_bytes: Option<u64>,
}

impl Policy {
    pub fn is_unbounded(&self) -> bool {
        self.max_age_days.is_none() && self.max_bytes.is_none()
    }

    /// The policy in words, for the manifest and for the docs.
    ///
    /// A stated window is part of the dataset's contract, so it is written into
    /// the record rather than left in a config file the consumer never sees.
    pub fn describe(&self) -> String {
        let days = |d: u32| {
            if d == 1 {
                "1 day".to_string()
            } else {
                format!("{d} days")
            }
        };
        match (self.max_age_days, self.max_bytes) {
            (None, None) => "unbounded".to_string(),
            (Some(d), None) => format!("keep {}", days(d)),
            (None, Some(b)) => format!("keep under {}", size(b)),
            (Some(d), Some(b)) => format!("keep {} and under {}", days(d), size(b)),
        }
    }
}

/// A byte count in whatever unit reads sensibly, so a 3 MB budget does not
/// describe itself as "0.0 GB".
fn size(bytes: u64) -> String {
    const UNITS: [(&str, f64); 4] = [("GB", 1e9), ("MB", 1e6), ("kB", 1e3), ("B", 1.0)];
    for (name, scale) in UNITS {
        if bytes as f64 >= scale {
            return format!("{:.1} {name}", bytes as f64 / scale);
        }
    }
    format!("{bytes} B")
}

/// What one pass removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Removed {
    pub files: usize,
    pub rows: u64,
    pub bytes: u64,
    /// The oldest instant still in the archive afterwards.
    pub oldest_kept_wall: Option<i64>,
}

impl std::fmt::Display for Removed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.files == 0 {
            return write!(f, "nothing to remove");
        }
        write!(
            f,
            "removed {} file(s), {} rows, {:.2} MB",
            self.files,
            self.rows,
            self.bytes as f64 / 1e6
        )
    }
}

/// Choose which files a policy would remove, oldest first.
///
/// Separated from the deleting so the decision can be tested without a disk,
/// and so `--dry-run` runs exactly the code that a real pass would.
pub fn select(files: &[FileRecord], policy: Policy, now_wall: i64) -> Vec<FileRecord> {
    if policy.is_unbounded() || files.is_empty() {
        return Vec::new();
    }
    let mut ordered: Vec<FileRecord> = files.to_vec();
    // By the newest row each file holds. A file is only expendable once
    // everything in it has aged out, never because it happened to start early.
    ordered.sort_by_key(|f| f.last_recv_wall);

    let mut doomed: Vec<FileRecord> = Vec::new();
    let mut keep: Vec<&FileRecord> = Vec::new();

    if let Some(days) = policy.max_age_days {
        let cutoff = now_wall - (days as i64) * 86_400 * 1_000_000_000;
        for f in &ordered {
            if f.last_recv_wall < cutoff {
                doomed.push(f.clone());
            } else {
                keep.push(f);
            }
        }
    } else {
        keep.extend(ordered.iter());
    }

    if let Some(budget) = policy.max_bytes {
        let mut total: u64 = keep.iter().map(|f| f.bytes).sum();
        let mut i = 0;
        // Oldest first: the newest data is the data most likely to be wanted.
        while total > budget && i < keep.len() {
            total -= keep[i].bytes;
            doomed.push(keep[i].clone());
            i += 1;
        }
    }
    doomed
}

/// Apply a policy to an archive, deleting files and recording what went.
pub fn enforce(root: impl AsRef<Path>, policy: Policy, now_wall: i64) -> Result<Removed> {
    let root = root.as_ref();
    if policy.is_unbounded() {
        return Ok(Removed::default());
    }
    let mut manifest = Manifest::open(root)?;
    let doomed = select(manifest.files(), policy, now_wall);
    if doomed.is_empty() {
        return Ok(Removed {
            oldest_kept_wall: manifest.files().iter().map(|f| f.first_recv_wall).min(),
            ..Removed::default()
        });
    }

    let mut removed = Removed {
        files: doomed.len(),
        rows: doomed.iter().map(|f| f.rows).sum(),
        bytes: doomed.iter().map(|f| f.bytes).sum(),
        oldest_kept_wall: None,
    };

    // The manifest is written first. A file the manifest still lists but which
    // is gone from disk reads as corruption; a file the manifest has forgotten
    // but which is still on disk is merely litter, and recovery already knows
    // to leave unlisted files alone.
    let reason = if policy.max_age_days.is_some() {
        "aged out of the retention window"
    } else {
        "over the size budget"
    };
    manifest.record_retention(RetentionRecord {
        removed: doomed.iter().map(|f| f.path.clone()).collect(),
        reason: reason.to_string(),
        policy: policy.describe(),
        rows_removed: removed.rows,
        bytes_removed: removed.bytes,
        at_wall: now_wall,
    })?;

    for f in &doomed {
        let path = root.join(&f.path);
        // Already gone is fine: the manifest has forgotten it either way.
        if let Err(e) = std::fs::remove_file(&path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %path.display(), error = %e, "could not remove a retired file");
        }
    }
    manifest.sync_dir()?;

    removed.oldest_kept_wall = manifest.files().iter().map(|f| f.first_recv_wall).min();
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BookLevel, Symbol, VenueId};

    const DAY: i64 = 86_400 * 1_000_000_000;

    fn file(name: &str, last: i64, bytes: u64) -> FileRecord {
        FileRecord {
            path: name.to_string(),
            venue: VenueId::Kraken,
            symbol: Symbol::new("BTC", "USD"),
            date: "2026-08-25".to_string(),
            book_level: BookLevel::L2,
            feed_depth: Some(10),
            rows: 100,
            bytes,
            first_recv_wall: last - 1_000_000_000,
            last_recv_wall: last,
            closed_wall: last,
        }
    }

    #[test]
    fn an_unbounded_policy_removes_nothing() {
        let files = vec![file("a", 0, 1_000), file("b", 100 * DAY, 1_000)];
        assert!(select(&files, Policy::default(), 1_000 * DAY).is_empty());
        assert_eq!(Policy::default().describe(), "unbounded");
    }

    #[test]
    fn files_older_than_the_window_go_and_the_rest_stay() {
        let now = 30 * DAY;
        let files = vec![
            file("old", now - 20 * DAY, 1),
            file("edge", now - 6 * DAY, 1),
            file("fresh", now - DAY, 1),
        ];
        let policy = Policy {
            max_age_days: Some(7),
            max_bytes: None,
        };
        let doomed: Vec<String> = select(&files, policy, now)
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(doomed, vec!["old"]);
    }

    #[test]
    fn a_file_survives_until_everything_in_it_has_aged_out() {
        // Judged by its newest row, not its oldest. A file that starts before
        // the cutoff but runs past it still holds data inside the window.
        let now = 30 * DAY;
        let mut straddling = file("straddling", now - 6 * DAY, 1);
        straddling.first_recv_wall = now - 20 * DAY;
        let policy = Policy {
            max_age_days: Some(7),
            max_bytes: None,
        };
        assert!(select(&[straddling], policy, now).is_empty());
    }

    #[test]
    fn the_byte_budget_drops_the_oldest_first() {
        let now = 10 * DAY;
        let files = vec![
            file("oldest", now - 3 * DAY, 500),
            file("middle", now - 2 * DAY, 500),
            file("newest", now - DAY, 500),
        ];
        let policy = Policy {
            max_age_days: None,
            max_bytes: Some(1_000),
        };
        let doomed: Vec<String> = select(&files, policy, now)
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(
            doomed,
            vec!["oldest"],
            "the newest data is the likeliest wanted"
        );
    }

    #[test]
    fn both_bounds_apply_together() {
        let now = 30 * DAY;
        let files = vec![
            file("ancient", now - 40 * DAY, 900),
            file("old", now - 5 * DAY, 900),
            file("new", now - DAY, 900),
        ];
        let policy = Policy {
            max_age_days: Some(7),
            max_bytes: Some(1_000),
        };
        let doomed: Vec<String> = select(&files, policy, now)
            .into_iter()
            .map(|f| f.path)
            .collect();
        // Age takes "ancient"; the budget then still needs one more gone.
        assert_eq!(doomed, vec!["ancient", "old"]);
    }

    #[test]
    fn a_policy_describes_itself_for_the_record() {
        let p = Policy {
            max_age_days: Some(30),
            max_bytes: Some(50_000_000_000),
        };
        assert_eq!(p.describe(), "keep 30 days and under 50.0 GB");
        // The record is read by people, so a small budget says so in a unit
        // that means something rather than rounding itself to nothing.
        assert_eq!(
            Policy {
                max_age_days: None,
                max_bytes: Some(3_000_000)
            }
            .describe(),
            "keep under 3.0 MB"
        );
        assert_eq!(
            Policy {
                max_age_days: Some(1),
                max_bytes: None
            }
            .describe(),
            "keep 1 day"
        );
    }

    #[test]
    fn enforcing_records_what_went_and_why() {
        // The property that matters: an archive that starts late can explain
        // itself, so nobody has to guess whether the missing days are policy or
        // failure.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let now = 30 * DAY;

        let mut manifest = Manifest::open(root).unwrap();
        for (name, last) in [("old.parquet", now - 20 * DAY), ("new.parquet", now - DAY)] {
            std::fs::write(root.join(name), b"x").unwrap();
            manifest.record_file(file(name, last, 1)).unwrap();
        }
        drop(manifest);

        let policy = Policy {
            max_age_days: Some(7),
            max_bytes: None,
        };
        let removed = enforce(root, policy, now).unwrap();
        assert_eq!(removed.files, 1);
        assert!(!root.join("old.parquet").exists());
        assert!(root.join("new.parquet").exists());

        let reopened = Manifest::open(root).unwrap();
        assert_eq!(reopened.files().len(), 1);
        let record = &reopened.retentions()[0];
        assert_eq!(record.removed, vec!["old.parquet"]);
        assert_eq!(record.policy, "keep 7 days");
        assert!(record.reason.contains("aged out"));
        // And it is not mistaken for data we lost.
        assert!(reopened.truncations().is_empty());
    }

    #[test]
    fn enforcing_twice_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let now = 30 * DAY;
        let mut manifest = Manifest::open(root).unwrap();
        std::fs::write(root.join("old.parquet"), b"x").unwrap();
        manifest
            .record_file(file("old.parquet", now - 20 * DAY, 1))
            .unwrap();
        drop(manifest);

        let policy = Policy {
            max_age_days: Some(7),
            max_bytes: None,
        };
        assert_eq!(enforce(root, policy, now).unwrap().files, 1);
        assert_eq!(enforce(root, policy, now).unwrap().files, 0);
    }
}
