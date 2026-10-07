//! Conservative fixed-grid coverage classification.
//!
//! A checksum on the archive proves bytes were preserved, not that the venue's
//! feed could detect loss. Capability metadata is therefore required before a
//! point can become `verified`; absent that attestation, a clean reconstructed
//! book remains observed-but-unverifiable.
use crate::{Result, dataset::DatasetManifest};
use serde::{Deserialize, Serialize};
use tickvault::store::schema::EventKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageState {
    Verified,
    ObservedUnverifiable,
    Invalid,
    Missing,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoveragePoint {
    pub available_at: i64,
    pub state: CoverageState,
    pub reason: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation {
    pub available_at: i64,
    pub event: EventKind,
    pub suspect: bool,
}

/// Classify a fixed grid from arrival observations. Observations must already
/// be restricted to one venue/symbol and sorted by signed receipt time.
/// No caller-provided boolean can upgrade an archive to verified. Capture
/// capability metadata must first become part of the immutable manifest.
pub fn classify_grid(
    manifest: &DatasetManifest,
    grid: &[i64],
    observations: &[Observation],
) -> Result<Vec<CoveragePoint>> {
    if grid.windows(2).any(|w| w[0] >= w[1]) {
        return Err("coverage grid must be strictly increasing".into());
    }
    if observations
        .windows(2)
        .any(|w| w[0].available_at > w[1].available_at)
    {
        return Err("coverage observations must be sorted".into());
    }
    let mut out = Vec::with_capacity(grid.len());
    let mut cursor = 0usize;
    let mut snapshot_seen = false;
    let mut invalid = false;
    for &at in grid {
        while cursor < observations.len() && observations[cursor].available_at <= at {
            let row = observations[cursor];
            if cursor > 0
                && i128::from(row.available_at) - i128::from(observations[cursor - 1].available_at)
                    > 60_000_000_000
            {
                snapshot_seen = false;
            }
            if manifest.truncations.iter().any(|t| {
                t.lost_to_wall <= row.available_at
                    && cursor > 0
                    && t.lost_to_wall > observations[cursor - 1].available_at
            }) {
                snapshot_seen = false;
            }
            if row.event == EventKind::Snapshot {
                snapshot_seen = true;
                invalid = false;
            }
            if row.suspect {
                invalid = true;
            }
            cursor += 1;
        }
        let previous = cursor.checked_sub(1).map(|i| observations[i].available_at);
        let next = observations.get(cursor).map(|o| o.available_at);
        let outside = previous.is_none() || observations.last().is_none_or(|o| at > o.available_at);
        let silence = previous
            .zip(next)
            .is_some_and(|(a, b)| i128::from(b) - i128::from(a) > 60_000_000_000 && at > a);
        let truncated = manifest
            .truncations
            .iter()
            .any(|t| at < t.lost_to_wall && t.lost_from_wall.is_none_or(|start| at > start));
        let state = if outside || silence || truncated {
            CoverageState::Missing
        } else if invalid {
            CoverageState::Invalid
        } else if !snapshot_seen {
            CoverageState::Unknown
        } else if manifest
            .capability
            .as_ref()
            .is_some_and(|c| c.can_detect_loss)
        {
            CoverageState::Verified
        } else {
            CoverageState::ObservedUnverifiable
        };
        let reason = match state {
            CoverageState::Missing => "no archived row covers this availability instant",
            CoverageState::Invalid => "a suspect interval has not been rebuilt by a clean snapshot",
            CoverageState::Unknown => {
                "rows exist but no establishing snapshot preceded this instant"
            }
            CoverageState::ObservedUnverifiable => {
                "book was established, but capture capability metadata cannot prove feed continuity"
            }
            CoverageState::Verified => "capture capability attestation and clean reconstruction",
        };
        out.push(CoveragePoint {
            available_at: at,
            state,
            reason: reason.into(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::DatasetManifest;
    fn manifest() -> DatasetManifest {
        DatasetManifest {
            id: "x".into(),
            version: 1,
            schema_version: 1,
            files: vec![],
            truncations: vec![],
            clock: "x".into(),
            known_blind_spots: vec![],
            creation_command: "x".into(),
            manifest_sha256: "x".into(),
            capability: None,
        }
    }
    fn verified_manifest() -> DatasetManifest {
        let mut m = manifest();
        m.capability = Some(crate::dataset::CapabilityAttestation {
            adapter_version: "a".into(),
            validator_version: "v".into(),
            can_detect_loss: true,
            scope: "symbol".into(),
        });
        m
    }
    #[test]
    fn preserves_the_five_state_gate_order() {
        let rows = vec![
            Observation {
                available_at: 10,
                event: EventKind::Delta,
                suspect: false,
            },
            Observation {
                available_at: 20,
                event: EventKind::Snapshot,
                suspect: false,
            },
            Observation {
                available_at: 30,
                event: EventKind::Delta,
                suspect: true,
            },
            Observation {
                available_at: 40,
                event: EventKind::Snapshot,
                suspect: false,
            },
        ];
        let points = classify_grid(&manifest(), &[0, 10, 15, 25, 35, 40], &rows).unwrap();
        assert_eq!(
            classify_grid(&verified_manifest(), &[25], &rows).unwrap()[0].state,
            CoverageState::Verified
        );
        assert_eq!(
            points.iter().map(|p| p.state).collect::<Vec<_>>(),
            vec![
                CoverageState::Missing,
                CoverageState::Unknown,
                CoverageState::Unknown,
                CoverageState::ObservedUnverifiable,
                CoverageState::Invalid,
                CoverageState::ObservedUnverifiable
            ]
        );
    }
    #[test]
    fn rejects_unsorted_grid_or_observations() {
        assert!(classify_grid(&manifest(), &[2, 1], &[]).is_err());
        let rows = [
            Observation {
                available_at: 2,
                event: EventKind::Snapshot,
                suspect: false,
            },
            Observation {
                available_at: 1,
                event: EventKind::Delta,
                suspect: false,
            },
        ];
        assert!(classify_grid(&manifest(), &[1], &rows).is_err());
    }
}
