//! Fixed-grid book replay and labels confined to chronological split boundaries.
use crate::{
    Result,
    coverage::{CoverageState, Observation, classify_grid},
    dataset::VerifiedDataset,
    spec::ValidatedSpec,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};
use tickvault::{
    book::replay::BookReplayer,
    store::{rows::MessageStream, schema::EventKind},
    types::{Symbol, VenueId},
};
use tickvault_features::{Engine, Feature, Point, Verification};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub point: Point,
    pub split: Option<String>,
    pub label: Option<f64>,
    pub label_exclusion: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineMetric {
    pub split: String,
    pub baseline: String,
    pub evaluated: u64,
    pub mean_absolute_error: Option<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchResult {
    pub dataset_sha256: String,
    pub samples: Vec<Sample>,
    pub baselines: Vec<BaselineMetric>,
    pub exclusions: BTreeMap<String, u64>,
    pub limitations: Vec<String>,
}

pub fn grid(spec: &ValidatedSpec) -> Result<Vec<i64>> {
    if spec.step_ns <= 0 || spec.window.end_ns <= spec.window.start_ns {
        return Err("invalid sampling grid".into());
    }
    let count = (i128::from(spec.window.end_ns) - i128::from(spec.window.start_ns)
        + i128::from(spec.step_ns)
        - 1)
        / i128::from(spec.step_ns);
    // Admission bounds output storage before allocating it. This is a
    // conservative estimate, not an operating-system memory limit.
    if count > i128::from(spec.resources.memory_mb) * 1024 * 1024 / 4096 {
        return Err("sampling grid exceeds memory admission budget".into());
    }
    (0..count)
        .map(|i| {
            i64::try_from(i128::from(spec.window.start_ns) + i * i128::from(spec.step_ns))
                .map_err(Into::into)
        })
        .collect()
}
fn verification(state: CoverageState) -> Verification {
    match state {
        CoverageState::Verified => Verification::Verified,
        CoverageState::ObservedUnverifiable => Verification::ObservedUnverifiable,
        CoverageState::Invalid => Verification::Invalid,
        CoverageState::Missing => Verification::Missing,
        CoverageState::Unknown => Verification::Unknown,
    }
}

/// Replays only private, checksum-verified files. Every message is applied as
/// a whole, and no message received after a grid instant can affect that sample.
pub fn run(
    dataset: &VerifiedDataset,
    reference: &str,
    venue: VenueId,
    symbol: &Symbol,
    spec: &ValidatedSpec,
    cancel: &AtomicBool,
) -> Result<ResearchResult> {
    if spec.datasets.get(reference) != Some(&dataset.manifest().manifest_sha256) {
        return Err("experiment dataset digest does not match opened snapshot".into());
    }
    let files: Vec<_> = dataset
        .manifest()
        .files
        .iter()
        .filter(|f| f.record.venue == venue && &f.record.symbol == symbol)
        .collect();
    let first = files
        .first()
        .ok_or("dataset does not contain requested partition")?;
    if files.iter().any(|f| {
        f.record.book_level != first.record.book_level
            || f.record.feed_depth != first.record.feed_depth
    }) {
        return Err("partition changes book level or depth; create separate datasets".into());
    }
    let grid = grid(spec)?;
    let end = spec.window.end_ns;
    let mut observations = Vec::new();
    // First pass determines coverage, including entire long silence intervals.
    // It holds message metadata, not all rows or reconstructed books.
    for message in MessageStream::new(dataset.rows(venue, symbol, end)) {
        if cancel.load(Ordering::Acquire) {
            return Err("experiment cancelled".into());
        }
        let message = message?;
        let row = message.first().ok_or("empty archive message")?;
        observations.push(Observation {
            available_at: row.recv_wall,
            event: row.event,
            suspect: message.iter().any(|r| r.suspect),
        });
        if observations.len() as u64 > spec.resources.memory_mb * 1024 * 1024 / 128 {
            return Err("observation metadata exceeds memory admission budget".into());
        }
    }
    let mut partition = dataset.manifest().clone();
    partition.truncations.retain(|t| {
        t.venue.is_none_or(|v| v == venue) && t.symbol.as_ref().is_none_or(|s| s == symbol)
    });
    let coverage = classify_grid(&partition, &grid, &observations)?;
    let mut stream = MessageStream::new(dataset.rows(venue, symbol, end));
    let mut next = stream.next().transpose()?;
    let mut replay = BookReplayer::new(
        symbol.clone(),
        first.record.book_level,
        first.record.feed_depth,
    );
    let mut engine = Engine::new(spec.depth, spec.volatility_returns).map_err(|e| e.to_string())?;
    let mut samples = Vec::with_capacity(grid.len());
    for coverage in coverage {
        if cancel.load(Ordering::Acquire) {
            return Err("experiment cancelled".into());
        }
        while next
            .as_ref()
            .is_some_and(|message| message[0].recv_wall <= coverage.available_at)
        {
            let message = next.take().unwrap();
            if message[0].event == EventKind::Snapshot || !message.iter().any(|r| r.suspect) {
                replay.apply_message(&message);
            }
            next = stream.next().transpose()?;
        }
        let mut point = engine
            .sample(
                coverage.available_at,
                verification(coverage.state),
                replay.book_ref(),
                None,
            )
            .map_err(|e| e.to_string())?;
        // Mid-price is retained internally to form labels, even when omitted
        // from the requested output feature set. It remains null on bad data.
        let split = spec
            .splits
            .iter()
            .zip(["train", "validation", "test"])
            .find(|(s, _)| point.available_at >= s.start_ns && point.available_at < s.end_ns)
            .map(|(_, name)| name.to_owned());
        point
            .exclusions
            .retain(|feature, _| spec.features.contains(feature) || *feature == Feature::MidPrice);
        samples.push(Sample {
            point,
            split,
            label: None,
            label_exclusion: None,
        });
    }
    attach_labels(&mut samples, spec)?;
    let baselines = baseline_metrics(&samples, spec);
    let mut exclusions = BTreeMap::new();
    for sample in &mut samples {
        for reason in sample.point.exclusions.values() {
            *exclusions.entry(format!("{reason:?}")).or_insert(0) += 1;
        }
        if let Some(reason) = &sample.label_exclusion {
            *exclusions.entry(reason.clone()).or_insert(0) += 1;
        }
        sample
            .point
            .values
            .retain(|feature, _| spec.features.contains(feature));
        sample
            .point
            .exclusions
            .retain(|feature, _| spec.features.contains(feature));
    }
    Ok(ResearchResult { dataset_sha256: dataset.manifest().manifest_sha256.clone(), samples, baselines, exclusions, limitations: vec!["No transaction-cost or execution model; these are prediction baselines, not profitability estimates.".into(), "Capture validation-capability metadata is absent in legacy archives. Unverified observations cannot yield numeric research features or labels.".into(), "Trade imbalance remains null unless classified trade-volume support is available.".into()] })
}

fn attach_labels(samples: &mut [Sample], spec: &ValidatedSpec) -> Result<()> {
    let horizon = usize::try_from(spec.horizon_ns / spec.step_ns)?;
    let mut prefix = vec![0usize];
    for sample in samples.iter() {
        prefix.push(
            prefix.last().unwrap()
                + usize::from(
                    sample.point.verification != Verification::Verified
                        || sample
                            .point
                            .values
                            .get(&Feature::MidPrice)
                            .copied()
                            .flatten()
                            .is_none(),
                ),
        );
    }
    for i in 0..samples.len() {
        let Some(j) = i.checked_add(horizon).filter(|j| *j < samples.len()) else {
            samples[i].label_exclusion = Some("label horizon outside recorded grid".into());
            continue;
        };
        if samples[i].split.is_none() || samples[i].split != samples[j].split {
            samples[i].label_exclusion = Some("label crosses a split or embargo boundary".into());
            continue;
        }
        if prefix[j + 1] != prefix[i] {
            samples[i].label_exclusion =
                Some("label interval contains unverified or missing data".into());
            continue;
        }
        let a = samples[i].point.values[&Feature::MidPrice].unwrap();
        let b = samples[j].point.values[&Feature::MidPrice].unwrap();
        let value = b.ln() - a.ln();
        if a > 0.0 && b > 0.0 && value.is_finite() {
            samples[i].label = Some(value);
        } else {
            samples[i].label_exclusion = Some("invalid label price".into());
        }
    }
    Ok(())
}
fn baseline_metrics(samples: &[Sample], spec: &ValidatedSpec) -> Vec<BaselineMetric> {
    let mut metrics = Vec::new();
    for split in ["train", "validation", "test"] {
        for baseline in &spec.baselines {
            let mut count = 0u64;
            let mut mean = 0.0;
            for sample in samples.iter().filter(|s| s.split.as_deref() == Some(split)) {
                let prediction = if baseline == "constant" {
                    Some(0.0)
                } else {
                    sample
                        .point
                        .values
                        .get(&Feature::Returns)
                        .copied()
                        .flatten()
                };
                if let Some((prediction, label)) = prediction.zip(sample.label) {
                    count += 1;
                    mean += ((prediction - label).abs() - mean) / count as f64;
                }
            }
            metrics.push(BaselineMetric {
                split: split.into(),
                baseline: baseline.clone(),
                evaluated: count,
                mean_absolute_error: (count > 0).then_some(mean),
            });
        }
    }
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CodeIdentity, spec::Spec};
    fn specification() -> ValidatedSpec {
        let text = include_str!("../../../examples/imbalance-baseline.toml");
        Spec::parse(text)
            .unwrap()
            .validate(
                &BTreeMap::from([("kraken-demo@1".into(), "a".repeat(64))]),
                CodeIdentity::compiled(),
            )
            .unwrap()
    }
    #[test]
    fn checked_in_example_parses_and_normalizes() {
        let spec = specification();
        assert_eq!(spec.baselines.len(), 2);
        assert!(
            grid(&spec)
                .unwrap()
                .windows(2)
                .all(|w| w[1] - w[0] == 250_000_000)
        );
    }
    #[test]
    fn labels_do_not_cross_splits_or_gaps() {
        let mut spec = specification();
        spec.step_ns = 1;
        spec.horizon_ns = 2;
        let mut samples: Vec<_> = (0..8)
            .map(|i| Sample {
                point: Point {
                    available_at: i,
                    verification: Verification::Verified,
                    values: BTreeMap::from([(Feature::MidPrice, Some(100.0 + i as f64))]),
                    exclusions: BTreeMap::new(),
                },
                split: Some(if i < 4 { "train" } else { "test" }.into()),
                label: None,
                label_exclusion: None,
            })
            .collect();
        samples[5].point.verification = Verification::Missing;
        attach_labels(&mut samples, &spec).unwrap();
        assert!(samples[0].label.is_some());
        assert!(samples[2].label.is_none());
        assert!(samples[4].label.is_none());
        assert!(samples[6].label.is_none());
    }
}
