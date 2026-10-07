//! UTC normalization and chronological split admission, before scheduling.
use crate::{CodeIdentity, Result, digest};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tickvault_features::Feature;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    pub start: String,
    pub end: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sampling {
    pub step_ms: u64,
    pub depth: usize,
    pub volatility_returns: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureSpec {
    pub name: Feature,
    #[serde(default)]
    pub parameters: BTreeMap<String, usize>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Labels {
    pub horizon_seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Splits {
    pub kind: String,
    pub embargo_seconds: u64,
    pub train: Window,
    pub validation: Window,
    pub test: Window,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub workers: usize,
    pub memory_mb: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub name: String,
    pub spec_version: u32,
    pub code_version: String,
    pub input_datasets: Vec<String>,
    pub seed: u64,
    pub window: Window,
    pub sampling: Sampling,
    pub features: Vec<FeatureSpec>,
    pub labels: Labels,
    pub splits: Splits,
    pub baselines: Vec<String>,
    pub resources: Resources,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interval {
    pub start_ns: i64,
    pub end_ns: i64,
}
fn instant(s: &str) -> Result<i64> {
    // RFC3339 requires an explicit offset. Signed nanoseconds preserve pre-epoch data.
    DateTime::parse_from_rfc3339(s)?
        .timestamp_nanos_opt()
        .ok_or_else(|| "timestamp outside nanosecond range".into())
}
fn interval(w: &Window) -> Result<Interval> {
    let value = Interval {
        start_ns: instant(&w.start)?,
        end_ns: instant(&w.end)?,
    };
    if value.end_ns <= value.start_ns {
        return Err("empty or inverted window".into());
    }
    Ok(value)
}
#[derive(Debug, Clone, Serialize)]
pub struct ValidatedSpec {
    pub name: String,
    pub spec_version: u32,
    pub datasets: BTreeMap<String, String>,
    pub seed: u64,
    pub window: Interval,
    pub step_ns: i64,
    pub depth: usize,
    pub volatility_returns: usize,
    pub features: BTreeSet<Feature>,
    pub horizon_ns: i64,
    pub embargo_ns: i64,
    pub splits: [Interval; 3],
    pub baselines: BTreeSet<String>,
    pub resources: Resources,
    pub code: CodeIdentity,
}
impl Spec {
    pub fn parse(text: &str) -> Result<Self> {
        Ok(toml::from_str(text)?)
    }
    pub fn validate(
        &self,
        manifests: &BTreeMap<String, String>,
        code: CodeIdentity,
    ) -> Result<ValidatedSpec> {
        if self.spec_version != 1 || self.name.trim().is_empty() {
            return Err("invalid specification version or name".into());
        }
        if self.code_version != "HEAD" && self.code_version != code.git_commit {
            return Err("code version differs from executable".into());
        }
        let mut datasets = BTreeMap::new();
        for reference in &self.input_datasets {
            let (id, version) = reference
                .rsplit_once('@')
                .ok_or("dataset requires @version")?;
            if id.is_empty() || version.parse::<u32>().ok().is_none_or(|v| v == 0) {
                return Err("invalid dataset reference".into());
            }
            let hash = manifests
                .get(reference)
                .ok_or("dataset reference does not resolve")?;
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            {
                return Err("invalid manifest digest".into());
            }
            if datasets.insert(reference.clone(), hash.clone()).is_some() {
                return Err("duplicate dataset".into());
            }
        }
        if datasets.is_empty() {
            return Err("no input datasets".into());
        }
        let window = interval(&self.window)?;
        let splits = [
            interval(&self.splits.train)?,
            interval(&self.splits.validation)?,
            interval(&self.splits.test)?,
        ];
        let step_ns = self
            .sampling
            .step_ms
            .checked_mul(1_000_000)
            .and_then(|n| i64::try_from(n).ok())
            .filter(|n| *n > 0)
            .ok_or("invalid sampling step")?;
        let seconds = |n: u64| {
            n.checked_mul(1_000_000_000)
                .and_then(|n| i64::try_from(n).ok())
                .ok_or("duration overflow")
        };
        let horizon_ns = seconds(self.labels.horizon_seconds)?;
        let embargo_ns = seconds(self.splits.embargo_seconds)?;
        if horizon_ns <= 0 || horizon_ns % step_ns != 0 || embargo_ns < horizon_ns {
            return Err("invalid label horizon or insufficient embargo".into());
        }
        if self.splits.kind != "chronological" {
            return Err("only chronological splits supported".into());
        }
        for split in &splits {
            if split.start_ns < window.start_ns
                || split.end_ns > window.end_ns
                || i128::from(split.end_ns) - i128::from(split.start_ns) <= i128::from(horizon_ns)
            {
                return Err("split outside window or shorter than horizon".into());
            }
            if (i128::from(split.start_ns) - i128::from(window.start_ns)) % i128::from(step_ns) != 0
                || (i128::from(split.end_ns) - i128::from(window.start_ns)) % i128::from(step_ns)
                    != 0
            {
                return Err("split boundaries must follow sampling grid".into());
            }
        }
        for pair in splits.windows(2) {
            if i128::from(pair[1].start_ns) - i128::from(pair[0].end_ns) < i128::from(embargo_ns) {
                return Err("overlapping splits or insufficient separation".into());
            }
        }
        if self.sampling.depth == 0
            || self.sampling.volatility_returns < 2
            || self.sampling.volatility_returns == usize::MAX
        {
            return Err("invalid feature sampling parameters".into());
        }
        let mut features = BTreeSet::new();
        for feature in &self.features {
            if !features.insert(feature.name) {
                return Err("duplicate feature".into());
            }
            for (key, value) in &feature.parameters {
                if key != "depth"
                    || !matches!(
                        feature.name,
                        Feature::OrderBookImbalance | Feature::DepthWeightedPrice
                    )
                    || *value != self.sampling.depth
                {
                    return Err("unsupported or conflicting feature parameter".into());
                }
            }
        }
        let baselines: BTreeSet<_> = self.baselines.iter().cloned().collect();
        if features.is_empty()
            || baselines.is_empty()
            || baselines.len() != self.baselines.len()
            || baselines
                .iter()
                .any(|b| b != "constant" && b != "previous-return")
        {
            return Err("missing features or invalid baselines".into());
        }
        if self.resources.workers == 0
            || self.resources.workers > 256
            || self.resources.memory_mb == 0
            || self.resources.memory_mb.checked_mul(1024 * 1024).is_none()
        {
            return Err("invalid resource limits".into());
        }
        Ok(ValidatedSpec {
            name: self.name.clone(),
            spec_version: 1,
            datasets,
            seed: self.seed,
            window,
            step_ns,
            depth: self.sampling.depth,
            volatility_returns: self.sampling.volatility_returns,
            features,
            horizon_ns,
            embargo_ns,
            splits,
            baselines,
            resources: self.resources.clone(),
            code,
        })
    }
}
impl ValidatedSpec {
    pub fn identity(&self) -> Result<String> {
        Ok(digest(&serde_json::to_vec(self)?))
    }
    pub fn task_identity(&self, reference: &str) -> Result<String> {
        let mut value = serde_json::to_value(self)?;
        let hash = self.datasets.get(reference).ok_or("unknown task dataset")?;
        value["datasets"] = serde_json::to_value(BTreeMap::from([(reference, hash)]))?;
        Ok(digest(&serde_json::to_vec(&value)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn spec() -> Spec {
        let w = |a, b| Window {
            start: format!("2026-01-01T00:00:{a:02}Z"),
            end: format!("2026-01-01T00:00:{b:02}Z"),
        };
        Spec {
            name: "baseline".into(),
            spec_version: 1,
            code_version: "HEAD".into(),
            input_datasets: vec!["tape@1".into()],
            seed: 42,
            window: w(0, 40),
            sampling: Sampling {
                step_ms: 250,
                depth: 10,
                volatility_returns: 4,
            },
            features: vec![FeatureSpec {
                name: Feature::Returns,
                parameters: BTreeMap::new(),
            }],
            labels: Labels { horizon_seconds: 1 },
            splits: Splits {
                kind: "chronological".into(),
                embargo_seconds: 2,
                train: w(0, 10),
                validation: w(12, 22),
                test: w(24, 40),
            },
            baselines: vec!["constant".into()],
            resources: Resources {
                workers: 2,
                memory_mb: 256,
            },
        }
    }
    fn validate(s: &Spec) -> Result<ValidatedSpec> {
        s.validate(
            &BTreeMap::from([
                ("tape@1".into(), "a".repeat(64)),
                ("other@1".into(), "b".repeat(64)),
            ]),
            CodeIdentity::compiled(),
        )
    }
    #[test]
    fn rejects_leakage_timezone_and_overflow() {
        let mut s = spec();
        s.splits.validation.start = s.splits.train.end.clone();
        assert!(validate(&s).is_err());
        let mut s = spec();
        s.window.start = "2026-01-01T00:00:00".into();
        assert!(validate(&s).is_err());
        let mut s = spec();
        s.labels.horizon_seconds = u64::MAX;
        assert!(validate(&s).is_err());
        let mut s = spec();
        s.sampling.step_ms = u64::MAX;
        assert!(validate(&s).is_err());
    }
    #[test]
    fn identities_normalize_offsets_and_reuse_independent_tasks() {
        let s = spec();
        let original = validate(&s).unwrap();
        let mut other = s.clone();
        other.window.start = "2025-12-31T19:00:00-05:00".into();
        assert_eq!(
            original.identity().unwrap(),
            validate(&other).unwrap().identity().unwrap()
        );
        other.input_datasets.push("other@1".into());
        let extended = validate(&other).unwrap();
        assert_ne!(original.identity().unwrap(), extended.identity().unwrap());
        assert_eq!(
            original.task_identity("tape@1").unwrap(),
            extended.task_identity("tape@1").unwrap()
        );
        let mut changed = original.clone();
        changed.code.source_sha256 = "c".repeat(64);
        assert_ne!(original.identity().unwrap(), changed.identity().unwrap());
    }
    #[test]
    fn rejects_unresolved_duplicate_and_unsupported_inputs() {
        let mut s = spec();
        s.input_datasets = vec!["tape".into()];
        assert!(validate(&s).is_err());
        let mut s = spec();
        s.features[0].parameters.insert("magic".into(), 1);
        assert!(validate(&s).is_err());
        let mut s = spec();
        s.input_datasets.push("tape@1".into());
        assert!(validate(&s).is_err());
        let mut s = spec();
        s.code_version = "not-the-build".into();
        assert!(validate(&s).is_err());
    }
}
