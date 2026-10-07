//! Deterministic research reports. Reports preserve provenance and negative
//! results instead of reducing an experiment to a headline metric.
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use tickvault_experiment::{CodeIdentity, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExclusionSummary {
    pub reason: String,
    pub count: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub value: Option<f64>,
    pub unit: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub report_id: String,
    pub experiment_id: String,
    pub code: CodeIdentity,
    pub dataset_manifests: Vec<String>,
    pub seed: u64,
    pub status: String,
    pub metrics: Vec<Metric>,
    pub exclusions: Vec<ExclusionSummary>,
    pub limitations: Vec<String>,
}
impl Report {
    pub fn json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
    pub fn markdown(&self) -> String {
        let mut out = format!(
            "# Indicator Lab report `{}`\n\nStatus: **{}**\n\nExperiment: `{}`\n\n",
            self.report_id, self.status, self.experiment_id
        );
        out.push_str("## Provenance\n\n");
        out.push_str(&format!("- Seed: `{}`\n- Code commit: `{}`\n- Source digest: `{}`\n- Cargo.lock digest: `{}`\n- Dirty build: `{}`\n", self.seed, self.code.git_commit, self.code.source_sha256, self.code.lock_sha256, self.code.dirty));
        out.push_str("- Dataset manifests:\n");
        for dataset in &self.dataset_manifests {
            out.push_str(&format!("  - `{dataset}`\n"));
        }
        out.push_str("\n## Metrics\n\n| Metric | Value | Unit |\n|---|---:|---|\n");
        for metric in &self.metrics {
            out.push_str(&format!(
                "| {} | {} | {} |\n",
                metric.name,
                metric
                    .value
                    .map_or_else(|| "null".into(), |v| v.to_string()),
                metric.unit
            ));
        }
        out.push_str("\n## Exclusions\n\n");
        if self.exclusions.is_empty() {
            out.push_str("No exclusions were recorded.\n");
        } else {
            for exclusion in &self.exclusions {
                out.push_str(&format!("- {}: {}\n", exclusion.reason, exclusion.count));
            }
        }
        out.push_str("\n## Limitations\n\n");
        if self.limitations.is_empty() {
            out.push_str("None recorded.\n");
        } else {
            for limitation in &self.limitations {
                out.push_str(&format!("- {limitation}\n"));
            }
        }
        out
    }
    pub fn write(&self, directory: &Path) -> Result<()> {
        fs::create_dir_all(directory)?;
        fs::write(
            directory.join(format!("{}.json", self.report_id)),
            self.json()?,
        )?;
        fs::write(
            directory.join(format!("{}.md", self.report_id)),
            self.markdown(),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn markdown_keeps_null_metrics_and_provenance_visible() {
        let report = Report {
            report_id: "r".into(),
            experiment_id: "e".into(),
            code: CodeIdentity {
                git_commit: "g".into(),
                source_sha256: "s".into(),
                lock_sha256: "l".into(),
                dirty: true,
            },
            dataset_manifests: vec!["d".into()],
            seed: 1,
            status: "partial".into(),
            metrics: vec![Metric {
                name: "accuracy".into(),
                value: None,
                unit: "ratio".into(),
            }],
            exclusions: vec![],
            limitations: vec!["no transaction costs".into()],
        };
        let text = report.markdown();
        assert!(text.contains("null"));
        assert!(text.contains("no transaction costs"));
        assert!(text.contains("Dirty build: `true`"));
    }
}
