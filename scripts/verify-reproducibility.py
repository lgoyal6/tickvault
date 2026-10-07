#!/usr/bin/env python3
"""Check the public experiment CLI using the committed, offline archive.

Legacy captures lack loss-detection attestations. Reproducibility must preserve
that uncertainty rather than manufacture usable performance metrics.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("--bin", required=True)
args = parser.parse_args()
repo = Path(__file__).resolve().parents[1]
binary = str(Path(args.bin).resolve())
archive = repo / "docs/data/kraken"
with tempfile.TemporaryDirectory(prefix="tickvault-repro-") as temporary:
    root = Path(temporary)
    dataset_dir = root / "datasets"
    dataset = dataset_dir / "kraken-demo@v1.json"
    subprocess.run([binary, "dataset", "--archive", str(archive), "--id",
                    "kraken-demo", "--out", str(dataset_dir)], check=True)
    manifest = json.loads(dataset.read_text())
    assert manifest["capability"] is None
    reports = []
    for index in range(2):
        out = root / str(index)
        subprocess.run([binary, "experiment-run", "--spec",
                        str(repo / "examples/imbalance-baseline.toml"),
                        "--archive", str(archive), "--dataset", str(dataset),
                        "--dataset-ref", "kraken-demo@1", "--venue", "kraken",
                        "--symbol", "BTC-USD", "--out", str(out)], check=True)
        reports.append({p.suffix: p.read_bytes() for p in out.iterdir()})
    assert reports[0] == reports[1], "same inputs produced different reports"
    assert set(reports[0]) == {".json", ".md"}
    report = json.loads(reports[0][".json"])
    assert report["dataset_manifests"] == [manifest["manifest_sha256"]]
    assert report["seed"] == 42
    assert report["metrics"] and all(m["value"] is None for m in report["metrics"])
    assert report["limitations"], "legacy uncertainty must be visible"
    for field in ("git_commit", "source_sha256", "lock_sha256"):
        assert report["code"][field] not in ("", "unknown")
    assert b"null" in reports[0][".md"]
print("Reproducibility verified: identical reports, frozen provenance, honest null metrics")
