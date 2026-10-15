"""Exercise period_study against valid and corrupt TickVault panel fixtures.
Use tvstudy and manifest checks to verify temporal and byte-level boundaries.
Test costs against fixed arithmetic and preserve existing output directories.
These small fixtures check correctness without establishing market performance.
"""
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import period_study as study
import tvmanifest
import tvstudy


def fixture():
    windows, rows = [], []
    rng = np.random.default_rng(42)
    for day in range(3):
        start = int(pd.Timestamp(f"2026-08-{30 + day:02}" if day < 2 else "2026-09-01", tz="UTC").value)
        label = pd.Timestamp(start, tz="UTC").isoformat().replace("+00:00", "Z")
        windows.append({"from": label, "from_ns": start, "to_ns": start + 240_000_000_000})
        for venue in ["a", "b"]:
            for t in range(240):
                rows.append({"venue": venue, "symbol": "BTC-USD", "window": label,
                             "t_ns": start + t * 1_000_000_000, "last_msg_ns": start + t * 1_000_000_000,
                             "staleness_ms": 0., "suspect_msgs": 0, "mid": 100 + .01 * t + rng.normal(0, .01),
                             "spread_bps": 1., "imb1": rng.normal(), "imb5": rng.normal(), "imb10": rng.normal()})
    protocol = {"windows": windows, "instruments": [["a", "BTC-USD"], ["b", "BTC-USD"]]}
    return pd.DataFrame(rows), protocol


class PeriodTests(unittest.TestCase):
    def test_clock_and_window_valid_control(self):
        panel, protocol = fixture()
        study.validate(panel, protocol)

    def test_coverage_retains_an_entire_missing_venue_date(self):
        panel, protocol = fixture()
        missing = (panel.venue == "b") & (panel.window == protocol["windows"][1]["from"])
        panel = panel[~missing]
        study.validate(panel, protocol)
        result = study.coverage(panel, protocol)
        absent = [r for r in result if r["status"] == "MISSING_WINDOW"]
        self.assertEqual(len(result), 6)
        self.assertEqual(len(absent), 1)
        self.assertEqual(absent[0]["missing_grid_rows"], 240)

    def test_future_asof_corruption_rejected(self):
        panel, protocol = fixture()
        panel.loc[0, "last_msg_ns"] += 1
        with self.assertRaisesRegex(ValueError, "future availability"):
            study.validate(panel, protocol)

    def test_duplicate_and_grid_hole_rejected(self):
        panel, protocol = fixture()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            study.validate(pd.concat([panel, panel.iloc[:1]]), protocol)
        with self.assertRaisesRegex(ValueError, "noncontiguous"):
            study.validate(panel.drop(index=10), protocol)

    def test_invented_window_and_crossed_book_rejected(self):
        panel, protocol = fixture()
        panel.loc[0, "t_ns"] -= 1_000_000_000
        panel.loc[0, "last_msg_ns"] -= 1_000_000_000
        with self.assertRaisesRegex(ValueError, "timestamps"):
            study.validate(panel, protocol)
        panel, protocol = fixture()
        panel.loc[0, "spread_bps"] = -1
        with self.assertRaisesRegex(ValueError, "crossed"):
            study.validate(panel, protocol)

    def test_explicitly_excluded_venue_does_not_block_valid_venue(self):
        panel, protocol = fixture()
        panel.loc[panel.venue == "b", "spread_bps"] = -1.
        protocol["excluded_venues"] = ["b"]
        study.validate(panel, protocol)
        protocol["excluded_venues"] = []
        with self.assertRaisesRegex(ValueError, "crossed"):
            study.validate(panel, protocol)

    def test_date_split_purges_targets_and_keeps_venues_separate(self):
        panel, _ = fixture()
        ready = tvstudy.prepare(panel, [5], lag=1)
        frame = ready[(ready.venue == "a") & ready.use_5]
        for train, test in study.folds(frame, 5, 1, 100):
            self.assertLess(max(train.day), min(test.day))
            self.assertLess(int(train.t_ns.max()) + 6_000_000_000, int(test.t_ns.min()))
            self.assertEqual(set(train.venue) | set(test.venue), {"a"})

    def test_future_rewrite_cannot_change_earlier_model(self):
        panel, _ = fixture()
        ready = tvstudy.prepare(panel, [1], lag=1)
        frame = ready[(ready.venue == "a") & ready.use_1]
        train, test = next(study.folds(frame, 1, 1, 100))
        config = dict(max_iter=5, max_leaf_nodes=3, early_stopping=False, random_state=42)
        first = study.predict(train, test, ["imb1"], 1, config)
        corrupted = frame.copy()
        corrupted.loc[corrupted.day == max(frame.day), ["imb1", "y_1"]] = 1e6
        tr2, te2 = next(study.folds(corrupted, 1, 1, 100))
        second = study.predict(tr2, te2, ["imb1"], 1, config)
        for name in first:
            np.testing.assert_array_equal(first[name], second[name])
        tr2 = train.copy()
        tr2["y_1"] += 100  # Planted training signal must change both models.
        planted = study.predict(tr2, test, ["imb1"], 1, config)
        for name in first:
            self.assertGreater(np.max(np.abs(first[name] - planted[name])), 50)

    def test_costs_nonoverlap_and_no_trade_oracle(self):
        raw = pd.DataFrame({"grid_index": np.arange(10), "prediction": np.ones(10),
                            "target": np.zeros(10), "entry_spread": np.ones(10), "exit_spread": np.ones(10)})
        r = study.economics(raw, 5, .5, 2, 1)
        self.assertEqual(r["opportunities"], 4)
        self.assertEqual(r["mean_net_bps_per_opportunity"], -12)
        self.assertEqual(r["max_drawdown_unit_notional_bps"], 48)
        raw.prediction = 0.
        self.assertEqual(study.economics(raw, 30, .5, 2, 1)["sum_net_unit_notional_bps"], 0)

    def test_crossed_clusters_do_not_count_seconds_as_dates(self):
        rows = [{"day": d, "venue": v, "loss": -2.} for d in ["a", "b"] for v in ["x", "y"]]
        result = study.crossed_interval(rows, "loss", 100, 42)
        self.assertEqual(result["ci95"], [-2., -2.])
        self.assertEqual((result["dates"], result["venues"]), (2, 2))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            study.crossed_interval(rows + rows[:1], "loss", 100, 42)

    def test_missing_and_changed_archive_shards_fail_hash_oracle(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "venue").mkdir()
            file = base / "venue" / "part"
            file.write_bytes(b"original")
            sha, size = tvmanifest.sha256_file(file)
            manifest = {"files": [{"archive": "venue", "path": "part", "sha256": sha, "bytes": size}]}
            self.assertEqual(tvmanifest.check(manifest, base), [])
            file.write_bytes(b"modified")
            self.assertTrue(any("sha256" in p for p in tvmanifest.check(manifest, base)))
            file.unlink()
            self.assertTrue(any("missing" in p for p in tvmanifest.check(manifest, base)))

    def test_existing_results_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel, manifest, protocol = (root / n for n in ["panel", "manifest", "protocol"])
            panel.write_bytes(b"not loaded before output guard")
            manifest.write_text(json.dumps({"files": []}))
            protocol.write_text(json.dumps({"panel_sha256": study.digest(panel),
                                           "manifest": str(manifest), "manifest_sha256": study.digest(manifest),
                                           "source_sha256": {}}))
            out = root / "result"
            out.mkdir()
            (out / "sentinel").write_text("preserve")
            argv = ["period_study.py", "--protocol", str(protocol), "--panel", str(panel),
                    "--base", str(root), "--out", str(out)]
            with patch("sys.argv", argv), self.assertRaises(FileExistsError):
                study.main()
            self.assertEqual((out / "sentinel").read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
