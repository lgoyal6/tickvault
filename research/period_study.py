"""Compare fixed linear and nonlinear forecasts on historical TickVault panels.
Reuse tvstudy features and tvmanifest hashes to preserve clocks and provenance.
Retain chronological folds and paired date/venue comparisons for review.
Cost diagnostics use fixed notional round trips without modeling fills or orders.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingRegressor

import tvmanifest
import tvstudy


def digest(path):
    return tvmanifest.sha256_file(Path(path))[0]


def validate(panel, protocol):
    """Reject ambiguous clocks, overlapping identities and invented dates."""
    if panel.empty or panel.duplicated(["venue", "symbol", "t_ns"]).any():
        raise ValueError("empty panel or duplicate venue/symbol/time")
    instruments = {(v, s) for v, s in protocol["instruments"]}
    if set(map(tuple, panel[["venue", "symbol"]].drop_duplicates().values)) != instruments:
        raise ValueError("panel instruments differ from protocol")
    windows = {w["from"][:19] + "Z": w for w in protocol["windows"]}
    if set(panel.window.unique()) != set(windows):
        raise ValueError("panel windows differ from protocol")
    if (panel.last_msg_ns > panel.t_ns).any() or (panel.staleness_ms < 0).any():
        raise ValueError("future availability or negative staleness")
    included = panel[~panel.venue.isin(protocol.get("excluded_venues", []))]
    if (included.spread_bps.dropna() < 0).any():
        raise ValueError("crossed book")
    for (_, _, label), g in panel.groupby(["venue", "symbol", "window"]):
        w = windows[label]
        t = g.t_ns.to_numpy()
        if ((t < w["from_ns"]) | (t >= w["to_ns"])).any():
            raise ValueError("window label disagrees with timestamps")
        if len(t) > 1 and not np.all(np.diff(np.sort(t)) == 1_000_000_000):
            raise ValueError("noncontiguous one-second grid")
        if not np.allclose((g.t_ns - g.last_msg_ns) / 1e6, g.staleness_ms):
            raise ValueError("staleness disagrees with availability")


def folds(frame, horizon, lag, min_rows):
    """Split by date within one venue, purging by actual target end time."""
    days = sorted(frame.day.unique())
    for i, day in enumerate(days[1:], 1):
        test = frame[frame.day == day].copy()
        boundary = int(test.t_ns.min())
        train = frame[(frame.day.isin(days[:i])) &
                      (frame.t_ns + (horizon + lag) * 1_000_000_000 < boundary)].copy()
        if len(train) < min_rows or len(test) < min_rows:
            continue
        yield train, test


def coverage(panel, protocol):
    """Keep every planned venue/window, including windows with zero rows."""
    counts = panel.groupby(["venue", "symbol", "window"]).size().to_dict()
    rows = []
    for venue, symbol in protocol["instruments"]:
        for window in protocol["windows"]:
            label = window["from"][:19] + "Z"
            observed = int(counts.get((venue, symbol, label), 0))
            expected = (window["to_ns"] - window["from_ns"]) // 1_000_000_000
            rows.append({"venue": venue, "symbol": symbol, "window": label,
                         "expected_grid_rows": expected, "observed_rows": observed,
                         "missing_grid_rows": expected - observed,
                         "excluded_venue": venue in protocol.get("excluded_venues", []),
                         "status": "MISSING_WINDOW" if not observed else "OBSERVED"})
    return rows


def predict(train, test, features, horizon, config):
    x = train[features].to_numpy(float)
    y = train[f"y_{horizon}"].to_numpy(float)
    xt = test[features].to_numpy(float)
    linear = tvstudy.Fit(x, y, features)
    nonlinear = HistGradientBoostingRegressor(**config).fit(x, y)
    return {"linear": linear.predict(xt), "nonlinear": nonlinear.predict(xt)}


def score(y, pred):
    error = float(np.mean((y - pred) ** 2))
    reference = float(np.mean(y ** 2))
    var = float(np.var(pred))
    slope = float(np.mean((pred - pred.mean()) * (y - y.mean())) / var) if var > 1e-20 else None
    return {"samples": len(y), "mse": error,
            "r2_vs_zero": 1 - error / reference if reference else None,
            "calibration_slope": slope,
            "calibration_intercept_bps": float(y.mean() - slope * pred.mean()) if slope is not None else None}


def economics(rows, fee, slippage, horizon, lag):
    # Rows retain the original grid offset. Filtering cannot make trades closer.
    chosen = rows[(rows.grid_index % (horizon + lag)) == 0]
    side = np.sign(chosen.prediction.to_numpy())
    gross = side * np.expm1(chosen.target.to_numpy() / 1e4) * 1e4
    cost = (chosen.entry_spread.to_numpy() + chosen.exit_spread.to_numpy()) / 2 + 2 * (fee + slippage)
    net = gross - np.abs(side) * cost
    equity = np.r_[0.0, np.cumsum(net)]
    return {"opportunities": len(net), "round_trips": int(np.count_nonzero(side)),
            "turnover_notional_units": int(2 * np.count_nonzero(side)),
            "mean_net_bps_per_opportunity": float(net.mean()) if len(net) else None,
            "sum_net_unit_notional_bps": float(net.sum()),
            "max_drawdown_unit_notional_bps": float(np.max(np.maximum.accumulate(equity) - equity)),
            "fee_bps_per_side": fee, "slippage_bps_per_side": slippage}


def crossed_interval(rows, metric, draws, seed):
    """Crossed date and venue bootstrap of equal-weight cell means.

    Resample dates as clusters shared by venues, and venues as clusters shared
    by dates. This preserves each axis rather than counting seconds as trials.
    Few days/venues limit the interval's interpretation.
    """
    frame = pd.DataFrame(rows)
    if frame.duplicated(["day", "venue"]).any():
        raise ValueError("duplicate date/venue uncertainty unit")
    table = frame.pivot(index="day", columns="venue", values=metric).to_numpy(float)
    rng = np.random.default_rng(seed)
    samples = []
    for _ in range(draws):
        a = rng.integers(0, table.shape[0], size=table.shape[0])
        b = rng.integers(0, table.shape[1], size=table.shape[1])
        cells = table[np.ix_(a, b)]
        if np.isfinite(cells).any():
            samples.append(float(np.nanmean(cells)))
    return {"mean": float(np.nanmean(table)), "ci95": np.quantile(samples, [.025, .975]).tolist(),
            "dates": table.shape[0], "venues": table.shape[1], "draws": len(samples),
            "missing_cells": int(np.isnan(table).sum()), "unit": "equal-weight date/venue cell"}


def summarize(records, protocol):
    summaries = []
    for h in protocol["horizons_seconds"]:
        for features in protocol["feature_sets"]:
            for model in ["linear", "nonlinear"]:
                rows = [r for r in records if r["horizon"] == h and r["features"] == features and r["model"] == model]
                if not rows:
                    continue
                summaries.append({"horizon": h, "features": features, "model": model,
                                  "folds": len(rows), "r2_range": [min(r["r2_vs_zero"] for r in rows), max(r["r2_vs_zero"] for r in rows)],
                                  "positive_r2_folds": sum(r["r2_vs_zero"] > 0 for r in rows),
                                  "regimes": {label: sum(r["regime"] == label for r in rows) for label in sorted({r["regime"] for r in rows})},
                                  "net_costs": [{"fee_bps_per_side": fee, **crossed_interval(
                                      [{"day": r["day"], "venue": r["venue"],
                                        "net": next(c["mean_net_bps_per_opportunity"] for c in r["costs"] if c["fee_bps_per_side"] == fee)} for r in rows],
                                      "net", protocol["bootstrap_draws"], protocol["seed"])} for fee in protocol["fees_bps_per_side"]]})
    return summaries


def run(panel, protocol, output):
    validate(panel, protocol)
    horizons, lag = protocol["horizons_seconds"], protocol["lag_seconds"]
    prepared = tvstudy.prepare(panel, horizons, lag)
    # Add execution-cost observations within each availability-time window.
    for _, index in prepared.groupby(["venue", "symbol", "window"]).groups.items():
        g = prepared.loc[index]
        prepared.loc[index, "grid_index"] = np.arange(len(g))
        prepared.loc[index, "entry_spread"] = g.spread_bps.shift(-lag)
        for h in horizons:
            prepared.loc[index, f"exit_spread_{h}"] = g.spread_bps.shift(-(lag + h))
    records, predictions, excluded, skipped = [], [], [], []
    for (venue, symbol), frame in prepared.groupby(["venue", "symbol"], sort=True):
        clean = float((frame.suspect_msgs == 0).mean())
        if venue in protocol["excluded_venues"] or clean < protocol["min_clean_fraction"]:
            excluded.append({"venue": venue, "symbol": symbol, "clean_fraction": clean,
                             "crossed_book_rows": int((frame.spread_bps < 0).sum()),
                             "reason": "unverifiable venue or insufficient clean fraction"})
            continue
        for h in horizons:
            eligible = frame[frame[f"use_{h}"]].copy()
            required = sorted({f for fs in protocol["feature_sets"].values() for f in fs})
            use = eligible[np.isfinite(eligible[required + [f"y_{h}", "entry_spread", f"exit_spread_{h}"]]).all(axis=1)]
            for name, features in protocol["feature_sets"].items():
                seen_days = []
                for train, test in folds(use, h, lag, protocol["min_rows"]):
                    day = test.day.iloc[0]
                    seen_days.append(day)
                    train_vol = train.groupby("day").ret_1.std().median()
                    regime = "higher_prior_volatility" if test.ret_1.std() > train_vol else "lower_prior_volatility"
                    for model, pred in predict(train, test, features, h, protocol["nonlinear"]).items():
                        raw = pd.DataFrame({"t_ns": test.t_ns.to_numpy(), "target": test[f"y_{h}"].to_numpy(),
                                            "prediction": pred, "entry_spread": test.entry_spread.to_numpy(),
                                            "exit_spread": test[f"exit_spread_{h}"].to_numpy(),
                                            "grid_index": test.grid_index.to_numpy(),
                                            "rolling_prediction": test.base_roll.to_numpy()})
                        identity = {"venue": venue, "symbol": symbol, "day": day,
                                    "horizon": h, "features": name, "model": model, "regime": regime}
                        raw = raw.assign(**identity)
                        predictions.append(raw)
                        records.append({**identity, "train_days": sorted(train.day.unique()),
                                        "max_training_target_ns": int(train.t_ns.max()) + (h + lag) * 1_000_000_000,
                                        "test_start_ns": int(test.t_ns.min()),
                                        "train_rows": len(train), **score(raw.target.to_numpy(), pred),
                                        "rolling_mse": float(np.mean((raw.target - raw.rolling_prediction) ** 2)),
                                        "costs": [economics(raw, fee, protocol["slippage_bps_per_side"], h, lag)
                                                  for fee in protocol["fees_bps_per_side"]]})
                for day in sorted(set(frame.day.unique()) - set(seen_days) - {min(frame.day)}):
                    skipped.append({"venue": venue, "day": day, "horizon": h, "features": name,
                                    "reason": "insufficient eligible training or test rows"})
    if not records:
        raise ValueError("no eligible folds")
    pq.write_table(pa.Table.from_pandas(pd.concat(predictions, ignore_index=True), preserve_index=False), output / "predictions.parquet")
    comparisons = []
    for h in horizons:
        for features in protocol["feature_sets"]:
            subset = [r for r in records if r["horizon"] == h and r["features"] == features]
            units = {}
            for r in subset:
                units.setdefault((r["day"], r["venue"]), {})[r["model"]] = r
            paired = [{"day": d, "venue": v, "mse_delta": x["nonlinear"]["mse"] - x["linear"]["mse"]}
                      for (d, v), x in units.items()]
            comparisons.append({"horizon": h, "features": features,
                                "nonlinear_minus_linear_mse": crossed_interval(paired, "mse_delta", protocol["bootstrap_draws"], protocol["seed"])})
    return {"folds": records, "excluded": excluded, "skipped": skipped, "comparisons": comparisons,
            "coverage": coverage(panel, protocol),
            "summary": summarize(records, protocol),
            "multiplicity": "All cells retained; intervals are descriptive, unadjusted, with no significance or winner claim.",
            "limits": protocol["limitations"], "protocol_sha256": protocol["_sha256"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    protocol = json.loads(args.protocol.read_text())
    protocol["_sha256"] = digest(args.protocol)
    if digest(args.panel) != protocol["panel_sha256"]:
        raise ValueError("panel hash changed after protocol freeze")
    if digest(protocol["manifest"]) != protocol["manifest_sha256"]:
        raise ValueError("archive manifest changed after freeze")
    for name, sha in protocol["source_sha256"].items():
        if digest(Path(__file__).parent / name) != sha:
            raise ValueError(f"study source changed after freeze: {name}")
    problems = tvmanifest.check(json.loads(Path(protocol["manifest"]).read_text()), args.base)
    if problems:
        raise ValueError(f"archive integrity failed: {problems[:3]}")
    args.out.mkdir(parents=True, exist_ok=False)
    result = run(tvstudy.load(str(args.panel)), protocol, args.out)
    (args.out / "result.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(f"Retained {len(result['folds'])} model folds, {len(result['comparisons'])} matched comparisons; excluded {len(result['excluded'])} instruments")


if __name__ == "__main__":
    main()
