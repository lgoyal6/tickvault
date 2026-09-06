"""The study: does a book feature available at T predict the mid over the next h?

The design decisions that matter are all defensive, and each of them costs the
result some apparent strength:

* **One clock.** Features and targets are both read off the availability grid
  built by `tvpanel.py`. Nothing is joined on the venue's own timestamp.
* **Chronological folds.** Expanding window by day: train on every earlier day
  of the same instrument, test on the next one. No shuffling, no k-fold, and a
  purge of `h` seconds at the end of training so an overlapping target cannot
  appear on both sides of the split.
* **Baselines that are hard to beat.** A random walk (`mid_{t+h} = mid_t`, so
  the forecast return is exactly zero) and a rolling mean of the last minute of
  mids. Out-of-sample R-squared is quoted against the random walk, which is the
  honest denominator for a price series.
* **Suspect data is not used.** A sample counts only when every message behind
  the whole span it touches, from `t - 30s` to `t + h`, was one the recorder
  could vouch for.

Nothing here places an order, connects to a venue, or claims a profit. The cost
block exists so the size of any measured edge can be compared against what it
would take to act on it, which is the only way a bps number means anything.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

#: The feature sets the study compares, all readable at T.
FEATURE_SETS = {
    # The stated order-book feature on its own. This is the question.
    "imbalance": ["imb1", "imb5", "imb10"],
    # Imbalance plus the touch geometry.
    "book": ["imb1", "imb5", "imb10", "micro_dev_bps", "spread_bps"],
    # Everything, including trailing returns, so the book features have to earn
    # their place next to plain momentum.
    "book_momentum": [
        "imb1",
        "imb5",
        "imb10",
        "micro_dev_bps",
        "spread_bps",
        "ret_1",
        "ret_5",
        "ret_30",
    ],
    # The control group: trailing returns only, no book at all.
    "momentum": ["ret_1", "ret_5", "ret_30"],
}

#: Panel steps of history a row's trailing features reach back over.
LOOKBACK = 30
#: Panel steps of mid history the rolling-mean baseline averages.
ROLL = 60
#: Staleness beyond which the book is not a description of the present.
MAX_STALENESS_MS = 2000.0
#: Moving-block bootstrap. The block spans a minute so that autocorrelation
#: inside a minute survives resampling.
BLOCK_SECONDS = 60
BOOTSTRAP_DRAWS = 2000
BOOTSTRAP_SEED = 20260905


def load(path: str) -> pd.DataFrame:
    return pq.read_table(path).to_pandas()


def prepare(
    panel: pd.DataFrame,
    horizons: list[int],
    lag: int = 0,
    lookback: int = LOOKBACK,
    roll: int = ROLL,
) -> pd.DataFrame:
    """Attach trailing features, forward targets and per-horizon eligibility.

    Every shift below is inside one `(venue, symbol, window)` group, so nothing
    reaches across a window boundary or across a venue.
    """
    out = []
    for (venue, symbol, window), g in panel.groupby(
        ["venue", "symbol", "window"], sort=True
    ):
        g = g.sort_values("t_ns").reset_index(drop=True)
        step = int(np.median(np.diff(g["t_ns"]))) if len(g) > 1 else 1_000_000_000
        # A contiguous grid is assumed by every shift; assert it rather than
        # silently averaging over a hole.
        if len(g) > 1 and not np.all(np.diff(g["t_ns"]) == step):
            raise SystemExit(f"{venue} {window}: sampling grid is not contiguous")
        logmid = np.log(g["mid"].to_numpy(dtype=float))
        g["logmid"] = logmid
        for name, k in (("ret_1", 1), ("ret_5", 5), ("ret_30", lookback)):
            g[name] = (g["logmid"] - g["logmid"].shift(k)) * 1e4
        # The rolling-mean baseline: closed on the left so the mean uses only
        # mids already observed at t.
        g["roll_mid"] = g["mid"].rolling(roll, min_periods=roll).mean()
        g["base_roll"] = np.log(g["roll_mid"] / g["mid"]) * 1e4
        g["base_zero"] = 0.0
        # A run of clean, priced, fresh samples. `clean_run` counts how many
        # consecutive rows up to and including this one were usable.
        ok = (
            (g["suspect_msgs"] == 0)
            & g["mid"].notna()
            & (g["staleness_ms"] <= MAX_STALENESS_MS)
        )
        g["ok"] = ok
        run = np.zeros(len(g), dtype=np.int64)
        count = 0
        for i, good in enumerate(ok.to_numpy()):
            count = count + 1 if good else 0
            run[i] = count
        g["clean_run"] = run
        for h in horizons:
            g[f"y_{h}"] = (g["logmid"].shift(-(h + lag)) - g["logmid"].shift(-lag)) * 1e4
            forward_ok = (
                g["ok"][::-1].rolling(h + lag + 1, min_periods=h + lag + 1).min()[::-1]
            )
            g[f"use_{h}"] = (
                (g["clean_run"] >= max(lookback, roll) + 1)
                & (forward_ok == 1)
                & g[f"y_{h}"].notna()
                & g["base_roll"].notna()
            )
        g["day"] = window.split("T")[0]
        out.append(g)
    return pd.concat(out, ignore_index=True)


#: A candidate column is redundant when the columns already kept explain this
#: much of it. 0.999 is not a tuning knob: it is close enough to 1 that only an
#: algebraic identity clears it.
REDUNDANT_R2 = 0.999


class Fit:
    """An OLS fit, standardised and de-duplicated on the training set alone.

    The de-duplication is here because of a trap this study walked into. The
    microprice deviation is not an independent description of the touch: for a
    two-sided book it is exactly

        (microprice - mid) / mid = (spread / 2) * imb1 / mid

    so `micro_dev_bps` is `spread_bps / 2` times `imb1`. On the four venues
    whose spread is one tick almost always, that makes it a scalar multiple of
    `imb1`, and putting both in one regression puts the same column in twice.
    Plain `lstsq` answered with coefficients of -12.68 and +12.70 that cancel in
    sample and diverge out of it; the first run of this study reported an
    out-of-sample R-squared of **-1523** for OKX on that basis. That is an
    artefact of a rank-deficient design, not a result, and reporting it as a
    result would have been the whole failure mode this package is about.

    So: columns are added in the order given, each one kept only if the columns
    already kept do not already explain it, and whatever is dropped is named in
    the output rather than silently absorbed.
    """

    def __init__(self, x: np.ndarray, y: np.ndarray, names: list[str]):
        mean = x.mean(axis=0)
        sd = x.std(axis=0)
        keep: list[int] = []
        dropped: list[dict] = []
        for j, name in enumerate(names):
            if sd[j] <= 1e-12:
                dropped.append({"feature": name, "why": "constant in training"})
                continue
            z = (x[:, j] - mean[j]) / sd[j]
            if keep:
                basis = np.column_stack(
                    [np.ones(len(x))] + [(x[:, k] - mean[k]) / sd[k] for k in keep]
                )
                coef, *_ = np.linalg.lstsq(basis, z, rcond=None)
                resid = z - basis @ coef
                explained = 1.0 - float(resid @ resid) / float(z @ z)
                if explained > REDUNDANT_R2:
                    dropped.append(
                        {
                            "feature": name,
                            "why": "already explained by kept features",
                            "r2_against_kept": explained,
                        }
                    )
                    continue
            keep.append(j)
        self.keep = np.array(keep, dtype=int)
        self.dropped = dropped
        self.names = [names[j] for j in keep]
        self.mean = mean[self.keep]
        self.sd = sd[self.keep]
        z = (x[:, self.keep] - self.mean) / self.sd
        design = np.column_stack([np.ones(len(z)), z])
        self.beta, *_ = np.linalg.lstsq(design, y, rcond=None)
        # The range the fit actually saw. A linear extrapolation far outside it
        # is not a forecast, it is arithmetic on a number the model has no
        # information about, and on this data it was catastrophic: OKX's spread
        # is one tick for the whole of 30 August, so its training standard
        # deviation is a rounding artefact, and the 0.828 bps spread on 31
        # August arrives as a z-score in the thousands. The unclipped fold
        # scored an out-of-sample R-squared of -107. Predictions are therefore
        # clipped to the training range. The bounds come from training data
        # only, so this adds no look-ahead.
        self.lo = z.min(axis=0)
        self.hi = z.max(axis=0)
        self.clipped = 0
        self.seen = 0

    def predict(self, x: np.ndarray, intercept: bool = True) -> np.ndarray:
        """`intercept=False` answers a separate question: what does the sign of
        the *features* say, with the fitted drift removed. A model whose
        intercept dominates is predicting that the training period's direction
        continues, which is a claim about the market and not about the book."""
        z = (x[:, self.keep] - self.mean) / self.sd
        clipped = np.clip(z, self.lo, self.hi)
        self.clipped += int(np.sum(clipped != z))
        self.seen += int(z.size)
        beta = self.beta if intercept else np.concatenate([[0.0], self.beta[1:]])
        return np.column_stack([np.ones(len(clipped)), clipped]) @ beta

    def coefficients(self) -> dict:
        """Coefficients per training standard deviation of the feature, which
        is the only reading that compares across features in different units."""
        return dict(zip(["intercept"] + self.names, self.beta.tolist()))


def _r2(y: np.ndarray, pred: np.ndarray, reference: np.ndarray) -> float:
    """Out-of-sample R-squared against a reference forecast, not against the
    test-set mean. Using the test mean would hand the model the average return
    of a period it is not supposed to have seen."""
    sse = float(np.sum((y - pred) ** 2))
    ref = float(np.sum((y - reference) ** 2))
    return float("nan") if ref == 0 else 1.0 - sse / ref


def _hac_t(series: np.ndarray, lag: int) -> float:
    """Newey-West t-statistic for a mean, with `lag` overlapping observations."""
    n = len(series)
    if n < 3:
        return float("nan")
    x = series - series.mean()
    gamma0 = float(x @ x) / n
    var = gamma0
    for k in range(1, min(lag, n - 1) + 1):
        gk = float(x[k:] @ x[:-k]) / n
        var += 2.0 * (1.0 - k / (lag + 1.0)) * gk
    if var <= 0:
        return float("nan")
    return float(series.mean() / np.sqrt(var / n))


def _block_bootstrap_ci(series: np.ndarray, block: int, draws: int, seed: int):
    """Moving-block bootstrap CI for the mean of an autocorrelated series."""
    n = len(series)
    if n < block * 2:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    starts = n - block + 1
    blocks = -(-n // block)
    means = np.empty(draws)
    for i in range(draws):
        idx = rng.integers(0, starts, size=blocks)
        take = (idx[:, None] + np.arange(block)[None, :]).ravel()[:n]
        means[i] = series[take].mean()
    return (float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975)))


def evaluate(
    frame: pd.DataFrame,
    horizon: int,
    features: list[str],
    min_train: int = 1,
) -> dict:
    """Expanding-window evaluation of one instrument at one horizon.

    Fold k trains on days 1..k and tests on day k+1. The last `horizon` seconds
    of every training day are purged, because their targets reach into the
    following day.
    """
    frame = frame[frame[f"use_{horizon}"]].sort_values("t_ns")
    days = sorted(frame["day"].unique())
    folds = []
    pooled_y, pooled_pred, pooled_zero, pooled_roll = [], [], [], []
    pooled_nodrift: list[np.ndarray] = []
    for i in range(min_train, len(days)):
        train_days, test_day = days[:i], days[i]
        train = frame[frame["day"].isin(train_days)]
        # Purge: a target that starts inside the training day but ends after the
        # last training sample would overlap the test period on that day.
        train = train.groupby("day", group_keys=False).apply(
            lambda g: g.iloc[: max(len(g) - horizon, 0)], include_groups=False
        )
        test = frame[frame["day"] == test_day]
        if len(train) < 100 or len(test) < 100:
            continue
        xtr = train[features].to_numpy(dtype=float)
        ytr = train[f"y_{horizon}"].to_numpy(dtype=float)
        xte = test[features].to_numpy(dtype=float)
        yte = test[f"y_{horizon}"].to_numpy(dtype=float)
        fit = Fit(xtr, ytr, features)
        in_sample = fit.predict(xtr)
        fit.clipped = fit.seen = 0
        pred = fit.predict(xte)
        zero = test["base_zero"].to_numpy(dtype=float)
        roll = test["base_roll"].to_numpy(dtype=float)
        folds.append(
            {
                "train_days": train_days,
                "test_day": test_day,
                "n_train": int(len(train)),
                "n_test": int(len(test)),
                "r2_vs_zero": _r2(yte, pred, zero),
                "r2_roll_vs_zero": _r2(yte, roll, zero),
                "in_sample_r2": _r2(ytr, in_sample, np.zeros(len(ytr))),
                "dropped_features": fit.dropped,
                "clipped_cells": fit.clipped,
                "clipped_fraction": fit.clipped / max(fit.seen, 1),
                "coef_per_sd": fit.coefficients(),
            }
        )
        pooled_y.append(yte)
        pooled_pred.append(pred)
        pooled_zero.append(zero)
        pooled_roll.append(roll)
        pooled_nodrift.append(fit.predict(xte, intercept=False))

    if not folds:
        return {"folds": [], "pooled": None}

    y = np.concatenate(pooled_y)
    pred = np.concatenate(pooled_pred)
    zero = np.concatenate(pooled_zero)
    roll = np.concatenate(pooled_roll)
    nodrift = np.concatenate(pooled_nodrift)
    edge = np.sign(pred) * y
    moved = y != 0
    hit = float(np.mean(np.sign(pred[moved]) == np.sign(y[moved]))) if moved.any() else float("nan")
    lo, hi = _block_bootstrap_ci(edge, BLOCK_SECONDS, BOOTSTRAP_DRAWS, BOOTSTRAP_SEED)
    return {
        "folds": folds,
        "pooled": {
            "n": int(len(y)),
            "r2_vs_zero": _r2(y, pred, zero),
            "r2_vs_roll": _r2(y, pred, roll),
            "r2_roll_vs_zero": _r2(y, roll, zero),
            "hit_rate": hit,
            "moved_fraction": float(np.mean(moved)),
            "target_sd_bps": float(np.std(y)),
            "gross_edge_bps": float(edge.mean()),
            "gross_edge_hac_t": _hac_t(edge, horizon),
            "gross_edge_ci95": [lo, hi],
            # The strategy that ignores the features and always takes the same
            # side. Any edge below this is the period's drift wearing a model
            # as a hat.
            "drift_bps": float(y.mean()),
            "best_constant_edge_bps": float(abs(y.mean())),
            "pred_positive_fraction": float(np.mean(pred > 0)),
            # The same signal with the fitted intercept removed, so the sign is
            # the features' and not the training period's direction.
            "edge_no_drift_bps": float(np.mean(np.sign(nodrift) * y)),
            "edge_no_drift_hac_t": _hac_t(np.sign(nodrift) * y, horizon),
        },
    }


def costs(frame: pd.DataFrame, taker_fee_bps: float) -> dict:
    """What it would cost to act, per round trip, in the same basis points.

    The half-spread is measured off this panel. The fee is an assumption and is
    labelled as one: no fee schedule was fetched and no account exists.
    """
    spread = frame.loc[frame["ok"], "spread_bps"].to_numpy(dtype=float)
    spread = spread[np.isfinite(spread)]
    if len(spread) == 0:
        return {}
    half = float(np.median(spread)) / 2.0
    return {
        "median_spread_bps": float(np.median(spread)),
        "p90_spread_bps": float(np.quantile(spread, 0.9)),
        "half_spread_bps_measured": half,
        "taker_fee_bps_assumed": taker_fee_bps,
        "round_trip_cost_bps": 2 * half + 2 * taker_fee_bps,
    }


def run(
    panel: pd.DataFrame,
    horizons: list[int],
    feature_sets: dict[str, list[str]],
    taker_fee_bps: float,
    lag: int = 0,
    lookback: int = LOOKBACK,
    roll: int = ROLL,
    min_clean_fraction: float = 0.99,
) -> dict:
    prepared = prepare(panel, horizons, lag, lookback, roll)
    excluded = {}
    kept = []
    for (venue, symbol), g in prepared.groupby(["venue", "symbol"], sort=True):
        clean = float((g["suspect_msgs"] == 0).mean())
        if clean < min_clean_fraction:
            excluded[f"{venue}:{symbol}"] = {
                "reason": "the recorder could not vouch for these rows",
                "clean_fraction": clean,
                "samples": int(len(g)),
            }
            continue
        kept.append(g)
    if not kept:
        raise SystemExit("every instrument was excluded as unverifiable")
    usable = pd.concat(kept, ignore_index=True)

    results = {
        "config": {
            "step_ms": int(panel["t_ns"].sort_values().diff().median() // 1_000_000),
            "horizons_steps": horizons,
            "lag_steps": lag,
            "feature_sets": feature_sets,
            "lookback_steps": lookback,
            "roll_steps": roll,
            "max_staleness_ms": MAX_STALENESS_MS,
            "min_clean_fraction": min_clean_fraction,
            "bootstrap": {
                "block_steps": BLOCK_SECONDS,
                "draws": BOOTSTRAP_DRAWS,
                "seed": BOOTSTRAP_SEED,
            },
        },
        "excluded_instruments": excluded,
        "coverage": [],
        "costs": {},
        "results": {},
    }
    for (venue, symbol), g in usable.groupby(["venue", "symbol"], sort=True):
        per_day = (
            g.groupby("day")
            .agg(samples=("t_ns", "size"), usable=(f"use_{horizons[0]}", "sum"))
            .reset_index()
        )
        results["coverage"].append(
            {
                "venue": venue,
                "symbol": symbol,
                "days": per_day["day"].tolist(),
                "samples_per_day": per_day["samples"].astype(int).tolist(),
                "usable_per_day": per_day["usable"].astype(int).tolist(),
            }
        )
        results["costs"][venue] = costs(g, taker_fee_bps)
        for h in horizons:
            for name, feats in feature_sets.items():
                key = f"{venue}|{symbol}|h{h}|{name}"
                results["results"][key] = evaluate(g, h, feats)
    return results


def summarise(results: dict) -> str:
    lines = []
    lines.append("excluded instruments:")
    if not results["excluded_instruments"]:
        lines.append("  (none)")
    for name, why in results["excluded_instruments"].items():
        lines.append(
            f"  {name}: {why['reason']}, clean={100 * why['clean_fraction']:.2f}% "
            f"of {why['samples']} samples"
        )
    lines.append("")
    header = (
        f"{'venue':11s} {'h':>3s} {'features':14s} {'n':>7s} {'R2 vs RW':>9s} "
        f"{'R2 roll':>9s} {'hit':>7s} {'edge bp':>8s} {'HACt':>6s} "
        f"{'nodrift':>8s} {'const':>8s} {'pred+':>6s}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for key, res in results["results"].items():
        venue, _symbol, h, name = key.split("|")
        p = res["pooled"]
        if p is None:
            lines.append(f"{venue:11s} {h[1:]:>3s} {name:14s} no folds")
            continue
        lines.append(
            f"{venue:11s} {h[1:]:>3s} {name:14s} {p['n']:7d} {p['r2_vs_zero']:9.5f} "
            f"{p['r2_roll_vs_zero']:9.5f} {p['hit_rate']:7.4f} "
            f"{p['gross_edge_bps']:8.5f} {p['gross_edge_hac_t']:6.1f} "
            f"{p['edge_no_drift_bps']:8.5f} {p['best_constant_edge_bps']:8.5f} "
            f"{p['pred_positive_fraction']:6.3f}"
        )
    lines.append("")
    lines.append(f"{'venue':11s} {'spread bp':>10s} {'half':>7s} {'fee*':>6s} {'round trip':>11s}")
    for venue, c in results["costs"].items():
        if not c:
            continue
        lines.append(
            f"{venue:11s} {c['median_spread_bps']:10.4f} "
            f"{c['half_spread_bps_measured']:7.4f} {c['taker_fee_bps_assumed']:6.1f} "
            f"{c['round_trip_cost_bps']:11.4f}"
        )
    lines.append("* fee is an assumption, not a measurement. Nothing was traded.")
    lines.append("")
    lines.append("what it would take to act on the measured edge (feature set 'imbalance'):")
    header = (
        f"{'venue':11s} {'h':>3s} {'gross bp':>9s} {'cost bp':>9s} {'net bp':>9s} "
        f"{'breakeven fee bp/side':>22s}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for key, res in results["results"].items():
        venue, _symbol, h, name = key.split("|")
        if name != "imbalance" or res["pooled"] is None:
            continue
        c = results["costs"].get(venue) or {}
        if not c:
            continue
        gross = res["pooled"]["gross_edge_bps"]
        spread_cost = 2 * c["half_spread_bps_measured"]
        breakeven = (gross - spread_cost) / 2.0
        lines.append(
            f"{venue:11s} {h[1:]:>3s} {gross:9.5f} {c['round_trip_cost_bps']:9.4f} "
            f"{gross - c['round_trip_cost_bps']:9.4f} {breakeven:22.5f}"
        )
    lines.append(
        "breakeven is the per-side fee at which the gross mid-to-mid edge exactly"
    )
    lines.append(
        "covers crossing the measured spread twice. Market impact, queue position,"
    )
    lines.append(
        "partial fills and the fact that the mid is not a tradable price are all"
    )
    lines.append("unmodelled, so the real bar is higher than the number shown.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--panel", required=True)
    ap.add_argument("--horizons", default="1,5,30")
    ap.add_argument(
        "--lag",
        type=int,
        default=0,
        help="panel steps between reading the feature and the target starting, "
        "as a stand-in for decision and network latency",
    )
    ap.add_argument("--lookback", type=int, default=LOOKBACK)
    ap.add_argument("--roll", type=int, default=ROLL)
    ap.add_argument(
        "--taker-fee-bps",
        type=float,
        default=5.0,
        help="assumed taker fee per side; stated in the output as an assumption",
    )
    ap.add_argument("--features", default=",".join(FEATURE_SETS))
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    horizons = [int(x) for x in args.horizons.split(",")]
    sets = {k: FEATURE_SETS[k] for k in args.features.split(",")}
    panel = load(args.panel)
    results = run(
        panel,
        horizons,
        sets,
        args.taker_fee_bps,
        args.lag,
        args.lookback,
        args.roll,
    )
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(results, fh, indent=1)
        fh.write("\n")
    print(summarise(results))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
