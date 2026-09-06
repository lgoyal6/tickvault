"""Two controls, because a study that only ever reports its own result is a
study with no way of being wrong.

**The positive control** asks whether the harness can see anything at all. A
signal of a known size is planted in the target and the same evaluation is run
unchanged; if it does not come back out at roughly the size it went in, a null
result would mean nothing, because a broken harness returns a null for every
question. Three strengths are planted, including zero, and the zero case has to
come back at zero.

**The look-ahead control** asks whether the pipeline can see the future. It is
run in two places, because they can fail independently:

* on the **feature extractor**, by rewriting the archive after an instant T -
  not deleting it, rewriting it, so the post-T book is materially different -
  and demanding that every panel row at or before T is bit-identical, including
  the book digest, which is a hash of every level on both sides. The rewrite has
  to actually change the post-T panel too, or the test proves nothing.
* on the **model**, by corrupting every panel row after a split boundary and
  demanding that the predictions for the earlier rows hash to the same value.

Both are run against real archives and a real study, not a fixture.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import shutil
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import tvmanifest
import tvpanel
import tvstudy


# ----------------------------------------------------------------------------
# Positive control: plant a signal of known size and see whether it comes back.
# ----------------------------------------------------------------------------


def plant(
    panel: pd.DataFrame,
    horizon: int,
    feature: str,
    target_r2: float,
    seed: int,
) -> pd.DataFrame:
    """Replace the target with `beta * feature + noise` at a known R-squared.

    The feature is the real one, with its real time alignment, so a harness that
    is accidentally misaligning features and targets by a row will fail this as
    surely as one that cannot fit at all.
    """
    out = panel.copy()
    rng = np.random.default_rng(seed)
    x = out[feature].to_numpy(dtype=float)
    x = np.nan_to_num(x, nan=0.0)
    sd = x.std()
    if sd == 0:
        raise SystemExit(f"{feature} is constant; nothing to plant against")
    z = (x - x.mean()) / sd
    if target_r2 <= 0:
        signal = np.zeros(len(z))
        noise = rng.standard_normal(len(z))
    else:
        # var(signal) / (var(signal) + var(noise)) == target_r2
        signal = z
        noise = rng.standard_normal(len(z)) * np.sqrt((1 - target_r2) / target_r2)
    # A basis-point scale close to the real targets, so nothing overflows or
    # underflows differently from the genuine run.
    out[f"planted_{horizon}"] = (signal + noise) * 0.1
    return out


def positive_control(
    panel: pd.DataFrame,
    horizon: int,
    feature: str,
    strengths: list[float],
    seed: int,
) -> list[dict]:
    prepared = tvstudy.prepare(panel, [horizon])
    prepared = prepared[prepared["venue"] != "bitstamp"]
    rows = []
    for r2 in strengths:
        planted = plant(prepared, horizon, feature, r2, seed)
        planted[f"y_{horizon}"] = planted[f"planted_{horizon}"]
        recovered = []
        for (venue, _symbol), g in planted.groupby(["venue", "symbol"], sort=True):
            res = tvstudy.evaluate(g, horizon, [feature])
            if res["pooled"] is not None:
                recovered.append((venue, res["pooled"]["r2_vs_zero"]))
        rows.append(
            {
                "planted_r2": r2,
                "feature": feature,
                "per_venue_recovered_r2": {v: r for v, r in recovered},
                "median_recovered_r2": float(np.median([r for _, r in recovered])),
            }
        )
    return rows


# ----------------------------------------------------------------------------
# Look-ahead control, part one: rewrite the archive's future.
# ----------------------------------------------------------------------------


def rewrite_future(
    src: pathlib.Path,
    dst: pathlib.Path,
    cut_ns: int,
    qty_multiplier: int = 7,
) -> dict:
    """Copy an archive, multiplying every quantity in every file that starts at
    or after `cut_ns`.

    Quantities rather than prices, so the book stays well formed and the change
    lands squarely on the imbalance features. The manifest is rewritten with the
    new byte counts, because the archive checks them and a mismatch would fail
    for the wrong reason.
    """
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)
    entries = tvmanifest.read_archive_manifest(src)
    changed = 0
    rewritten = []
    for entry in entries:
        source = src / entry["path"]
        target = dst / entry["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        if entry["first_recv_wall"] < cut_ns:
            shutil.copy2(source, target)
            rewritten.append(entry)
            continue
        table = pq.read_table(source)
        qty = table.column("qty").to_numpy(zero_copy_only=False) * qty_multiplier
        field = table.schema.field("qty")
        table = table.set_column(
            table.schema.get_field_index("qty"),
            field,
            pa.array(qty, type=field.type),
        )
        pq.write_table(table, target, compression="zstd")
        entry = dict(entry, bytes=target.stat().st_size)
        rewritten.append(entry)
        changed += 1
    with (dst / "_manifest.jsonl").open("w") as fh:
        for entry in rewritten:
            fh.write(json.dumps(entry, separators=(",", ":")) + "\n")
    return {"files": len(entries), "files_rewritten": changed}


def lookahead_features(
    base: pathlib.Path,
    scratch: pathlib.Path,
    venue: str,
    symbol: str,
    start_ns: int,
    end_ns: int,
    cut_ns: int,
    step_ns: int,
) -> dict:
    """Panel rows at or before the cut must survive a rewritten future."""
    src = base / venue
    dst = scratch / f"{venue}-rewritten"
    stats = rewrite_future(src, dst, cut_ns)
    before = pd.DataFrame(
        tvpanel.sample(str(src), venue, symbol, start_ns, end_ns, step_ns, "control")
    )
    after = pd.DataFrame(
        tvpanel.sample(str(dst), venue, symbol, start_ns, end_ns, step_ns, "control")
    )
    early_b = before[before["t_ns"] <= cut_ns].reset_index(drop=True)
    early_a = after[after["t_ns"] <= cut_ns].reset_index(drop=True)
    late_b = before[before["t_ns"] > cut_ns].reset_index(drop=True)
    late_a = after[after["t_ns"] > cut_ns].reset_index(drop=True)

    def digest(frame: pd.DataFrame) -> str:
        return hashlib.sha256(
            pd.util.hash_pandas_object(frame, index=False).values.tobytes()
        ).hexdigest()

    identical = len(early_b) == len(early_a) and digest(early_b) == digest(early_a)
    late_changed = len(late_b) > 0 and digest(late_b) != digest(late_a)
    differing = 0
    if len(late_b) == len(late_a) and len(late_b):
        differing = int((late_b["book_digest"] != late_a["book_digest"]).sum())
    shutil.rmtree(dst)
    return {
        **stats,
        "venue": venue,
        "cut": tvmanifest.iso(cut_ns),
        "rows_before_cut": int(len(early_b)),
        "rows_after_cut": int(len(late_b)),
        "before_cut_sha256": digest(early_b),
        "after_rewrite_sha256": digest(early_a),
        "before_cut_identical": bool(identical),
        "after_cut_changed": bool(late_changed),
        "post_cut_books_differing": differing,
        "verdict": "pass" if identical and late_changed else "FAIL",
    }


# ----------------------------------------------------------------------------
# Look-ahead control, part two: corrupt the model's future.
# ----------------------------------------------------------------------------


def lookahead_model(
    panel: pd.DataFrame,
    horizon: int,
    features: list[str],
    boundary_day: str,
    seed: int = 7,
) -> dict:
    """Predictions for folds that end before `boundary_day` must not move when
    every row from `boundary_day` onwards is replaced with noise."""
    prepared = tvstudy.prepare(panel, [horizon])
    prepared = prepared[prepared["venue"] != "bitstamp"]

    rng = np.random.default_rng(seed)
    corrupted = prepared.copy()
    future = corrupted["day"] >= boundary_day
    for column in features + [f"y_{horizon}", "mid", "base_roll"]:
        values = corrupted[column].to_numpy(dtype=float).copy()
        values[future.to_numpy()] = rng.standard_normal(int(future.sum())) * 100.0
        corrupted[column] = values

    def predictions(frame: pd.DataFrame) -> dict[str, str]:
        out = {}
        for (venue, _symbol), g in frame.groupby(["venue", "symbol"], sort=True):
            res = tvstudy.evaluate(g, horizon, features)
            for fold in res["folds"]:
                if fold["test_day"] >= boundary_day:
                    continue
                key = f"{venue}|{fold['test_day']}"
                out[key] = hashlib.sha256(
                    json.dumps(fold["coef_per_sd"], sort_keys=True).encode()
                    + str(fold["r2_vs_zero"]).encode()
                    + str(fold["n_train"]).encode()
                ).hexdigest()
        return out

    clean = predictions(prepared)
    dirty = predictions(corrupted)
    same = {k: clean[k] == dirty.get(k) for k in clean}
    return {
        "boundary_day": boundary_day,
        "rows_corrupted": int(future.sum()),
        "rows_total": int(len(prepared)),
        "folds_checked": len(clean),
        "folds_identical": int(sum(same.values())),
        "mismatches": [k for k, ok in same.items() if not ok],
        "verdict": "pass" if clean and all(same.values()) else "FAIL",
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--panel", required=True)
    ap.add_argument("--base", required=True, help="directory holding the archives")
    ap.add_argument("--scratch", required=True)
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--feature", default="imb1")
    ap.add_argument("--strengths", default="0.0,0.005,0.02,0.10")
    ap.add_argument("--seed", type=int, default=20260905)
    ap.add_argument(
        "--lookahead",
        action="append",
        default=None,
        help="venue,symbol,START,END,CUT with the instants in RFC 3339, repeatable",
    )
    ap.add_argument("--boundary-day", default="2026-09-03")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    panel = tvstudy.load(args.panel)
    report: dict = {}

    strengths = [float(s) for s in args.strengths.split(",")]
    report["positive_control"] = positive_control(
        panel, args.horizon, args.feature, strengths, args.seed
    )
    print("positive control: plant a signal, see whether the harness finds it")
    for row in report["positive_control"]:
        per = " ".join(
            f"{v}={r:+.4f}" for v, r in row["per_venue_recovered_r2"].items()
        )
        print(
            f"  planted R2={row['planted_r2']:.3f}  "
            f"median recovered={row['median_recovered_r2']:+.4f}   {per}"
        )

    report["lookahead_features"] = []
    scratch = pathlib.Path(args.scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    for spec in args.lookahead or []:
        venue, symbol, start, end, cut = spec.split(",")
        result = lookahead_features(
            pathlib.Path(args.base),
            scratch,
            venue,
            symbol,
            tvpanel.parse_iso(start),
            tvpanel.parse_iso(end),
            tvpanel.parse_iso(cut),
            tvpanel.NS,
        )
        report["lookahead_features"].append(result)
        print(
            f"\nlook-ahead (features) {venue}: rewrote {result['files_rewritten']} of "
            f"{result['files']} files after {result['cut']}"
        )
        print(
            f"  rows at or before the cut: {result['rows_before_cut']}, "
            f"identical={result['before_cut_identical']} "
            f"sha={result['before_cut_sha256'][:16]}"
        )
        print(
            f"  rows after the cut: {result['rows_after_cut']}, "
            f"changed={result['after_cut_changed']}, "
            f"books differing={result['post_cut_books_differing']}"
        )
        print(f"  verdict: {result['verdict']}")

    report["lookahead_model"] = lookahead_model(
        panel, args.horizon, tvstudy.FEATURE_SETS["imbalance"], args.boundary_day
    )
    m = report["lookahead_model"]
    print(
        f"\nlook-ahead (model): corrupted {m['rows_corrupted']} of {m['rows_total']} "
        f"panel rows from {m['boundary_day']} on"
    )
    print(
        f"  earlier folds identical: {m['folds_identical']}/{m['folds_checked']}  "
        f"verdict: {m['verdict']}"
    )

    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=1)
        fh.write("\n")
    print(f"\nwrote {args.out}")
    failed = [
        r["verdict"] for r in report["lookahead_features"] if r["verdict"] != "pass"
    ]
    return 1 if failed or m["verdict"] != "pass" else 0


if __name__ == "__main__":
    sys.exit(main())
