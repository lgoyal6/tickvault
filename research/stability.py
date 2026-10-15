"""Summarize held-out rows from the simulator's strategy-eval JSON.
Keep only verifiable, safe, complete runs and validate their identities.
Window summaries describe the supplied runs without refitting a strategy.
Adjacent windows are not independent market dates or regimes.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any


PNL_FIELD = "after_fee_pnl_liquidated_quote"


def summarize(evaluation: dict[str, Any], min_windows: int = 3) -> dict[str, Any]:
    if type(min_windows) is not int or min_windows < 1:
        raise ValueError("min_windows must be positive")
    runs = evaluation.get("runs")
    if not isinstance(runs, list):
        raise ValueError("evaluation must contain a runs array")

    groups: dict[tuple[str, str, str], dict[int, float]] = defaultdict(dict)
    excluded = {"unverifiable": 0, "risk_controls_not_held": 0, "truncated": 0}
    for row in runs:
        if not isinstance(row, dict):
            raise ValueError("each run must be an object")
        if row.get("verifiable") is not True:
            excluded["unverifiable"] += 1
            continue
        if row.get("risk_controls_held") is not True:
            excluded["risk_controls_not_held"] += 1
            continue
        if row.get("truncated") is not False:
            excluded["truncated"] += 1
            continue
        try:
            venue, strategy, symbol = (row[name] for name in ("venue", "strategy", "symbol"))
            window, pnl = row["window"], row[PNL_FIELD]
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid eligible run row: {exc}") from exc
        if not all(isinstance(value, str) and value.strip() for value in (venue, strategy, symbol)):
            raise ValueError("eligible run requires nonempty string identities")
        if type(window) is not int or window < 0:
            raise ValueError("eligible run requires a nonnegative integer window")
        if type(pnl) not in (int, float) or not math.isfinite(pnl):
            raise ValueError("eligible run requires numeric, finite PnL; non-finite PnL is invalid")
        key = (venue, strategy, symbol)
        if window in groups[key]:
            raise ValueError(f"duplicate held-out window {window} for {key}")
        groups[key][window] = pnl

    reports = []
    for (venue, strategy, symbol), window_pnl in sorted(groups.items()):
        values = list(window_pnl.values())
        count = len(values)
        total = sum(values)
        if not math.isfinite(total):
            raise ValueError("window PnL total is non-finite")
        reports.append(
            {
                "venue": venue,
                "strategy": strategy,
                "symbol": symbol,
                "held_out_windows": sorted(window_pnl),
                "window_count": count,
                "positive_windows": sum(value > 0 for value in values),
                "positive_window_fraction": sum(value > 0 for value in values) / count,
                "total_after_fee_pnl_liquidated_quote": total,
                "worst_window_pnl_liquidated_quote": min(values),
                "best_window_pnl_liquidated_quote": max(values),
                "coverage": "window_only" if count >= min_windows else "insufficient_windows",
            }
        )
    return {
        "schema_version": 1,
        "source": "strategy-eval runs; eligible, risk controls held, non-truncated rows",
        "min_windows": min_windows,
        "period_interpretation": "Window identifiers are not independent dates or regimes.",
        "excluded_rows": excluded,
        "groups": reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evaluation", type=Path, help="strategy-eval.json")
    parser.add_argument("--output", type=Path, help="write JSON here instead of stdout")
    parser.add_argument("--min-windows", type=int, default=3)
    args = parser.parse_args()
    result = summarize(json.loads(args.evaluation.read_text()), args.min_windows)
    rendered = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
