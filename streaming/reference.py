"""The oracle: the same features, computed straight off the archive.

This is deliberately not Spark and deliberately not SQL. It is a plain Python
fold over the archive rows, because two implementations that agree are only
evidence when they are not the same implementation twice. If the streaming job
and this file agree to the last integer, the disagreement space that is left is
"both are wrong in the same way", and a hand-written fold and a distributed
query planner do not usually get there together.

Every aggregate here is commutative and associative on purpose. That is what
makes the parity claim mean something: a feature that depended on arrival order
would agree with a replay only by luck, and would disagree the moment a record
was retried or arrived late.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tvcommon  # noqa: E402

WINDOW_NS = tvcommon.WINDOW_SECONDS * 1_000_000_000

# The sink's columns, in the order both sides emit them. Written once so the
# comparison cannot quietly compare a subset.
FEATURE_COLUMNS = (
    "updates",
    "bid_updates",
    "ask_updates",
    "removals",
    "snapshot_rows",
    "suspect_rows",
    "price_min",
    "price_max",
    "qty_sum",
    "traded_qty_sum",
    "traded_qty_known",
    "max_abs_skew_ns",
    "min_conn_epoch",
    "max_conn_epoch",
    "event_time_min_ns",
    "event_time_max_ns",
    "availability_time_min_ns",
    "availability_time_max_ns",
    "max_schema_version",
    "unknown_field_rows",
)


def _blank() -> dict:
    return {
        "updates": 0,
        "bid_updates": 0,
        "ask_updates": 0,
        "removals": 0,
        "snapshot_rows": 0,
        "suspect_rows": 0,
        "price_min": None,
        "price_max": None,
        "qty_sum": 0,
        "traded_qty_sum": None,
        "traded_qty_known": 0,
        "max_abs_skew_ns": None,
        "min_conn_epoch": None,
        "max_conn_epoch": None,
        "event_time_min_ns": None,
        "event_time_max_ns": None,
        "availability_time_min_ns": None,
        "availability_time_max_ns": None,
        "max_schema_version": None,
        "unknown_field_rows": 0,
    }


def _lo(current, value):
    return value if current is None else min(current, value)


def _hi(current, value):
    return value if current is None else max(current, value)


def fold(rows) -> dict[tuple[str, str, int], dict]:
    """Fold payload dicts into the feature table.

    Takes the same payloads the producer publishes, so the only difference
    between this and the streaming job is the engine and the transport.
    """
    out: dict[tuple[str, str, int], dict] = {}
    for row in rows:
        event_ns = row["venue_ts_ns"]
        if event_ns is None:
            # No event time, so no event-time window. Counted by the caller and
            # reported, never bucketed by arrival time instead.
            continue
        key = (row["venue"], row["symbol"], (event_ns // WINDOW_NS) * WINDOW_NS)
        acc = out.setdefault(key, _blank())
        acc["updates"] += 1
        acc["bid_updates"] += 1 if row["side"] == 0 else 0
        acc["ask_updates"] += 1 if row["side"] == 1 else 0
        acc["removals"] += 1 if row["qty"] == 0 else 0
        acc["snapshot_rows"] += 1 if row["event"] == 0 else 0
        acc["suspect_rows"] += 1 if row["suspect"] else 0
        acc["price_min"] = _lo(acc["price_min"], row["price"])
        acc["price_max"] = _hi(acc["price_max"], row["price"])
        acc["qty_sum"] += row["qty"]
        if row["traded_qty"] is not None:
            acc["traded_qty_sum"] = (acc["traded_qty_sum"] or 0) + row["traded_qty"]
            acc["traded_qty_known"] += 1
        if row["skew_ns"] is not None:
            acc["max_abs_skew_ns"] = _hi(acc["max_abs_skew_ns"], abs(row["skew_ns"]))
        acc["min_conn_epoch"] = _lo(acc["min_conn_epoch"], row["conn_epoch"])
        acc["max_conn_epoch"] = _hi(acc["max_conn_epoch"], row["conn_epoch"])
        acc["event_time_min_ns"] = _lo(acc["event_time_min_ns"], event_ns)
        acc["event_time_max_ns"] = _hi(acc["event_time_max_ns"], event_ns)
        acc["availability_time_min_ns"] = _lo(
            acc["availability_time_min_ns"], row["recv_wall_ns"]
        )
        acc["availability_time_max_ns"] = _hi(
            acc["availability_time_max_ns"], row["recv_wall_ns"]
        )
        acc["max_schema_version"] = _hi(
            acc["max_schema_version"], row["schema_version"]
        )
        known = set(EXPECTED_FIELDS)
        acc["unknown_field_rows"] += 1 if set(row) - known else 0
    return out


# The fields a version 1 payload carries. Anything else in a payload is a
# schema change, and the count of rows carrying one is a published feature
# rather than something the parser swallows.
EXPECTED_FIELDS = (
    "schema_version",
    "venue",
    "symbol",
    "conn_epoch",
    "row_ordinal",
    "event",
    "msg_index",
    "recv_wall_ns",
    "venue_ts_ns",
    "skew_ns",
    "side",
    "price",
    "qty",
    "seq",
    "first_seq",
    "prev_seq",
    "suspect",
    "traded_qty",
)


def from_archives(paths) -> tuple[dict, dict]:
    """Fold every archive and report what could not be windowed."""
    rows = []
    without_event_time = 0
    counts = {}
    for path in paths:
        archive = tvcommon.read_archive(path)
        rows.extend(archive.rows)
        without_event_time += archive.without_event_time
        counts[f"{archive.venue}|{archive.symbol}"] = {
            "archive_rows": len(archive.rows),
            "without_event_time": archive.without_event_time,
            "epochs": len({r["conn_epoch"] for r in archive.rows}),
        }
    table = fold(rows)
    return table, {
        "rows": len(rows),
        "without_event_time": without_event_time,
        "windows": len(table),
        "per_instrument": counts,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive-root", default="docs/data")
    ap.add_argument("--archive", action="append", default=None)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    paths = args.archive or tvcommon.archive_dirs(args.archive_root)
    table, meta = from_archives(paths)
    print(json.dumps(meta, indent=2))
    if args.out:
        serialisable = [
            {"venue": k[0], "symbol": k[1], "window_start_ns": k[2], **v}
            for k, v in sorted(table.items())
        ]
        with open(args.out, "w") as handle:
            json.dump(serialisable, handle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
