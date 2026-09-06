"""Point-in-time reads over the versioned sink, and serving parity.

A feature has two times and a read that uses only one of them is wrong.

* **Event time** says which window the feature describes.
* **Availability time** says when the feature could first have been known.
  Here it is ``max(recv_wall)`` over the rows folded into that revision: the
  moment the recorder had every row the number is made of.

A backtest that selects on event time reads values that did not exist yet. The
window ``05:56:00-05:56:10`` is *about* 05:56:00, but if the last row in it
reached the recorder at 05:56:10.3 then at 05:56:10.0 that number was not
available to anyone. Selecting on event time hands it to the backtest anyway.
``--leak-demo`` runs exactly that mistake so the size of it can be measured
rather than argued about.

Two readers are implemented, deliberately by different routes:

* **historical** is a SQL window function over the whole revision log, the way
  a training-set builder would ask.
* **online** is a fold that walks the log once in availability order and keeps
  the latest value per key, the way a serving store is actually updated.

They must agree exactly at every as-of point. That is the parity claim, and it
is the one that decides whether a model sees in production what it was trained
on.

The read is conservative by construction. A revision is visible only when
*every* row in it was available, so a value can be one revision staler than a
perfect oracle would allow. That is the safe direction to be wrong in: it can
cost accuracy, and it cannot leak the future. Staleness is returned rather than
hidden: every read carries the age of what it returned and whether that age is
past the caller's tolerance.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duckdb  # noqa: E402

import reference  # noqa: E402
import tvcommon  # noqa: E402

VALUE_COLUMNS = ("window_start_ns", "window_end_ns", "batch_id") + tuple(
    reference.FEATURE_COLUMNS
)


def connect(sink: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        f"CREATE VIEW revisions AS "
        f"SELECT * FROM read_parquet({tvcommon.revision_globs(sink)!r}, "
        f"hive_partitioning = true)"
    )
    return con


def historical(con, as_of_ns: int, leak: bool) -> dict:
    """As-of read, ordered by whichever clock the caller asked for.

    ``leak`` swaps the visibility test from availability time to event time,
    which is what a feature store that only models one timestamp does. It is
    here to be measured, not to be used.
    """
    cutoff = "window_end_ns" if leak else "availability_time_max_ns"
    rows = con.execute(
        f"""
        SELECT venue, symbol, {', '.join(VALUE_COLUMNS)}, availability_time_max_ns
        FROM (
          SELECT *, ROW_NUMBER() OVER (
              PARTITION BY venue, symbol
              ORDER BY window_start_ns DESC, batch_id DESC
          ) AS rn
          FROM revisions
          WHERE {cutoff} <= {as_of_ns}
        ) WHERE rn = 1
        """
    ).fetchall()
    columns = ["venue", "symbol"] + list(VALUE_COLUMNS) + ["availability_time_max_ns"]
    return {
        (r[0], r[1]): dict(zip(columns, r))
        for r in rows
    }


def online_snapshots(con, as_of_points: list[int]) -> dict[int, dict]:
    """Replay the log once in availability order, snapshotting as it goes.

    This is what a serving store is: a thing the same stream updates, holding
    one current value per key. Rebuilding it by fold rather than by query is the
    point; if the two agree, the agreement is not an artefact of asking the same
    engine the same question twice.
    """
    columns = ["venue", "symbol"] + list(VALUE_COLUMNS) + ["availability_time_max_ns"]
    log = con.execute(
        f"""
        SELECT {', '.join(columns)} FROM revisions
        ORDER BY availability_time_max_ns, batch_id, venue, symbol, window_start_ns
        """
    ).fetchall()

    out: dict[int, dict] = {}
    state: dict[tuple[str, str], dict] = {}
    index = 0
    for as_of in sorted(as_of_points):
        while index < len(log) and log[index][columns.index("availability_time_max_ns")] <= as_of:
            row = dict(zip(columns, log[index]))
            key = (row["venue"], row["symbol"])
            current = state.get(key)
            if current is None or (row["window_start_ns"], row["batch_id"]) > (
                current["window_start_ns"],
                current["batch_id"],
            ):
                state[key] = row
            index += 1
        out[as_of] = {k: dict(v) for k, v in state.items()}
    return out


def as_of_points(con, count: int) -> list[int]:
    """As-of instants spread over the run, plus the awkward ones.

    Evenly spaced points test the ordinary case. The instants either side of a
    revision becoming available are where an off-by-one lives, so every one of
    those is included too.
    """
    lo, hi = con.execute(
        "SELECT MIN(availability_time_max_ns), MAX(availability_time_max_ns) "
        "FROM revisions"
    ).fetchone()
    step = max(1, (hi - lo) // max(1, count))
    points = list(range(lo - step, hi + step, step))
    edges = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT availability_time_max_ns FROM revisions "
            "ORDER BY 1"
        ).fetchall()
    ]
    for edge in edges:
        points.extend([edge - 1, edge, edge + 1])
    return sorted(set(points))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sink", required=True)
    ap.add_argument("--points", type=int, default=64)
    ap.add_argument("--ttl-seconds", type=float, default=30.0)
    ap.add_argument("--leak-demo", action="store_true",
                    help="run the historical read on event time instead of "
                         "availability time, to measure what that leaks")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    con = connect(args.sink)
    points = as_of_points(con, args.points)
    online = online_snapshots(con, points)

    ttl_ns = int(args.ttl_seconds * 1e9)
    disagreements = []
    lookahead = []
    compared = 0
    stale_reads = 0
    fresh_reads = 0
    empty_reads = 0
    for as_of in points:
        want = online[as_of]
        got = historical(con, as_of, args.leak_demo)
        if not got:
            empty_reads += 1
        for key in sorted(set(want) | set(got)):
            compared += 1
            a, b = want.get(key), got.get(key)
            if a is None or b is None:
                disagreements.append(
                    {"as_of": as_of, "key": list(key),
                     "online": a is not None, "historical": b is not None}
                )
                continue
            for column in VALUE_COLUMNS:
                if a[column] != b[column]:
                    disagreements.append(
                        {"as_of": as_of, "key": list(key), "column": column,
                         "online": a[column], "historical": b[column]}
                    )
            # The look-ahead check. Whatever the read returned must have been
            # available at the instant it was asked for.
            if b["availability_time_max_ns"] > as_of:
                lookahead.append(
                    {"as_of": as_of, "key": list(key),
                     "available_at": b["availability_time_max_ns"],
                     "ahead_by_ns": b["availability_time_max_ns"] - as_of}
                )
            age = as_of - b["availability_time_max_ns"]
            if age > ttl_ns:
                stale_reads += 1
            else:
                fresh_reads += 1

    result = {
        "sink": args.sink,
        "leak_demo": args.leak_demo,
        "as_of_points": len(points),
        "reads_compared": compared,
        "parity_disagreements": len(disagreements),
        "first_disagreements": disagreements[:5],
        "lookahead_reads": len(lookahead),
        "worst_lookahead_ns": max((x["ahead_by_ns"] for x in lookahead), default=0),
        "first_lookahead": lookahead[:5],
        "ttl_seconds": args.ttl_seconds,
        "fresh_reads": fresh_reads,
        "stale_reads": stale_reads,
        "empty_reads": empty_reads,
    }
    print(json.dumps(result, indent=2, default=str))
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(result, handle, indent=2, default=str)
    con.close()

    if args.leak_demo:
        # The demo has done its job when it leaks. Returning success for a
        # leaking read would make this flag look like a supported mode.
        return 0 if lookahead else 1
    return 0 if not disagreements and not lookahead else 1


if __name__ == "__main__":
    raise SystemExit(main())
