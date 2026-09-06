"""What a global watermark will discard, computed off the archive in seconds.

The streaming runs are the authority for these numbers. This exists because
they cost a minute each and this costs three seconds, so the ordering finding
can be re-checked after any change to the producer without standing anything up.

It answers three questions, and the third one is the one that cost a run to
find.

**How late can one venue be on its own?** The largest event-time inversion
inside a single archive, in arrival order. That is the lateness a watermark has
to tolerate even for a single well-behaved feed.

**How much worse does publish order make it?** Publishing archive after archive
rewinds event time by a whole recording every time a new venue starts, so
almost everything after the first venue is behind a global watermark.
``producer.py --order arrival`` merges on ``recv_wall`` instead.

**How far apart do the topic's partitions drift?** This is the one that is easy
to miss. Kafka's source rate limiter (``maxOffsetsPerTrigger``) advances every
partition by the same *fraction of its backlog*, and a book feed opens with a
snapshot burst, so record count and elapsed time are not proportional. The
partition holding the venues with the biggest snapshots is still near the start
of the recording while the others are a minute and a half in, and because the
watermark is global, the laggard's rows are all late. Publishing in arrival
order does not help: the drift is created downstream of the producer.

The floor printed at the end is the sum of the two, and it is what
``tvcommon.WATERMARK`` is set from.
"""

from __future__ import annotations

import argparse
import os
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tvcommon  # noqa: E402


def partition_of(key: bytes, partitions: int) -> int:
    """librdkafka's ``consistent_random`` partitioner for a keyed record.

    Verified against the partition every record actually landed in on a live
    topic; see the record. Reimplemented rather than asked of the broker so
    this file needs no broker.
    """
    return zlib.crc32(key) % partitions


def worst_inversion(rows) -> int:
    """Largest amount by which a record's event time is behind an earlier one."""
    high = None
    worst = 0
    for row in rows:
        event = row["venue_ts_ns"]
        if event is None:
            continue
        if high is not None and event < high:
            worst = max(worst, high - event)
        high = event if high is None else max(high, event)
    return worst


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive-root", default="docs/data")
    ap.add_argument("--archive", action="append", default=None)
    ap.add_argument("--partitions", type=int, default=tvcommon.PARTITIONS)
    ap.add_argument("--steps", type=int, default=10)
    args = ap.parse_args()

    paths = args.archive or tvcommon.archive_dirs(args.archive_root)
    archives = [tvcommon.read_archive(path) for path in paths]

    print("per archive, in arrival order")
    print(f"  {'venue':11s} {'symbol':9s} {'rows':>7s} {'no_event_time':>13s} "
          f"{'worst_inversion':>16s} {'part':>4s}")
    venue_lateness = 0
    for archive in archives:
        inversion = worst_inversion(archive.rows)
        venue_lateness = max(venue_lateness, inversion)
        key = f"{archive.venue}|{archive.symbol}".encode()
        print(f"  {archive.venue:11s} {archive.symbol:9s} {len(archive.rows):7d} "
              f"{archive.without_event_time:13d} {inversion / 1e9:15.3f}s "
              f"{partition_of(key, args.partitions):4d}")

    rows = [row for archive in archives for row in archive.rows]
    with_event_time = [r for r in rows if r["venue_ts_ns"] is not None]
    print(f"\n  {len(rows)} rows, {len(with_event_time)} with an event time")
    print(f"  worst single-venue inversion: {venue_lateness / 1e9:.3f}s")

    # How far apart the partitions sit when each has been read to the same
    # fraction of its backlog, which is what the source rate limiter does.
    ordered = sorted(rows, key=lambda r: (r["recv_wall_ns"], r["venue"],
                                          r["row_ordinal"]))
    parts: dict[int, list[int]] = {p: [] for p in range(args.partitions)}
    for row in ordered:
        key = f"{row['venue']}|{row['symbol']}".encode()
        parts[partition_of(key, args.partitions)].append(row["recv_wall_ns"])
    parts = {p: v for p, v in parts.items() if v}
    origin = min(v[0] for v in parts.values())

    print("\narrival time each partition has reached at the same fraction of "
          "its backlog")
    header = " ".join(f"p{p}(n={len(parts[p])})".rjust(15) for p in sorted(parts))
    print(f"  {'frac':>5s} {header}   {'spread':>9s}")
    drift = 0
    for step in range(1, args.steps + 1):
        fraction = step / args.steps
        reached = []
        for p in sorted(parts):
            index = min(len(parts[p]) - 1, int(fraction * len(parts[p])))
            reached.append((parts[p][index] - origin) / 1e9)
        spread = max(reached) - min(reached)
        drift = max(drift, spread)
        print(f"  {fraction:5.1f} " + " ".join(f"{x:15.1f}" for x in reached)
              + f"   {spread:8.1f}s")

    floor = venue_lateness + drift * 1e9
    print(f"\n  worst partition drift: {drift:.1f}s")
    print(f"  watermark floor = single-venue inversion + partition drift = "
          f"{floor / 1e9:.1f}s")
    print(f"  tvcommon.WATERMARK is {tvcommon.WATERMARK!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
