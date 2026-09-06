"""Publish an archive into Kafka, keyed by venue and instrument.

The archive is the source of truth and this only reads it. Every flag that
distorts the stream exists because a downstream property has to be tested
against the distortion, and each one lands in ``produced.json`` so the
consumer's reconciliation is checked against what was really sent rather than
against what was meant to be sent.

Delivery is at-least-once and this does not pretend otherwise:

* ``--idempotence`` turns on Kafka's producer-side idempotence, which suppresses
  duplicates caused by a *broker retry inside one producer session*. It does
  nothing about a producer that dies and starts again, because the new session
  gets a new producer id and replays from wherever it decides to.
* So a restart re-sends, and the consumer is the layer that has to make a
  redelivery harmless. That is the boundary, stated in one place.

``--order`` decides how six single-instrument archives become one stream, and
the default is not cosmetic. Spark's watermark is global across every key, so a
producer that publishes one archive at a time hands the consumer six streams
that each rewind five minutes, and almost everything after the first venue is
already late when it arrives. Measured end to end on the published archives:

    --order archive   247,672 of 298,090 records (83.1%) dropped by watermark
    --order arrival   101,496 of 298,090 records (34.0%) dropped by watermark

``arrival`` merges every archive on ``recv_wall``, which is what a fan-in
recorder actually produces. ``archive`` keeps the naive order so the failure
stays reproducible.

The residual 34% is *not* a producer defect and no publish order fixes it: it
is the Spark source rate-limiting each topic partition by the same fraction of
its backlog, which drifts the partitions 103.9s apart. The watermark has to
cover that, and with ``tvcommon.WATERMARK`` set from the measurement the same
topic reconciles exactly. See ``watermark_audit.py``.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from confluent_kafka import Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

import tvcommon  # noqa: E402


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=15).topics:
        return
    admin.create_topics([NewTopic(topic, partitions, 1)])[topic].result(timeout=30)


def delete_topic(bootstrap: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic not in admin.list_topics(timeout=15).topics:
        return
    admin.delete_topics([topic])[topic].result(timeout=30)
    # Deletion is asynchronous in the broker, so a create straight afterwards
    # races it and quietly gets the old topic back.
    for _ in range(60):
        client = AdminClient({"bootstrap.servers": bootstrap})
        if topic not in client.list_topics(timeout=15).topics:
            return
        time.sleep(0.5)
    raise RuntimeError(f"topic {topic} did not disappear")


def shuffle_within(rows: list[dict], window: int, seed: int) -> list[dict]:
    """Reorder arrivals inside a bounded window, keeping every record.

    This is arrival disorder, not event-time disorder: the venue timestamps are
    untouched and only the order they reach the broker in changes. A consumer
    that buckets by event time has to be indifferent to it.
    """
    if window <= 1:
        return rows
    rng = random.Random(seed)
    out: list[dict] = []
    for start in range(0, len(rows), window):
        chunk = rows[start : start + window]
        rng.shuffle(chunk)
        out.extend(chunk)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive-root", default="docs/data")
    ap.add_argument("--archive", action="append", default=None,
                    help="one archive directory; repeatable. Defaults to every "
                         "venue under --archive-root")
    ap.add_argument("--topic", default=tvcommon.TOPIC)
    ap.add_argument("--bootstrap", default=tvcommon.BOOTSTRAP)
    ap.add_argument("--partitions", type=int, default=tvcommon.PARTITIONS)
    ap.add_argument("--recreate-topic", action="store_true")
    ap.add_argument("--order", choices=("arrival", "archive"), default="arrival")
    ap.add_argument("--limit", type=int, default=0,
                    help="keep only this many rows per archive; 0 is all")
    ap.add_argument("--stop-after", type=int, default=0,
                    help="stop the run after this many records, as a producer "
                         "that died partway would. 0 sends everything")
    ap.add_argument("--shuffle", type=int, default=0,
                    help="reorder arrivals inside a window of this many records")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--drop", default="",
                    help="A:B, withhold the arrival-order slice [A,B) of one "
                         "archive, standing in for a source gap")
    ap.add_argument("--drop-venue", default="",
                    help="which venue --drop applies to; empty means all")
    ap.add_argument("--resend", default="",
                    help="A:B, publish the arrival-order slice [A,B) a second "
                         "time, standing in for a retry nothing suppressed")
    ap.add_argument("--resend-lag", type=int, default=200,
                    help="how many records later a resent record reappears")
    ap.add_argument("--schema-change-at", type=int, default=-1,
                    help="from this row ordinal on, add a field and bump "
                         "schema_version. -1 disables")
    ap.add_argument("--idempotence", action="store_true")
    ap.add_argument("--linger-ms", type=int, default=20)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    if args.recreate_topic:
        delete_topic(args.bootstrap, args.topic)
    ensure_topic(args.bootstrap, args.topic, args.partitions)

    drop_lo, drop_hi = (
        (int(x) for x in args.drop.split(":")) if args.drop else (-1, -1)
    )
    resend_lo, resend_hi = (
        (int(x) for x in args.resend.split(":")) if args.resend else (-1, -1)
    )

    dirs = args.archive or tvcommon.archive_dirs(args.archive_root)
    per_instrument: dict[str, dict] = {}
    stream: list[dict] = []

    for path in dirs:
        archive = tvcommon.read_archive(path)
        rows = archive.rows[: args.limit] if args.limit else list(archive.rows)

        dropped = 0
        if drop_lo >= 0 and (not args.drop_venue or args.drop_venue == archive.venue):
            before = len(rows)
            rows = [r for r in rows if not (drop_lo <= r["row_ordinal"] < drop_hi)]
            dropped = before - len(rows)

        if args.schema_change_at >= 0:
            for row in rows:
                if row["row_ordinal"] >= args.schema_change_at:
                    row["schema_version"] = tvcommon.SCHEMA_VERSION + 1
                    # Additive: a reader of version 1 neither needs this nor is
                    # broken by it, which is the only kind of change the
                    # archive's own schema policy allows without a version break.
                    row["venue_seq_source"] = (
                        "counter" if row["seq"] is not None else "timestamp"
                    )

        per_instrument[f"{archive.venue}|{archive.symbol}"] = {
            "archive": path,
            "venue": archive.venue,
            "symbol": archive.symbol,
            "archive_rows": len(archive.rows),
            "rows_dropped_at_source": dropped,
            "epochs": len({r["conn_epoch"] for r in rows}),
        }
        stream.extend(rows)

    if args.order == "arrival":
        # One recorder per venue, one broker downstream: what reaches it is the
        # merge of them on receipt time. Anything else asks a global watermark
        # to tolerate the stream rewinding once per venue.
        stream.sort(key=lambda r: (r["recv_wall_ns"], r["venue"], r["row_ordinal"]))

    if resend_lo >= 0:
        with_resends: list[dict] = []
        pending: list[tuple[int, dict]] = []
        for position, row in enumerate(stream):
            with_resends.append(row)
            if resend_lo <= row["row_ordinal"] < resend_hi:
                pending.append((position + args.resend_lag, dict(row)))
            while pending and pending[0][0] <= position:
                with_resends.append(pending.pop(0)[1])
        with_resends.extend(row for _, row in pending)
        stream = with_resends

    stream = shuffle_within(stream, args.shuffle, args.seed)
    if args.stop_after:
        stream = stream[: args.stop_after]

    conf = {
        "bootstrap.servers": args.bootstrap,
        "linger.ms": args.linger_ms,
        "acks": "all",
        "compression.type": "lz4",
    }
    if args.idempotence:
        conf["enable.idempotence"] = True
    producer = Producer(conf)

    failures: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    sent = 0
    for row in stream:
        while True:
            try:
                producer.produce(
                    args.topic,
                    key=tvcommon.key_for(row),
                    value=tvcommon.encode(row),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.2)
        sent += 1
        if sent % 20000 == 0:
            producer.poll(0)
    producer.flush(120)

    if failures:
        print(f"delivery failures: {len(failures)}; first: {failures[0]}",
              file=sys.stderr)
        return 1

    for row in stream:
        entry = per_instrument[f"{row['venue']}|{row['symbol']}"]
        entry["records_sent"] = entry.get("records_sent", 0) + 1
        if row["venue_ts_ns"] is None:
            entry["records_without_event_time"] = (
                entry.get("records_without_event_time", 0) + 1
            )
        entry.setdefault("_ordinals", set()).add(row["row_ordinal"])
    for entry in per_instrument.values():
        ordinals = entry.pop("_ordinals", set())
        entry["distinct_row_ordinals"] = len(ordinals)
        entry["duplicate_records"] = entry.get("records_sent", 0) - len(ordinals)
        entry.setdefault("records_without_event_time", 0)
        print(
            f"{entry['venue']:11s} {entry['symbol']:9s} "
            f"sent={entry.get('records_sent', 0):7d} "
            f"distinct={entry['distinct_row_ordinals']:7d} "
            f"dup={entry['duplicate_records']:6d} "
            f"dropped={entry['rows_dropped_at_source']:5d} "
            f"no_event_time={entry['records_without_event_time']:6d} "
            f"epochs={entry['epochs']}",
            flush=True,
        )

    summary = {
        "topic": args.topic,
        "records_sent": sent,
        "flags": {
            "order": args.order,
            "limit": args.limit,
            "stop_after": args.stop_after,
            "shuffle": args.shuffle,
            "drop": args.drop,
            "drop_venue": args.drop_venue,
            "resend": args.resend,
            "resend_lag": args.resend_lag,
            "schema_change_at": args.schema_change_at,
            "idempotence": args.idempotence,
        },
        "instruments": list(per_instrument.values()),
    }
    print(f"total records_sent={sent}", flush=True)
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(summary, handle, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
