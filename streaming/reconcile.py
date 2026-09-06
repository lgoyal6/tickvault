"""Account for every record, then compare the sink to the archive.

This is the file that decides whether a run is honest. It answers two questions
that a streaming job normally leaves unanswered.

**Did anything vanish?** Every record on the topic is one of four things: folded
into a window, discarded as a redelivery, carrying no event time, or dropped by
the watermark. The identity

    distinct records with an event time == windowed + dropped by watermark

either closes or the run is reported as failed. There is no fifth bucket called
"lateness", and a watermark that quietly ate a hundred rows fails this check
rather than producing a slightly smaller number nobody looks at.

**Does the sink say what the archive says?** The latest revision of every window
is compared, field by field, with the fold in ``reference.py``. That fold is a
different engine reading the archive directly, so agreement is evidence rather
than a tautology.

The sink is compared against three folds and they answer different questions.
``vs_topic`` folds the topic after removing redeliveries and is the pipeline's
own correctness claim, so it must always be exact. ``vs_naive_topic`` folds
every byte on the topic and is expected to disagree whenever a redelivery was
published; the size of that disagreement is what deduplication was worth.
``vs_archive`` is the end-to-end claim and it is exact only when nothing was
withheld at the source, which is why a deliberate gap fails it on purpose.

The sink is read with DuckDB and the topic with a plain consumer, again so the
checker is not the system under test.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duckdb  # noqa: E402
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

import reference  # noqa: E402
import tvcommon  # noqa: E402


def read_topic(bootstrap: str, topic: str) -> dict:
    """Drain the topic once and census it. No consumer group, no offsets kept."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "tickvault-reconcile",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    meta = consumer.list_topics(topic, timeout=15).topics[topic]
    parts = []
    total = 0
    for pid in meta.partitions:
        tp = TopicPartition(topic, pid)
        low, high = consumer.get_watermark_offsets(tp, timeout=15)
        parts.append(TopicPartition(topic, pid, low))
        total += high - low
    consumer.assign(parts)

    seen = 0
    ids = set()
    without_event_time = 0
    distinct_without_event_time = set()
    versions = Counter()
    unknown_field_records = 0
    keys = Counter()
    rows = []
    while seen < total:
        batch = consumer.consume(num_messages=5000, timeout=10.0)
        if not batch:
            break
        for message in batch:
            if message.error():
                raise RuntimeError(str(message.error()))
            seen += 1
            payload = json.loads(message.value())
            rows.append(payload)
            keys[message.key().decode()] += 1
            ident = (payload["venue"], payload["symbol"], payload["row_ordinal"])
            ids.add(ident)
            versions[payload["schema_version"]] += 1
            if set(payload) - set(reference.EXPECTED_FIELDS):
                unknown_field_records += 1
            if payload["venue_ts_ns"] is None:
                without_event_time += 1
                distinct_without_event_time.add(ident)
    consumer.close()

    return {
        "records": seen,
        "broker_reports": total,
        "distinct_records": len(ids),
        "duplicate_records": seen - len(ids),
        "records_without_event_time": without_event_time,
        "distinct_without_event_time": len(distinct_without_event_time),
        "distinct_with_event_time": len(ids) - len(distinct_without_event_time),
        "schema_versions": dict(sorted(versions.items())),
        "unknown_field_records": unknown_field_records,
        "keys": dict(sorted(keys.items())),
        "_rows": rows,
    }


def read_sink(sink: str, max_batch_id: int | None = None) -> dict:
    """The sink as it stood after ``max_batch_id``, plus its revision history.

    Reading with an upper bound on the batch id is the replay: the sink is a
    log of emissions, so any point in its history can be reconstructed by
    ignoring the batches after it. Nothing is mutated in place, which is what
    makes that possible.
    """
    globs = tvcommon.revision_globs(sink)
    where = "" if max_batch_id is None else f" WHERE batch_id <= {max_batch_id}"
    con = duckdb.connect()
    con.execute(
        f"""
        CREATE VIEW revisions AS
        SELECT * FROM read_parquet({globs!r}, hive_partitioning = true){where}
        """
    )
    con.execute(
        """
        CREATE VIEW latest AS
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, ROW_NUMBER() OVER (
              PARTITION BY venue, symbol, window_start_ns ORDER BY batch_id DESC
          ) AS rn
          FROM revisions
        ) WHERE rn = 1
        """
    )
    columns = ["venue", "symbol", "window_start_ns", "window_end_ns", "batch_id"]
    columns += list(reference.FEATURE_COLUMNS)
    latest = con.execute(
        f"SELECT {', '.join(columns)} FROM latest ORDER BY venue, symbol, window_start_ns"
    ).fetchall()
    revision_counts = con.execute(
        """
        SELECT venue, symbol, window_start_ns, COUNT(*) AS revisions,
               MIN(batch_id) AS first_batch, MAX(batch_id) AS last_batch
        FROM revisions GROUP BY 1,2,3 HAVING COUNT(*) > 1
        ORDER BY revisions DESC, 1, 2, 3
        """
    ).fetchall()
    totals = con.execute(
        "SELECT COUNT(*), SUM(updates), MAX(batch_id) FROM latest"
    ).fetchone()
    con.close()
    return {
        "columns": columns,
        "latest": latest,
        "windows": totals[0],
        "windowed_records": int(totals[1] or 0),
        "max_batch_id": totals[2],
        "revised_windows": revision_counts,
    }


def read_metrics(sink: str) -> dict:
    path = os.path.join(sink, "metrics", "progress.jsonl")
    if not os.path.exists(path):
        return {"batches": 0, "dropped_by_watermark": {}, "final_watermark": None}
    dropped: Counter = Counter()
    batches = 0
    watermark = None
    per_batch = {}
    with open(path) as handle:
        for line in handle:
            record = json.loads(line)
            batches += 1
            for op in record["stateOperators"]:
                dropped[op["operatorName"]] += op.get("numRowsDroppedByWatermark", 0)
            mark = record.get("eventTime", {}).get("watermark")
            if mark:
                watermark = mark
            if record.get("batchId") is not None:
                per_batch[record["batchId"]] = mark
    return {
        "batches": batches,
        "dropped_by_watermark": dict(dropped),
        "dropped_total": sum(dropped.values()),
        "final_watermark": watermark,
        "watermark_by_batch": per_batch,
    }


def compare(sink_rows, sink_columns, table) -> dict:
    """Field-by-field comparison of the sink against the archive fold."""
    index = {c: i for i, c in enumerate(sink_columns)}
    sink = {
        (r[index["venue"]], r[index["symbol"]], r[index["window_start_ns"]]): r
        for r in sink_rows
    }
    missing = sorted(set(table) - set(sink))
    extra = sorted(set(sink) - set(table))
    mismatches = []
    for key in sorted(set(sink) & set(table)):
        row = sink[key]
        want = table[key]
        for column in reference.FEATURE_COLUMNS:
            got = row[index[column]]
            expected = want[column]
            if got != expected:
                mismatches.append(
                    {
                        "key": [key[0], key[1], key[2]],
                        "column": column,
                        "sink": got,
                        "archive": expected,
                    }
                )
    return {
        "compared_windows": len(set(sink) & set(table)),
        "windows_missing_from_sink": len(missing),
        "windows_only_in_sink": len(extra),
        "field_mismatches": len(mismatches),
        "first_mismatches": mismatches[:5],
        "missing_examples": [list(k) for k in missing[:5]],
        "extra_examples": [list(k) for k in extra[:5]],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sink", required=True)
    ap.add_argument("--topic", default=tvcommon.TOPIC)
    ap.add_argument("--bootstrap", default=tvcommon.BOOTSTRAP)
    ap.add_argument("--archive-root", default="docs/data")
    ap.add_argument("--archive", action="append", default=None)
    ap.add_argument("--max-batch-id", type=int, default=None)
    ap.add_argument("--expect-parity", action="store_true",
                    help="fail unless the sink matches the archive exactly")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    topic = read_topic(args.bootstrap, args.topic)
    rows = topic.pop("_rows")
    sink = read_sink(args.sink, args.max_batch_id)
    metrics = read_metrics(args.sink)

    # What the archive says, and what the topic says. They differ whenever the
    # producer was told to distort the stream, so both are reported.
    paths = args.archive or tvcommon.archive_dirs(args.archive_root)
    archive_table, archive_meta = reference.from_archives(paths)

    # Fold the topic twice. The sink is compared against the *deduplicated*
    # topic, because a redelivery is not a second event and the pipeline is
    # supposed to make it harmless; comparing against every byte on the topic
    # would make a working deduplicator look like a discrepancy. The naive fold
    # is kept because it is the evidence that deduplication did something: when
    # the two disagree, the difference is exactly what a consumer without a
    # dedup boundary would have over-counted.
    distinct: dict[tuple, dict] = {}
    for row in rows:
        distinct.setdefault(
            (row["venue"], row["symbol"], row["row_ordinal"]), row
        )
    topic_table = reference.fold(distinct.values())
    naive_table = reference.fold(rows)

    accounted = sink["windowed_records"] + metrics.get("dropped_total", 0)
    identity_holds = accounted == topic["distinct_with_event_time"]

    result = {
        "topic": topic,
        "sink": {
            "windows": sink["windows"],
            "windowed_records": sink["windowed_records"],
            "max_batch_id": sink["max_batch_id"],
            "revised_window_count": len(sink["revised_windows"]),
            "most_revised": [
                {
                    "venue": r[0],
                    "symbol": r[1],
                    "window_start_ns": r[2],
                    "revisions": r[3],
                    "first_batch": r[4],
                    "last_batch": r[5],
                }
                for r in sink["revised_windows"][:5]
            ],
        },
        "metrics": {k: v for k, v in metrics.items() if k != "watermark_by_batch"},
        "accounting": {
            "distinct_with_event_time": topic["distinct_with_event_time"],
            "windowed": sink["windowed_records"],
            "dropped_by_watermark": metrics.get("dropped_total", 0),
            "deduped_redeliveries": topic["duplicate_records"],
            "without_event_time": topic["distinct_without_event_time"],
            "identity_holds": identity_holds,
            "unaccounted": topic["distinct_with_event_time"] - accounted,
        },
        "vs_topic": compare(sink["latest"], sink["columns"], topic_table),
        "vs_naive_topic": compare(sink["latest"], sink["columns"], naive_table),
        "vs_archive": compare(sink["latest"], sink["columns"], archive_table),
        "archive": archive_meta,
    }

    print(json.dumps(result, indent=2, default=str))
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(result, handle, indent=2, default=str)

    ok = identity_holds and result["vs_topic"]["field_mismatches"] == 0
    if args.expect_parity:
        against = result["vs_archive"]
        ok = (
            ok
            and against["field_mismatches"] == 0
            and against["windows_missing_from_sink"] == 0
            and against["windows_only_in_sink"] == 0
        )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
