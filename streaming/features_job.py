"""Event-time features from Kafka, into a replayable analytical sink.

Three decisions carry the whole design.

**Event time is the venue's clock.** ``venue_ts`` decides which window a change
belongs to. ``recv_wall`` decides when that window became *readable*. A row with
no ``venue_ts`` is dropped from the windowing and counted, never bucketed by
arrival time, because that would be the streaming layer inventing an ordering
the venue never published.

**The watermark bounds state, it does not license silence.** Spark's watermark
throws away what arrives too late, and by default nothing says how much. This
job records ``numRowsDroppedByWatermark`` for every stateful operator of every
batch to ``metrics/progress.jsonl``, and ``reconcile.py`` refuses to call a run
complete unless every record published is accounted for as windowed, deduped,
without an event time, or dropped by name.

**The sink is append-only and versioned.** Update mode emits a window again
whenever it changes, and each emission is written to its own ``batch_id=``
directory. The current answer is the highest ``batch_id`` per key; the earlier
ones are the record of how it got there. A window revised after the watermark
had already passed its end is a late correction, and it is visible as an extra
revision instead of being merged away.

Aggregates are commutative and associative without exception, which is what
makes them survive retries, reordering and a restart from a checkpoint.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

import tvcommon  # noqa: E402

# The payload's version 1 shape. Declared rather than inferred: inferring a
# schema from a stream means the schema changes when the traffic does.
PAYLOAD = StructType(
    [
        StructField("schema_version", LongType()),
        StructField("venue", StringType()),
        StructField("symbol", StringType()),
        StructField("conn_epoch", LongType()),
        StructField("row_ordinal", LongType()),
        StructField("event", LongType()),
        StructField("msg_index", LongType()),
        StructField("recv_wall_ns", LongType()),
        StructField("venue_ts_ns", LongType()),
        StructField("skew_ns", LongType()),
        StructField("side", LongType()),
        StructField("price", LongType()),
        StructField("qty", LongType()),
        StructField("seq", LongType()),
        StructField("first_seq", LongType()),
        StructField("prev_seq", LongType()),
        StructField("suspect", BooleanType()),
        StructField("traded_qty", LongType()),
    ]
)

KNOWN_FIELDS = [f.name for f in PAYLOAD.fields]


class ProgressLog:
    """Write the numbers a watermark would otherwise keep to itself.

    ``numRowsDroppedByWatermark`` is the count of records the engine discarded
    because they arrived after their window had been evicted. A job that does
    not publish it is a job whose output cannot be told apart from one that lost
    nothing.

    This polls ``recentProgress`` from the driver rather than registering a
    listener. A Python ``StreamingQueryListener`` needs a py4j callback server,
    and when that goes wrong it takes the stream execution thread down with it,
    which is a large amount of new failure surface in exchange for a callback.
    Polling a ring buffer the engine already keeps cannot break the query.
    """

    def __init__(self, path: str):
        self._path = path
        self._seen: set[int] = set()

    def drain(self, query) -> None:
        for progress in query.recentProgress:
            batch_id = progress.get("batchId")
            if batch_id is None or batch_id in self._seen:
                continue
            self._seen.add(batch_id)
            record = {
                "batchId": batch_id,
                "timestamp": progress.get("timestamp"),
                "numInputRows": progress.get("numInputRows"),
                "eventTime": progress.get("eventTime", {}),
                "stateOperators": [
                    {
                        "operatorName": op.get("operatorName"),
                        "numRowsTotal": op.get("numRowsTotal"),
                        "numRowsUpdated": op.get("numRowsUpdated"),
                        "numRowsRemoved": op.get("numRowsRemoved"),
                        "numRowsDroppedByWatermark": op.get(
                            "numRowsDroppedByWatermark"
                        ),
                    }
                    for op in progress.get("stateOperators", [])
                ],
            }
            with open(self._path, "a") as handle:
                handle.write(json.dumps(record) + "\n")


def build(spark, args):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        # A malformed record must stop the run rather than become nulls that
        # look like a quiet venue.
        .option("failOnDataLoss", "true")
        .load()
    )

    text = raw.selectExpr("CAST(value AS STRING) AS json")
    parsed = text.select(
        F.from_json("json", PAYLOAD).alias("p"),
        # The same bytes parsed a second time as a bare map, purely to see the
        # keys. A field the declared schema does not mention is an additive
        # schema change, and this is what makes it visible instead of silently
        # discarded by from_json.
        F.map_keys(F.from_json("json", MapType(StringType(), StringType()))).alias(
            "keys"
        ),
    ).select(
        "p.*",
        F.size(F.array_except("keys", F.array(*[F.lit(k) for k in KNOWN_FIELDS])))
        .alias("unknown_fields"),
    )

    # Rows with no venue timestamp have no event time. They are excluded here,
    # deliberately and in one place, and reconcile.py counts them off the topic
    # so the total still closes.
    parsed = parsed.filter(F.col("venue_ts_ns").isNotNull())

    # Spark timestamps are microseconds, so the nanosecond stamp is truncated
    # for the watermark. That cannot change which window a row lands in: a
    # window boundary is a whole number of seconds, hence a whole number of
    # microseconds, and flooring to microseconds never carries a value below a
    # boundary it was already at or above. Exact nanosecond extremes are
    # aggregated from the integer column, not from this.
    parsed = parsed.withColumn(
        "event_time", F.expr("timestamp_micros(venue_ts_ns div 1000)")
    )

    deduped = parsed.withWatermark("event_time", args.watermark).dropDuplicatesWithinWatermark(
        # The record's identity. row_ordinal is the producer's, derived from the
        # archive's own arrival order, because the archive has no per-row key:
        # Kraken repeats a (msg_index, side, price) inside one message, so the
        # natural key is not unique. Deduplication only holds inside the
        # watermark; a redelivery older than that is not caught here.
        ["venue", "symbol", "row_ordinal"]
    )

    windowed = (
        deduped.groupBy(
            F.window("event_time", f"{tvcommon.WINDOW_SECONDS} seconds").alias("w"),
            "venue",
            "symbol",
        )
        .agg(
            F.count(F.lit(1)).alias("updates"),
            F.sum((F.col("side") == 0).cast("long")).alias("bid_updates"),
            F.sum((F.col("side") == 1).cast("long")).alias("ask_updates"),
            F.sum((F.col("qty") == 0).cast("long")).alias("removals"),
            F.sum((F.col("event") == 0).cast("long")).alias("snapshot_rows"),
            F.sum(F.col("suspect").cast("long")).alias("suspect_rows"),
            F.min("price").alias("price_min"),
            F.max("price").alias("price_max"),
            F.sum("qty").alias("qty_sum"),
            # Null, never zero, when the feed cannot say what traded. Spark's
            # sum returns null for an all-null group, which is the archive's own
            # convention and the one the schema doc insists on.
            F.sum("traded_qty").alias("traded_qty_sum"),
            F.count("traded_qty").alias("traded_qty_known"),
            F.max(F.abs("skew_ns")).alias("max_abs_skew_ns"),
            F.min("conn_epoch").alias("min_conn_epoch"),
            F.max("conn_epoch").alias("max_conn_epoch"),
            F.min("venue_ts_ns").alias("event_time_min_ns"),
            F.max("venue_ts_ns").alias("event_time_max_ns"),
            F.min("recv_wall_ns").alias("availability_time_min_ns"),
            # The point-in-time column. A window is not readable before this,
            # whatever its event-time bounds say.
            F.max("recv_wall_ns").alias("availability_time_max_ns"),
            F.max("schema_version").alias("max_schema_version"),
            F.sum((F.col("unknown_fields") > 0).cast("long")).alias(
                "unknown_field_rows"
            ),
        )
        .select(
            "venue",
            "symbol",
            (F.unix_micros(F.col("w.start")) * F.lit(1000)).alias("window_start_ns"),
            (F.unix_micros(F.col("w.end")) * F.lit(1000)).alias("window_end_ns"),
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
    )
    return windowed


def writer(revisions_dir: str):
    def write_batch(batch_df, batch_id: int) -> None:
        # One directory per micro-batch, overwritten in place. Re-executing a
        # batch after a failure rewrites the same directory with the same rows,
        # so the sink cannot end up holding a batch twice. That is where this
        # pipeline's idempotence lives, and it is the only place it lives.
        (
            batch_df.withColumn("written_at_ns", F.lit(int(time.time() * 1e9)))
            .write.mode("overwrite")
            .parquet(os.path.join(revisions_dir, f"batch_id={batch_id}"))
        )

    return write_batch


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--topic", default=tvcommon.TOPIC)
    ap.add_argument("--bootstrap", default=tvcommon.BOOTSTRAP)
    ap.add_argument("--sink", required=True)
    ap.add_argument("--checkpoint", required=True)
    ap.add_argument("--watermark", default=tvcommon.WATERMARK)
    ap.add_argument("--max-offsets-per-trigger", type=int, default=25000)
    ap.add_argument("--run-seconds", type=float, default=0,
                    help="stop cleanly after this long; 0 waits for termination")
    ap.add_argument("--idle-stop-seconds", type=float, default=8,
                    help="stop once this long has passed with no new input")
    args = ap.parse_args()

    os.makedirs(args.sink, exist_ok=True)
    metrics_dir = os.path.join(args.sink, "metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    revisions_dir = os.path.join(args.sink, "revisions")

    spark = (
        SparkSession.builder.appName("tickvault-features")
        .master("local[2]")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # recentProgress is a ring buffer and the metrics file is built from it,
        # so it has to be deep enough to hold every batch of a run.
        .config("spark.sql.streaming.numRecentProgressUpdates", "2000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    progress_log = ProgressLog(os.path.join(metrics_dir, "progress.jsonl"))

    query = (
        build(spark, args)
        .writeStream.outputMode("update")
        .foreachBatch(writer(revisions_dir))
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="2 seconds")
        .start()
    )

    started = time.time()
    last_input = time.time()
    # The idle clock must not start before the first batch has actually
    # processed something. Spark spends the first seconds resolving the Kafka
    # source, so a naive "no input for N seconds" test fires during startup and
    # stops the query in the middle of batch zero.
    seen_input = False
    while query.isActive:
        time.sleep(1)
        progress_log.drain(query)
        progress = query.lastProgress
        if progress and progress.get("numInputRows", 0) > 0:
            seen_input = True
            last_input = time.time()
        if args.run_seconds and time.time() - started > args.run_seconds:
            break
        if (
            args.idle_stop_seconds
            and seen_input
            and time.time() - last_input > args.idle_stop_seconds
        ):
            break
    query.stop()
    query.awaitTermination(30)
    progress_log.drain(query)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
