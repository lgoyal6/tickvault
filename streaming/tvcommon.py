"""Shared vocabulary for the downstream streaming package.

Nothing in here is imported by the recorder. The archive is the source and this
directory is a consumer of it, which is the whole point: adding a broker to the
recorder would make the trusted artefact depend on a system that can lose
messages.

Two clocks run through everything below and they are never mixed:

* **event time** is ``venue_ts``, the venue's own statement of when a change
  happened. Features are bucketed by this.
* **availability time** is ``recv_wall``, when the recorder had the row in hand.
  A feature computed over a window is not readable until this, and that is what
  makes a point-in-time read honest rather than a look-ahead.

A row with no ``venue_ts`` has no event time. It is counted and reported, never
back-filled from ``recv_wall``: substituting arrival order for venue order is
exactly the conflation the archive refuses to make.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import pyarrow.dataset as ds

# The broker the compose file publishes on the host. 19092 rather than 9092 so
# it cannot collide with anything else already running.
BOOTSTRAP = os.environ.get("TICKVAULT_KAFKA", "localhost:19092")

TOPIC = os.environ.get("TICKVAULT_TOPIC", "tickvault.book")

# One partition per instrument would be tidy but the published archive has six
# instruments and a single broker, so three partitions with the key deciding
# placement exercises the thing that matters: every record for one instrument
# lands in one partition and therefore keeps its broker order.
PARTITIONS = int(os.environ.get("TICKVAULT_PARTITIONS", "3"))

# Ten seconds of event time. Short enough that a five minute recording gives
# thirty windows to compare, long enough that a window holds real traffic.
WINDOW_SECONDS = 10

# A watermark has to cover two different kinds of lateness and only one of them
# is a property of the data. Both numbers come from `watermark_audit.py`.
#
#   0.3s  the worst event-time inversion inside any single published archive
#         (Coinbase, 0.252s). This is the venue being late with itself.
# 103.9s  how far apart the topic's partitions drift when the Spark source
#         rate-limits each one by the same fraction of its backlog. A book feed
#         opens with a snapshot burst, so records and elapsed time are not
#         proportional, and the partition carrying the big snapshots is still
#         near the start of the tape while the others are 100s in. The
#         watermark is global, so the laggard's rows are all late.
#
# The floor is 104.1s and this is the next round number clear of it. Two
# seconds, which is what the first draft used, reads as generous and silently
# discarded 101,496 of 298,090 records; measured, see the record.
WATERMARK = "150 seconds"

# Bumped by the producer when it starts emitting a new field. The consumer
# reports the versions it saw rather than assuming one.
SCHEMA_VERSION = 1

# Every column the archive contributes to a payload. Written out rather than
# taken from the file so that a column appearing or disappearing upstream is a
# visible edit here rather than a silent change of meaning.
ARCHIVE_COLUMNS = (
    "venue",
    "symbol",
    "event",
    "msg_index",
    "recv_mono",
    "recv_wall",
    "venue_ts",
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


@dataclass(frozen=True)
class ArchiveRows:
    """One archive, read in arrival order, with the two derived identities."""

    venue: str
    symbol: str
    rows: list[dict]
    # Rows the archive holds that carry no venue timestamp. They are real data
    # and they are excluded from event-time windows, so the count has to travel
    # with the rest or the reconciliation cannot close.
    without_event_time: int


def _ns(value) -> int | None:
    """pyarrow hands back a pandas Timestamp for ns precision; take its value."""
    if value is None:
        return None
    if hasattr(value, "value"):
        return int(value.value)
    return int(value)


def read_archive(path: str) -> ArchiveRows:
    """Read one venue archive in arrival order and derive what Kafka needs.

    Two identities are manufactured here because the archive does not carry
    them, and both are documented as derived rather than recorded:

    ``conn_epoch`` is the index of the snapshot run this row belongs to. Every
    reconnect re-snapshots, so a run of ``event == 0`` rows is where a
    connection began. This is the same reading ``tests/gate_restore.rs`` takes
    of the same data.

    ``row_ordinal`` is the row's position in arrival order within its archive.
    It exists because the archive has no per-row identity and one is needed for
    a consumer to tell a redelivery from a new record. Kraken proves the point:
    it sends the same ``(msg_index, side, price)`` more than once inside a
    single book update, so the natural key is not unique. See the record.
    """
    table = ds.dataset(path, format="parquet", partitioning="hive").to_table()
    # recv_mono is the archive's documented ordering column and is valid
    # within one recorder run, which is what a published file is.
    table = table.sort_by([("recv_mono", "ascending")])
    cols = {name: table[name].to_pylist() for name in ARCHIVE_COLUMNS}

    venues = set(cols["venue"])
    symbols = set(cols["symbol"])
    if len(venues) != 1 or len(symbols) != 1:
        raise ValueError(f"{path}: expected one instrument, got {venues} {symbols}")

    rows: list[dict] = []
    epoch = -1
    previous_event = None
    without_event_time = 0
    for i in range(table.num_rows):
        event = cols["event"][i]
        if event == 0 and previous_event != 0:
            epoch += 1
        previous_event = event
        venue_ts = _ns(cols["venue_ts"][i])
        if venue_ts is None:
            without_event_time += 1
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "venue": cols["venue"][i],
                "symbol": cols["symbol"][i],
                "conn_epoch": epoch,
                "row_ordinal": i,
                "event": event,
                "msg_index": cols["msg_index"][i],
                "recv_wall_ns": _ns(cols["recv_wall"][i]),
                "venue_ts_ns": venue_ts,
                "skew_ns": cols["skew_ns"][i],
                "side": cols["side"][i],
                "price": cols["price"][i],
                "qty": cols["qty"][i],
                "seq": cols["seq"][i],
                "first_seq": cols["first_seq"][i],
                "prev_seq": cols["prev_seq"][i],
                "suspect": bool(cols["suspect"][i]),
                "traded_qty": cols["traded_qty"][i],
            }
        )

    return ArchiveRows(
        venue=venues.pop(),
        symbol=symbols.pop(),
        rows=rows,
        without_event_time=without_event_time,
    )


def key_for(row: dict) -> bytes:
    """Kafka key: venue and instrument, so one instrument keeps one partition.

    ``BTC-USD`` and ``BTC-USDT`` produce different keys because they are
    different instruments quoted in different currencies, which the archive is
    careful about and a downstream key must not undo.
    """
    return f"{row['venue']}|{row['symbol']}".encode()


def encode(row: dict) -> bytes:
    return json.dumps(row, separators=(",", ":")).encode()


def archive_dirs(root: str) -> list[str]:
    return sorted(
        os.path.join(root, name)
        for name in os.listdir(root)
        if os.path.isdir(os.path.join(root, name))
    )


def committed_revisions(sink: str) -> list[str]:
    """Every batch directory the writer finished, and nothing else.

    ``foreachBatch`` writes each micro-batch through Spark's file committer,
    which stages part files under ``_temporary`` and renames them into place on
    commit, dropping a ``_SUCCESS`` marker last. A job killed mid-batch leaves
    the directory holding a truncated part file and no marker.

    That matters more than it looks. A reader globbing ``revisions/**`` picks up
    the truncated staging file and cannot parse it, so one killed batch makes
    the *whole* sink unreadable rather than one batch of it. Measured: the read
    fails with "too small to be a Parquet file". Selecting on the marker is what
    lets the sink be read while a job is running and after one has died.
    """
    root = os.path.join(sink, "revisions")
    if not os.path.isdir(root):
        return []
    return [
        os.path.join(root, name)
        for name in sorted(os.listdir(root))
        if name.startswith("batch_id=")
        and os.path.exists(os.path.join(root, name, "_SUCCESS"))
    ]


def revision_globs(sink: str) -> list[str]:
    """The committed batch directories as globs a reader can hand to DuckDB."""
    dirs = committed_revisions(sink)
    if not dirs:
        raise ValueError(f"{sink}: no committed batch directories to read")
    return [os.path.join(d, "*.parquet") for d in dirs]
