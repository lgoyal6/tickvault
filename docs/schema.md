# Archive schema

The published artifact is a Hive-partitioned Parquet dataset. `pyarrow`,
`polars`, `duckdb`, and Spark all discover it without being told the layout:

```python
import pyarrow.dataset as ds
t = ds.dataset("archive", format="parquet", partitioning="hive").to_table()
```

## Layout

```
archive/
  _manifest.jsonl                     # what the archive vouches for
  _quarantine/                        # files a crash left unreadable
  venue=kraken/
    symbol=BTC-USD/
      date=2026-08-25/
        part-<opened-ns>-<n>.parquet  # written live
        daily-2026-08-25.parquet      # after compaction
```

`venue`, `symbol`, and `date` are partition keys *and* columns, so a single file
still identifies itself when read on its own. Files beginning with `_` are
ignored by partition discovery.

## Columns

One row per **level change**. `msg_index` groups the levels that arrived in one
message, so flattening loses nothing.

| column | type | notes |
|---|---|---|
| `venue` | `string` | recording venue |
| `symbol` | `string` | canonical `BASE-QUOTE` |
| `event` | `uint8` | 0 = snapshot level, 1 = delta level |
| `msg_index` | `uint64` | groups levels from one message |
| `recv_mono` | `uint64` | monotonic ns since the recorder started. **Order by this** |
| `recv_wall` | `timestamp[ns, UTC]` | wall clock at receipt |
| `venue_ts` | `timestamp[ns, UTC]`, nullable | the venue's own stamp, never reconciled with ours |
| `skew_ns` | `int64`, nullable | `recv_wall - venue_ts`, signed and unclamped |
| `side` | `uint8` | 0 = bid, 1 = ask |
| `price` | `int64` | **exact count of 1e-9 units** |
| `qty` | `int64` | same scale; **zero means the level was removed** |
| `seq` | `uint64`, nullable | venue identifier, where it has one |
| `first_seq` | `uint64`, nullable | first id covered, on range feeds (Binance) |
| `prev_seq` | `uint64`, nullable | id this message follows, on chained feeds (OKX) |
| `checksum` | `uint32`, nullable | the venue's book checksum (Kraken) |
| `suspect` | `bool` | **true if this row fell inside a suspect window** |
| `book_level` | `uint8` | 2 = aggregated price levels, 3 = order by order |
| `order_id` | `string`, nullable | the venue's order identifier, L3 rows only |
| `action` | `string`, nullable | `add`, `modify`, `cancel`, or `execute` |
| `queue_index` | `uint32`, nullable | orders ahead of this one at its price |
| `queue_qty_ahead` | `int64`, nullable | quantity that must trade first, at 1e-9 |
| `queue_certainty` | `string`, nullable | `observed`, `seeded`, or `unknown` |
| `size_change_explained` | `bool`, nullable | false when the venue's numbers do not account for the size change |
| `traded_qty` | `int64`, nullable | quantity this event traded, at 1e-9; null when the feed cannot say |

### `qty` is what is left, `traded_qty` is what changed hands

On an L3 row these are different questions and the difference matters. `qty` is
the quantity still resting after the event, so a fully filled order records
zero. `traded_qty` is how much that event traded.

The venue publishes a *cumulative* traded figure per order, so the size of one
execution is the rise in that figure since the previous event for the same
order. That subtraction is only possible while the order's earlier state is
still in hand, which is during recording; it cannot be recovered from the
archive afterwards. So it is computed once, at record time, and written down.

`traded_qty` is null, never zero, when the feed cannot answer:

- on every aggregated feed, since a level shrinking may be a fill or a
  cancellation and the venue never says which;
- on an execution against an order created before the recording began, since
  what it had been resting for was never observed.

Summing `traded_qty` over a window gives traded volume. Summing `qty` over
execution rows gives zero, which is a different and false statement.

### Prices are integers, on purpose

`price` and `qty` are exact integers at 1e-9, not floats and not decimals:

```python
from decimal import Decimal
px = Decimal(row["price"]) / Decimal(10) ** 9
```

The scale is in the file metadata (`tickvault.price_scale`) and on each field,
so the file is self-describing. The reason is the same one that runs through
this whole project: Kraken validates a book by CRC32 over price *strings*, and a
float round trip silently breaks that. A dataset that cannot survive its own
validation is not worth publishing. The cost is that a naive reader gets
integers, which is a deliberate trade: a wrong number that looks right is worse
than a right number that needs dividing.

### `suspect` is the column that matters

Every row carries whether it fell inside a window the recorder could not vouch
for. The one-line filter is the whole point:

```python
clean = t.filter(pc.equal(t["suspect"], False))
```

The gap report says *why* and *how much*. This column is what makes acting on it
a single expression rather than a join.

### Queue position comes with its own warranty

`queue_index` and `queue_qty_ahead` are worth exactly what `queue_certainty`
says they are:

- **`observed`** means we watched every order at that level arrive. The position
  is a fact.
- **`seeded`** means some of it came from a snapshot, so the relative order is
  the venue's listing order rather than something seen.
- **`unknown`** means the level is known to hold orders we never saw, or the
  order was re-sized and forfeited its place. Counting the orders we know about
  would understate the queue.

A position inherits the *weakest* certainty of anything ahead of it: knowing
exactly where you are in a queue is worth nothing if the sizes in front of you
are assumed. The filter a strategy actually wants is one expression:

```python
usable = t.filter((pc.equal(t["book_level"], 3)) &
                  (pc.equal(t["queue_certainty"], "observed")))
```

Which book level a partition holds is also recorded per file in the manifest, so
the dataset states what it has per venue per day rather than leaving a consumer
to infer it from whether `order_id` happens to be populated.

### Nulls are meaningful

A null `venue_ts` means the venue sent no timestamp, not that it sent zero. A
null `seq` means the venue publishes no sequence number, which is true for
Kraken and Bitstamp and is a fact about the feed rather than missing data. Which
identifier columns a venue populates is exactly its row in
[`venues.md`](venues.md).

### A note on nanosecond timestamps

Timestamps are nanosecond precision because microstructure work needs them.
`pyarrow` alone will not convert those to Python `datetime` (its range is
microseconds); use `pandas`, or read the integer directly with
`t["recv_wall"][i].value`. Casting to `timestamp[us]` raises rather than
silently rounding, which is the behaviour to want.

## Durability

- Files are written as `.parquet.partial` and renamed on close, so a file with
  the final name is always complete.
- The manifest is appended and fsynced once per completed file. It is the
  archive's definition of what is safe to publish.
- **A crash costs at most one flush interval plus one rotation interval per
  partition**, and the manifest records a truncation entry naming the window.
  Arrow buffers a whole file in memory, so an interrupted file is zero bytes on
  disk rather than partially readable; there is no partial recovery to attempt,
  which is why the bound is the rotation interval and not something finer.
- Anything unreadable is moved to `_quarantine/`, never deleted.
- Compaction swaps many files for one in a single manifest entry, so the archive
  can never be caught double-counting rows or missing them.

Check any archive with:

```
tickvault verify --archive <dir>
```

which recovers, then opens every file the manifest vouches for and reads it
through. It checks the pages rather than trusting the footer's row count.
