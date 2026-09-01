# How a query decides what to read

Every question this archive is asked has the same shape: a range of
`recv_wall`, within one `(venue, symbol, date)` partition. This is what happens
between asking and getting rows back.

## The four levels

They are applied cheapest first, and each one only exists because the one
above it could not do the job.

| Level | Where the answer comes from | What it costs to check |
|---|---|---|
| Partition | The directory name, `venue=/symbol=/date=` | Nothing; it is a path |
| File | `first_recv_wall` and `last_recv_wall` in `_manifest.jsonl` | One read of the manifest, already done at open |
| Row group | Parquet footer statistics, per column per group | One read of the footer |
| Page | The Parquet page index, per column per page | A few KB at the tail of the file |

And orthogonally to all four, the **column**. The schema has 24 of them and a
replay reads 14. `venue` and `symbol` are the partition key repeated on every
row; `recv_mono` is a monotonic reading that restarts with the recorder and is
never ordered on; `seq`, `first_seq`, `prev_seq`, `checksum` and `skew_ns`
exist for the gap report rather than for the book.

`tickvault query --explain` prints exactly this, as counts. Thirty seconds out
of a compacted 45 minute Coinbase recording, 788,834 rows in one file:

```
manifest           1 of        1 files      (0.0% pruned on recorded span)
row groups         1 of       16 groups     (93.8% pruned on footer statistics)
pages              1 of        3 pages      (66.7% pruned on the page index)
rows           20418 of   788834 rows       (2.59% decoded)
columns           14 of       24 columns    (5.4% of bytes in scope)
```

Counts rather than one ratio, because the four levels prune for different
reasons and collapsing them would hide which one did the work. Compaction is
what makes this the interesting shape: once a day is one file the manifest
prunes nothing at all, and the footer and the page index are everything.

## Why this works, and where it would not

All of it rests on one property: **the archive is written in arrival order, so
`recv_wall` is sorted.** A range predicate on a sorted column lands in a
contiguous run of row groups and pages, and everything else can be skipped
from metadata.

That is a fact about this writer, not a fact about Parquet. Nothing else in
the schema has it:

- A predicate on `price` prunes nothing. Prices walk up and down all day, so
  every row group's price range spans most of the book and a mid-book price is
  inside almost all of them. `tests/gate_scan.rs` asserts this rather than
  leaving it implied: it reads a real file's footer and checks that an instant
  lands in exactly one row group while a mid-book price lands in most of them.
  If that ever stopped being true, the headline pruning number would have
  quietly come to mean something else.
- A predicate on `order_id` would be worse still, and is the case a Bloom
  filter exists for. There is none here, because nothing asks that question.

The honest summary is that this is one well-clustered column being indexed
properly, not a general query engine.

## The one non-time predicate

`event == Snapshot` is pushed down as well, because the reconstructor asks it
on every rebuild and the layout answers it for free.

`Reconstructor::last_snapshot_before` walks a partition's files backwards
looking for the last snapshot at or before an instant. `event` is 0 for a
snapshot level and 1 for a delta, so a row group whose minimum is 1 holds no
snapshot, and a file where that is true of every group can be answered from
its footer without reading a page.

This matters most where it looks like it should matter least. A venue that
snapshots on every reconnect gives the walk an early exit. Bitstamp's
order-by-order channel never snapshots at all, so before this the walk decoded
every row of every file in the day to return `None`.

## Column projection saves CPU, not bytes

Projecting 24 columns down to 14 does **not** remove 40% of the bytes read.
The columns dropped are the ones that compress to almost nothing: `venue` and
`symbol` are one dictionary entry repeated per row, and `seq` and `recv_mono`
are dense ascending integers that delta-encode away. The columns kept are the
ones carrying real entropy, `price` and `qty` and `order_id`.

What projection actually saves is decode work, and on an order-by-order
archive that is not small: `order_id`, `action` and `queue_certainty` are
`Utf8`, and building a `Row` from them allocates a `String` per row per
column. The snapshot probe, which needs two columns rather than fourteen, is
where this shows up most.

The `Explain` output reports bytes in scope and rows decoded separately for
this reason. One of them is I/O and the other is CPU, and on this schema they
do not move together.

## Page granularity is a choice, and it is not free

The page index is only as fine as the pages. Parquet's default is 20,000 rows
per page, and at this archive's 50,000-row row groups that is three pages per
group, which is barely finer than the group itself.

Smaller pages prune harder and cost storage, because each page is its own
compression context and zstd has less to work with. The cost is in the data,
not in the metadata: the page index itself stays small.

The row group size is the same tradeoff one level up, and it is already fixed
at 50,000 by a different constraint. A row group is what the writer buffers
before flushing, so it bounds resident memory on the write side, and it is
also the unit an interrupted file loses. Tuning it for read pruning would move
a number the crash story depends on, which is why the page size is the dial
that moved here and the row group size is not.

`scripts/scan-bench.sh` measures both sides. Sweeping the dial over the
compacted Coinbase archive above, asking the same thirty second question,
median of seven runs on an M3 Pro:

| rows/page | archive bytes | rows decoded | pages read | query |
|---|---|---|---|---|
| 20,000 (Parquet's default) | 6,281,525 | 20,016 | 1 of 3 | 0.074 s |
| 8,000 | 6,402,121 (+1.9%) | 16,944 | 2 of 7 | 0.068 s |
| 2,000 | 6,885,057 (+9.6%) | 10,800 | 5 of 25 | 0.075 s |
| 500 | 8,090,883 (+28.8%) | 8,192 | 8 of 50 | 0.093 s |
| 200 | 9,896,605 (+57.6%) | 8,192 | 8 of 50 | 0.113 s |

Three things worth reading off that.

**The curve is U-shaped.** Rows decoded falls the whole way, and query time
does not: it bottoms out around 8,000 and then climbs. Each page carries a
fixed decode cost and its own compression frame, and past some point that
overtakes what the pruning saves.

**It stops helping before it stops costing.** At 500 and at 200 rows per page
the query decodes the same 8,192 rows, because that is the reader's own batch
granularity and no page index can go below it. The archive keeps growing
anyway.

**The cost is in the data, not the metadata.** The footer only moved from
45,194 to 46,227 bytes across the whole sweep. What grows is the file, because
zstd has less to work with per page.

**So the default is left at Parquet's 20,000.** The measured optimum is 8,000,
worth about 6 ms on a 74 ms query of which 26 ms is process startup, on one
45 minute archive of one instrument. That is not enough evidence to move a
storage format default, and the dial is there for anyone whose archive says
otherwise. The exact commands and the raw output are with the rest of the
benchmark material, outside this repository.

On an order-by-order archive the dial does almost nothing at all: Bitstamp's
rows carry `order_id`, `action` and `queue_certainty` as `Utf8`, so Parquet's
1 MB page *byte* limit binds long before any row count does.

## Reading the plan yourself

```sh
# The plan for a narrow window out of a long recording.
tickvault query --archive ./archive --venue coinbase --symbol BTC-USD \
  --from 2026-09-01T09:20:00Z --to 2026-09-01T09:20:30Z --explain

# The same rows written at a different page granularity, to compare.
tickvault transcode --archive ./archive --out ./fine --compression zstd \
  --row-group-rows 50000 --data-page-rows 2000

# Both comparisons at once, against a binary built from a base commit.
scripts/scan-bench.sh ./archive coinbase BTC-USD main
```

## What is deliberately not here

- **No `RowFilter`, and no late materialisation.** arrow-rs can evaluate a
  predicate on a few decoded columns and only materialise the rest for the rows
  that survive, which is the right tool when the predicate needs few columns
  and the output needs many. Neither of this archive's two predicates is
  shaped like that. The time range is answered from the page index without
  decoding anything at all, which is strictly better than decoding
  `recv_wall` to filter on it. And the snapshot probe needs the same two
  columns for the predicate and for the answer, so there is nothing left to
  defer.
- **No predicate pushdown on anything but time and `event`.** A general
  expression pushdown would be a query engine, and nothing asks for one.
- **No Bloom filters.** They answer point lookups on high-cardinality columns,
  which is a question this archive is never asked.
- **No parallel file reads.** The read path is a `RowStream` that holds one
  open file and one decoded batch on purpose, so a query over a day costs the
  same memory as a query over a second. Reading files concurrently would trade
  that away for wall clock, and the streaming bound is the more useful
  property.
