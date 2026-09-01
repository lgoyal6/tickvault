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

`tickvault query --explain` prints exactly this, as counts:

```
manifest          2 of       46 files      (95.7% pruned on recorded span)
row groups        3 of      120 groups     (97.5% pruned on footer statistics)
pages            11 of      360 pages      (96.9% pruned on the page index)
rows          22000 of  6000000 rows       (0.37% decoded)
columns          14 of       24 columns    (61.2% of bytes in scope)
```

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

`scripts/scan-bench.sh` measures both sides on a real recording. The measured
table for this archive lives outside the repository with the rest of the
benchmark output; the shape of the result is that going finer buys scan
granularity roughly linearly until pages stop containing whole queries, and
costs compression ratio roughly linearly the whole way.

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

- **No predicate pushdown on anything but time and `event`.** A general
  expression pushdown would be a query engine, and nothing asks for one.
- **No Bloom filters.** They answer point lookups on high-cardinality columns,
  which is a question this archive is never asked.
- **No parallel file reads.** The read path is a `RowStream` that holds one
  open file and one decoded batch on purpose, so a query over a day costs the
  same memory as a query over a second. Reading files concurrently would trade
  that away for wall clock, and the streaming bound is the more useful
  property.
