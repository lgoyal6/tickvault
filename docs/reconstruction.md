# Rebuilding a book from the archive

```
tickvault reconstruct --archive ./archive --venue kraken --symbol BTC-USD \
    --at 2026-08-25T12:00:00Z --depth 10
```

```
kraken BTC-USD at 1787624889561665667 [L2]: 10 bid / 10 ask levels
  from 2166 rows in 3 file(s), from checkpoint at 1787624876215202459, clean
digest 8fc06b4f
```

## Ordering

The archive is replayed in **arrival order**, and arrival order is defined by
the files, not by sorting a column. Within a partition, files hold
non-overlapping increasing time ranges and rows within a file are in the order
they arrived, so reading files in manifest order and rows in file order is
exact.

It matters that this is not a sort on `recv_mono`. That column is monotonic
*within a recorder process* and restarts at zero every time the recorder does.
It is exact within one run and meaningless across two. `recv_wall` orders across
processes and venues, and being a wall clock it can step.

Merging venues orders by receipt wall clock, and the tie-break is **stated
rather than left to chance**: equal timestamps order by venue name, then symbol.
Two venues really can stamp the same nanosecond, and a caller comparing two runs
needs the same answer both times.

## Where a rebuild starts

Replaying a whole day to answer a question about 23:00 is the obvious
implementation and the wrong one. Two things avoid it:

- **A snapshot resets the book**, so everything before the last snapshot at or
  before the requested instant is irrelevant. This costs no storage and is
  tried first.
- **Checkpoints**, for feeds that snapshot rarely or never. Bitstamp's
  order-by-order feed has no snapshot at all.

Measured on a real 45-second Kraken recording, asking about the end:

| | rows applied | files read |
|---|---|---|
| full replay | 7,193 | 8 |
| from a checkpoint 13s earlier | 2,166 | 3 |

Both produce digest `8fc06b4f`. That equality is the point: a checkpoint is an
optimisation, not a second implementation of the book.

```
tickvault checkpoint --archive ./archive --every-secs 300
```

Checkpoints live under `_checkpoints/`, outside the published tree. They are
*derived*, and a consumer discovering them as data would double every level in
them. Deleting the directory costs nothing but the time to rebuild it.

## Depth

`--depth N` keeps the best N levels a side, applied **at the end and never
during the replay**. Truncating as you go is wrong: a level pushed out of the
window can be updated later and has to come back, and a book that dropped it
would be quietly missing depth. The gate asserts a depth-limited rebuild equals
the truncation of the full one.

That is separate from the *feed's* own depth window, below.

## Gaps are surfaced, not smoothed

A rebuild reports what it could not vouch for:

- rows the recorder marked `suspect`, with the span they covered
- recorded truncations overlapping the replayed window

Suspect rows are still applied. The caller decides what to do about them; the
rebuild's job is to make sure they cannot be used without knowing.

## Two things determinism does not catch

The stated gate is that rebuilding the same instant twice is byte-identical.
That is necessary and, on its own, weak: a reconstruction that is
deterministically *wrong* passes it. Both of the following bugs did.

**The feed's depth window has to be maintained.** Kraken's book channel is ten
deep and expects the client to keep it that way. Replaying its deltas without
doing so leaves levels that fell out of the feed long ago, and the rebuilt book
crossed within seconds while the recorder's never had. The archive now records
`feed_depth` per file so a rebuild truncates exactly as the recorder did.

**A message's levels must be applied together.** Applying each archived row as
its own update truncates once per *level* instead of once per *message*, and a
message that adds a new best while removing the old worst gives a different book
each way. That is what `msg_index` is in the schema for. Caught on OKX, whose
400-deep feed rebuilt to 399 levels with a different digest.

Both were found by the property that determinism does not give you: **a rebuild
at the end of a recording must equal the book the recorder itself held.** It is
checked across four venues in `tests/gate_reconstruct.rs`, and it is the test
worth writing first.
