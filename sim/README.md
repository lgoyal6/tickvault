# sim

A walk-forward strategy evaluator that replays this repository's own recordings
with simulated execution.

> **The boundary, before anything else.** Every number here comes from replaying
> files already on disk, on one host. No venue is contacted, no order is placed
> anywhere, and no money moves. Fills come from observed level changes and
> observed crossings of the recorded book, not from a matching engine. Queue
> position is approximate everywhere it appears and every field carrying one is
> named `approx_`. No profitability, no alpha and no live trading is claimed.

## Why it lives here

The recorder does not import it and does not know it exists, exactly like
`streaming/` and `research/`. It is here because it is the consumer that asks
the hardest question of the archive: if you tried to trade against this data,
would the honesty machinery hold? The sequence verdicts, the `suspect` column,
the null-not-zero rule and the refusal to stitch across a hole are all load
bearing in a way they are not when the archive is only being read.

## The frozen experiment

[`manifest.json`](manifest.json) was written and committed **before the
evaluator existed**, and nothing in it has moved since. It carries the exact
input files with their sha256, the chronological windows per venue in absolute
nanoseconds, the cost block, the latency model and its seed, the risk limits,
the parameter grids and the procedure that fits them, the promotion gate
clauses, and the three negative controls. A threshold that can move after a
result is seen is not a threshold.

It also records three of its own corrections, because three assumptions this
experiment started from did not survive measurement. `traded_qty` is null on all
300,090 rows of all six recordings, so every one of them is an aggregated feed
and the trade-driven fill path is inert here. Coinbase's per-message counter
skips forward once, thirteen microseconds after its predecessor, on rows the
recorder vouched for, because Advanced Trade numbers messages that carry no book
levels. And Kraken's checksums can be rechecked from the archive after all:
price precision 1 with quantity precision 8 reproduces all 11,386 of them.

## Running it

```bash
./scripts/run-strategy-eval.sh
```

That checks every input against the manifest's hashes with `shasum`, builds,
lints and tests the crate, runs the evaluation, runs the three controls and
requires each to fail, reruns the evaluation and requires byte-identical output,
and writes `results/`. It takes a few minutes.

The binary on its own:

```bash
cargo run --release -p tickvault-sim -- verify-inputs
cargo run --release -p tickvault-sim -- replay
cargo run --release -p tickvault-sim -- evaluate --out-json /tmp/eval.json
```

## How a window is replayed

The order of operations is the whole design, because every leak a backtest can
have is an ordering mistake.

1. The message draws its own market data delay and is queued to become visible
   later. Anything whose visible time has arrived is applied to the **delayed**
   book, which is the only book a strategy ever sees.
2. The message is applied to the **true** book and judged by the venue's own
   loss detector. A violation stops the window there and nothing is applied past
   it.
3. Our resting orders are worked against what the message did.
4. Orders whose entry latency has elapsed become live, after the fills, so an
   order cannot fill from the message that arrived when it reached the venue.
5. Risk samples the state and fires the kill switch if a limit broke.
6. Only then does the strategy decide, and the decision is checked for leakage
   before it is acted on.

## What stops a window

Stricter than a live feed, on purpose. A duplicate or a stale identifier is
harmless on a socket, where a venue may resend; in a file written in receive
order it means the identifiers are not the ones the recorder saw. So a
duplicate, a backwards identifier, a broken chain, a broken update-id range, a
diverging checksum, receive time going backwards, and any row the recorder
marked `suspect` all stop the window.

One thing does not: a forward skip on a per-message counter. The archive cannot
tell a lost book message from a venue message that carried no levels, so it is
counted by name as `counter_forward_skips` and the recorder's own `suspect`
column is what stops the book. Blaming the venue for a limit of our row
projection is exactly the fault `CONTRIBUTING.md` forbids.

Bitstamp publishes neither a sequence number nor a checksum, so loss on it is
undetectable by construction. Its windows are replayed and reported, and kept
out of every gate aggregate: unverifiable is not clean.

## The fill model, and how approximate it is

When an order rests at a price, `approx_queue_ahead` starts as the size the
venue was displaying there, which is a sum over orders we never saw
individually. Every observed decrease of that size reduces it.

That reduction is the approximation. A level shrinking on an aggregated feed is
either a trade or a cancellation and **the venue never says which**. Both
shorten the queue; only a trade can fill us. So the split is by trade evidence:
the reported traded quantity consumes the queue and is the only thing that can
reach the order, and whatever decrease is left over is treated as cancelled. On
these recordings the reported traded quantity is null everywhere, so no maker
fill comes from a level decrease at all.

What does fill a resting order here is a **crossing**: the opposite best
arriving at or through the order's price, which is direct evidence somebody
traded there. That volume consumes the queue first.

Three more approximations, stated rather than buried:

- A marketable order walks the recorded book without removing those levels from
  it, so a larger order than the small ones used here would be flattered.
  `extra_slippage_bps` stands in for the impact that is not modelled.
- A queue position held across a snapshot is reseeded from the size the snapshot
  shows, which is the archive's own `seeded` rather than `observed` certainty.
  Reseeds are counted.
- A crossing or a book walk looks at most 64 levels deep. Both directions of
  that bound understate rather than overstate.

## The negative controls

Each is a deliberate corruption, selected only by the gate script, and each is
expected to fail. A control that can pass proves nothing.

| control | injection | must fail with |
|---|---|---|
| `--control leak-future` | the next message's price is put into a feature | the leakage detector rejects the run |
| `--control no-costs` | fees, rebates and slippage set to zero | the result is refused as non-comparable, and the difference against the frozen-cost run is quantified |
| `--control shuffle-seq` | identifiers swapped at the window midpoint and one message observed twice | the replay invariants stop the window |

A control run exits with status 2 when it fails the way it must, which is a
different thing from any other failure, so the gate can tell a working control
from one that quietly did nothing.

## What these numbers are not

- One recording date, under six minutes per venue. The walk-forward unit is a
  window inside one recording, not a trading day, so the bootstrap interval says
  nothing about day-to-day variation.
- The maker rebate is zero, meaning a maker fill pays nothing and receives
  nothing. Real spot maker fees are usually a positive cost, so every quoting
  number here is an upper bound rather than an estimate.
- The half-spread grid is frozen in basis points, and these books quote a
  fraction of a basis point wide. The measured median spread per venue is in the
  results next to the fill counts, and it is the first thing to read before
  taking any fill rate here as typical.
- Nothing here is evidence about another day, another instrument or another
  regime.

## Reading the output

`results/strategy-report.md` is the table. `results/strategy-eval.json` carries
every per venue, window and strategy figure plus the gate evaluation and the
manifest hash. `results/strategy-trades.parquet` is every simulated fill,
including `approx_queue_ahead_at_fill` and which of the three causes produced
it.
