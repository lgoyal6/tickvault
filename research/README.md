# research

A small research harness that asks one question of the archive and answers it
with the archive's own guarantees, not around them.

> **The question.** On clean, verified windows, does order book imbalance -
> measured from the book as it stood at the moment the recorder had received it -
> predict the mid price over the next 1, 5 or 30 seconds, out of sample, better
> than a random walk?

Nothing here places an order, connects to a venue, or claims a profit. The word
"edge" below always means a mid-to-mid basis point number, and the mid is not a
price anyone can trade at. The cost block exists so that any measured number can
be put next to what it would take to act on it.

## Why it lives in this repository

The recorder does not import it and does not know it exists, exactly like
`streaming/`. It is here because it is the consumer that tests whether the
archive's promises are worth anything: the `suspect` column, the availability
clock, and a reconstruction that does not silently stitch across a hole. It is
built on the published Python bindings, so it exercises what a user installs.

## The four pieces

| File | What it does |
|---|---|
| `tvmanifest.py` | Hashes every source file. A result that cannot say which bytes produced it is not reproducible |
| `tvpanel.py` | Photographs the book on a fixed grid **in receive order**, so a feature at T cannot see past T |
| `tvstudy.py` | Chronological folds, two baselines, out-of-sample R-squared, block-bootstrap intervals, and what it would cost to act |
| `tvcontrols.py` | A planted-signal positive control and a look-ahead control that rewrites the future |

## Running it

The bindings must be installed (`maturin build --release` in `bindings/`, then
`pip install` the wheel). `pyarrow`, `pandas` and `numpy` are the only other
requirements.

```bash
# one archive per venue, laid out the way six concurrent writers actually land
python research/tvmanifest.py build \
  --archive data/coinbase --archive data/kraken \
  --window 2026-09-02T09:00:00Z..2026-09-02T11:00:00Z \
  --out manifest.json
python research/tvmanifest.py check --manifest manifest.json --base data

python research/tvpanel.py --base data \
  --instrument coinbase:BTC-USD --instrument kraken:BTC-USD \
  --window 2026-09-02T09:00:00Z..2026-09-02T11:00:00Z \
  --step-ms 1000 --warmup-min 55 --out panel.parquet

python research/tvstudy.py --panel panel.parquet --horizons 1,5,30 --out results.json

PYTHONPATH=research python research/tvcontrols.py \
  --panel panel.parquet --base data --scratch /tmp/tvc \
  --lookahead "kraken,BTC-USD,2026-09-02T09:00:00Z,2026-09-02T11:00:00Z,2026-09-02T10:00:00Z" \
  --out controls.json
```

`--warmup-min` is not decoration. The reconstructor seeds the book from
everything the archive holds before the window, and a window that starts a
minute after the first file has a book built from a minute of deltas with no
snapshot behind it. Requiring an hour of archive before the first sample is what
stops that from being invisible.

## The rules the harness enforces

**One clock.** Every row is stamped with an availability time (`recv_wall`) and
built only from messages received by then. The venue's own `venue_ts` is never
used to select. This repository has already measured what happens otherwise:
selecting point-in-time reads on event time returned data that did not exist yet
in 182 of 4,629 reads, the worst by 17.766 seconds (`streaming/pit.py`).

**Suspect data is not quietly used.** A sample counts only when every message
behind the span it touches was one the recorder could vouch for, and an
instrument whose rows are mostly suspect is excluded by name in the output
rather than averaged in.

**No random splits.** Expanding window by day, training only on earlier days,
with the tail of each training day purged by the horizon so an overlapping
target cannot land on both sides of a split.

**Nothing is fitted on the test period.** Standardisation, the redundant-column
check and the clipping bounds all come from training rows only.
