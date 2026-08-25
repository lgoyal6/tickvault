# The Python package

Almost nobody doing a backtest wants to learn Rust to read a Parquet file. This
is the same query and replay code the recorder runs, exposed as a package.

Not on PyPI yet. Tagging a release builds wheels for every platform, verifies
one of them actually installs and passes the suite, and publishes; until that
tag exists, build it from a checkout:

```bash
cd bindings && maturin develop --release
```

Once published:

```bash
pip install tickvault
pip install 'tickvault[polars,plot]'   # optional: dataframes and the depth chart
```

One wheel per platform covers every Python from 3.9 up. The extension is built
against the stable ABI, so a new Python release does not leave you compiling
Rust at install time.

## The shape of it

| call | gives you |
|---|---|
| `tickvault.open(path)` | an `Archive` |
| `archive.partitions()` | what the archive holds, per venue, symbol and date |
| `archive.symbols()` | the `(venue, symbol)` pairs present |
| `archive.span_ns(venue, symbol)` | first and last receipt instant |
| `archive.book_at(venue, symbol, at)` | a `Book` rebuilt at that instant |
| `archive.bars(venue, symbol, bar_seconds=, frame=)` | OHLCV as arrow, polars or pandas |
| `archive.replay(venue, symbol, speed=)` | an iterator of `Tick` |
| `archive.plot_book(venue, symbol, at)` | a depth chart |
| `archive.verify()` | what a read-back of every file found |
| `tickvault.capabilities()` | per venue: what it can prove, and its blind spots |

## Three conventions

**`None` is never zero.** A one-sided book has no `mid` and no `spread`. An
empty book has no `imbalance`. A bar from an aggregated feed has no `volume`,
because a level shrinking might be a fill or a cancellation and the venue never
says which. Only an order-by-order feed reports executions, so `volume` is a
real number on Bitstamp at `--book-level 3` and `None` on the other five. Zero
would be the claim that nothing traded, which is a different statement.

**Suspect data is labelled, not withheld.** `book.suspect`, `tick.suspect` and
`book.suspect_rows` say when the recorder could not vouch for a window.
`plot_book` writes it on the chart. The archive never quietly interpolates
across a gap, so neither does this.

**Exactness runs out at the float boundary, and says where.** Prices reach
Python as floats for convenience. The archive holds them as exact integers
counting 1e-9 units, and `book.to_arrow()` hands those over untouched, so
anything that needs to reproduce a venue's own checksum has the digits to do it.
Bars are floats throughout: a bar's mid is already a truncated midpoint, so
there is no exactness left to preserve.

## Timestamps

Everything is nanoseconds since the Unix epoch, as an `int`. `datetime` stops at
microseconds and feeds routinely put several messages inside one, so converting
would discard exactly the digits that tell them apart. `tickvault.isoformat(ns)`
renders one with all nine digits when you want to read it.

Anywhere an instant is accepted, an RFC 3339 string works too:

```python
archive.book_at("kraken", "BTC-USD", "2026-08-25T12:00:00.500Z")
archive.book_at("kraken", "BTC-USD", 1787626160284980917)
```

## Replaying

```python
for tick in archive.replay("kraken", "BTC-USD", speed=0):
    strategy.on_book(tick.at_ns, tick.bid, tick.ask)
```

`speed=0` runs as fast as the archive reads. `speed=1` preserves the recorded
spacing, so a burst that arrived in one millisecond is delivered as one
millisecond of work; that is the difference between testing whether a strategy
works and whether it can keep up. Any other value scales the same spacing.

`start` positions the book rather than filtering the stream. The archive is
rebuilt to exactly that instant first, so the message that landed on it is
already reflected in `replay.book` and is not delivered again. Re-delivering it
would apply the same order-by-order event twice.

The iterator is lazy: one open file and one decoded batch at a time. Reading the
first tick of a day costs the same resident memory as reading all of it, and
`replay.files_opened` reports how many files it actually had to touch.

## Dataframes

```python
bars = archive.bars("kraken", "BTC-USD", bar_seconds=60, frame="polars")
bars = archive.bars("kraken", "BTC-USD", bar_seconds=60, frame="pandas")
batch = archive.bars("kraken", "BTC-USD", bar_seconds=60, frame="arrow")
```

Arrow is the native form; polars is backed by Arrow so it shares the buffers
rather than copying them. pandas copies, because pandas has its own memory
model, and that is a property of pandas rather than of this.

## Type stubs

The package ships `py.typed` and full stubs, and a test fails if the stubs drift
from what the module actually exposes.
