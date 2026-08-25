# tickvault

Full-depth crypto order book archives, honest about their own gaps.

There is no good free source of full-depth crypto order book history. Everything
free is top-of-book, resampled to bars, or carries undocumented gaps that
quietly poison a backtest. This reads the archives produced by
[tickvault](https://github.com/lakshgoyal/tickvault), which publish their gap
statistics on the front page.

```bash
pip install tickvault-ob
```

`tickvault-ob` on PyPI, `tickvault` when you import it. The plain name
normalises to `tick-vault` under PEP 503, and that belongs to an unrelated
Dukascopy tick downloader, so the distribution carries a suffix and the import
does not.

Not published yet. Until the first release tag, build it from a checkout of the
repository with `cd bindings && maturin develop --release`.

## Ten lines to a plot

```python
import tickvault

archive = tickvault.open("./archive")
bars = archive.bars("kraken", "BTC-USD", bar_seconds=60, frame="polars")
print(bars.select("start", "open", "high", "low", "close", "volume"))

start, end = archive.span_ns("kraken", "BTC-USD")
book = archive.book_at("kraken", "BTC-USD", end)
print(book.mid, book.spread, book.imbalance(depth=10))

archive.plot_book("kraken", "BTC-USD", end, show=True)
```

Nine lines, and no date to look up first. A test runs exactly this block against a
real archive on every commit, so it cannot rot into an example that no longer works.

## Two things to know

**`None` means the data cannot answer, and never zero.**

```python
>>> bars["volume"][0] is None
True
```

An aggregated feed shows a price level shrinking and never says whether it
traded or was cancelled, so the traded volume of a Kraken or Coinbase bar is not
something the data contains. Zero would claim nothing traded. Only an
order-by-order feed reports executions, and only Bitstamp offers one without an
API key, so `volume` is a real number there and `None` on the other five.

Same rule elsewhere: a one-sided book has no `mid`, and an empty one has no
`imbalance`.

**Suspect data is labelled, not hidden.**

```python
book = archive.book_at("kraken", "BTC-USD", when)
if book.suspect:
    print(book.origin, book.suspect_rows)   # why, and how much
```

Every `Tick` carries `suspect` too, and `plot_book` writes it on the chart. The
recorder knows which stretches of time it could not vouch for; this hands that
through rather than quietly averaging over it.

## Replaying

```python
for tick in archive.replay("kraken", "BTC-USD"):
    if tick.mid is not None:
        strategy.on_book(tick.at_ns, tick.bid, tick.ask)
```

`speed=0` is as fast as the archive reads, which is what a backtest wants.
`speed=1` preserves the recorded gaps, so a burst that arrived in one
millisecond is delivered as one millisecond of work. That is the difference
between testing whether a strategy works and whether it can keep up.

## Exact numbers

The float properties above are conveniences. The archive holds prices as exact
integers counting 1e-9 units, and that is what you get from Arrow:

```python
batch = book.to_arrow()          # price and qty are int64, nothing rounded
price = batch["price"][0].as_py() / 10**9
```

Bars are floats: a bar's mid is already a truncated midpoint, so exactness has
run out by then.

## What the venues can prove

```python
>>> for v in tickvault.capabilities():
...     print(v["venue"], v["detects_loss"], v["validation"])
coinbase True Counter { step: 1, restart_floor: 0 }
kraken True Checksum { depth: 10 }
okx True Chained
bybit True Counter { step: 1, restart_floor: 1 }
binance-us True UpdateIdRange
bitstamp False MonotonicTimestamp
```

Bitstamp's aggregated feed carries no sequence number, update id or checksum, so
a lost message on it is undetectable by any means. It says so rather than
reporting itself as clean. Its **order-by-order** feed chains every event, and
recording it that way makes it the best validated of the six.

Each entry also carries `blind_spots`: the things that venue cannot see, in
plain terms.

## Licence

MIT or Apache-2.0, at your option.
