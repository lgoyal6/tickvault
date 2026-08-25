# tickvault

I invest, and I built myself a morning brief for AI infrastructure market news.
When I went looking for actual order book data to test an idea against, I found
that there is no good free source of full-depth crypto order book history.
Everything free is top-of-book, resampled to bars, or carries undocumented gaps.
Paid sources start in the hundreds per month. So students and hobbyists doing
microstructure work either pay or use data they cannot trust.

tickvault is a recorder that produces the dataset I wanted: **full-depth book
snapshots and deltas across six venues, in Parquet, free, with the gap
statistics on the front page.** Every free dataset has holes. This one tells you
exactly where they are, per venue per day, so you know which windows to exclude
rather than finding out when your backtest looks suspiciously good.

The recorder is not the point. The dataset is the point, and **the gap report is
its front page.**

> **Thesis.** For market data the interesting property is not throughput, it is
> whether the file is telling you the truth. A recorder that silently stitches
> deltas across a dropped message produces a book that looks continuous and is
> wrong, and you cannot tell by looking. tickvault makes that mechanically
> checkable: every message is validated against whatever the venue gives you to
> validate with, every window it could not vouch for is marked suspect and
> counted, and the venues that cannot be checked at all are published as 0%
> verified rather than rounded up to look like the rest.

**Live demo:** [lgoyal6.github.io/tickvault](https://lgoyal6.github.io/tickvault/),
the real reconstruction engine compiled to WebAssembly. Pick a venue, drag to any
instant, and watch the order book rebuild from the archive message by message,
with the coverage grid beside it saying which windows are trustworthy.

```bash
cargo run --release -- record --venue kraken --seconds 60 --archive ./archive
pip install tickvault
```

## Six venues, five validation schemes

Chosen to disagree with each other as much as possible. An abstraction
validated against two similar feeds is one that breaks on the third.

| | Coinbase | Kraken | OKX | Bybit | Binance.US | Bitstamp |
|---|---|---|---|---|---|---|
| loss detection | counter | CRC32 | chained id | counter | id range | **none** |
| can detect loss | yes | yes | yes | yes | yes | **no** |
| size of a loss | exact | unknown | unknown | exact | update ids | n/a |
| scope | **connection** | symbol | symbol | symbol | symbol | symbol |
| check runs | before apply | **after apply** | before apply | before apply | before apply | before apply |
| snapshot | in band | in band | in band | in band | **REST** | **REST** |
| feed depth | full (~44k) | truncated to N | 400 | 50 | full | full |
| decimals | strings | **JSON numbers** | strings | strings | strings | strings |
| orphan deletes | **routine** | never | never | never | **routine** | rare |
| keepalive | protocol | protocol | **text ping** | **JSON ping** | protocol | protocol |
| quotes | USD | USD | USDT | USDT | USDT | USD |

Every row was measured against the live feed, not read off a doc page, and some
rows contradict what the docs imply. **USD and USDT are different instruments at
different prices**, so half these venues do not list the pair the other half
does, and asking for the wrong one is an error rather than a silent
substitution.

The ingest loop branches on those capabilities in exactly **two** places:
whether the validator's state belongs to the connection or to the symbol, and
whether the check runs before or after applying. Everything else is data.

## Verified against live feeds

All six, twenty seconds each:

```
venue       symbol          msgs   gaps  missing  unsized  orphan crossed  resnap verified%    clean%
coinbase    BTC-USD          376      0        0        0       0       0       1  100.0000  100.0000
kraken      BTC-USD         1049      0        0        0       0       0       1  100.0000  100.0000
okx         BTC-USDT         193      0        0        0       0       0       1  100.0000  100.0000
bybit       BTC-USDT         433      0        0        0       0       0       1  100.0000  100.0000
binance-us  BTC-USDT          49      0        0        0       0       0       1  100.0000  100.0000
bitstamp    BTC-USD           61      0        0        0      21       1       2    0.0000   99.6115
```

That last row is the whole point. Bitstamp reads **0% verified, 99.6% clean**:
no reason to think it is broken, and no way to know, because its aggregated feed
carries nothing to check against. It is not rounded up to look like the others.
Record it order-by-order instead and it becomes the best validated of the six.

Kraken's 2,413 live book messages across two symbols all matched our recomputed
CRC32. The algorithm is pinned to a captured frame as a regression test, so a
refactor that breaks it fails immediately rather than producing a dataset that
blames Kraken for our arithmetic.

## The honesty rules

1. **On a detected gap, stop applying and rebuild from a snapshot.** Never
   stitch deltas across a hole. A stitched book looks continuous and is wrong,
   which is the exact failure that makes free order book data untrustworthy.
2. **Unverifiable is not clean.** A message we could not check is counted apart
   from one we checked and passed.
3. **Never blame the venue for our own defect.** If our decimal rendering lost
   digits, or the book is shallower than the checksum covers, that message is
   recorded as unverifiable, not as a gap.
4. **Publish the blind spots.** Kraken's checksum covers ten levels a side, so
   at depth 1000 a lost message touching level 400 is undetectable by
   construction. That is a test, not a footnote.
5. **A loss that changed nothing is not a gap.** If a dropped update was
   overwritten before it mattered, the archive holds exactly the right book, and
   flagging it would send people to discard good data.
6. **Null is not zero.** Where the data cannot answer a question it returns
   nothing rather than a number. A one-sided book has no mid. An aggregated feed
   does not know what traded.

## What measuring against live feeds actually changed

Every one of these overturned something I had already written.

**Coinbase Exchange is unusable for this.** `ws-feed.exchange.coinbase.com` now
answers `full`, `level2` and `level3` with *"these channels now require
authentication"*. Its one remaining keyless book channel sends no sequence
number and no checksum, so a recorder built on it cannot detect loss at all.
`advanced-trade-ws.coinbase.com` is keyless and numbers every message.

**Coinbase's counter spans the connection, including control frames.** A
subscription acknowledgement arrives numbered *between* two book messages, so a
validator watching only book frames reports a phantom gap every time the venue
acknowledges something.

**Coinbase deletes price levels it never published.** In one 30-second capture
it sent 210 deletes for prices absent from its own 44,067-level snapshot; Kraken
sent zero in 958 updates. Treating that as corruption made the recorder rebuild
a 4.8 MB book every two seconds and report a healthy feed as 2% clean. Whether
an absent-level delete means anything is a per-venue fact, and it is declared as
one.

**OKX's `seqId` is an identifier, not a counter.** It jumped by 7, 12, 13, 20,
45 and 93 between consecutive messages. Its `prevSeqId` proves continuity
exactly and says nothing about how much a gap swallowed. Binance measures loss
in update ids, which is a third quantity again. The report keeps all three
apart rather than inventing a common unit.

**Bitstamp's REST book contradicts its own diff stream.** Starting from one
snapshot and applying its own subsequent diffs, the book crosses by the
fifteenth message and never heals: the snapshot holds an ask the stream never
retracts.

**A REST snapshot needs a replay buffer.** Bitstamp's REST book lags its feed by
about a second, so resetting to it and resuming from the next live message drops
that second. Those drops resurface as deletes for unknown levels, forcing
another snapshot. Measured before the fix: twelve re-snapshots in forty seconds
on a healthy feed.

## Bugs the gates actually caught

The gates are worth more than the features. Three of these were found only
because a gate asserted a property rather than an output.

- **A determinism gate passes on a book that is deterministically wrong.** Mine
  did, twice. Adding *a rebuild at the end of a recording must equal the book
  the recorder itself held* caught both immediately: Kraken rebuilt to 35 levels
  from a 10-deep feed and crossed, because the feed's depth window was not being
  maintained on replay; and OKX's 400-deep feed rebuilt to 399 with a different
  digest, because each archived row was applied as its own delta, truncating per
  level instead of per message.
- **Every L3 bar was reporting a traded volume of exactly zero.** An execution
  row carries the quantity *still resting*, which on a full fill is nothing, and
  volume was being read off it. Zero is not "unknown"; it is the claim that
  nothing traded, made by the one feed that actually knows. The size that
  changed hands is only knowable while the order's previous state is still in
  hand, so it is now computed during recording into its own `traded_qty` column.
- **A checkpoint re-applied its own rows.** Harmless for idempotent level
  setting, corrupting for order-by-order.
- **The writer deadlocked on shutdown**, because a ticker task held a strong
  sender clone so aborting it never released the channel.
- **A feed that went silent was never noticed**, and the coverage grid is what
  caught it. Found on a three hour capture:
  Bitstamp stopped sending without closing the connection, so the socket stayed
  ESTABLISHED and the reader waited on it at zero CPU for sixty two minutes.
  That is the worst failure here, because the archive ends up with no rows and
  the gap report has nothing to say, so a lost feed reads as a quiet market.
  There is now an idle timeout, and the silence is recorded as downtime.

## Order by order

Exactly one of the six exposes an order-by-order feed without a key, and it is
the one that is worst in aggregate. Recorded at `--book-level 3`, Bitstamp goes
from 0% verifiable to 100%: every event chains to the last, so a break proves
loss occurred.

Queue position is inferred where the data permits and marked where it does not.
Every position carries one of three certainties, and a position inherits the
weakest certainty of anything ahead of it:

| certainty | means |
|---|---|
| `observed` | we watched every order ahead of this one arrive |
| `seeded` | some order ahead came from a snapshot, so its place is the venue's listing order rather than something seen |
| `unknown` | something happened that price-time priority does not determine |

Two things the feed cannot settle, recorded rather than guessed. Orders that
predate the recording leave their level marked incomplete, so no position on it
is ever reported as observed. And on 19 of 3,999 measured events the resting
quantity moved by more than the reported trade explains, which makes execution
versus cancellation undecidable; those carry `size_change_explained = false`.

## The archive

Parquet, partitioned `venue=/symbol=/date=`, one row per price level or order
event. Prices and quantities are **exact integers at 1e-9**, never floats:
Kraken validates its own book by CRC32 over the price *strings*, so a rounding
error makes the archive unverifiable against the venue that produced it.

**Backpressure is a decision, not a default.** When the writer falls behind the
socket, `--backpressure block` stalls the reader and risks a venue disconnect,
and `--backpressure drop` sheds rows. Either is defensible. Doing one silently
is not, so drops are counted into the same gap report as venue-caused holes,
under a column that says we caused them.

**A crash costs a bounded, named window.** Arrow buffers a whole Parquet file in
memory, so an interrupted file is zero bytes rather than half-written; there is
no such thing as a torn row here. Recovery quarantines partials, adopts complete
files the manifest never listed, discards abandoned compactions, and records the
truncation point. The gate kills the process with a real `SIGKILL` at randomized
offsets during sustained write, restarts, and asserts every file reads back.

## Running it as a service

`record` captures one venue for a fixed time and prints a report when it stops.
That is a tool. This is the service:

```bash
cp tickvault.example.toml tickvault.toml
tickvault serve --config tickvault.toml --dry-run   # check it
tickvault serve --config tickvault.toml             # run it
```

One process, every venue in the file, each with its own archive and its own
restart loop so one failing venue cannot stall the rest. Ctrl-C closes the open
Parquet files before exiting, which matters more than it sounds: a file is
buffered whole in memory, so a process that simply dies loses everything since
the last rotation. Measured on a run with a ten minute rotation: forty seconds
in, nothing on disk and twenty seven thousand rows in memory; after the signal,
four files, all verifying.

And it answers for itself while it runs, which is the part that was missing:

```
$ curl -s localhost:8080/
venue          frames last frame    reconn  restarts  state
kraken           4075   0.7s ago         0         0  ok
okx               426   0.6s ago         0         0  ok
bitstamp        11031   0.3s ago         0         0  ok
```

`/healthz` returns 503 naming any venue that is down or has gone quiet, and
`/metrics` is Prometheus text. **The field that matters is when a frame last
arrived.** A feed that is connected and silent looks identical to a healthy one
in every other number, which is exactly how three of them went unnoticed for an
hour.

## The viewer

The archive is the product, and a Parquet file shows a visitor nothing. So the
reconstruction layer, which is the hardest part of this and otherwise entirely
invisible, runs in the browser:

```bash
scripts/build-viewer.sh                     # wasm into docs/
python3 -m http.server -d docs 8000
```

It is the real engine. `viewer/` compiles the same `BookReplayer` the recorder
and the query layer use to `wasm32-unknown-unknown`, hands it archived rows, and
draws what comes back. Five tests hold the browser to the library: the final
book must match the digest the recorder held, every prefix must match a fresh
replay of that prefix, and scrubbing backwards must land where scrubbing
forwards did.

Two constraints the browser imposed on the library, both worth having anyway.
Capture now sits behind a default `record` feature, so the read half compiles
without tokio, a TLS stack or a C compression codec; and the rule for where one
message ends is public rather than buried in the file reader, so a second
consumer cannot get it subtly wrong.

The archive shipped with the page is transcoded to snappy, because zstd ships
hand-written amd64 assembly and cannot target wasm at all:

```bash
scripts/refresh-demo-data.sh ~/path/to/recording
```

which transcodes, generates the coverage grid over the whole recording rather
than over the slice that ships, and cuts the slice starting at the opening
snapshot.

## From Python

Almost nobody doing a backtest wants to learn Rust to read a Parquet file, so
the query and replay layers are also a package. One wheel covers Python 3.9 up.

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

That is the whole distance from install to a plot, and a test executes exactly
that block against a real archive on every commit, so it cannot rot into an
example that no longer works.

It is the same code the recorder runs, not a reimplementation, so the same rules
hold. `volume` is `None` rather than zero on the five venues that publish
aggregated levels. `book.suspect` is true when the recorder could not vouch for
the window, and `plot_book` writes it on the chart. Prices arrive as floats for
convenience and as exact 1e-9 integers through `to_arrow()`, which hands Arrow
buffers to polars without a copy. Details in [`docs/python.md`](docs/python.md).


## Architecture

| module | what it holds |
|---|---|
| `fixed` | exact decimals; checksum validation cannot survive floats |
| `book` | the L2 book, its crossed-book invariant, Kraken's CRC32 |
| `book::l3` | order-by-order book, lifecycle, and queue position |
| `venue` | the `Venue` trait, six implementations, the capability matrix |
| `symbols` | one pair, seven spellings, and the ambiguities between them |
| `sequence` | five validators, and what each provably cannot prove |
| `gap` | suspect windows and the report they feed |
| `limits` | rate limiting, and laying symbols out to bound blast radius |
| `session` | the ingest loop; exactly two branches on venue capabilities |
| `transport` | websocket and replay, behind one trait |
| `recorder` | raw frame capture, which is what makes the gates possible |
| `pipeline` | the bounded channel, the backpressure decision, and its cost |
| `store` | Parquet schema, writer, manifest, recovery, reader, compaction |
| `reconstruct` | rebuild a book at an instant, with checkpoints |
| `query` | streaming cursor, aggregations, and paced replay |
| `bindings` | the pyo3 crate and the Python package built on it |
| `viewer` | the same engine compiled to wasm, behind the demo page |

Blast radius follows the capability matrix. Because Coinbase numbers the socket
rather than the instrument, packing symbols onto one connection means a single
gap invalidates all of them, so the planner spreads them; Kraken numbers per
symbol, so it packs them. `tickvault plan` shows the reasoning.

Further reading: [`docs/venues.md`](docs/venues.md) for the per-venue findings,
[`docs/schema.md`](docs/schema.md) for the columns,
[`docs/reconstruction.md`](docs/reconstruction.md),
[`docs/querying.md`](docs/querying.md), [`docs/python.md`](docs/python.md).

## Build and test

```bash
cargo test                     # 310 tests
cargo test -- --ignored        # real SIGKILLs and live venue reconciliation

cd bindings
maturin develop --release
pytest tests                   # 44 tests, built from tapes so they need no network
```

The Python tests replay committed tapes through the real ingest path into a
byte-identical archive, so they need neither a live venue nor a Parquet blob
checked into git.

```bash
cargo run --release -- capabilities        # what each venue can prove
cargo run --release -- record --venue kraken --seconds 60 --archive ./archive
cargo run --release -- verify  --archive ./archive
cargo run --release -- compact --archive ./archive
cargo run --release -- query   --archive ./archive --venue kraken --symbol BTC-USD --bar-secs 10
cargo run --release -- replay  --archive ./archive --venue kraken --symbol BTC-USD --speed 1
```

## Limitations (deliberate)

- **Bitstamp cannot be validated at all in aggregate.** No sequence number, no
  checksum. Reported as 0% verified, never rounded up.
- **Order-by-order is one venue deep.** Every other keyless L3 feed now requires
  authentication, so five of six fall back to L2 and the archive records which
  level it holds per partition.
- **Kraken and OKX never say how much they lost**, only that something was.
- **Kraken past depth 10 is partly blind.** Declared in the matrix and
  demonstrated by a test.
- **Coinbase cannot attribute a gap to a symbol.** Its counter belongs to the
  connection, so one gap marks every symbol on that socket suspect.
- **Volume is unknown on five of six venues.** Only an order-by-order feed says
  what traded, and only Bitstamp offers one without a key.
- **Orders predating an L3 recording are invisible**, and the levels they rest
  on cannot report an observed queue position until they empty.
- **A loss at the very end of a recording is undetectable.** Nothing follows it
  to reveal the absence.
- **A quiet feed and a dead one are indistinguishable from here.** After sixty
  seconds of silence the recorder reconnects and records downtime, which costs a
  re-snapshot if the venue was merely quiet. Waiting instead costs the data.
- **Bybit's REST cross-check is geo-blocked** from a US address. Its websocket
  is fine, so recording works and the out-of-band comparison does not.
- **A crash still costs a bounded window**, not zero. The manifest names it, and
  the raw tape is a separate line-durable record that could be reprocessed to
  fill it, but that reprocessing is not built.
- **Blocking and dropping both cost something** once the disk is the
  bottleneck, and above roughly 2.4M rows/s one of them will happen.
- **The demo ships five minutes per venue, not the dataset.** Enough for a
  browser to fetch and rebuild; the archive itself belongs on Hugging Face.
- **The viewer skips checkpoints.** It replays from the partition's opening
  snapshot rather than selecting files, which is fine for minutes and would not
  be for a day.
- **No dataset is published yet.** The continuous validation service, the
  alerting, and the automated daily publish are the remaining phase.

## License

MIT or Apache-2.0, at your option. See [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).
