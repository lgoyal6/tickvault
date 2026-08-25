# Querying and replaying the archive

```
tickvault query  --archive ./archive --venue kraken --symbol BTC-USD --bar-secs 10
tickvault replay --archive ./archive --venue kraken --symbol BTC-USD --speed 5
```

## Everything streams

A range query opens one file and holds one Parquet batch at a time. Asking
about a day costs the same resident memory as asking about a second, and a
consumer never has to decide how much of the archive fits in RAM. Measured on a
63-file archive: a query over the whole thing opened all 63 files, one narrowed
to the last eighth opened 9.

The starting book comes from [reconstruction](reconstruction.md), so a query
over the last hour of a day costs the last hour rather than the day. The two
share one implementation of applying archived rows to a book; they differ in
which rows they feed it, which is where the bugs have actually been.

The cursor is deliberately **not** an `Iterator`. Yielding the book would copy
it once per message, and a day of a busy venue is millions of messages.
Borrowing it is what a lending iterator would do and Rust has no such trait, so
the cursor advances and the caller reads the book in place.

## Aggregations come from the book

```
 bar start (s)         open         high          low        close     msgs  spread bp       volume
1787626110.000     80768.15     80784.25     80768.15     80775.95      652       0.10      unknown
1787626120.000     80775.95     80775.95     80751.05     80751.05      408       0.07      unknown

final book: mid 80723.05 spread 0.1 imbalance -0.8375 over 5 levels
```

Open, high, low and close are the **mid price**, sampled from the book after
every message. Not from bars: a bar built from a bar has already lost the thing
a microstructure question is about.

Also available, all from the book that was actually there: spread (absolute and
in basis points), order book imbalance over the top N levels, and the quantity
resting within a price offset of the mid, which is the question "how much can I
trade before moving the price by X".

### Volume says `unknown`, and that is the point

An OHLCV bar normally carries traded volume, and this is a *book* archive. An
aggregated feed shows a level shrinking and never says whether it traded or was
cancelled, so the volume of a Kraken or Coinbase bar is not something the data
contains. It is reported as `unknown` rather than zero, because **zero would
claim nothing traded.**

An order-by-order feed does report executions, so a Bitstamp L3 bar carries a
real traded quantity. Same column, honest in both cases, with the difference
visible rather than averaged away.

### Empty intervals are absent, not flat

An interval with no messages does not produce a bar. A bar that was never
observed is not the same as a flat one, and filling it in would hide exactly the
outages this archive exists to publish.

Two more things that are `None` rather than a plausible number: the mid of a
one-sided book, and the imbalance of an empty one. Both are undefined, and
neither is zero.

## Replay

```
$ tickvault replay --archive ./archive --venue kraken --symbol BTC-USD --every 250
 1787626116056015084      80775.9 / 80776        spread 0.1
 1787626120695287167      80759.1 / 80762.9      spread 3.8
 1787626128889858875      80752.7 / 80752.8      spread 0.1

2828 messages (0 suspect) covering 58.105s in 0.017s, 3508.0x real time
```

`--speed 0` runs as fast as the archive reads, which is what a backtest wants.
Any other value preserves the recorded gaps, scaled: `--speed 1` is real time,
`--speed 5` is five times faster. Measured: 4.848s of archive replayed in 1.221s
at a 5x request.

Pacing is against the recorded receipt gaps, so a replay reproduces the feed's
own rhythm rather than a smoothed version of it. That is the difference between
testing whether a strategy *works* and testing whether it can *keep up*: a burst
that arrived in one millisecond is delivered as one millisecond of work.

A consumer that takes longer than a gap simply proceeds. Sleeping the full gap
anyway would make every later message later still, and the replay would drift
further behind the whole way through.

## Suspect data travels with the query

A cursor reports what it could not vouch for: the seed's doubts plus anything
the range itself carried, as suspect rows and their span. Suspect messages are
still delivered, and each `Tick` says whether it was one. The caller decides
what to do; the query's job is to make sure it cannot be used without knowing.
