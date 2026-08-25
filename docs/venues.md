# Venue capability matrix

Six venues, all keyless. Everything here was measured against the live feeds in
August 2026, not taken from documentation. Where a venue's docs and its
behaviour disagreed, the behaviour is recorded and the disagreement noted.

The matrix lives in code as `VenueCapabilities` and is printed by
`tickvault capabilities`. This document explains *why* each row is what it is;
the code is the authority on the values, and `tests/conformance.rs` is what
stops the two drifting apart.

## The matrix

| | Coinbase | Kraken | OKX | Bybit | Binance.US | Bitstamp |
|---|---|---|---|---|---|---|
| loss detection | counter | CRC32 | chained id | counter | id range | **none** |
| can detect loss | yes | yes | yes | yes | yes | **no** |
| size of a loss | exact | unknown | unknown | exact | update ids | n/a |
| scope | **connection** | symbol | symbol | symbol | symbol | symbol |
| check runs | before apply | **after apply** | before apply | before apply | before apply | before apply |
| snapshot | in band | in band | in band | in band | **REST** | **REST** |
| feed depth | full (~44k) | client-truncated | 400 | 50 | full | full |
| decimals | strings | **JSON numbers** | strings | strings | strings | strings |
| orphan deletes | **routine** | never | never | never | **routine** | rare |
| keepalive | protocol | protocol | **text ping** | **JSON ping** | protocol | protocol |
| quotes | USD | USD | USDT | USDT | USDT | USD |

Two structural facts fall out of that table.

**USD and USDT are different instruments.** Three venues list Bitcoin against
actual dollars and three against Tether. They trade at different prices. The
canonical symbol keeps them apart, the archive partitions on it, and asking a
venue for the pair it does not list is an error rather than a silent
substitution.

**No two venues validate the same way.** Five distinct schemes across six
venues is not an accident of selection; it is what the space actually looks
like, and it is why the abstraction is built around declared capabilities
rather than around an assumed common shape.

---

## Coinbase Advanced Trade

- **Websocket** `wss://advanced-trade-ws.coinbase.com`, channel `level2`
- **REST book** `https://api.coinbase.com/api/v3/brokerage/market/product_book`

### Why not Coinbase Exchange

`ws-feed.exchange.coinbase.com` is the better-known API and is no longer usable
here. Subscribing to `full`, `level2`, or `level3` returns:

```json
{"type":"error","message":"Failed to subscribe",
 "reason":"level2, level3, and full channels now require authentication."}
```

That also rules out its order-by-order `full` channel, which would have been
the natural L3 source for phase 4. The one keyless book channel remaining,
`level2_batch`, sends neither sequence number nor checksum, so loss on it is
undetectable and a dataset built from it could not publish a gap report at all.

### The counter belongs to the connection

`sequence_num` increments by one per **frame on the socket**, restarting at zero
on each connection. Two consequences, both of which shape code elsewhere:

**It cannot be attributed to a symbol.** Every symbol on the connection is
invalidated by any one gap on it. That is why `SequenceScope::PerConnection`
exists, and why the subscription planner spreads Coinbase symbols across
sockets instead of packing them: four symbols on four sockets means one gap
costs one symbol, not four.

**It includes control frames.** Observed in one capture:

| frame | channel | `sequence_num` |
|---|---|---|
| 4 | `l2_data` | 4 |
| 5 | `subscriptions` | 5 |
| 6 | `l2_data` | 6 |

A validator watching only book frames sees 4 then 6 and reports a gap that
never happened, once per acknowledgement.

### Deletes for levels it never published

The snapshot is genuinely full depth: 22,773 bids and 21,294 asks for BTC-USD,
about 4.8 MB. Despite that, Coinbase sends `"new_quantity": "0"` for prices that
appear neither in the snapshot nor in any earlier update. The fourth update of
one connection deleted `78851.5`, never mentioned before; there were 210 such
deletes on that connection.

Treating them as corruption produced 14 reconnects in 30 seconds, 84 MB of
snapshot traffic, and a report claiming a healthy feed was 2.3% clean. With
`RedundantDeletes::Expected` the same recording is 0 reconnects, 9 MB, 100%
clean.

---

## Kraken Spot v2

- **Websocket** `wss://ws.kraken.com/v2`, channel `book`
- **REST** `Depth` for books, `AssetPairs` for precision

### No sequence numbers at all

Kraken numbers nothing. Every book message carries a CRC32 over the ten best
levels a side, and loss is detected by recomputing it. Three consequences:

1. **The check runs after applying**, because the hash describes the resulting
   book. A mismatch means the book is already contaminated.
2. **The size of a loss is never knowable.** Only that one happened.
3. **It only sees ten levels.** Subscribed deeper, a lost message touching
   level 400 changes the book and leaves the checksum matching.

Hence the default depth of 10: the only depth at which the checksum covers
every level the feed can touch. Deeper is more useful data and strictly weaker
validation. `tests/gate_gap_detection.rs` demonstrates the blind spot at depth
100 rather than asserting it, so changing the default breaks a test.

### The checksum

Ten best asks ascending, then ten best bids descending: render price and
quantity at the pair's precision, delete the decimal point, strip leading
zeros, concatenate, CRC32. Verified against a captured frame:

```
price precision 1, quantity precision 8
computed 3709077306   venue 3709077306   match
```

Pinned as a regression test, and matched on all 2,413 messages of a live
30-second two-symbol recording.

### Three spellings of one instrument

| context | BTC-USD is spelled |
|---|---|
| websocket v2 | `BTC/USD` |
| REST `AssetPairs.wsname` | `XBT/USD` |
| REST result key | `XXBTZUSD` |

`XBT` is Bitcoin, `XDG` is Dogecoin. Note that `XBTUSD` is genuinely ambiguous
once TrueUSD is a known quote asset: it splits as `XBT`+`USD` or `XB`+`TUSD`.
The symbol mapper reports the ambiguity rather than guessing, and registering
the pair resolves it.

---

## OKX v5

- **Websocket** `wss://ws.okx.com:8443/ws/v5/public`, channel `books`
- **REST book** `https://www.okx.com/api/v5/market/books`

### Chained identifiers

The strongest continuity proof of the six and the least informative about
magnitude. Every message carries `seqId` and `prevSeqId`, so a message is in
order when it *names* the one we last applied. Nothing is assumed about step
size, which matters because there is none: consecutive ids observed live jumped
by 7, 12, 13, 20, 45, and 93. A snapshot carries `prevSeqId: -1`, meaning "this
begins a chain".

The consequence is that a broken chain proves loss exactly and can say nothing
about how much, so OKX gaps are recorded with an explicitly unknown size rather
than a fabricated count.

### The checksum field is always zero

`books` publishes a `checksum` field. It was **zero on every frame** across 400
messages on BTC-USDT and again on ETH-USDT, snapshot and update alike. It is
therefore treated as absent rather than as a check that always fails, and the
chain is the only validation used.

### Depth

The channel carries 400 levels a side and maintains that depth itself, sending
the removal for every level leaving the window. Measured over 398 updates: the
book never overflowed 400 and no delete ever orphaned.

---

## Bybit v5 spot

- **Websocket** `wss://stream.bybit.com/v5/public/spot`, topic `orderbook.50`
- **REST book** `https://api.bybit.com/v5/market/orderbook`

A per-symbol counter, which after Coinbase's connection-wide one is the useful
contrast: a gap here really does belong to one instrument, so symbols pack onto
a socket without widening the blast radius.

One Bybit-specific rule: `u` increments by one per message, but the venue resets
it to `1` when its service restarts, and that message is a fresh snapshot rather
than a loss. A validator that only knew "numbers go up" would report a
catastrophic backwards jump every restart, so the counter carries an explicit
restart floor.

**Its REST book is geo-blocked from a US address**, answering *"The Amazon
CloudFront distribution is configured to block access from your country"*,
while the websocket connects and streams normally. Recording is unaffected
because the snapshot arrives in band, but the REST cross-check is unavailable
there.

Measured over 398 updates at depth 50 with client-side truncation: zero orphan
deletes.

---

## Binance.US

- **Websocket** `wss://stream.binance.us:9443/ws` plus a `SUBSCRIBE` frame
- **REST book** `https://api.binance.us/api/v3/depth`

### No in-band snapshot at all

The only venue of the six whose websocket carries nothing but diffs. The
starting book comes from REST and must be spliced into the stream:

1. Buffer diffs before fetching.
2. Fetch `depth`, which returns a `lastUpdateId`.
3. Discard every buffered diff whose final id is at or below it.
4. Require the first survivor to satisfy `U <= lastUpdateId + 1 <= u`. If it
   does not, the snapshot and the stream do not meet, and the only correct move
   is another snapshot rather than a book with a hole behind it.

### Ranges, not counters

Each message spans a *range* of update ids. A gap is therefore measured in
update ids, and that is not the number of messages: one observed message
covered five ids at once. The report shows those in their own unit rather than
adding them to a message count.

### Orphan deletes are the norm

156 orphan deletes across 211 live diffs. The stream reports the net state of
each price level per 100 ms window, so a level added and cancelled inside one
window arrives as a delete for something that never existed as far as we saw.

---

## Bitstamp

- **Websocket** `wss://ws.bitstamp.net`, channel `diff_order_book_*`
- **REST book** `https://www.bitstamp.net/api/v2/order_book/*`

The honest case, and the reason it is in the set.

### Loss here is undetectable

The diff channel carries a microsecond timestamp and **nothing else**. No
sequence number, no update id, no checksum. A stream with a hole in it is
bit-for-bit indistinguishable from a complete one. That is a property of the
feed, not a limitation of this recorder.

So every book message is reported unverifiable rather than clean, and a
Bitstamp day reads **"100% clean, 0% verified"**: no reason to think it is
broken, no way to know. Any free dataset showing Bitstamp as fully validated is
not measuring something we failed to measure; it is asserting something the wire
cannot support.

Two weaker checks remain and are used. Ordering is checkable, and the
microtimestamp must increase. Deletes for levels we never held are counted and
published as the only weak health signal available.

### The REST book disagrees with the diff stream

The most useful thing measured on this venue. Starting from one REST snapshot
and applying its own subsequent diffs, **the book crosses by the fifteenth
message and never heals**: the snapshot contains an ask the diff stream never
retracts. A consumer trusting a single snapshot would hold a crossed book all
day without noticing. The crossed-book check catches it and rebuilds, which is
why Bitstamp re-snapshots a couple of times a minute here.

### Two things measurement reversed

**Orphan deletes are counted, not acted on.** The first implementation treated
them as loss evidence, on the reasoning that they are the only signal Bitstamp
leaves. Measurement said otherwise: from a single snapshot the book stays
consistent across 327 diffs with three orphan deletes, but tearing it down on
the first one means splicing a REST book that runs a second behind the stream,
and each splice is another chance to go wrong. Acting on the signal took runs
from 98% clean to 0% clean, with up to 58 re-snapshots in twenty seconds.

**A REST snapshot needs a replay buffer.** Its REST book lags its own feed by
about a second. Resetting to it and resuming from the next live message drops
every change in that second, and those surface later as orphan deletes, forcing
another re-snapshot, which drops another second. The session keeps recent deltas
and replays the ones the snapshot predates. Binance needs the same mechanism for
the same reason.

---

## Order-by-order (L3)

Only one of the six exposes an order-by-order feed without a key, and it is the
one that was worst at L2.

| venue | L3 feed | keyless |
|---|---|---|
| Bitstamp | `live_orders_*` | **yes** |
| Coinbase | REST `level=3` snapshot only; the `full` stream needs auth | snapshot only |
| Kraken | `level3` | no: *"Private data and trading are unavailable on this endpoint"* |
| OKX | `books-l2-tbt` | no: *"Please log in"* |
| Bybit, Binance.US | none | n/a |

### Bitstamp at L3 is a different venue from Bitstamp at L2

The same exchange, recorded two ways, gives opposite answers:

| | L2 `diff_order_book` | L3 `live_orders` |
|---|---|---|
| loss detection | **none at all** | every event names its predecessor |
| verified fraction, live | **0%** | **100%** |
| clean fraction, live | 76-99%, variable | 100% |
| re-snapshots per 25s | 2 to 8 | **0** |

Its L3 events carry `event_id` and `pre_event_id`, and across 3,999 measured
events there was not one break. So the venue that cannot be validated at all in
aggregate is exactly chained order by order. If you record Bitstamp, record it
at L3.

### Three things measurement decided

**An order-by-order book crosses by construction.** An aggressive order rests on
the book for the instant between being created and being matched. Observed with
both events sharing a microtimestamp: an order created as a bid at 79644.33 and
deleted at 79626.93, with an ask simultaneously reduced by exactly its size. The
crossed-book rule that is right at L2 would flag a healthy feed dozens of times
a minute, so crossings are counted and only a crossing that *outlasts* a match
is treated as a fault.

**A deletion can report a different price than the creation.** 62 of 1,865
measured deletions did. It is usually not a modification: an aggressive order is
deleted at the price it executed at. Either way a removal must detach the order
from where it actually rests, never from where the event says, or a phantom
stays at the old level for the rest of the day.

**Do not seed from the REST book.** Bitstamp's order-level snapshot has the same
defect as its aggregated one: it holds orders the stream never retracts. Seeding
a 25-second recording left the book crossed on 6,097 of 6,596 events, against a
single transient crossing when starting empty. Re-seeding on each crossing made
it worse, because every new snapshot plants the phantoms again. So the book
starts empty and fills in as orders churn, and `SnapshotSource::None` says so.

The cost is that orders predating the recording are invisible. That is counted,
and any level where an event arrives for an order we never saw is marked: no
queue position on such a level is reported as observed, because counting the
orders we happen to know about would understate the real queue.

### Reconciling the two feeds

The strongest available check needs no snapshot at all: between two consecutive
reports of a price level on the aggregated feed, the order-by-order events at
that price must net to exactly the quantity change reported. Both feeds start
from whatever was already resting, so the starting state cancels.

Measured live over 45 seconds: **2,173 of 2,260 level windows agree, 96.15%.**
The residual is not a reconstruction error. The two feeds stamp differently,
with the aggregated one running about 300 ms behind, so an event near a batch
boundary falls on the wrong side of it. `tests/gate_l3.rs` runs this against the
venue rather than a fixture, because a frozen capture would stop being evidence
the moment Bitstamp changed anything.

## Adding a venue

1. Write the module. Implement `Venue`, fill in `VenueCapabilities` honestly.
2. Add the id to `VenueId::ALL` and an arm to `venue::registry::build`.
3. Capture a fixture with `cargo run --example capture` and drop it beside the
   others; add it to `fixture_files` in `tests/common`.
4. Run `cargo test`. `tests/conformance.rs` runs its whole battery against the
   new venue automatically: symbol round-trip, subscribe frames, parsing its own
   capture, capability self-consistency, that its deltas carry whatever its
   declared scheme needs, that replaying its capture invents no gap, and the
   drop gate.

The session loop branches on capabilities in exactly two places: whether the
validator's state belongs to the connection or the symbol, and whether the check
runs before or after applying. A new venue should need neither touched. If it
does, the matrix is missing a row, and adding that row is the fix rather than a
special case at the call site.
