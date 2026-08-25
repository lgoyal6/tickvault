# Fixtures

Captured verbatim from the live feeds on 2026-08-24 with `examples/capture.rs`.
They exist so the parsers are tested against bytes the venues actually sent,
rather than against a reading of their documentation. Several details in them
contradicted that reading, which is the point.

`tests/conformance.rs` runs its whole battery against every fixture here, so
adding a venue means adding a capture beside the others and one line in
`fixture_files`.

| file | venue | contains |
|---|---|---|
| `coinbase_l2.jsonl` | Coinbase Advanced Trade | one connection: snapshot, updates, and the `subscriptions` ack numbered between them |
| `kraken_book.jsonl` | Kraken v2 | complete and unmodified: status banner, subscribe reply, snapshot, heartbeat, updates |
| `okx_books.jsonl` | OKX v5 | 400-level snapshot and updates carrying `seqId`/`prevSeqId` |
| `bybit_orderbook.jsonl` | Bybit v5 spot | depth-50 snapshot and deltas carrying `u` |
| `binance_us_depth.jsonl` | Binance.US | diffs only; this venue has no in-band snapshot |
| `binance_us_snapshot.json` | Binance.US | the REST book, whose `lastUpdateId` falls inside the diff range above |
| `bitstamp_diff.jsonl` | Bitstamp | diffs carrying only a microtimestamp |
| `bitstamp_snapshot.json` | Bitstamp | the REST book, taken during the capture |

The two REST snapshots were fetched *while* the corresponding websocket capture
was running, so the splice they exercise is the real one: the snapshot's anchor
genuinely falls inside the stream, and the stale prefix genuinely has to be
discarded.

## Reductions

Only `coinbase_l2.jsonl` is reduced, and only to keep a 4.8 MB frame out of the
repository:

- the snapshot keeps the 15 best levels a side
- updates are filtered to that same price band

The band is contiguous and anchored at the top of book, so every in-band price
is present in the trimmed snapshot and no delete refers to a level the fixture
does not hold. Every value is the venue's own; nothing was synthesised.

## Regenerating

```bash
cargo run --example capture -- <url> <subscribe-json> <frames> <max-bytes>
```

Pass `-` as the subscribe argument for a stream selected by URL alone.

Fixtures should only be regenerated when a venue changes its wire format. If a
test starts failing after a regeneration, the venue changed something, and that
is a finding rather than a fixture to paper over.
