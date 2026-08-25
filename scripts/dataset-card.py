#!/usr/bin/env python3
"""Generate the dataset card from the archive's own coverage report.

The card's whole claim is that the gap statistics are on the front page. A card
whose coverage table was typed by hand would be the one part of the dataset
nobody checked, and it would drift the first time the archive changed.

So the table is generated, and the only input is what `tickvault coverage`
measured:

    tickvault coverage --archive <dir>/... --bucket-secs 300 --out coverage.json
    scripts/dataset-card.py coverage.json > README.md

Refusing rather than guessing is the point: if the coverage file does not cover
the archive being uploaded, the card should not be written at all.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from collections import Counter
from pathlib import Path

FRONT_MATTER = """---
license: mit
task_categories:
  - time-series-forecasting
tags:
  - finance
  - cryptocurrency
  - order-book
  - market-microstructure
  - l2
  - l3
pretty_name: TickVault Full-Depth Crypto Order Book Archive
size_categories:
  - {size}
configs:
  - config_name: default
    data_files: "**/*.parquet"
---
"""


def size_bucket(messages: int) -> str:
    for limit, name in [
        (1_000, "n<1K"),
        (10_000, "1K<n<10K"),
        (100_000, "10K<n<100K"),
        (1_000_000, "100K<n<1M"),
        (10_000_000, "1M<n<10M"),
        (100_000_000, "10M<n<100M"),
        (1_000_000_000, "100M<n<1B"),
    ]:
        if messages < limit:
            return name
    return "n>1B"


def stamp(ns: str) -> str:
    seconds, _ = divmod(int(ns), 1_000_000_000)
    return dt.datetime.fromtimestamp(seconds, dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("coverage", type=Path, help="output of `tickvault coverage`")
    args = ap.parse_args()

    cov = json.loads(args.coverage.read_text())
    series = cov["series"]
    if not series:
        print("coverage file describes no venues", file=sys.stderr)
        return 1

    bucket_min = cov["bucket_seconds"] / 60
    span_ns = int(cov["to_ns"]) - int(cov["from_ns"])
    span_hours = span_ns / 3.6e12
    total_messages = sum(b["messages"] for s in series for b in s["buckets"])

    rows = []
    any_absent = False
    for s in series:
        c = Counter(b["state"] for b in s["buckets"])
        total = sum(c.values())
        clean = c.get("clean", 0)
        if c.get("absent", 0):
            any_absent = True
        notes = ", ".join(
            f"{n} {state}" for state, n in sorted(c.items()) if state != "clean"
        )
        rows.append(
            "| {venue} | {symbol} | {verifiable} | {clean}/{total} | {notes} |".format(
                venue=s["venue"],
                symbol=s["symbol"],
                verifiable="yes" if s["verifiable"] else "**no**",
                clean=clean,
                total=total,
                notes=notes or "",
            )
        )

    verifiable = sum(1 for s in series if s["verifiable"])
    out = [FRONT_MATTER.format(size=size_bucket(total_messages))]
    out.append(
        f"""
# TickVault: full-depth crypto order books, with the gaps published

Full-depth order book snapshots and deltas across {len(series)} venue feeds, in Parquet.

Everything free is top of book, resampled to bars, or carries undocumented gaps
that quietly poison a backtest. This one says exactly where its holes are, per
venue per {bucket_min:.0f} minute interval, so you can exclude the bad windows instead of
finding them inside a result.

## Coverage

{span_hours:.1f} hours, {stamp(cov['from_ns'])} to {stamp(cov['to_ns'])}, over {total_messages:,} messages.
{verifiable} of {len(series)} feeds can prove they lost nothing.

| venue | symbol | verifiable | clean | other intervals |
|---|---|---|---|---|
"""
    )
    out.append("\n".join(rows))
    out.append(
        """

Four states, not two, because collapsing them is the dishonesty this exists to
avoid:

- **clean**: checked against what the venue publishes to check with, and passed.
- **suspect**: recorded, and the recorder could not vouch for it.
- **unverifiable**: recorded, nothing looks wrong, and the venue publishes
  nothing that could tell us if it were. Never upgraded to clean.
- **absent**: nothing recorded.

## The unverifiable row is why this exists

Bitstamp's aggregated feed carries no sequence number, no update id and no
checksum, so a dropped message on it **cannot be detected by any means**. It
reads 0% verified rather than being rounded up to look like the others.

Recorded order-by-order instead, the same venue becomes the best validated here,
because every event names its predecessor.
"""
    )

    if any_absent:
        out.append(
            """
## Absent intervals

Some intervals above have no data. Where that was the recorder's fault rather
than the venue's, it is still shown: a coverage grid that only ever displayed
good days would not be evidence of anything.
"""
        )

    out.append(
        """
## Schema

Prices and quantities are **exact integers at 1e-9**, never floats. Kraken
validates its own book by CRC32 over the price strings, so a rounding error
anywhere would make the archive unverifiable against the venue that produced it.

`null` means the data cannot answer, and never zero. An aggregated feed shows a
level shrinking without saying whether it traded or was cancelled, so `volume`
is null except on the order-by-order feed. Only that feed reports executions.

Full column list: https://github.com/lgoyal6/tickvault/blob/main/docs/schema.md

## Reading it

```bash
pip install tickvault-ob
```

```python
import tickvault

archive = tickvault.open("./archive")
book = archive.book_at("kraken", "BTC-USD", "2026-08-25T12:00:00Z")
print(book.mid, book.spread, book.suspect)
```

Or with anything that reads Parquet: the layout is Hive-partitioned
`venue=/symbol=/date=`, which pandas, polars and pyarrow discover unaided.

## Limitations

- The unverifiable feed can never be proven intact.
- Order-by-order is one venue deep; every other keyless L3 feed requires
  authentication.
- Kraken and OKX prove a loss occurred but never how large it was.
- Kraken's checksum covers ten levels a side, so beyond that a loss is
  undetectable by construction.
- A loss at the very end of a recording is undetectable: nothing follows it.
- Against a paid feed this has no venue support, no SLA, and no order-by-order
  data on most venues.

## Links

- Live viewer, the real reconstruction engine in your browser:
  https://lgoyal6.github.io/tickvault/
- Source: https://github.com/lgoyal6/tickvault
"""
    )
    print("".join(out).strip() + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
