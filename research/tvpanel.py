"""The availability-time feature panel.

One row per instrument per sampling instant. The whole point of this file is a
single rule, enforced structurally rather than by discipline:

    the row stamped T is built from the messages the recorder had **received**
    by T, and from nothing else.

`recv_wall` is that clock. The venue's own `venue_ts` is not, and using it is
the mistake this repository has already measured: selecting on event time
returned data that did not exist yet in 182 of 4,629 point-in-time reads, the
worst of them by 17.766 seconds (see `streaming/pit.py`). So the sampler walks
the archive in receive order through `tickvault.Archive.replay`, and the book it
photographs at T is the book after the last message whose `recv_wall <= T`. A
feature cannot see the future here because the cursor has not read it yet.

The other thing this file refuses to do is smooth over what the recorder could
not vouch for. Every sample carries how many of the messages behind it were
inside a suspect window, so "clean verified windows" is a filter downstream and
not an assumption here.
"""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import tickvault

NS = 1_000_000_000

#: Book depths the imbalance is measured at. 1 is the touch; the deeper ones
#: say whether the signal, if there is one, lives at the touch or in the queue
#: behind it.
DEPTHS = (1, 5, 10, 20)

COLUMNS = [
    ("venue", pa.string()),
    ("symbol", pa.string()),
    ("window", pa.string()),
    ("t_ns", pa.int64()),
    ("last_msg_ns", pa.int64()),
    ("staleness_ms", pa.float64()),
    ("msgs", pa.int64()),
    ("suspect_msgs", pa.int64()),
    ("suspect_cum", pa.int64()),
    ("bid", pa.float64()),
    ("ask", pa.float64()),
    ("mid", pa.float64()),
    ("bid_qty1", pa.float64()),
    ("ask_qty1", pa.float64()),
    ("spread_bps", pa.float64()),
    ("micro_dev_bps", pa.float64()),
    ("bid_levels", pa.int64()),
    ("ask_levels", pa.int64()),
    ("book_digest", pa.uint32()),
] + [(f"imb{d}", pa.float64()) for d in DEPTHS]

SCHEMA = pa.schema(COLUMNS)


def _features(book) -> dict:
    """Everything derived from one book photograph.

    Returns floats and Nones, never a partially-filled record: a book with one
    empty side has no mid, and inventing one would put a price in the panel that
    never existed.
    """
    best_bid = book.best_bid
    best_ask = book.best_ask
    bid_levels, ask_levels = book.depth
    row = {
        "bid": None,
        "ask": None,
        "mid": None,
        "bid_qty1": None,
        "ask_qty1": None,
        "spread_bps": None,
        "micro_dev_bps": None,
        "bid_levels": bid_levels,
        "ask_levels": ask_levels,
        # A stable hash of every level on both sides. The derived features could
        # agree by luck on a book that differs; this could not.
        "book_digest": book.digest,
    }
    for d in DEPTHS:
        row[f"imb{d}"] = book.imbalance(d)
    if best_bid is None or best_ask is None:
        return row
    bid_px, bid_qty = best_bid
    ask_px, ask_qty = best_ask
    mid = book.mid
    row.update(
        bid=bid_px,
        ask=ask_px,
        mid=mid,
        bid_qty1=bid_qty,
        ask_qty1=ask_qty,
        spread_bps=book.spread_bps,
    )
    total = bid_qty + ask_qty
    if total > 0 and mid:
        # The size-weighted touch. Weights are crossed on purpose: a heavy bid
        # pushes the fair price towards the ask.
        micro = (bid_px * ask_qty + ask_px * bid_qty) / total
        row["micro_dev_bps"] = (micro - mid) / mid * 1e4
    return row


def sample(
    archive_root: str,
    venue: str,
    symbol: str,
    start_ns: int,
    end_ns: int,
    step_ns: int = NS,
    window_label: str = "",
) -> list[dict]:
    """Photograph the book on a fixed grid, in receive order.

    `start_ns` also establishes the book: everything the archive holds before it
    is replayed to seed the state, and none of it is sampled.
    """
    archive = tickvault.Archive(archive_root)
    replay = archive.replay(venue, symbol, 0.0, start_ns, end_ns)

    rows: list[dict] = []
    grid = ((start_ns + step_ns - 1) // step_ns) * step_ns
    pending: dict | None = None  # features after the last tick handed over
    pending_ns: int | None = None
    msgs = 0
    suspect_msgs = 0
    suspect_cum = 0

    def emit(at: int) -> None:
        nonlocal msgs, suspect_msgs
        assert pending is not None and pending_ns is not None
        rows.append(
            {
                "venue": venue,
                "symbol": symbol,
                "window": window_label,
                "t_ns": at,
                "last_msg_ns": pending_ns,
                "staleness_ms": (at - pending_ns) / 1e6,
                "msgs": msgs,
                "suspect_msgs": suspect_msgs,
                "suspect_cum": suspect_cum,
                **pending,
            }
        )
        msgs = suspect_msgs = 0

    for tick in replay:
        at = tick.at_ns
        # Every grid point strictly before this message is described by the
        # book as it stood after the previous one. That is the availability
        # rule: a message received at 09:00:00.4 is not in the 09:00:00 sample.
        while at > grid and grid < end_ns:
            if pending is not None:
                emit(grid)
            else:
                msgs = suspect_msgs = 0
            grid += step_ns
        if grid >= end_ns:
            break
        # The loop above ran until `at <= grid`, so this message belongs to the
        # sample now being accumulated.
        msgs += 1
        suspect_msgs += tick.suspect
        suspect_cum += tick.suspect
        pending = _features(replay.book)
        pending_ns = at

    while grid < end_ns and pending is not None:
        emit(grid)
        grid += step_ns
    return rows


def parse_iso(text: str) -> int:
    stamp = dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc
    )
    return int(stamp.timestamp()) * NS


def iso(ns: int) -> str:
    return (
        dt.datetime.fromtimestamp(ns / 1e9, dt.timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def build(
    base: pathlib.Path,
    instruments: list[tuple[str, str]],
    windows: list[tuple[int, int]],
    step_ns: int,
    warmup_ns: int,
    quiet: bool = False,
) -> pa.Table:
    rows: list[dict] = []
    for venue, symbol in instruments:
        root = base / venue
        if not root.exists():
            raise SystemExit(f"no archive for {venue} at {root}")
        archive = tickvault.Archive(str(root))
        try:
            first, last = archive.span_ns(venue, symbol)
        except KeyError:
            if not quiet:
                print(f"{venue:11s} {symbol:9s} holds nothing, skipped")
            continue
        for start, end in windows:
            label = iso(start)
            if start < first or end > last + step_ns:
                if not quiet:
                    print(f"{venue:11s} {label} outside the archive span, skipped")
                continue
            seeded = start - warmup_ns
            if seeded < first:
                if not quiet:
                    print(
                        f"{venue:11s} {label} has {(start - first) / 60e9:.1f} min of "
                        f"warm-up, wanted {warmup_ns / 60e9:.0f}, skipped"
                    )
                continue
            got = sample(str(root), venue, symbol, start, end, step_ns, label)
            rows.extend(got)
            if not quiet:
                clean = sum(1 for r in got if r["suspect_msgs"] == 0)
                priced = sum(1 for r in got if r["mid"] is not None)
                print(
                    f"{venue:11s} {symbol:9s} {label} samples={len(got):6d} "
                    f"priced={priced:6d} clean={clean:6d}"
                )
    if not rows:
        raise SystemExit("no samples: check the archives and the windows")
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--base", required=True, help="directory holding one archive per venue"
    )
    ap.add_argument(
        "--instrument",
        action="append",
        required=True,
        help="venue:symbol, repeatable",
    )
    ap.add_argument(
        "--window",
        action="append",
        required=True,
        help="FROM..TO in RFC 3339, repeatable",
    )
    ap.add_argument("--step-ms", type=int, default=1000)
    ap.add_argument(
        "--warmup-min",
        type=float,
        default=60.0,
        help="minutes of archive required before a window, to seed the book",
    )
    ap.add_argument("--out", required=True)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    instruments = []
    for spec in args.instrument:
        venue, _, symbol = spec.partition(":")
        instruments.append((venue, symbol))
    windows = []
    for spec in args.window:
        a, _, b = spec.partition("..")
        windows.append((parse_iso(a), parse_iso(b)))

    table = build(
        pathlib.Path(args.base),
        instruments,
        windows,
        args.step_ms * 1_000_000,
        int(args.warmup_min * 60 * NS),
        args.quiet,
    )
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, args.out, compression="zstd")
    if not args.quiet:
        print(f"wrote {args.out}: {table.num_rows} rows, {table.num_columns} columns")
    return 0


if __name__ == "__main__":
    sys.exit(main())
