#!/usr/bin/env python3
"""Assemble the bounded archive slice the viewer ships with.

The published dataset lives on Hugging Face. This is a few minutes of it, small
enough that a browser can fetch one venue and rebuild the book locally.

Two rules the slice has to respect or the page would show a book that never
existed:

  * a slice starts at a snapshot. Deltas are meaningless without the book they
    were applied to, so the first file of a recording, which holds the
    connection's opening snapshot, is the only honest place to begin.
  * the manifest travels with it, filtered to the files actually copied. The
    manifest is the archive's record of what is safe to read, and it carries
    the feed depth a rebuild has to truncate to.

Usage:
    scripts/make-demo-data.py <recording-dir> <out-dir> [--files N]

where <recording-dir> holds one sub-directory per venue, which is how a
multi-venue recording lands: six writers cannot share one manifest.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path


def slice_venue(src: Path, dst: Path, keep: int) -> dict | None:
    """Copy the first `keep` files of one venue archive, with a manifest."""
    manifest = src / "_manifest.jsonl"
    if not manifest.exists():
        print(f"  {src.name}: no manifest, skipped", file=sys.stderr)
        return None

    records = []
    for line in manifest.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        entry = json.loads(line)
        if entry.get("kind") == "file":
            records.append(entry)

    if not records:
        print(f"  {src.name}: manifest lists no files, skipped", file=sys.stderr)
        return None

    # Arrival order. The archive is an append-only record of when things
    # happened, so reading it in any other order rebuilds a different book.
    records.sort(key=lambda r: r["first_recv_wall"])
    chosen = records[:keep]

    dst.mkdir(parents=True, exist_ok=True)
    total = 0
    for record in chosen:
        source = src / record["path"]
        target = dst / record["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        total += target.stat().st_size

    with (dst / "_manifest.jsonl").open("w") as out:
        for record in chosen:
            out.write(json.dumps(record) + "\n")

    first, last = chosen[0], chosen[-1]
    span = (last["last_recv_wall"] - first["first_recv_wall"]) / 1e9
    return {
        "venue": first["venue"],
        "symbol": first["symbol"],
        "date": first["date"],
        "book_level": 3 if first.get("book_level") == "l3" else 2,
        "feed_depth": first.get("feed_depth"),
        "files": [r["path"] for r in chosen],
        "rows": sum(r["rows"] for r in chosen),
        "first_ns": str(first["first_recv_wall"]),
        "last_ns": str(last["last_recv_wall"]),
        "bytes": total,
        "seconds": round(span, 1),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("recording", type=Path)
    ap.add_argument("out", type=Path)
    ap.add_argument(
        "--files",
        type=int,
        default=1,
        help="files to keep per venue, starting from the snapshot (default 1)",
    )
    args = ap.parse_args()

    if not args.recording.is_dir():
        print(f"{args.recording} is not a directory", file=sys.stderr)
        return 1

    venues = sorted(p for p in args.recording.iterdir() if (p / "_manifest.jsonl").exists())
    if not venues:
        print(f"no venue archives under {args.recording}", file=sys.stderr)
        return 1

    index = []
    for venue_dir in venues:
        summary = slice_venue(venue_dir, args.out / venue_dir.name, args.files)
        if summary:
            index.append(summary)
            print(
                f"  {summary['venue']:<11} {summary['symbol']:<9} "
                f"{summary['rows']:>7} rows  {summary['seconds']:>6.1f}s  "
                f"{summary['bytes'] / 1e6:.2f} MB"
            )

    (args.out / "index.json").write_text(json.dumps({"partitions": index}, indent=2) + "\n")
    total = sum(p["bytes"] for p in index)
    print(f"\n{len(index)} partitions, {total / 1e6:.2f} MB total -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
