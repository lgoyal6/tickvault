"""The data manifest: exactly which bytes a result was computed from.

A research result that names its dataset by description ("a week of BTC from six
venues") cannot be rerun by anyone, including its author six months later. This
writes the other kind: every source file by path, byte length and SHA-256, the
window it was selected for, and the archive's own verification verdict.

`build` produces one. `check` re-hashes what is on disk and reports every file
that moved, changed or disappeared. A study refuses to run against an archive
whose manifest does not check out, which is the only way the hashes are worth
writing down.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import subprocess
import sys

CHUNK = 1 << 20


def sha256_file(path: pathlib.Path) -> tuple[str, int]:
    """Digest and byte length of one file, streamed."""
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def iso(ns: int) -> str:
    return (
        dt.datetime.fromtimestamp(ns / 1e9, dt.timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def parse_iso(text: str) -> int:
    """RFC 3339 to nanoseconds since the epoch."""
    stamp = dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc
    )
    return int(stamp.timestamp()) * 1_000_000_000


def read_archive_manifest(root: pathlib.Path) -> list[dict]:
    """The archive's own `_manifest.jsonl`, file entries only."""
    entries = []
    with (root / "_manifest.jsonl").open() as fh:
        for line in fh:
            record = json.loads(line)
            if record.get("kind") == "file":
                entries.append(record)
    return entries


def build(
    archive_roots: list[pathlib.Path],
    windows: list[tuple[int, int]],
    label: str,
) -> dict:
    """Hash every file the archives vouch for, and record the windows."""
    out_files = []
    for root in archive_roots:
        for entry in read_archive_manifest(root):
            path = root / entry["path"]
            digest, size = sha256_file(path)
            if size != entry["bytes"]:
                raise SystemExit(
                    f"{path}: manifest says {entry['bytes']} bytes, disk says {size}"
                )
            out_files.append(
                {
                    "archive": root.name,
                    "path": entry["path"],
                    "venue": entry["venue"],
                    "symbol": entry["symbol"],
                    "date": entry["date"],
                    "rows": entry["rows"],
                    "bytes": size,
                    "first_recv_wall": entry["first_recv_wall"],
                    "last_recv_wall": entry["last_recv_wall"],
                    "sha256": digest,
                }
            )
    out_files.sort(key=lambda f: (f["archive"], f["path"]))
    rollup = {}
    for f in out_files:
        key = (f["venue"], f["symbol"])
        agg = rollup.setdefault(
            key, {"files": 0, "rows": 0, "bytes": 0, "first": None, "last": None}
        )
        agg["files"] += 1
        agg["rows"] += f["rows"]
        agg["bytes"] += f["bytes"]
        first, last = f["first_recv_wall"], f["last_recv_wall"]
        agg["first"] = first if agg["first"] is None else min(agg["first"], first)
        agg["last"] = last if agg["last"] is None else max(agg["last"], last)

    return {
        "label": label,
        "built_at": iso(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1e9)),
        "tickvault_revision": git_revision(),
        "windows": [
            {"from_ns": a, "to_ns": b, "from": iso(a), "to": iso(b)}
            for a, b in windows
        ],
        "totals": {
            "files": len(out_files),
            "rows": sum(f["rows"] for f in out_files),
            "bytes": sum(f["bytes"] for f in out_files),
        },
        "instruments": [
            {
                "venue": venue,
                "symbol": symbol,
                "files": agg["files"],
                "rows": agg["rows"],
                "bytes": agg["bytes"],
                "first_recv_wall": agg["first"],
                "last_recv_wall": agg["last"],
                "first": iso(agg["first"]),
                "last": iso(agg["last"]),
            }
            for (venue, symbol), agg in sorted(rollup.items())
        ],
        "files": out_files,
    }


def git_revision() -> str | None:
    """The tickvault revision the manifest was built against, if there is one."""
    here = pathlib.Path(__file__).resolve().parent
    try:
        return subprocess.run(
            ["git", "-C", str(here), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def check(manifest: dict, base: pathlib.Path) -> list[str]:
    """Re-hash the manifest's files. Returns one line per disagreement."""
    problems = []
    for f in manifest["files"]:
        path = base / f["archive"] / f["path"]
        if not path.exists():
            problems.append(f"missing: {path}")
            continue
        digest, size = sha256_file(path)
        if size != f["bytes"]:
            problems.append(f"size: {path} {f['bytes']} -> {size}")
        if digest != f["sha256"]:
            problems.append(f"sha256: {path} {f['sha256'][:12]} -> {digest[:12]}")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="hash the archives and write a manifest")
    b.add_argument("--archive", action="append", required=True)
    b.add_argument(
        "--window",
        action="append",
        required=True,
        help="FROM..TO in RFC 3339, e.g. 2026-08-30T09:00:00Z..2026-08-30T11:00:00Z",
    )
    b.add_argument("--label", default="tickvault-c26")
    b.add_argument("--out", required=True)

    c = sub.add_parser("check", help="re-hash what a manifest vouches for")
    c.add_argument("--manifest", required=True)
    c.add_argument("--base", required=True, help="directory holding the archives")

    args = ap.parse_args(argv)

    if args.cmd == "build":
        windows = []
        for spec in args.window:
            a, _, b_ = spec.partition("..")
            windows.append((parse_iso(a), parse_iso(b_)))
        manifest = build([pathlib.Path(p) for p in args.archive], windows, args.label)
        os.makedirs(pathlib.Path(args.out).parent, exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(manifest, fh, indent=1)
            fh.write("\n")
        t = manifest["totals"]
        print(
            f"wrote {args.out}: {t['files']} files, {t['rows']} rows, "
            f"{t['bytes'] / 1e6:.1f} MB, {len(manifest['instruments'])} instruments"
        )
        return 0

    manifest = json.load(open(args.manifest))
    problems = check(manifest, pathlib.Path(args.base))
    if problems:
        for p in problems:
            print(p)
        print(f"MANIFEST FAILS: {len(problems)} problem(s)")
        return 1
    print(
        f"manifest ok: {manifest['totals']['files']} files re-hashed, "
        f"{manifest['totals']['bytes'] / 1e6:.1f} MB, all digests match"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
