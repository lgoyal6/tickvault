#!/usr/bin/env bash
# Rebuild everything the demo page serves, from a raw multi-venue recording.
#
# Takes a directory holding one archive per venue, which is how a multi-venue
# capture lands on disk: six writers cannot share one manifest.
#
#   scripts/refresh-demo-data.sh ~/Desktop/tickvault_materials/recording
#
# Three steps, in this order for a reason:
#
#   1. transcode to snappy. The archive is written with zstd, which ships
#      hand-written amd64 assembly and cannot target wasm at all, so a browser
#      cannot read it.
#   2. coverage over the WHOLE recording, not the shipped slice. The grid is
#      supposed to describe the capture; describing only the few minutes that
#      ship with the page would flatter it.
#   3. slice out the first file per venue, which is the one holding the opening
#      snapshot. Deltas without the book they applied to are meaningless.
set -euo pipefail

recording="${1:?usage: refresh-demo-data.sh <recording-dir> [bucket-secs]}"
buckets="${2:-300}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="$here/target/release/tickvault"
snappy="$(mktemp -d)/snappy"

[ -x "$bin" ] || { echo "build it first: cargo build --release" >&2; exit 1; }

venues=()
for dir in "$recording"/*/; do
  [ -f "$dir/_manifest.jsonl" ] || continue
  name="$(basename "$dir")"
  echo "transcoding $name"
  "$bin" transcode --archive "$dir" --out "$snappy/$name" --compression snappy
  venues+=(--archive "$snappy/$name")
done

[ ${#venues[@]} -gt 0 ] || { echo "no venue archives under $recording" >&2; exit 1; }

# Start from empty: a venue that changed file names would otherwise leave its
# old Parquet behind, still served and no longer listed.
rm -rf "$here/docs/data"
mkdir -p "$here/docs/data"

echo
"$bin" coverage "${venues[@]}" --bucket-secs "$buckets" --out "$here/docs/data/coverage.json"

echo
python3 "$here/scripts/make-demo-data.py" "$snappy" "$here/docs/data" --files 1

echo
du -sh "$here/docs/data"
