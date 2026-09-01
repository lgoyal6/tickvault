#!/usr/bin/env bash
#
# Measure what the scan planner prunes, and what the page granularity costs.
#
# Two comparisons, kept apart on purpose.
#
#   1. Planner on vs off, on the same bytes. Built from the base commit rather
#      than behind a flag, because a flag that only the benchmark sets is a
#      flag that can quietly stop matching what the binary really does.
#   2. Page granularity. The same rows written at several rows-per-page, so the
#      storage cost of a finer index and the scan cost of a coarser one are
#      both visible instead of only the flattering one.
#
# Usage:
#   scripts/scan-bench.sh <archive> <venue> <symbol> [base-ref]
#
# Writes a table to standard output. Put the output somewhere outside the
# repository; measured numbers are not source.

set -euo pipefail

ARCHIVE=${1:?archive directory}
VENUE=${2:?venue}
SYMBOL=${3:?symbol}
BASE=${4:-main}

ROOT=$(git rev-parse --show-toplevel)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

NEW="$ROOT/target/release/tickvault"
BASE_BIN="$WORK/tickvault-base"

echo "# scan-bench $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "# host      $(uname -sm), $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
echo "# head      $(git -C "$ROOT" rev-parse --short HEAD)"
echo "# base      $(git -C "$ROOT" rev-parse --short "$BASE")"
echo "# archive   $ARCHIVE"
echo

# The base binary, built from the commit before any of this existed.
echo "building the base binary from $BASE ..." >&2
git -C "$ROOT" worktree add --detach "$WORK/base" "$BASE" >/dev/null 2>&1
( cd "$WORK/base" && cargo build --release --all-features --bin tickvault >/dev/null 2>&1 )
cp "$WORK/base/target/release/tickvault" "$BASE_BIN"
git -C "$ROOT" worktree remove --force "$WORK/base" >/dev/null 2>&1

# The archive's own span, so the queries below are a fraction of it rather
# than a fixed wall clock that may fall outside it.
span=$("$NEW" coverage --archive "$ARCHIVE" --bucket-secs 3600 2>/dev/null \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["first_wall"], d["last_wall"])' 2>/dev/null || true)
if [ -z "$span" ]; then
  echo "could not read the archive span from coverage; falling back to the manifest" >&2
  span=$(python3 - "$ARCHIVE" <<'PY'
import json, sys, pathlib
first, last = None, None
for line in (pathlib.Path(sys.argv[1]) / "_manifest.jsonl").read_text().splitlines():
    if not line.strip():
        continue
    e = json.loads(line)
    rec = e.get("produced") if e.get("kind") == "compaction" else e
    if e.get("kind") not in ("file", "compaction"):
        continue
    f, l = rec["first_recv_wall"], rec["last_recv_wall"]
    first = f if first is None else min(first, f)
    last = l if last is None else max(last, l)
print(first, last)
PY
)
fi
FIRST=$(echo "$span" | cut -d' ' -f1)
LAST=$(echo "$span" | cut -d' ' -f2)
TOTAL=$(( LAST - FIRST ))
echo "# span      ${TOTAL} ns ($(python3 -c "print(f'{$TOTAL/1e9:.1f}')") s)"
echo

# A narrow window out of the middle, and the whole thing.
NARROW_FROM=$(( FIRST + TOTAL / 2 ))
NARROW_TO=$(( NARROW_FROM + 30000000000 ))   # 30 seconds
[ "$NARROW_TO" -gt "$LAST" ] && NARROW_TO=$LAST

timeit() {
  # Best of three. The interesting number is the one where the page cache is
  # warm and nothing else is running, and the minimum is the closest estimate
  # of that available without a quiet machine.
  local best=""
  for _ in 1 2 3; do
    local start=$(python3 -c 'import time; print(time.perf_counter())')
    "$@" >/dev/null 2>&1 || true
    local end=$(python3 -c 'import time; print(time.perf_counter())')
    local d=$(python3 -c "print($end - $start)")
    if [ -z "$best" ] || python3 -c "import sys; sys.exit(0 if $d < $best else 1)"; then best=$d; fi
  done
  printf "%.3f" "$best"
}

# The floor. Both binaries pay process start, a manifest read and a venue
# registry build before any Parquet is touched, and on a short query that is
# most of the wall clock. Printed rather than subtracted: subtracting it would
# be a correction nobody can check.
echo "## startup floor (no archive work)"
printf "%-10s %10s %10s\n" "binary" "base(s)" "planned(s)"
fb=$(timeit "$BASE_BIN" --version)
fn=$(timeit "$NEW" --version)
printf "%-10s %10s %10s\n" "--version" "$fb" "$fn"
echo

echo "## query, planner off vs on (same archive, same query)"
printf "%-10s %10s %10s %8s\n" "window" "base(s)" "planned(s)" "speedup"
for name in narrow whole; do
  if [ "$name" = narrow ]; then from=$NARROW_FROM; to=$NARROW_TO; else from=$FIRST; to=$LAST; fi
  b=$(timeit "$BASE_BIN" query --archive "$ARCHIVE" --venue "$VENUE" --symbol "$SYMBOL" --from "$from" --to "$to")
  n=$(timeit "$NEW"      query --archive "$ARCHIVE" --venue "$VENUE" --symbol "$SYMBOL" --from "$from" --to "$to")
  s=$(python3 -c "print(f'{$b/$n:.2f}x' if $n > 0 else 'n/a')")
  printf "%-10s %10s %10s %8s\n" "$name" "$b" "$n" "$s"
done
echo
echo "## the plan for the narrow window"
"$NEW" query --archive "$ARCHIVE" --venue "$VENUE" --symbol "$SYMBOL" \
  --from "$NARROW_FROM" --to "$NARROW_TO" --explain 2>/dev/null | sed -n '1,7p'
echo
echo "## the plan for the whole archive"
"$NEW" query --archive "$ARCHIVE" --venue "$VENUE" --symbol "$SYMBOL" \
  --from "$FIRST" --to "$LAST" --explain 2>/dev/null | sed -n '1,7p'
echo

echo "## reconstruct at the end of the archive (the snapshot probe path)"
printf "%-10s %10s %10s %8s\n" "mode" "base(s)" "planned(s)" "speedup"
b=$(timeit "$BASE_BIN" reconstruct --archive "$ARCHIVE" --venue "$VENUE" --symbol "$SYMBOL" --at "$LAST" --no-checkpoints)
n=$(timeit "$NEW"      reconstruct --archive "$ARCHIVE" --venue "$VENUE" --symbol "$SYMBOL" --at "$LAST" --no-checkpoints)
s=$(python3 -c "print(f'{$b/$n:.2f}x' if $n > 0 else 'n/a')")
printf "%-10s %10s %10s %8s\n" "no-ckpt" "$b" "$n" "$s"
echo

echo "## page granularity: what a finer index costs and buys"
printf "%-12s %12s %12s %12s %12s\n" "rows/page" "bytes" "footer" "rows read" "narrow(s)"
for rows in 20000 8000 2000 500; do
  out="$WORK/pages-$rows"
  "$NEW" transcode --archive "$ARCHIVE" --out "$out" --compression zstd \
    --row-group-rows 50000 --data-page-rows "$rows" >/dev/null 2>&1
  bytes=$(find "$out" -name '*.parquet' -exec stat -f %z {} + 2>/dev/null | paste -sd+ - | bc)
  # Footer size stands in for the index cost: it is where the page index lives.
  footer=$(python3 - "$out" <<'PY'
import pathlib, struct, sys
total = 0
for p in pathlib.Path(sys.argv[1]).rglob("*.parquet"):
    with p.open("rb") as fh:
        fh.seek(-8, 2)
        n = struct.unpack("<I", fh.read(4))[0]
        # The page index sits between the last data page and the footer, so
        # the whole tail after the first column index offset is the metadata.
        total += n + 8
print(total)
PY
)
  plan=$("$NEW" query --archive "$out" --venue "$VENUE" --symbol "$SYMBOL" \
    --from "$NARROW_FROM" --to "$NARROW_TO" --explain 2>/dev/null | grep '^rows' | awk '{print $2}')
  t=$(timeit "$NEW" query --archive "$out" --venue "$VENUE" --symbol "$SYMBOL" --from "$NARROW_FROM" --to "$NARROW_TO")
  printf "%-12s %12s %12s %12s %12s\n" "$rows" "$bytes" "$footer" "${plan:-n/a}" "$t"
done
