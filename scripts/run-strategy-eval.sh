#!/usr/bin/env bash
#
# The strategy evaluation gate.
#
# Everything here replays files that are already in this repository. No venue is
# contacted, no order is placed anywhere, and no money moves. Execution is
# simulated throughout and queue position is approximate; `sim/README.md` says
# exactly how approximate.
#
# What this asserts, in order:
#
#   1. every input is the bytes the frozen manifest names, checked by shasum
#   2. the simulator is formatted, lints clean and its own tests pass
#   3. the evaluation runs over every venue, window, baseline and the candidate
#   4. all three negative controls fail the way they are supposed to
#   5. the evaluation is deterministic: two runs produce identical output
#
# A rejected candidate does not fail this script. A negative result is a result,
# and the gate exists to stop a wrong number rather than a disappointing one.

set -euo pipefail

cd "$(dirname "$0")/.."

MANIFEST="sim/manifest.json"
SIM="target/release/tickvault-sim"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "==> building the simulator"
# Before the hash check on purpose: the build reads no archive, and printing the
# manifest's declared hashes for shasum to check needs the binary.
cargo build --release -p tickvault-sim

echo "==> checking every input against the hash the manifest declares"
"$SIM" --manifest "$MANIFEST" --repo-root . verify-inputs --print-declared > "$WORK/declared.sha256"
shasum -a 256 -c "$WORK/declared.sha256"

echo "==> formatting, lints and the simulator's own tests"
# The workspace root package is what a bare `cargo test` and `cargo clippy`
# select, so the simulator needs naming to be covered at all.
cargo fmt --check
cargo clippy -p tickvault-sim --all-targets -- -D warnings
cargo test -p tickvault-sim

echo "==> evaluation, first run"
"$SIM" --manifest "$MANIFEST" --repo-root . evaluate \
    --control none \
    --out-json "$WORK/first.json"

echo "==> negative control: leak-future"
# Exit status 2 means the control failed the way it must. Any other status,
# including success, means the control proved nothing and the gate stops.
set +e
"$SIM" --manifest "$MANIFEST" --repo-root . evaluate \
    --control leak-future \
    --out-json "$WORK/leak-future.json"
status=$?
set -e
if [ "$status" -ne 2 ]; then
    echo "leak-future did not fail as required (exit $status)" >&2
    exit 1
fi

echo "==> negative control: no-costs"
set +e
"$SIM" --manifest "$MANIFEST" --repo-root . evaluate \
    --control no-costs \
    --out-json "$WORK/no-costs.json"
status=$?
set -e
if [ "$status" -ne 2 ]; then
    echo "no-costs did not fail as required (exit $status)" >&2
    exit 1
fi

echo "==> negative control: shuffle-seq"
set +e
"$SIM" --manifest "$MANIFEST" --repo-root . evaluate \
    --control shuffle-seq \
    --out-json "$WORK/shuffle-seq.json"
status=$?
set -e
if [ "$status" -ne 2 ]; then
    echo "shuffle-seq did not fail as required (exit $status)" >&2
    exit 1
fi

echo "==> evaluation, second run for determinism"
"$SIM" --manifest "$MANIFEST" --repo-root . evaluate \
    --control none \
    --out-json "$WORK/second.json"

if ! cmp -s "$WORK/first.json" "$WORK/second.json"; then
    echo "two runs of the same evaluation produced different output" >&2
    diff "$WORK/first.json" "$WORK/second.json" | head -40 >&2
    exit 1
fi
echo "    two runs byte for byte identical"

echo "==> writing the published result"
"$SIM" --manifest "$MANIFEST" --repo-root . evaluate \
    --control none \
    --out-json results/strategy-eval.json \
    --out-parquet results/strategy-trades.parquet \
    --out-report results/strategy-report.md \
    --control-result "$WORK/leak-future.json" \
    --control-result "$WORK/no-costs.json" \
    --control-result "$WORK/shuffle-seq.json"

echo
echo "results/strategy-eval.json, results/strategy-trades.parquet and"
echo "results/strategy-report.md are local replays of recorded data on one host,"
echo "with simulated execution and approximate queue position. No profitability,"
echo "no alpha and no live trading is claimed by any of them."
