#!/usr/bin/env bash
# Upload the archive to Hugging Face, with a card generated from its own gaps.
#
#   hf auth login                                  # once
#   scripts/publish-dataset.sh <archive-dir> [repo]
#
# The order is deliberate. Coverage is measured first, the card is generated
# from that measurement, and only then does anything upload. A card written
# before the measurement would be a claim about data nobody had checked, which
# is the failure this dataset exists to not repeat.
set -euo pipefail

archive="${1:?usage: publish-dataset.sh <archive-dir> [repo]}"
repo="${2:-lgoyal6/tickvault}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="$here/target/release/tickvault"
hf="${HF:-hf}"

[ -x "$bin" ] || { echo "build it first: cargo build --release" >&2; exit 1; }
command -v "$hf" >/dev/null || { echo "install the CLI: pip install -U huggingface_hub" >&2; exit 1; }
"$hf" auth whoami >/dev/null 2>&1 || { echo "log in first: hf auth login" >&2; exit 1; }

venues=()
for dir in "$archive"/*/; do
  [ -f "$dir/_manifest.jsonl" ] && venues+=(--archive "$dir")
done
[ ${#venues[@]} -gt 0 ] || { echo "no venue archives under $archive" >&2; exit 1; }

staging="$(mktemp -d)"
echo "measuring coverage"
"$bin" coverage "${venues[@]}" --bucket-secs 300 --out "$staging/coverage.json"

echo "generating the card from it"
python3 "$here/scripts/dataset-card.py" "$staging/coverage.json" > "$staging/README.md"
head -30 "$staging/README.md"
echo
read -r -p "publish this to $repo? [y/N] " reply
[ "$reply" = "y" ] || { echo "stopped"; exit 0; }

"$hf" repo create "$repo" --repo-type dataset --exist-ok
# The card and the coverage it was generated from travel together, so anyone can
# check the table against the file it came from.
"$hf" upload "$repo" "$staging/README.md" README.md --repo-type dataset \
  --commit-message "Coverage as measured"
"$hf" upload "$repo" "$staging/coverage.json" coverage.json --repo-type dataset \
  --commit-message "The gap report the card was generated from"

# hf_transfer is worth having for anything over a few GB.
export HF_HUB_ENABLE_HF_TRANSFER="${HF_HUB_ENABLE_HF_TRANSFER:-1}"
"$hf" upload "$repo" "$archive" . --repo-type dataset \
  --include "*.parquet" "*_manifest.jsonl" \
  --commit-message "Archive"

echo
echo "https://huggingface.co/datasets/$repo"
