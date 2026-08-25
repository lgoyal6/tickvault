#!/usr/bin/env bash
# Rebuild the viewer's wasm into docs/, where GitHub Pages serves it from.
#
# The built artefact is committed, the same way strata commits its engine, so
# the page works from a plain checkout with no build step. That means it can
# drift from viewer/src, so this is the one command that regenerates it and it
# should be run whenever the viewer or the core crate changes.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here/viewer"

# Homebrew's rust ships no wasm32 std, so the toolchain has to come from rustup.
if [ -d /opt/homebrew/opt/rustup/bin ]; then
  export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
fi

# Panic messages from dependencies embed the absolute path of whatever machine
# built them, so without this the shipped wasm carries the builder's home
# directory. Remapping also makes the artefact reproducible across machines.
export RUSTFLAGS="${RUSTFLAGS:-} --remap-path-prefix=$HOME=~ --remap-path-prefix=$here=."

wasm-pack build --target web --release --out-dir pkg
rm -f pkg/.gitignore

rm -rf "$here/docs/pkg"
mkdir -p "$here/docs/pkg"
cp pkg/tickvault_viewer.js pkg/tickvault_viewer_bg.wasm \
   pkg/tickvault_viewer.d.ts pkg/tickvault_viewer_bg.wasm.d.ts \
   "$here/docs/pkg/"

echo "docs/pkg: $(du -h "$here/docs/pkg/tickvault_viewer_bg.wasm" | cut -f1) of wasm"
