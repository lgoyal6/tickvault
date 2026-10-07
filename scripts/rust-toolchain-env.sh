#!/usr/bin/env bash
# Source this before building when cargo on PATH comes from a package manager.
# cargo and rustc must use the same toolchain, including WASM libraries and rust-lld.
tickvault_rust_version=1.98.0
tickvault_rust_compiler="$(rustup which --toolchain "$tickvault_rust_version" rustc)" || return 1
tickvault_rust_bin="$(dirname "$tickvault_rust_compiler")"
export PATH="$tickvault_rust_bin:$PATH"
if [ "$(uname -s)" = Darwin ]; then
  export DYLD_LIBRARY_PATH="$(dirname "$tickvault_rust_bin")/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi
unset tickvault_rust_version tickvault_rust_bin tickvault_rust_compiler
