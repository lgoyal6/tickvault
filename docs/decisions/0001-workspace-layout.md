# 0001: Workspace layout for the Indicator Lab

Status: accepted, 2026-10-06

## Context

The build prompt for the Indicator Lab lists ten crates: core, ingest, store,
query, features, experiment, report, cli, bindings and viewer. The repository
already has a single `tickvault` library crate of about 29,000 lines holding
core, ingest, store and query as modules, plus `bindings/`, `viewer/` and
`sim/` as separate packages that depend on it by path.

The boundary the prompt cares about is that the CLI, the viewer and the Python
bindings hold no business rules. The boundary that already exists is stronger
than a crate split in one respect: capture (`venue`, `transport`, `session`,
`recorder`, `pipeline`) sits behind the default `record` feature, and CI
compiles the remainder to `wasm32-unknown-unknown`, so the read half provably
carries no tokio, TLS or C codec.

## Decision

1. Keep `tickvault` as one crate holding the core, ingest, store and query
   responsibilities, separated by module and by the `record` feature.
2. Add four crates under `crates/`:
   - `tickvault-features`: indicator implementations and their contracts;
   - `tickvault-experiment`: specification, validation, planning, execution,
     caching, and the evaluation statistics;
   - `tickvault-report`: machine and human readable reports, and comparison;
   - `tickvault-cli`: the `tickvault` binary, moved out of the library crate.
3. The binary moves because the new crates depend on the library, so the
   library cannot depend on them, and the stable command surface
   (`tickvault experiment run`, `tickvault report render`) has to live in a
   crate that can see everything. The two library tests that spawn the binary
   (the SIGKILL crash gate and the restore drill) move with it, because
   `CARGO_BIN_EXE_*` is only defined for binaries of the same package.
4. Business rules the old binary held move into the library: coverage
   classification and the per-venue verifiability table now live in
   `tickvault::query::verified` and `tickvault::venue::registry`, and the CLI
   only formats what they return.

## Why not split the library crate

Moving 29,000 lines into four crates changes no behaviour, touches every path
in `bindings/`, `viewer/`, `sim/`, the examples and the gates, and gains no
boundary that the feature split and the wasm build do not already enforce. The
cost is real and the benefit is a directory listing.

Revisit if a consumer needs the store without the book, or the book without the
store; neither exists today.

## Consequences

- `cargo run --release -- verify ...` still works from the repository root,
  because the workspace's default members include the CLI and it is the only
  binary among them.
- `cargo build --release --bin tickvault` still produces
  `target/release/tickvault`, which the Python tests, the scripts and the
  attestation workflow rely on.
- `cargo test` at the root now runs the default members, which includes the
  new crates. The bindings stay out of the default members because building a
  pyo3 `cdylib` for `cargo test` needs a Python toolchain that most
  contributors do not have.
