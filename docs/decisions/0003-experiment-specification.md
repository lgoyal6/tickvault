# 0003: The experiment specification, its validation, and its identity

Status: accepted, 2026-10-06

## Context

An experiment result is only worth something if a stranger can rerun it and get
the same bytes, and if the specification could not have been bent after the
result was seen. `sim/manifest.json` already does this for one experiment by
hand. The Indicator Lab makes it the contract for every experiment.

## Decision

### Format: TOML, not YAML

The build prompt sketches the specification in YAML. It is TOML here. The
mainstream Rust YAML crate (`serde_yaml`) is archived and unmaintained, its
forks are young, and the `toml` crate is already a pinned dependency of the
recorder's configuration. TOML also has no implicit typing (`no` is not
`false`, `1:30` is not a number), which matters for a file that pins
timestamps. The fields are the prompt's, one for one.

```toml
name = "imbalance-baseline"
spec_version = 1
code_version = "HEAD"           # or a 40-hex sha that must match the build
input_datasets = ["kraken-btc-usd@1", "okx-btc-usdt@1"]
seed = 42

[window]
start = "2026-08-25T00:49:20Z"
end   = "2026-08-25T00:53:26Z"

[sampling]
step_ms = 250
depth = 10

[[features]]
name = "order_book_imbalance"
parameters = { depth = 10 }

[labels]
horizon_seconds = 1

[splits]
kind = "chronological"
embargo_seconds = 2
train      = { start = "...", end = "..." }
validation = { start = "...", end = "..." }
test       = { start = "...", end = "..." }

baselines = ["constant", "previous-return"]

[resources]
workers = 4
memory_mb = 4096
```

### Validation, before anything is scheduled

Rejected, each with its own error kind so a test can assert which rule fired:

- a dataset reference without `@version`, or one that does not resolve;
- an instant without an explicit UTC offset (`2026-08-25T00:49:20` is refused;
  a naive time is a timezone error waiting to happen);
- splits that overlap, are out of order, or leave the window;
- an embargo shorter than the label horizon, which is look-ahead leakage: the
  last training label would be computed from prices inside the validation
  split;
- a horizon that is not positive, is not a whole number of sampling steps, or
  is longer than the shortest split;
- a feature or baseline name the lab does not implement, or a parameter it
  does not accept;
- randomness without a seed: the bootstrap needs one, so `seed` is required
  whenever a statistic that resamples is requested, which today is always;
- a `code_version` sha that differs from the code actually running.

### Normalization and identity

The specification is parsed into typed structures, instants are converted to
integer nanoseconds, features are sorted by name and parameters, and the result
is serialized as canonical JSON (sorted keys, no whitespace). The experiment id
is the sha256 of that JSON, together with the sha256 of every input dataset
manifest and the code identity. Two specifications that differ only in key
order, whitespace, comments or instant spelling have the same id.

A task is one input dataset under one experiment. Its id is the sha256 of the
normalized specification minus the other inputs, plus its own dataset manifest
hash and the code identity, so adding a dataset to an experiment reuses the
cached results of the ones already there.

### Code identity

The code identity is captured at build time: the git commit plus a digest of
the relevant source files and manifests, including untracked files. A bare
`git diff HEAD` is insufficient because new crates can be untracked, and a
runtime git lookup can describe different code than the executable contains.
The dirty flag states whether the executable is reproducible from the named
commit alone. When source identity cannot be established, caching is disabled
and the report states the limitation. Identity is never invented.

The dependency lock hash is the sha256 of `Cargo.lock` compiled into the binary
with `include_bytes!`, so it describes what was built rather than whatever
lockfile happens to sit in the working directory at run time.
