# Indicator Lab executor benchmark

The benchmark is a fixed eight-task synthetic workload that measures executor
semantics rather than market-data throughput. It runs serially and with two
workers, then reruns both workloads through the content-addressed cache.

```bash
cargo run -p tickvault-experiment --release --bin executor-benchmark
```

The command reports elapsed time, peak active workers, runner calls, cache-hit
count, and the memory admission estimate. The estimate bounds scheduling; it
is not an operating-system RSS measurement. Run it on the target machine when
comparing hardware, and retain the JSON output with the experiment evidence.

The benchmark's cache run panics on a cache miss, so a successful run proves
that the second pass did not execute the runner again. Result equivalence is
checked by requiring every cached record to carry an output.

Latest local run (Apple M3 Pro, 2026-10-08): the eight-task workload reached
one active worker in serial mode and two with the bounded runner; both first
runs succeeded with eight runner calls, and both cached passes reported eight
cache hits. The measured first-pass times were 377.6 ms and 265.8 ms. These
times are local evidence only and are not a service SLO.
