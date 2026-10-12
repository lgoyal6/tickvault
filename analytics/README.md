# TickVault Scala and Flink analytics adapter

This module consumes normalized TickVault events without changing the Rust
archive or reconstruction engine. It provides the event-time boundary for a
bounded replay job, an offset checkpoint/restart contract, a deterministic
baseline backtest, a watermark model for tests, and a Hadoop-compatible Hive
partition layout for downstream Parquet writers.

## Build and test

```sh
cd analytics
gradle test --no-daemon --max-workers=1
```

The first build downloads the pinned Scala, Flink, Hadoop, and Parquet
dependencies. Keep Gradle worker count at one on constrained machines.

## Evidence boundary

The current milestone proves compilation, event replay with deterministic offset
checkpoints, a cost-aware baseline backtest, watermark semantics, and partition
layout. The checkpoint test proves local resume semantics; it does not claim a
live Flink cluster's exactly-once guarantees or production HDFS/S3 throughput.
Those require a cloud run with credentials.
