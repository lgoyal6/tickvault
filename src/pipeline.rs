//! Ingest to disk, with the backpressure decision made explicitly.
//!
//! The socket does not care that the disk is slow. When the writer falls
//! behind, something has to give, and there are only two honest answers:
//!
//! - **Block the reader.** Nothing is lost from the data's point of view, but
//!   the socket buffer fills and the venue may hang up on us. That disconnect
//!   is a gap in the dataset, and it is one *we* caused.
//! - **Drop the message and record the drop.** The feed keeps flowing and the
//!   archive has a hole in it that the gap report names precisely.
//!
//! Both are defensible and which is right depends on what the data is for.
//! Silently doing one is not defensible, which is why [`BackpressurePolicy`] is
//! a required choice rather than a default, why dropped messages land in the
//! gap report next to venue-caused gaps, and why the lag that provoked the
//! decision is measured and published.
//!
//! The writer runs on its own thread on the far side of a bounded channel.
//! Parquet encoding and fsync are blocking work, and doing them on the runtime
//! would turn a slow disk into latency injected straight into the socket
//! reader, which is precisely the confusion this separation exists to prevent.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;

use crate::error::{Error, Result};
use crate::store::writer::{ArchiveWriter, PartitionKey, WriterConfig};

/// What to do when the writer cannot keep up.
///
/// `Block` is the default only because one of the two has to be, and blocking
/// fails loudly: the venue disconnects and the gap report says so. Dropping
/// fails quietly unless someone reads the drop counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackpressurePolicy {
    /// Wait for room. Risks the venue disconnecting us, which the gap report
    /// then shows as a reconnect rather than as missing rows.
    #[default]
    Block,
    /// Discard the message rather than wait, and count it. The feed stays
    /// healthy and the archive is explicitly short of those rows.
    Drop,
}

impl std::fmt::Display for BackpressurePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            BackpressurePolicy::Block => "block",
            BackpressurePolicy::Drop => "drop",
        })
    }
}

impl std::str::FromStr for BackpressurePolicy {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "block" => Ok(BackpressurePolicy::Block),
            "drop" => Ok(BackpressurePolicy::Drop),
            other => Err(format!(
                "backpressure policy must be 'block' or 'drop', got {other:?}"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Batches the channel will hold before the policy applies.
    pub capacity: usize,
    pub policy: BackpressurePolicy,
    pub writer: WriterConfig,
    /// How often to nudge the writer so an idle partition still rotates.
    pub tick_interval: Duration,
}

impl PipelineConfig {
    pub fn new(writer: WriterConfig, policy: BackpressurePolicy) -> Self {
        PipelineConfig {
            capacity: 1024,
            policy,
            writer,
            tick_interval: Duration::from_secs(1),
        }
    }
}

/// Counters describing how the pipeline coped.
///
/// These are the phase 8 load numbers: how far behind real time the writer
/// fell, and what that cost.
#[derive(Debug, Default)]
pub struct PipelineStats {
    batches_submitted: AtomicU64,
    batches_written: AtomicU64,
    rows_submitted: AtomicU64,
    rows_written: AtomicU64,
    batches_dropped: AtomicU64,
    rows_dropped: AtomicU64,
    blocked_nanos: AtomicU64,
    max_lag_nanos: AtomicU64,
    max_queue_depth: AtomicU64,
}

impl PipelineStats {
    fn bump_max(slot: &AtomicU64, value: u64) {
        let mut current = slot.load(Ordering::Relaxed);
        while value > current {
            match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub fn batches_submitted(&self) -> u64 {
        self.batches_submitted.load(Ordering::Relaxed)
    }
    pub fn batches_written(&self) -> u64 {
        self.batches_written.load(Ordering::Relaxed)
    }
    pub fn rows_submitted(&self) -> u64 {
        self.rows_submitted.load(Ordering::Relaxed)
    }
    pub fn rows_written(&self) -> u64 {
        self.rows_written.load(Ordering::Relaxed)
    }
    pub fn batches_dropped(&self) -> u64 {
        self.batches_dropped.load(Ordering::Relaxed)
    }
    pub fn rows_dropped(&self) -> u64 {
        self.rows_dropped.load(Ordering::Relaxed)
    }
    /// Total time the reader spent waiting for the writer, under `Block`.
    pub fn blocked(&self) -> Duration {
        Duration::from_nanos(self.blocked_nanos.load(Ordering::Relaxed))
    }
    /// The worst gap between a batch being handed over and being written.
    ///
    /// This is "how far behind real time the writer fell under load", and it is
    /// the number the published load section reports.
    pub fn max_lag(&self) -> Duration {
        Duration::from_nanos(self.max_lag_nanos.load(Ordering::Relaxed))
    }
    pub fn max_queue_depth(&self) -> u64 {
        self.max_queue_depth.load(Ordering::Relaxed)
    }

    /// True when nothing was dropped and the reader never had to wait.
    pub fn kept_up(&self) -> bool {
        self.batches_dropped() == 0 && self.blocked_nanos.load(Ordering::Relaxed) == 0
    }
}

impl std::fmt::Display for PipelineStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} rows written, {} dropped, blocked {:.3}s, worst writer lag {:.3}s, peak queue {}",
            self.rows_written(),
            self.rows_dropped(),
            self.blocked().as_secs_f64(),
            self.max_lag().as_secs_f64(),
            self.max_queue_depth()
        )
    }
}

/// What happened to one submitted batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submitted {
    /// Handed over without waiting.
    Accepted,
    /// Handed over, but the reader waited this long for room.
    Blocked(Duration),
    /// Discarded under the `Drop` policy, losing this many rows.
    Dropped { rows: usize },
    /// The writer thread is gone.
    WriterStopped,
}

impl Submitted {
    pub fn rows_lost(&self) -> usize {
        match self {
            Submitted::Dropped { rows } => *rows,
            _ => 0,
        }
    }
}

enum Message {
    Data(Box<WriteRequest>),
    /// Wakes the writer so an idle partition still rotates on time.
    Tick,
}

struct WriteRequest {
    key: PartitionKey,
    batch: RecordBatch,
    span: (i64, i64),
    submitted_at: Instant,
}

/// Groups rows by partition until there are enough to be worth a write.
///
/// One Parquet write per websocket frame would mean thousands of tiny row
/// groups a second, which compresses badly and makes the writer the bottleneck
/// for no reason. Rows accumulate per partition and go over the channel in
/// batches, so a frame spanning several symbols still lands in the right files.
pub struct RowAccumulator {
    builders: std::collections::BTreeMap<PartitionKey, crate::store::schema::RowBuilder>,
    max_rows: usize,
    /// The window the feed maintained, recorded so a rebuild truncates the same
    /// way the recorder did.
    feed_depth: Option<usize>,
}

impl RowAccumulator {
    pub fn new(max_rows: usize) -> Self {
        RowAccumulator {
            builders: std::collections::BTreeMap::new(),
            max_rows: max_rows.max(1),
            feed_depth: None,
        }
    }

    pub fn with_feed_depth(mut self, depth: Option<usize>) -> Self {
        self.feed_depth = depth;
        self
    }

    fn builder(&mut self, key: PartitionKey) -> &mut crate::store::schema::RowBuilder {
        self.builders.entry(key).or_default()
    }

    pub fn push_snapshot(
        &mut self,
        venue: crate::types::VenueId,
        snapshot: &crate::book::BookSnapshot,
        msg_index: u64,
        suspect: bool,
    ) {
        let key = PartitionKey::new(venue, &snapshot.symbol, snapshot.stamps.recv.wall_nanos)
            .with_feed_depth(self.feed_depth);
        self.builder(key)
            .push_snapshot(venue, snapshot, msg_index, suspect);
    }

    pub fn push_delta(
        &mut self,
        venue: crate::types::VenueId,
        delta: &crate::book::BookDelta,
        msg_index: u64,
        suspect: bool,
    ) {
        let key = PartitionKey::new(venue, &delta.symbol, delta.stamps.recv.wall_nanos)
            .with_feed_depth(self.feed_depth);
        self.builder(key)
            .push_delta(venue, delta, msg_index, suspect);
    }

    pub fn push_order(
        &mut self,
        venue: crate::types::VenueId,
        event: &crate::book::l3::OrderEvent,
        position: Option<crate::book::l3::QueuePosition>,
        traded: Option<crate::Fixed>,
        msg_index: u64,
        suspect: bool,
    ) {
        let key = PartitionKey::at_level(
            venue,
            &event.symbol,
            event.stamps.recv.wall_nanos,
            crate::types::BookLevel::L3,
        )
        .with_feed_depth(self.feed_depth);
        self.builder(key)
            .push_order(venue, event, position, traded, msg_index, suspect);
    }

    pub fn pending_rows(&self) -> usize {
        self.builders.values().map(|b| b.len()).sum()
    }

    /// Batches for partitions that have accumulated enough rows.
    pub fn take_ready(&mut self) -> Vec<(PartitionKey, RecordBatch, (i64, i64))> {
        let ready: Vec<PartitionKey> = self
            .builders
            .iter()
            .filter(|(_, b)| b.len() >= self.max_rows)
            .map(|(k, _)| k.clone())
            .collect();
        self.drain(ready)
    }

    /// Everything held, whether or not it reached the batch size.
    pub fn take_all(&mut self) -> Vec<(PartitionKey, RecordBatch, (i64, i64))> {
        let all: Vec<PartitionKey> = self.builders.keys().cloned().collect();
        self.drain(all)
    }

    fn drain(&mut self, keys: Vec<PartitionKey>) -> Vec<(PartitionKey, RecordBatch, (i64, i64))> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(builder) = self.builders.get_mut(&key) else {
                continue;
            };
            let Some(span) = builder.wall_span() else {
                continue;
            };
            if let Some(batch) = builder.finish() {
                out.push((key, batch, span));
            }
        }
        self.builders.retain(|_, b| !b.is_empty());
        out
    }
}

/// The ingest side of the pipeline.
pub struct Pipeline {
    tx: tokio::sync::mpsc::Sender<Message>,
    stats: Arc<PipelineStats>,
    policy: BackpressurePolicy,
    capacity: usize,
    worker: Option<std::thread::JoinHandle<Result<ArchiveWriter>>>,
    ticker: Option<tokio::task::JoinHandle<()>>,
}

impl Pipeline {
    /// Start the writer thread.
    pub fn start(config: PipelineConfig) -> Result<Self> {
        let writer = ArchiveWriter::open(config.writer.clone())?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(config.capacity);
        let stats = Arc::new(PipelineStats::default());

        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("tickvault-writer".to_string())
            .spawn(move || -> Result<ArchiveWriter> {
                let mut writer = writer;
                while let Some(message) = rx.blocking_recv() {
                    match message {
                        Message::Data(request) => {
                            let WriteRequest {
                                key,
                                batch,
                                span,
                                submitted_at,
                            } = *request;
                            let rows = batch.num_rows() as u64;
                            writer.write(&key, &batch, span)?;
                            worker_stats.batches_written.fetch_add(1, Ordering::Relaxed);
                            worker_stats.rows_written.fetch_add(rows, Ordering::Relaxed);
                            PipelineStats::bump_max(
                                &worker_stats.max_lag_nanos,
                                submitted_at.elapsed().as_nanos() as u64,
                            );
                        }
                        Message::Tick => {
                            writer.rotate_aged()?;
                        }
                    }
                }
                // The channel closed, so ingest is done. Everything still open
                // gets a footer and a manifest entry before we return.
                writer.close()?;
                Ok(writer)
            })
            .map_err(|e| Error::Other(format!("spawning the writer thread: {e}")))?;

        // A *weak* sender on purpose. A strong clone here would keep the
        // channel open after ingest finished, and the writer thread would block
        // on a receive that could never complete: shutdown would hang forever
        // rather than closing the open files.
        let tick_tx = tx.downgrade();
        let interval = config.tick_interval;
        let ticker = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                let Some(sender) = tick_tx.upgrade() else {
                    // Ingest is gone; the writer is closing its files.
                    break;
                };
                // A full queue means the writer is busy anyway, and a tick
                // would only add to the backlog it is already working through.
                let _ = sender.try_send(Message::Tick);
            }
        });

        Ok(Pipeline {
            tx,
            stats,
            policy: config.policy,
            capacity: config.capacity,
            worker: Some(worker),
            ticker: Some(ticker),
        })
    }

    pub fn stats(&self) -> Arc<PipelineStats> {
        Arc::clone(&self.stats)
    }

    pub fn policy(&self) -> BackpressurePolicy {
        self.policy
    }

    /// Current backlog, for the log line that explains a drop.
    pub fn queue_depth(&self) -> usize {
        self.capacity.saturating_sub(self.tx.capacity())
    }

    /// Hand a batch to the writer, applying the configured policy.
    pub async fn submit(
        &self,
        key: PartitionKey,
        batch: RecordBatch,
        span: (i64, i64),
    ) -> Submitted {
        let rows = batch.num_rows();
        if rows == 0 {
            return Submitted::Accepted;
        }
        self.stats.batches_submitted.fetch_add(1, Ordering::Relaxed);
        self.stats
            .rows_submitted
            .fetch_add(rows as u64, Ordering::Relaxed);
        PipelineStats::bump_max(&self.stats.max_queue_depth, self.queue_depth() as u64 + 1);

        let request = Box::new(WriteRequest {
            key,
            batch,
            span,
            submitted_at: Instant::now(),
        });

        match self.policy {
            BackpressurePolicy::Drop => match self.tx.try_send(Message::Data(request)) {
                Ok(()) => Submitted::Accepted,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.stats.batches_dropped.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .rows_dropped
                        .fetch_add(rows as u64, Ordering::Relaxed);
                    Submitted::Dropped { rows }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Submitted::WriterStopped,
            },
            BackpressurePolicy::Block => {
                // Try without waiting first, so the common case costs nothing
                // and `blocked` really does mean the reader was held up.
                match self.tx.try_send(Message::Data(request)) {
                    Ok(()) => Submitted::Accepted,
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        Submitted::WriterStopped
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(message)) => {
                        let started = Instant::now();
                        match self.tx.send(message).await {
                            Ok(()) => {
                                let waited = started.elapsed();
                                self.stats
                                    .blocked_nanos
                                    .fetch_add(waited.as_nanos() as u64, Ordering::Relaxed);
                                Submitted::Blocked(waited)
                            }
                            Err(_) => Submitted::WriterStopped,
                        }
                    }
                }
            }
        }
    }

    /// Stop accepting work and wait for everything queued to reach disk.
    ///
    /// Returns the writer so its manifest can be inspected. Closing the channel
    /// is what makes the writer thread finish its open files, so skipping this
    /// leaves the last file of every partition unreadable.
    pub async fn shutdown(mut self) -> Result<ArchiveWriter> {
        if let Some(ticker) = self.ticker.take() {
            ticker.abort();
        }
        // Dropping the last sender is the signal to drain and close.
        let (tx, _) = tokio::sync::mpsc::channel::<Message>(1);
        let sender = std::mem::replace(&mut self.tx, tx);
        drop(sender);

        let worker = self
            .worker
            .take()
            .ok_or_else(|| Error::Other("pipeline already shut down".to_string()))?;
        tokio::task::spawn_blocking(move || worker.join())
            .await
            .map_err(|e| Error::Other(format!("joining the writer thread: {e}")))?
            .map_err(|_| Error::Other("the writer thread panicked".to_string()))?
    }
}

/// Shut down a pipeline held behind an [`Arc`].
///
/// The runner hands clones to each connection task, so ownership only comes
/// back once those have finished. Failing here means a task outlived the run,
/// which would leave the last file of every partition unreadable.
pub async fn shutdown_shared(pipeline: Arc<Pipeline>) -> Result<ArchiveWriter> {
    let owned = Arc::try_unwrap(pipeline)
        .map_err(|_| Error::Other("a recording task still holds the pipeline".to_string()))?;
    owned.shutdown().await
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        if let Some(ticker) = self.ticker.take() {
            ticker.abort();
        }
        if let Some(worker) = self.worker.take() {
            // Shutting down without `shutdown()` still has to close the open
            // files, or the last rotation of every partition is lost.
            let (tx, _) = tokio::sync::mpsc::channel::<Message>(1);
            drop(std::mem::replace(&mut self.tx, tx));
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{BookDelta, LevelChange};
    use crate::clock::{Stamp, Timestamps};
    use crate::fixed::Fixed;
    use crate::store::schema::RowBuilder;
    use crate::types::{Side, Symbol, VenueId};

    const BASE: i64 = 1_787_000_000_000_000_000;

    fn batch(rows: usize, wall: i64) -> (PartitionKey, RecordBatch, (i64, i64)) {
        let symbol = Symbol::new("BTC", "USD");
        let mut builder = RowBuilder::new();
        for i in 0..rows {
            let delta = BookDelta {
                symbol: symbol.clone(),
                changes: vec![LevelChange::new(
                    Side::Bid,
                    Fixed::from_decimal_str("1").unwrap(),
                    Fixed::from_decimal_str("1").unwrap(),
                )],
                seq: Some(i as u64),
                checksum: None,
                stamps: Timestamps::recv_only(Stamp {
                    mono_nanos: wall as u64,
                    wall_nanos: wall,
                }),
                prev_seq: None,
                first_seq: None,
            };
            builder.push_delta(VenueId::Kraken, &delta, i as u64, false);
        }
        let span = builder.wall_span().unwrap();
        (
            PartitionKey::new(VenueId::Kraken, &symbol, wall),
            builder.finish().unwrap(),
            span,
        )
    }

    fn config(
        dir: &std::path::Path,
        policy: BackpressurePolicy,
        capacity: usize,
    ) -> PipelineConfig {
        PipelineConfig {
            capacity,
            tick_interval: Duration::from_millis(50),
            ..PipelineConfig::new(WriterConfig::new(dir), policy)
        }
    }

    #[tokio::test]
    async fn everything_submitted_reaches_disk_and_is_vouched_for() {
        let dir = tempfile::tempdir().unwrap();
        let pipeline = Pipeline::start(config(dir.path(), BackpressurePolicy::Block, 64)).unwrap();
        let stats = pipeline.stats();
        for i in 0..20 {
            let (key, b, span) = batch(10, BASE + i * 1_000_000);
            assert_eq!(pipeline.submit(key, b, span).await, Submitted::Accepted);
        }
        let writer = pipeline.shutdown().await.unwrap();
        assert_eq!(stats.rows_submitted(), 200);
        assert_eq!(stats.rows_written(), 200);
        assert_eq!(writer.manifest().total_rows(), 200);
        assert!(
            crate::store::reader::ArchiveReader::open(dir.path())
                .unwrap()
                .verify()
                .is_clean()
        );
    }

    #[tokio::test]
    async fn a_full_queue_under_the_drop_policy_loses_rows_and_says_so() {
        let dir = tempfile::tempdir().unwrap();
        // Capacity of one, and no writer progress possible until we yield, so
        // the queue fills immediately.
        let pipeline = Pipeline::start(config(dir.path(), BackpressurePolicy::Drop, 1)).unwrap();
        let stats = pipeline.stats();
        let mut dropped = 0;
        for i in 0..200 {
            let (key, b, span) = batch(5, BASE + i * 1_000_000);
            if let Submitted::Dropped { rows } = pipeline.submit(key, b, span).await {
                dropped += rows;
            }
        }
        pipeline.shutdown().await.unwrap();
        assert!(dropped > 0, "the queue never filled, so nothing was proven");
        assert_eq!(stats.rows_dropped(), dropped as u64);
        assert_eq!(
            stats.rows_submitted(),
            stats.rows_written() + stats.rows_dropped(),
            "every submitted row is either written or counted as dropped"
        );
        assert!(!stats.kept_up());
        assert_eq!(stats.blocked(), Duration::ZERO, "dropping never waits");
    }

    #[tokio::test]
    async fn the_block_policy_waits_instead_of_losing_rows() {
        let dir = tempfile::tempdir().unwrap();
        let pipeline = Pipeline::start(config(dir.path(), BackpressurePolicy::Block, 1)).unwrap();
        let stats = pipeline.stats();
        for i in 0..200 {
            let (key, b, span) = batch(5, BASE + i * 1_000_000);
            let outcome = pipeline.submit(key, b, span).await;
            assert!(
                !matches!(outcome, Submitted::Dropped { .. }),
                "blocking must never drop"
            );
        }
        pipeline.shutdown().await.unwrap();
        assert_eq!(stats.rows_dropped(), 0);
        assert_eq!(stats.rows_written(), 1_000);
        assert!(
            crate::store::reader::ArchiveReader::open(dir.path())
                .unwrap()
                .verify()
                .is_clean()
        );
    }

    #[tokio::test]
    async fn writer_lag_is_measured_rather_than_assumed() {
        let dir = tempfile::tempdir().unwrap();
        let pipeline = Pipeline::start(config(dir.path(), BackpressurePolicy::Block, 256)).unwrap();
        let stats = pipeline.stats();
        for i in 0..100 {
            let (key, b, span) = batch(50, BASE + i * 1_000_000);
            pipeline.submit(key, b, span).await;
        }
        pipeline.shutdown().await.unwrap();
        assert!(
            stats.max_lag() > Duration::ZERO,
            "a real queue always has some lag; zero means it is not being measured"
        );
        assert!(stats.max_queue_depth() >= 1);
    }

    #[tokio::test]
    async fn an_empty_batch_is_not_counted_as_work() {
        // Otherwise an idle venue would inflate the throughput numbers with
        // batches that carried nothing.
        let dir = tempfile::tempdir().unwrap();
        let pipeline = Pipeline::start(config(dir.path(), BackpressurePolicy::Drop, 8)).unwrap();
        let stats = pipeline.stats();
        let empty = RecordBatch::new_empty(crate::store::schema::book_schema());
        let key = PartitionKey::new(VenueId::Kraken, &Symbol::new("BTC", "USD"), BASE);
        assert_eq!(
            pipeline.submit(key, empty, (BASE, BASE)).await,
            Submitted::Accepted
        );
        assert_eq!(stats.batches_submitted(), 0);
        assert_eq!(stats.rows_submitted(), 0);
        let writer = pipeline.shutdown().await.unwrap();
        assert!(writer.manifest().files().is_empty(), "no rows, no file");
    }

    #[test]
    fn every_row_of_a_partition_gets_the_same_key() {
        // A snapshot and a delta from one feed must land in one file. Deriving
        // the key differently on the two paths would silently split the
        // partition, and a rebuild would then see half of it.
        let mut acc = RowAccumulator::new(1_000).with_feed_depth(Some(10));
        let symbol = Symbol::new("BTC", "USD");
        let stamps = Timestamps::recv_only(Stamp {
            mono_nanos: BASE as u64,
            wall_nanos: BASE,
        });
        acc.push_snapshot(
            VenueId::Kraken,
            &crate::book::BookSnapshot {
                symbol: symbol.clone(),
                bids: vec![(
                    Fixed::from_decimal_str("1").unwrap(),
                    Fixed::from_decimal_str("1").unwrap(),
                )],
                asks: vec![],
                seq: None,
                checksum: None,
                stamps,
            },
            0,
            false,
        );
        acc.push_delta(
            VenueId::Kraken,
            &BookDelta {
                symbol,
                changes: vec![LevelChange::new(
                    Side::Bid,
                    Fixed::from_decimal_str("1").unwrap(),
                    Fixed::from_decimal_str("2").unwrap(),
                )],
                seq: None,
                checksum: None,
                stamps,
                prev_seq: None,
                first_seq: None,
            },
            1,
            false,
        );
        let batches = acc.take_all();
        assert_eq!(batches.len(), 1, "one feed produced two partitions");
        assert_eq!(batches[0].0.feed_depth, Some(10));
    }

    #[test]
    fn the_accumulator_keeps_partitions_apart() {
        let mut acc = RowAccumulator::new(1_000);
        let midnight = crate::clock::parse_rfc3339_nanos("2026-08-25T00:00:00Z").unwrap();
        for (symbol, wall) in [
            (Symbol::new("BTC", "USD"), midnight - 1),
            (Symbol::new("BTC", "USD"), midnight),
            (Symbol::new("ETH", "USD"), midnight),
        ] {
            let delta = BookDelta {
                symbol,
                changes: vec![LevelChange::new(
                    Side::Bid,
                    Fixed::from_decimal_str("1").unwrap(),
                    Fixed::from_decimal_str("1").unwrap(),
                )],
                seq: None,
                checksum: None,
                stamps: Timestamps::recv_only(Stamp {
                    mono_nanos: wall as u64,
                    wall_nanos: wall,
                }),
                prev_seq: None,
                first_seq: None,
            };
            acc.push_delta(VenueId::Kraken, &delta, 0, false);
        }
        assert_eq!(acc.pending_rows(), 3);
        // Two symbols and a date boundary: three partitions, not one batch.
        let batches = acc.take_all();
        assert_eq!(batches.len(), 3);
        assert_eq!(acc.pending_rows(), 0);
        assert!(acc.take_all().is_empty());
    }

    #[test]
    fn only_full_partitions_are_taken_early() {
        let mut acc = RowAccumulator::new(4);
        let symbol = Symbol::new("BTC", "USD");
        let delta = |wall: i64| BookDelta {
            symbol: symbol.clone(),
            changes: vec![LevelChange::new(
                Side::Bid,
                Fixed::from_decimal_str("1").unwrap(),
                Fixed::from_decimal_str("1").unwrap(),
            )],
            seq: None,
            checksum: None,
            stamps: Timestamps::recv_only(Stamp {
                mono_nanos: wall as u64,
                wall_nanos: wall,
            }),
            prev_seq: None,
            first_seq: None,
        };
        for i in 0..3 {
            acc.push_delta(VenueId::Kraken, &delta(BASE + i), 0, false);
        }
        assert!(acc.take_ready().is_empty(), "three rows is not four");
        acc.push_delta(VenueId::Kraken, &delta(BASE + 3), 0, false);
        let ready = acc.take_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].1.num_rows(), 4);
    }

    #[test]
    fn the_policy_has_no_default_and_parses_both_ways() {
        assert_eq!(
            "block".parse::<BackpressurePolicy>(),
            Ok(BackpressurePolicy::Block)
        );
        assert_eq!(
            "DROP".parse::<BackpressurePolicy>(),
            Ok(BackpressurePolicy::Drop)
        );
        assert!("silently".parse::<BackpressurePolicy>().is_err());
        assert_eq!(BackpressurePolicy::Drop.to_string(), "drop");
    }
}
