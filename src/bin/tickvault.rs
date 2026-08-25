//! The recorder CLI.
//!
//! Four commands. `record` captures raw frames from a live venue, `replay`
//! feeds a tape back through the identical ingest path with messages optionally
//! removed, `capabilities` prints what each venue can and cannot prove, and
//! `plan` shows how symbols would be laid out across sockets and why.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use tickvault::clock::{Clock, ManualClock, MonotonicClock};
use tickvault::pipeline::{BackpressurePolicy, Pipeline, PipelineConfig};
use tickvault::query::Query;
use tickvault::query::replay::Speed;
use tickvault::reconstruct::{Reconstructor, Request, checkpoint};
use tickvault::recorder::{RawRecorder, RawTape};
use tickvault::session::{BookSession, RunSinks, StopAfter};
use tickvault::store::reader::ArchiveReader;
use tickvault::store::{recovery, writer::WriterConfig};
use tickvault::transport::ReqwestFetch;
use tickvault::types::{Symbol, VenueId};
use tickvault::venue::kraken::Precision;
use tickvault::venue::registry::{self, VenueConfig};

#[derive(Parser)]
#[command(
    name = "tickvault",
    about = "Full-depth crypto order book recorder",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Options shared by every command that has to build a venue.
#[derive(clap::Args, Clone)]
struct VenueArgs {
    #[arg(long)]
    venue: VenueId,
    /// Canonical symbols, e.g. BTC-USD. Defaults to the pair this venue
    /// actually lists against the dollar, which is USDT on half of them.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Kraken book depth. Ten is the only depth at which its checksum covers
    /// every level the feed can touch.
    #[arg(long, default_value_t = 10)]
    kraken_depth: usize,
    #[arg(long, default_value_t = 50)]
    bybit_depth: usize,
    /// Kraken price,quantity precision, skipping the REST metadata lookup.
    #[arg(long)]
    precision: Option<String>,
    /// Record order by order rather than by price level, where the venue
    /// exposes it. Only Bitstamp does, keylessly.
    #[arg(long, default_value_t = 2)]
    book_level: u8,
}

impl VenueArgs {
    fn resolve(&self) -> Result<VenueConfig> {
        let symbols: Vec<Symbol> = if self.symbols.is_empty() {
            vec![VenueConfig::default_symbol(self.venue)]
        } else {
            self.symbols
                .iter()
                .map(|s| Symbol::parse(s).map_err(anyhow::Error::msg))
                .collect::<Result<_>>()?
        };
        for symbol in &symbols {
            if !VenueConfig::quotes_supported(self.venue, symbol) {
                bail!(
                    "{} does not list {symbol}; it quotes the other dollar. Try {} instead.",
                    self.venue,
                    VenueConfig::default_symbol(self.venue)
                );
            }
        }
        let precision = match self.precision.as_deref() {
            Some(spec) => {
                let (p, q) = spec
                    .split_once(',')
                    .context("--precision expects price,quantity")?;
                Some(Precision {
                    price: p.trim().parse()?,
                    qty: q.trim().parse()?,
                })
            }
            None => None,
        };
        let book_level = match self.book_level {
            2 => tickvault::types::BookLevel::L2,
            3 => tickvault::types::BookLevel::L3,
            other => bail!("--book-level must be 2 or 3, got {other}"),
        };
        if book_level == tickvault::types::BookLevel::L3 && self.venue != VenueId::Bitstamp {
            bail!(
                "{} publishes no keyless order-by-order feed. Coinbase, Kraken, and OKX all \
                 require authentication for theirs; only bitstamp is open.",
                self.venue
            );
        }
        Ok(VenueConfig {
            symbols,
            kraken_depth: self.kraken_depth,
            kraken_precision: precision,
            bybit_depth: self.bybit_depth,
            binance_rest_depth: 1000,
            bitstamp_book_level: book_level,
        })
    }
}

#[derive(Subcommand)]
enum Command {
    /// Record raw frames from a live venue and print the gap report.
    Record {
        #[command(flatten)]
        venue: VenueArgs,
        /// Where to write the raw tape.
        #[arg(long)]
        out: Option<String>,
        #[arg(long, default_value_t = 30)]
        seconds: u64,
        /// Write a partitioned Parquet archive here as well.
        #[arg(long)]
        archive: Option<String>,
        /// What to do when the writer cannot keep up: block the reader and
        /// risk a venue disconnect, or drop rows and record the drop. There is
        /// no default, because doing one silently is the thing to avoid.
        #[arg(long, default_value = "block")]
        backpressure: BackpressurePolicy,
        /// Batches the ingest-to-writer channel will hold.
        #[arg(long, default_value_t = 1024)]
        queue: usize,
        /// Rotate an archive file after this long. This is the bound on how
        /// much a crash can cost per partition.
        #[arg(long, default_value_t = 60)]
        rotate_secs: u64,
    },
    /// Replay a recorded raw tape, optionally with messages removed.
    ///
    /// A debugging tool for the ingest path. To replay the archive, see
    /// `replay`.
    #[command(name = "replay-tape")]
    ReplayTape {
        #[command(flatten)]
        venue: VenueArgs,
        #[arg(long)]
        tape: String,
        /// Frame indices to delete before replaying.
        #[arg(long, value_delimiter = ',')]
        drop: Vec<usize>,
        /// Delete one in every N book messages instead of naming indices.
        #[arg(long)]
        drop_every: Option<usize>,
        /// Treat each line as a bare venue payload rather than a recorded frame.
        ///
        /// The tapes under `tests/fixtures` are captured this way, and so is
        /// anything pasted out of a venue's docs or a browser's network tab.
        /// Receipt stamps do not exist in such a file, so they are synthesised
        /// one millisecond apart from `--start`, which keeps a replay of the
        /// same file byte-identical from one run to the next.
        #[arg(long)]
        payloads: bool,
        /// Receipt time of the first payload, RFC 3339. Only with --payloads.
        #[arg(long, default_value = "2020-01-01T00:00:00Z")]
        start: String,
        /// Also write the replayed frames to an archive here.
        ///
        /// A tape plus this flag is a reproducible archive: the same frames in,
        /// the same Parquet out, no network. That is what the Python tests read,
        /// so they do not need a live venue or a committed binary blob.
        #[arg(long)]
        archive: Option<String>,
    },
    /// Check an archive: recover if needed, then read every file back.
    Verify {
        #[arg(long)]
        archive: String,
        /// Only report; do not quarantine or adopt anything.
        #[arg(long)]
        read_only: bool,
    },
    /// Merge a day's many small files into one.
    Compact {
        #[arg(long)]
        archive: String,
        /// Leave this date alone, for the day still being recorded.
        #[arg(long)]
        exclude_date: Option<String>,
    },
    /// Write synthetic rows continuously, to be killed and restarted.
    ///
    /// This exists for the phase 3 crash gate: a process that can be SIGKILLed
    /// at a randomised offset and restarted, so recovery is exercised against a
    /// real kill rather than a simulated one.
    Soak {
        #[arg(long)]
        archive: String,
        /// First sequence number to write. Each run of the gate uses its own
        /// range, so a duplicated row is detectable rather than plausible.
        #[arg(long, default_value_t = 0)]
        start_seq: u64,
        /// Rotate a file after this many milliseconds.
        #[arg(long, default_value_t = 250)]
        rotate_ms: u64,
        /// Rows per batch handed to the writer.
        #[arg(long, default_value_t = 500)]
        batch_rows: usize,
        /// Stop after this long. Zero runs until killed.
        #[arg(long, default_value_t = 0)]
        seconds: u64,
        /// Pause this many microseconds between batches, to keep the volume
        /// sane when the point is crash behaviour rather than throughput.
        #[arg(long, default_value_t = 0)]
        pace_us: u64,
        /// What to do when the writer cannot keep up.
        #[arg(long, default_value = "block")]
        backpressure: BackpressurePolicy,
    },
    /// Rebuild a book as it stood at an instant.
    Reconstruct {
        #[arg(long)]
        archive: String,
        #[arg(long)]
        venue: VenueId,
        #[arg(long)]
        symbol: String,
        /// RFC 3339 (`2026-08-25T12:00:00Z`) or raw nanoseconds since the epoch.
        #[arg(long)]
        at: String,
        /// Keep only this many levels a side.
        #[arg(long)]
        depth: Option<usize>,
        /// Replay from the beginning rather than from a checkpoint.
        #[arg(long)]
        no_checkpoints: bool,
        /// Print the top levels.
        #[arg(long, default_value_t = 5)]
        show: usize,
    },
    /// Query a range: bars, spread, imbalance, all from the real book.
    Query {
        #[arg(long)]
        archive: String,
        #[arg(long)]
        venue: VenueId,
        #[arg(long)]
        symbol: String,
        /// RFC 3339 or raw nanoseconds. Defaults to the archive's own start.
        #[arg(long)]
        from: Option<String>,
        /// Defaults to the archive's own end.
        #[arg(long)]
        to: Option<String>,
        /// Bar width in seconds.
        #[arg(long, default_value_t = 60.0)]
        bar_secs: f64,
        /// Levels a side to use for imbalance.
        #[arg(long, default_value_t = 10)]
        depth: usize,
    },
    /// Replay a range, at wall-clock speed or as fast as it reads.
    Replay {
        #[arg(long)]
        archive: String,
        #[arg(long)]
        venue: VenueId,
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        /// Speed multiplier. Zero replays as fast as the archive reads.
        #[arg(long, default_value_t = 0.0)]
        speed: f64,
        /// Print every Nth message.
        #[arg(long, default_value_t = 100)]
        every: u64,
    },
    /// Write periodic checkpoints so late queries stay cheap.
    Checkpoint {
        #[arg(long)]
        archive: String,
        /// Seconds between checkpoints.
        #[arg(long, default_value_t = 300)]
        every_secs: u64,
    },
    /// Print what each venue can and cannot tell us.
    Capabilities {
        /// Restrict to one venue.
        #[arg(long)]
        venue: Option<VenueId>,
    },
    /// Show how symbols would be spread across sockets, and why.
    Plan {
        #[command(flatten)]
        venue: VenueArgs,
    },
}

/// Accept either an RFC 3339 instant or raw nanoseconds since the epoch.
fn parse_instant(raw: &str) -> Result<i64> {
    if let Some(nanos) = tickvault::clock::parse_rfc3339_nanos(raw) {
        return Ok(nanos);
    }
    raw.parse::<i64>().map_err(|_| {
        anyhow::anyhow!("{raw:?} is neither an RFC 3339 instant nor a nanosecond count")
    })
}

/// The wall-clock span an archive covers for one partition.
fn archive_span(
    reconstructor: &Reconstructor,
    venue: VenueId,
    symbol: &Symbol,
) -> Result<(i64, i64)> {
    let files: Vec<_> = reconstructor
        .reader()
        .files()
        .iter()
        .filter(|f| f.venue == venue && f.symbol == *symbol)
        .collect();
    let first = files.iter().map(|f| f.first_recv_wall).min();
    let last = files.iter().map(|f| f.last_recv_wall).max();
    match (first, last) {
        (Some(a), Some(b)) => Ok((a, b)),
        _ => bail!("the archive holds nothing for {venue} {symbol}"),
    }
}

fn wall_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Write synthetic rows as fast as the writer will take them, until killed.
///
/// Deliberately not a venue: the gate is about the storage layer surviving a
/// kill, and a live feed would make the test depend on an exchange being up and
/// on how busy the market happens to be.
async fn soak(
    archive: String,
    start_seq: u64,
    rotate_ms: u64,
    batch_rows: usize,
    seconds: u64,
    pace_us: u64,
    backpressure: BackpressurePolicy,
) -> Result<()> {
    use tickvault::book::{BookDelta, LevelChange};
    use tickvault::clock::{Stamp, Timestamps};
    use tickvault::fixed::Fixed;
    use tickvault::store::schema::RowBuilder;
    use tickvault::store::writer::PartitionKey;
    use tickvault::types::Side;

    // Always recover first. A soak run that wrote around the wreckage of the
    // previous one would defeat the point of the exercise.
    let recovered = recovery::recover(&archive, wall_now())?;
    println!("recovery: {recovered}");

    let pipeline = Pipeline::start(PipelineConfig {
        capacity: 64,
        ..PipelineConfig::new(
            WriterConfig {
                max_file_age: Duration::from_millis(rotate_ms),
                max_rows_per_file: 100_000,
                row_group_size: 8_192,
                ..WriterConfig::new(&archive)
            },
            backpressure,
        )
    })?;

    let symbol = Symbol::parse("BTC-USD").map_err(anyhow::Error::msg)?;
    let deadline =
        (seconds > 0).then(|| tokio::time::Instant::now() + Duration::from_secs(seconds));
    println!("soak: writing from seq {start_seq}");

    let mut seq = start_seq;
    loop {
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            break;
        }
        let wall = wall_now();
        let mut builder = RowBuilder::new();
        for _ in 0..batch_rows {
            let delta = BookDelta {
                symbol: symbol.clone(),
                changes: vec![LevelChange::new(
                    Side::Bid,
                    Fixed::from_mantissa(78_000_000_000_000 + (seq as i64 % 1_000)),
                    Fixed::from_mantissa(500_000_000),
                )],
                // The gate reads this back: one row per seq, no repeats, and
                // any hole confined to the tail of the run.
                seq: Some(seq),
                checksum: None,
                stamps: Timestamps::recv_only(Stamp {
                    mono_nanos: seq,
                    wall_nanos: wall,
                }),
                prev_seq: None,
                first_seq: None,
            };
            builder.push_delta(VenueId::Kraken, &delta, seq, false);
            seq += 1;
        }
        let Some(span) = builder.wall_span() else {
            continue;
        };
        let Some(batch) = builder.finish() else {
            continue;
        };
        let key = PartitionKey::new(VenueId::Kraken, &symbol, span.0);
        pipeline.submit(key, batch, span).await;
        // Let the runtime breathe so a kill can land anywhere, including
        // mid-write, rather than only between batches.
        if pace_us > 0 {
            tokio::time::sleep(Duration::from_micros(pace_us)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    let stats = pipeline.stats();
    let writer = pipeline.shutdown().await?;
    println!(
        "soak: wrote up to seq {seq}, {} ({} files)",
        stats,
        writer.manifest().files().len()
    );
    Ok(())
}

/// Hand batches to the writer, refusing to lose any.
///
/// The live recorder has a real choice to make when the writer lags, and it
/// records whichever way it went. A tape replay has no such pressure: every
/// frame is already on disk, so anything short of writing all of them would be
/// a hole this tool invented.
async fn submit_all(
    pipeline: &Arc<Pipeline>,
    batches: Vec<(
        tickvault::store::writer::PartitionKey,
        arrow::array::RecordBatch,
        (i64, i64),
    )>,
) {
    for (key, batch, span) in batches {
        match pipeline.submit(key.clone(), batch, span).await {
            tickvault::pipeline::Submitted::Accepted
            | tickvault::pipeline::Submitted::Blocked(_) => {}
            other => {
                eprintln!("archive refused a batch for {}: {other:?}", key.symbol);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tickvault=info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Record {
            venue: args,
            out,
            seconds,
            archive,
            backpressure,
            queue,
            rotate_secs,
        } => {
            let config = args.resolve()?;
            let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
            let venue = registry::build(
                args.venue,
                &config,
                ReqwestFetch::shared()?,
                Arc::clone(&clock),
            )
            .await?;
            let mut sinks = RunSinks::none();
            if let Some(path) = &out {
                sinks = RunSinks::tape(RawRecorder::create(path).await?);
            }
            let pipeline = match &archive {
                Some(dir) => {
                    // Recover before writing a byte, so an interrupted previous
                    // run is quarantined and its lost window recorded rather
                    // than being quietly written around.
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0);
                    let recovered = recovery::recover(dir, now)?;
                    if !recovered.was_clean() {
                        println!("recovery: {recovered}");
                    }
                    let writer = WriterConfig {
                        max_file_age: Duration::from_secs(rotate_secs),
                        ..WriterConfig::new(dir)
                    };
                    let p = Arc::new(Pipeline::start(PipelineConfig {
                        capacity: queue,
                        ..PipelineConfig::new(writer, backpressure)
                    })?);
                    sinks = sinks.with_archive(Arc::clone(&p));
                    Some(p)
                }
                None => None,
            };
            println!(
                "recording {} {:?} for {seconds}s{}",
                args.venue,
                config
                    .symbols
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
                out.as_deref()
                    .map(|p| format!(" to {p}"))
                    .unwrap_or_default()
            );
            let (outcome, sinks) = tickvault::session::run(
                venue,
                config.symbols.clone(),
                clock,
                StopAfter::duration(Duration::from_secs(seconds)),
                sinks,
            )
            .await?;
            if let Some(tape) = sinks.tape {
                let rec = tape.lock().await;
                println!(
                    "captured {} frames, {} payload bytes",
                    rec.frames(),
                    rec.bytes()
                );
            }
            drop(sinks.archive);
            if let (Some(p), Some(dir)) = (pipeline, &archive) {
                let stats = p.stats();
                let writer = tickvault::pipeline::shutdown_shared(p).await?;
                println!(
                    "archive: {} ({} files)\n{}",
                    stats,
                    writer.manifest().files().len(),
                    ArchiveReader::open(dir)?.verify()
                );
            }
            println!(
                "{} frames, {} reconnects\n\n{}",
                outcome.frames, outcome.reconnects, outcome.report
            );
        }

        Command::ReplayTape {
            venue: args,
            tape,
            drop,
            drop_every,
            payloads,
            start,
            archive,
        } => {
            let config = args.resolve()?;
            // Replay stamps come from the tape; the clock only supplies the
            // sealing timestamp at the end.
            let clock: Arc<dyn Clock> = Arc::new(ManualClock::default());
            let http = ReqwestFetch::shared()?;
            let venue = match registry::build_offline(
                args.venue,
                &config,
                Arc::clone(&http),
                Arc::clone(&clock),
            ) {
                Ok(v) => v,
                // Only Kraken needs the network here, and only for precision.
                Err(_) => registry::build(args.venue, &config, http, Arc::clone(&clock)).await?,
            };

            let original = if payloads {
                RawTape::read_payloads(&tape, args.venue, parse_instant(&start)?)?
            } else {
                RawTape::read_jsonl(&tape)?
            };
            if original.is_empty() {
                bail!("{tape} contains no frames");
            }
            let mut to_drop = drop;
            if let Some(n) = drop_every {
                if n == 0 {
                    bail!("--drop-every must be at least 1");
                }
                // Determined by parsing rather than by matching a substring:
                // the substrings collide across venues.
                let book_frames = registry::delta_frame_indices(venue.as_ref(), &original);
                to_drop.extend(book_frames.iter().step_by(n).copied());
            }
            let played = original.without(&to_drop);
            println!(
                "replaying {} of {} frames ({} removed)",
                played.len(),
                original.len(),
                original.len() - played.len()
            );

            let mut session = BookSession::new(venue, config.symbols.clone());
            let pipeline = match archive.as_deref() {
                Some(dir) => {
                    // Block, never drop: a replay has no live socket to protect,
                    // so a dropped row here would be a hole invented by the tool.
                    let p = Arc::new(Pipeline::start(PipelineConfig::new(
                        WriterConfig::new(dir),
                        BackpressurePolicy::Block,
                    ))?);
                    session.enable_archive(4_096);
                    Some(p)
                }
                None => None,
            };
            let mut last = tickvault::clock::Stamp::ZERO;
            let mut unparsed = 0usize;
            for frame in &played.frames {
                let raw = frame.to_raw();
                last = raw.stamp;
                if let Err(e) = session.ingest(&raw) {
                    if unparsed == 0 {
                        eprintln!("unparsed frame: {e}");
                    }
                    unparsed += 1;
                }
                if let Some(p) = pipeline.as_ref() {
                    submit_all(p, session.take_archive_batches()).await;
                }
            }
            session.seal(last);
            if let Some(p) = pipeline {
                submit_all(&p, session.flush_archive()).await;
                let stats = p.stats();
                let writer = tickvault::pipeline::shutdown_shared(p).await?;
                let dir = archive.as_deref().expect("archive dir");
                println!(
                    "archive: {} ({} files) at {dir}",
                    stats,
                    writer.manifest().files().len()
                );
            }
            if unparsed > 0 {
                println!("{unparsed} frames could not be parsed");
            }

            for symbol in &config.symbols {
                if let Some(book) = session.book(symbol) {
                    println!(
                        "{symbol}: best bid {:?} best ask {:?} digest {:08x} ready {}",
                        book.best_bid().map(|(p, _)| p.to_string()),
                        book.best_ask().map(|(p, _)| p.to_string()),
                        book.digest(),
                        session.is_ready(symbol),
                    );
                }
            }
            println!("\n{}", session.report());
        }

        Command::Verify { archive, read_only } => {
            if !read_only {
                let now = wall_now();
                let recovered = recovery::recover(&archive, now)?;
                println!("recovery: {recovered}");
            }
            let reader = ArchiveReader::open(&archive)?;
            let report = reader.verify();
            println!("{report}");
            for t in reader.manifest().truncations() {
                println!(
                    "truncation: {} lost {}, quarantined {}",
                    t.reason,
                    t.lost_nanos()
                        .map(|n| format!("{:.3}s", n as f64 / 1e9))
                        .unwrap_or_else(|| "an unknown span".to_string()),
                    t.quarantined.as_deref().unwrap_or("nothing")
                );
            }
            if !report.is_clean() {
                bail!("archive verification failed");
            }
        }

        Command::Compact {
            archive,
            exclude_date,
        } => {
            let outcome = tickvault::store::compact::compact_all(
                &archive,
                &WriterConfig::new(&archive),
                wall_now(),
                exclude_date.as_deref(),
            )?;
            println!("{outcome}");
            let report = ArchiveReader::open(&archive)?.verify();
            println!("{report}");
            if !report.is_clean() {
                bail!("archive verification failed after compaction");
            }
        }

        Command::Soak {
            archive,
            start_seq,
            rotate_ms,
            batch_rows,
            seconds,
            pace_us,
            backpressure,
        } => {
            soak(
                archive,
                start_seq,
                rotate_ms,
                batch_rows,
                seconds,
                pace_us,
                backpressure,
            )
            .await?;
        }

        Command::Reconstruct {
            archive,
            venue,
            symbol,
            at,
            depth,
            no_checkpoints,
            show,
        } => {
            let symbol = Symbol::parse(&symbol).map_err(anyhow::Error::msg)?;
            let at_wall = parse_instant(&at)?;
            let mut request = Request::new(venue, &symbol, at_wall);
            if let Some(depth) = depth {
                request = request.with_depth(depth);
            }
            if no_checkpoints {
                request = request.without_checkpoints();
            }
            let built = Reconstructor::open(&archive)?.at(&request)?;
            println!("{built}");
            println!("digest {:08x}", built.digest());
            if !built.trust.is_clean() {
                println!("NOT CLEAN: {}", built.trust);
            }
            let bids = built.book.top(tickvault::types::Side::Bid, show);
            let asks = built.book.top(tickvault::types::Side::Ask, show);
            println!(
                "\n{:>18}  {:>14} | {:>14}  {:<18}",
                "bid qty", "bid", "ask", "ask qty"
            );
            for i in 0..show.min(bids.len().max(asks.len())) {
                let b = bids
                    .get(i)
                    .map(|(p, q)| (q.to_string(), p.to_string()))
                    .unwrap_or_default();
                let a = asks
                    .get(i)
                    .map(|(p, q)| (p.to_string(), q.to_string()))
                    .unwrap_or_default();
                println!("{:>18}  {:>14} | {:>14}  {:<18}", b.0, b.1, a.0, a.1);
            }
        }

        Command::Checkpoint {
            archive,
            every_secs,
        } => {
            let reconstructor = Reconstructor::open(&archive)?;
            let interval = (every_secs as i64) * 1_000_000_000;
            if interval <= 0 {
                bail!("--every-secs must be at least 1");
            }
            // One series per partition, since each has its own book.
            let mut partitions: std::collections::BTreeMap<(VenueId, Symbol), (i64, i64)> =
                Default::default();
            for file in reconstructor.reader().files() {
                let entry = partitions
                    .entry((file.venue, file.symbol.clone()))
                    .or_insert((i64::MAX, i64::MIN));
                entry.0 = entry.0.min(file.first_recv_wall);
                entry.1 = entry.1.max(file.last_recv_wall);
            }
            if partitions.is_empty() {
                bail!("{archive} has no files to checkpoint");
            }

            let mut written = 0;
            for ((venue, symbol), (first, last)) in partitions {
                let mut at = first + interval;
                while at <= last {
                    let built = reconstructor.at(&Request::new(venue, &symbol, at))?;
                    if !built.is_empty() {
                        checkpoint::write(
                            &archive,
                            &checkpoint::Checkpoint::of_l2(venue, &built.book, at),
                        )?;
                        written += 1;
                    }
                    at += interval;
                }
                println!(
                    "{venue} {symbol}: {:.1}s of archive",
                    (last - first) as f64 / 1e9
                );
            }
            println!("{written} checkpoint(s) written every {every_secs}s");
        }

        Command::Query {
            archive,
            venue,
            symbol,
            from,
            to,
            bar_secs,
            depth,
        } => {
            let symbol = Symbol::parse(&symbol).map_err(anyhow::Error::msg)?;
            let reconstructor = Reconstructor::open(&archive)?;
            let (start, end) = archive_span(&reconstructor, venue, &symbol)?;
            let query = Query::new(
                venue,
                &symbol,
                from.as_deref()
                    .map(parse_instant)
                    .transpose()?
                    .unwrap_or(start - 1),
                to.as_deref().map(parse_instant).transpose()?.unwrap_or(end),
            )
            .with_depth(depth);

            let interval = (bar_secs * 1e9) as i64;
            if interval <= 0 {
                bail!("--bar-secs must be positive");
            }
            let mut cursor = query.cursor(&reconstructor)?;
            let bars = tickvault::query::aggregate::bars(&mut cursor, interval)?;

            println!(
                "{venue} {symbol} over {:.3}s: {} messages, {} rows, {} file(s), from {}",
                query.duration_nanos() as f64 / 1e9,
                cursor.messages(),
                cursor.rows(),
                cursor.files_opened(),
                cursor.origin()
            );
            let trust = cursor.trust();
            if !trust.is_clean() {
                println!("NOT CLEAN: {trust}");
            }
            println!(
                "\n{:>14} {:>12} {:>12} {:>12} {:>12} {:>8} {:>10} {:>12}",
                "bar start (s)", "open", "high", "low", "close", "msgs", "spread bp", "volume"
            );
            for bar in &bars {
                println!(
                    "{:>14.3} {:>12} {:>12} {:>12} {:>12} {:>8} {:>10} {:>12}",
                    bar.start_wall as f64 / 1e9,
                    bar.open.to_string(),
                    bar.high.to_string(),
                    bar.low.to_string(),
                    bar.close.to_string(),
                    bar.updates,
                    bar.mean_spread_bps
                        .map(|b| format!("{b:.2}"))
                        .unwrap_or_else(|| "n/a".into()),
                    bar.traded_qty
                        .map(|v| v.to_string())
                        // An aggregated feed does not say what traded, and zero
                        // would claim nothing did.
                        .unwrap_or_else(|| "unknown".into()),
                );
            }

            // The final book's own statistics, from the book that was there.
            let book = cursor.book().clone();
            let top = tickvault::query::aggregate::TopOfBook::of(&book);
            let d = tickvault::query::aggregate::Depth::of(&book, depth);
            println!(
                "\nfinal book: mid {} spread {} imbalance {} over {} levels",
                top.mid()
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "n/a".into()),
                top.spread()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "n/a".into()),
                d.imbalance()
                    .map(|i| format!("{i:+.4}"))
                    .unwrap_or_else(|| "n/a".into()),
                depth
            );
        }

        Command::Replay {
            archive,
            venue,
            symbol,
            from,
            to,
            speed,
            every,
        } => {
            let symbol = Symbol::parse(&symbol).map_err(anyhow::Error::msg)?;
            let reconstructor = Reconstructor::open(&archive)?;
            let (start, end) = archive_span(&reconstructor, venue, &symbol)?;
            let query = Query::new(
                venue,
                &symbol,
                from.as_deref()
                    .map(parse_instant)
                    .transpose()?
                    .unwrap_or(start - 1),
                to.as_deref().map(parse_instant).transpose()?.unwrap_or(end),
            );
            let pace = if speed > 0.0 {
                Speed::Scaled(speed)
            } else {
                Speed::Unpaced
            };

            let mut cursor = query.cursor(&reconstructor)?;
            let mut seen = 0u64;
            let stats = tickvault::query::replay::replay(&mut cursor, pace, |tick, cursor| {
                seen += 1;
                if seen.is_multiple_of(every.max(1)) {
                    let top = tickvault::query::aggregate::TopOfBook::of(cursor.book());
                    println!(
                        "{:>20} {:>12} / {:<12} spread {}{}",
                        tick.at_wall,
                        top.bid.map(|b| b.0.to_string()).unwrap_or_default(),
                        top.ask.map(|a| a.0.to_string()).unwrap_or_default(),
                        top.spread()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "n/a".into()),
                        if tick.suspect { "  SUSPECT" } else { "" },
                    );
                }
                Ok(())
            })
            .await?;

            println!(
                "\n{} messages ({} suspect) covering {:.3}s in {:.3}s{}",
                stats.messages,
                stats.suspect_messages,
                stats.archive_span_nanos as f64 / 1e9,
                stats.elapsed_nanos as f64 / 1e9,
                stats
                    .speedup()
                    .map(|s| format!(", {s:.1}x real time"))
                    .unwrap_or_default()
            );
        }

        Command::Capabilities { venue } => {
            let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
            let http = ReqwestFetch::shared()?;
            let ids: Vec<VenueId> = match venue {
                Some(v) => vec![v],
                None => VenueId::ALL.to_vec(),
            };
            for id in ids {
                let config = VenueConfig {
                    symbols: vec![VenueConfig::default_symbol(id)],
                    kraken_precision: Some(Precision { price: 1, qty: 8 }),
                    ..VenueConfig::default()
                };
                let v =
                    registry::build_offline(id, &config, Arc::clone(&http), Arc::clone(&clock))?;
                let c = v.capabilities();
                println!("{}", c.id);
                println!("  book level        {}", c.book_level);
                println!("  validation        {:?}", c.validation);
                println!("  detects loss      {}", c.can_detect_loss());
                println!("  sequence scope    {:?}", c.scope);
                println!("  validation timing {:?}", c.timing);
                println!("  snapshot source   {:?}", c.snapshot_source);
                println!("  feed depth        {:?}", c.feed_depth);
                println!("  decimals          {:?}", c.decimals);
                println!("  redundant deletes {:?}", c.redundant_deletes);
                println!("  venue timestamps  {:?}", c.timestamps);
                println!("  keepalive         {:?}", c.keepalive);
                println!("  requires auth     {}", c.requires_auth);
                for limit in &c.detection_limits {
                    println!("  blind spot ({}): {}", limit.scope, limit.consequence);
                }
                println!();
            }
        }

        Command::Plan { venue: args } => {
            let config = args.resolve()?;
            let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
            let http = ReqwestFetch::shared()?;
            let v = registry::build_offline(args.venue, &config, http, clock)
                .or_else(|_| bail!("{} needs the network to build; use --precision", args.venue))?;
            let caps = v.capabilities();
            let plan = caps
                .plan_subscriptions(&config.symbols)
                .map_err(anyhow::Error::msg)?;
            println!(
                "{}: {} symbols over {} connection(s)",
                args.venue,
                plan.symbol_count(),
                plan.connection_count()
            );
            println!("  {}", plan.rationale);
            println!(
                "  a single gap would invalidate at most {} symbol(s)",
                plan.worst_case_blast_radius(caps.gap_is_connection_wide())
            );
            for (i, conn) in plan.connections.iter().enumerate() {
                let names: Vec<String> = conn.iter().map(|s| s.to_string()).collect();
                println!("  connection {i}: {}", names.join(", "));
            }
        }
    }
    Ok(())
}
