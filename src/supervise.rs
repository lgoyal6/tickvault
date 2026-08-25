//! Keeps every configured venue recording, and knows when one is not.
//!
//! The failure this exists to prevent is the one that actually happened. Six
//! venues were recorded as six separate processes, three of them stopped
//! receiving data without exiting, and nothing noticed for an hour because
//! nothing was watching. A supervisor that restarts a venue is half the answer;
//! the other half is that it holds a live view of every venue so something can
//! be asked whether the recording is healthy *now*, rather than after it stops.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::clock::{Clock, Stamp};
use crate::config::Config;
use crate::error::Result;
use crate::gap::GapReport;
use crate::pipeline::{Pipeline, PipelineConfig};
use crate::session::{ProgressSink, RunSinks, RunSnapshot, StopAfter};
use crate::store::recovery;
use crate::store::writer::WriterConfig;
use crate::transport::ReqwestFetch;
use crate::types::VenueId;
use crate::venue::registry;

/// What one venue is doing, as far as the supervisor knows.
#[derive(Debug, Clone)]
pub struct VenueHealth {
    pub venue: VenueId,
    /// False while a venue is between attempts.
    pub running: bool,
    pub frames: u64,
    pub reconnects: u32,
    /// Receipt time of the last frame, wall nanoseconds.
    pub last_frame_wall: Option<i64>,
    /// How many times the run ended and had to be started again.
    pub restarts: u32,
    /// Why it ended, if it ended badly.
    pub last_error: Option<String>,
    pub report: Option<GapReport>,
}

impl VenueHealth {
    fn new(venue: VenueId) -> Self {
        VenueHealth {
            venue,
            running: false,
            frames: 0,
            reconnects: 0,
            last_frame_wall: None,
            restarts: 0,
            last_error: None,
            report: None,
        }
    }

    /// Nanoseconds since a frame last arrived, given now.
    ///
    /// The number a watcher actually wants. Every other field looks identical
    /// on a healthy feed and on one that is connected and saying nothing.
    pub fn silent_for(&self, now_wall: i64) -> Option<i64> {
        self.last_frame_wall.map(|at| (now_wall - at).max(0))
    }

    /// Whether this venue looks alive, and if not, why not.
    ///
    /// Deliberately more suspicious than the recorder's own idle timeout: by
    /// the time a feed has been quiet for several times that, a reconnect has
    /// already been attempted and has not helped.
    pub fn trouble(&self, now_wall: i64, silence_budget_nanos: i64) -> Option<String> {
        if !self.running {
            return Some(match &self.last_error {
                Some(e) => format!("not running: {e}"),
                None => "not running".to_string(),
            });
        }
        match self.silent_for(now_wall) {
            None => Some("connected but has never received a frame".to_string()),
            Some(quiet) if quiet > silence_budget_nanos => Some(format!(
                "no frame for {:.0}s",
                quiet as f64 / 1_000_000_000.0
            )),
            _ => None,
        }
    }
}

/// The live view of every venue, shared with whatever is watching.
#[derive(Debug, Default)]
pub struct Health {
    venues: Mutex<BTreeMap<VenueId, VenueHealth>>,
}

impl Health {
    pub fn snapshot(&self) -> Vec<VenueHealth> {
        self.venues
            .lock()
            .expect("health lock")
            .values()
            .cloned()
            .collect()
    }

    fn with<R>(&self, venue: VenueId, f: impl FnOnce(&mut VenueHealth) -> R) -> R {
        let mut guard = self.venues.lock().expect("health lock");
        f(guard
            .entry(venue)
            .or_insert_with(|| VenueHealth::new(venue)))
    }

    fn set_running(&self, venue: VenueId, running: bool) {
        self.with(venue, |h| h.running = running);
    }

    fn record_restart(&self, venue: VenueId, error: Option<String>) {
        self.with(venue, |h| {
            h.running = false;
            h.restarts += 1;
            h.last_error = error;
        });
    }
}

impl ProgressSink for Health {
    fn update(&self, snapshot: RunSnapshot) {
        self.with(snapshot.venue, |h| {
            h.running = true;
            h.frames = snapshot.frames;
            h.reconnects = snapshot.reconnects;
            // Never let a fresh snapshot move this backwards: a run that
            // restarts begins at zero and that is not the same as silence.
            if snapshot.last_frame_wall.is_some() {
                h.last_frame_wall = snapshot.last_frame_wall;
            }
            h.report = Some(snapshot.report);
        });
    }
}

/// Run every configured venue until the process is asked to stop.
///
/// Each venue is its own task with its own archive, its own writer thread and
/// its own restart loop, so one venue failing repeatedly cannot stall the rest.
pub async fn serve(
    config: Config,
    clock: Arc<dyn Clock>,
    health: Arc<Health>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let http = ReqwestFetch::shared()?;
    let mut tasks = Vec::new();

    for entry in config.venues.clone() {
        let config = config.clone();
        let clock = Arc::clone(&clock);
        let health = Arc::clone(&health);
        let http = Arc::clone(&http);
        let shutdown = shutdown.clone();
        tasks.push(tokio::spawn(async move {
            supervise_one(entry, config, clock, health, http, shutdown).await
        }));
    }

    if !config.retention.is_unbounded() {
        let config = config.clone();
        let clock = Arc::clone(&clock);
        let mut shutdown = shutdown.clone();
        tasks.push(tokio::spawn(async move {
            tracing::info!(policy = %config.retention.describe(), "retention");
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(config.retention_interval()) => {}
                    _ = shutdown.changed() => return,
                }
                let now = clock.stamp().wall_nanos;
                for entry in &config.venues {
                    let dir = config.archive_for(entry.name);
                    match crate::store::retention::enforce(&dir, config.retention, now) {
                        Ok(removed) if removed.files > 0 => {
                            tracing::info!(venue = %entry.name, "retention: {removed}");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(venue = %entry.name, error = %e, "retention failed");
                        }
                    }
                }
            }
        }));
    }

    for task in tasks {
        // A panicked venue task must not take the others with it.
        if let Err(e) = task.await {
            tracing::error!(error = %e, "a venue task ended abnormally");
        }
    }
    Ok(())
}

async fn supervise_one(
    entry: crate::config::VenueEntry,
    config: Config,
    clock: Arc<dyn Clock>,
    health: Arc<Health>,
    http: Arc<dyn crate::venue::HttpFetch>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let id = entry.name;
    loop {
        match attempt(&entry, &config, &clock, &health, &http, &mut shutdown).await {
            Ok(Ended::Shutdown) => {
                tracing::info!(venue = %id, "stopped");
                health.set_running(id, false);
                return;
            }
            Ok(Ended::RunFinished) => {
                tracing::info!(venue = %id, "run ended");
                health.record_restart(id, None);
            }
            Err(e) => {
                tracing::error!(venue = %id, error = %e, "run failed, restarting");
                health.record_restart(id, Some(e.to_string()));
            }
        }
        // Do not sit out the restart delay when we are on the way out.
        tokio::select! {
            _ = tokio::time::sleep(config.restart_delay()) => {}
            _ = shutdown.changed() => return,
        }
        if *shutdown.borrow() {
            return;
        }
    }
}

/// Why one attempt stopped.
enum Ended {
    /// The process is going down, so do not start another.
    Shutdown,
    RunFinished,
}

/// One attempt at recording one venue, from connect to whatever ends it.
async fn attempt(
    entry: &crate::config::VenueEntry,
    config: &Config,
    clock: &Arc<dyn Clock>,
    health: &Arc<Health>,
    http: &Arc<dyn crate::venue::HttpFetch>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Ended> {
    let id = entry.name;
    let venue_config = entry.resolve()?;
    let venue = registry::build(id, &venue_config, Arc::clone(http), Arc::clone(clock)).await?;

    let dir = config.archive_for(id);
    std::fs::create_dir_all(&dir)?;

    // Every start is a restart as far as the archive is concerned, so the
    // wreckage of however the last one ended is set aside before writing.
    let recovered = recovery::recover(&dir, clock.stamp().wall_nanos)?;
    if !recovered.was_clean() {
        tracing::warn!(venue = %id, "recovered: {recovered}");
    }

    let pipeline = Arc::new(Pipeline::start(PipelineConfig {
        capacity: config.queue,
        ..PipelineConfig::new(
            WriterConfig {
                max_file_age: config.rotate(),
                ..WriterConfig::new(&dir)
            },
            config.backpressure,
        )
    })?);

    let sinks = RunSinks::none()
        .with_archive(Arc::clone(&pipeline))
        .with_progress(Arc::clone(health) as Arc<dyn ProgressSink>);

    health.set_running(id, true);
    let running = crate::session::run(
        venue,
        venue_config.symbols.clone(),
        Arc::clone(clock),
        StopAfter::default(),
        sinks,
    );

    // Cancelling the run by dropping it is safe; cancelling the *writer* is
    // not. The open Parquet file is buffered whole in memory, so a process that
    // exits without closing it loses everything since the last rotation. So the
    // run is dropped here and the pipeline is drained below, in that order,
    // whichever way this ends.
    let (outcome, ending) = tokio::select! {
        result = running => (Some(result), Ended::RunFinished),
        _ = shutdown.changed() => (None, Ended::Shutdown),
    };

    let stats = pipeline.stats();
    if let Err(e) = crate::pipeline::shutdown_shared(pipeline).await {
        tracing::error!(venue = %id, error = %e, "the archive writer did not shut down cleanly");
    }
    tracing::info!(venue = %id, "archive: {stats}");

    match outcome {
        Some(Err(e)) => Err(e),
        Some(Ok((outcome, _))) => {
            tracing::info!(venue = %id, frames = outcome.frames, "run finished");
            Ok(ending)
        }
        None => Ok(ending),
    }
}

/// A supervisor-level verdict, for a health endpoint to answer with.
pub fn overall(
    health: &Health,
    now: Stamp,
    silence_budget_nanos: i64,
) -> std::result::Result<(), Vec<String>> {
    let troubles: Vec<String> = health
        .snapshot()
        .into_iter()
        .filter_map(|h| {
            h.trouble(now.wall_nanos, silence_budget_nanos)
                .map(|why| format!("{}: {why}", h.venue))
        })
        .collect();
    if troubles.is_empty() {
        Ok(())
    } else {
        Err(troubles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: i64 = 1_000_000_000;

    fn healthy(now: i64) -> VenueHealth {
        VenueHealth {
            running: true,
            frames: 100,
            last_frame_wall: Some(now),
            ..VenueHealth::new(VenueId::Kraken)
        }
    }

    #[test]
    fn a_feed_that_is_up_and_delivering_is_not_trouble() {
        let now = 1_000 * SECOND;
        assert_eq!(healthy(now).trouble(now, 60 * SECOND), None);
    }

    #[test]
    fn a_connected_feed_that_says_nothing_is_trouble() {
        // The exact case that went unnoticed for an hour. Every other field on
        // this venue looks healthy: it is running, it has frames, it has no
        // errors. Only the age of the last frame gives it away.
        let now = 1_000 * SECOND;
        let stalled = VenueHealth {
            last_frame_wall: Some(now - 300 * SECOND),
            ..healthy(now)
        };
        let why = stalled
            .trouble(now, 60 * SECOND)
            .expect("should be trouble");
        assert!(why.contains("no frame for 300s"), "{why}");
    }

    #[test]
    fn a_feed_that_never_delivered_anything_is_trouble() {
        let now = 1_000 * SECOND;
        let never = VenueHealth {
            last_frame_wall: None,
            ..healthy(now)
        };
        assert!(never.trouble(now, 60 * SECOND).is_some());
    }

    #[test]
    fn a_venue_between_restarts_reports_why_it_stopped() {
        let mut down = healthy(0);
        down.running = false;
        down.last_error = Some("connect failed".into());
        let why = down.trouble(0, 60 * SECOND).unwrap();
        assert!(why.contains("connect failed"), "{why}");
    }

    #[test]
    fn a_restart_does_not_reset_when_data_last_arrived() {
        // A fresh run starts its frame counter at zero. If that overwrote the
        // last-frame time, every restart would look like a feed that had just
        // delivered, which is the opposite of the truth.
        let health = Health::default();
        health.update(RunSnapshot {
            venue: VenueId::Kraken,
            frames: 10,
            reconnects: 0,
            last_frame_wall: Some(500 * SECOND),
            report: GapReport::default(),
        });
        health.update(RunSnapshot {
            venue: VenueId::Kraken,
            frames: 0,
            reconnects: 0,
            last_frame_wall: None,
            report: GapReport::default(),
        });
        assert_eq!(
            health.snapshot()[0].last_frame_wall,
            Some(500 * SECOND),
            "a restart erased the only evidence of when data last arrived"
        );
    }

    #[test]
    fn overall_names_every_venue_in_trouble() {
        let health = Health::default();
        let now = 1_000 * SECOND;
        health.with(VenueId::Kraken, |h| {
            *h = healthy(now);
        });
        health.with(VenueId::Bitstamp, |h| {
            *h = VenueHealth {
                venue: VenueId::Bitstamp,
                last_frame_wall: Some(now - 900 * SECOND),
                ..healthy(now)
            };
        });
        let err = overall(
            &health,
            Stamp {
                mono_nanos: 0,
                wall_nanos: now,
            },
            60 * SECOND,
        )
        .expect_err("bitstamp is silent");
        assert_eq!(err.len(), 1);
        assert!(err[0].starts_with("bitstamp"), "{:?}", err);
    }
}
