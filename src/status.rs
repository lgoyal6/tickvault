//! The recorder, answering for itself while it runs.
//!
//! Three endpoints, and the first one is the point of the other two:
//!
//! - `/healthz` returns 503 when any venue is in trouble, naming which and why.
//!   Something has to be able to page on this, or the next hour-long stall is
//!   as invisible as the last one.
//! - `/metrics` in Prometheus text format, so the same facts can be graphed
//!   rather than only alerted on.
//! - `/` is the gap report as it stands, which is the same thing the published
//!   dataset leads with, only live.
//!
//! Nothing here computes anything. Every number comes from the counters the
//! recorder already keeps, so the page cannot disagree with the archive.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::supervise::{Health, VenueHealth, overall};

/// How long a venue may be quiet before the health endpoint calls it down.
///
/// Five minutes, against the recorder's own sixty second idle timeout. By the
/// time this fires, a reconnect has already been tried and has not helped, so
/// the problem is not a blip and is worth waking someone for.
const SILENCE_BUDGET_NANOS: i64 = 300 * 1_000_000_000;

#[derive(Clone)]
struct Ctx {
    health: Arc<Health>,
    clock: Arc<dyn Clock>,
}

/// Serve until the process ends.
pub async fn serve(listen: &str, health: Arc<Health>, clock: Arc<dyn Clock>) -> Result<()> {
    let app = Router::new()
        .route("/", get(report))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(Ctx { health, clock });

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| Error::Other(format!("status service cannot bind {listen}: {e}")))?;
    tracing::info!(%listen, "status service listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| Error::Other(format!("status service: {e}")))
}

async fn healthz(State(ctx): State<Ctx>) -> impl IntoResponse {
    let now = ctx.clock.stamp();
    match overall(&ctx.health, now, SILENCE_BUDGET_NANOS) {
        Ok(()) => (StatusCode::OK, "ok\n".to_string()),
        // 503 rather than 200-with-a-body, so a health check that only reads
        // the status code still fails.
        Err(troubles) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{}\n", troubles.join("\n")),
        ),
    }
}

async fn metrics(State(ctx): State<Ctx>) -> impl IntoResponse {
    let now = ctx.clock.stamp().wall_nanos;
    let mut out = String::new();
    let venues = ctx.health.snapshot();

    push_help(
        &mut out,
        "tickvault_seconds_since_last_frame",
        "gauge",
        "Age of the most recent frame. The one that catches a live socket that has stopped delivering.",
    );
    for v in &venues {
        // Absent rather than zero where a venue has never delivered: never
        // having received a frame is not the same as having just received one.
        if let Some(quiet) = v.silent_for(now) {
            push(
                &mut out,
                "tickvault_seconds_since_last_frame",
                v,
                quiet as f64 / 1e9,
            );
        }
    }

    push_help(
        &mut out,
        "tickvault_up",
        "gauge",
        "1 while a venue's run is in progress.",
    );
    for v in &venues {
        push(&mut out, "tickvault_up", v, u8::from(v.running) as f64);
    }

    push_help(
        &mut out,
        "tickvault_frames_total",
        "counter",
        "Frames received on the current run.",
    );
    for v in &venues {
        push(&mut out, "tickvault_frames_total", v, v.frames as f64);
    }

    push_help(
        &mut out,
        "tickvault_reconnects_total",
        "counter",
        "Reconnects on the current run.",
    );
    for v in &venues {
        push(
            &mut out,
            "tickvault_reconnects_total",
            v,
            v.reconnects as f64,
        );
    }

    push_help(
        &mut out,
        "tickvault_restarts_total",
        "counter",
        "Times the supervisor had to start a venue again.",
    );
    for v in &venues {
        push(&mut out, "tickvault_restarts_total", v, v.restarts as f64);
    }

    push_help(
        &mut out,
        "tickvault_messages_missing_total",
        "counter",
        "Messages a venue's own sequencing proves were lost.",
    );
    push_help(
        &mut out,
        "tickvault_rows_dropped_total",
        "counter",
        "Rows the archive lost because our writer could not keep up. Ours, not the venue's.",
    );
    push_help(
        &mut out,
        "tickvault_unverifiable_total",
        "counter",
        "Messages accepted that nothing could be checked against. Never counted as clean.",
    );
    for v in &venues {
        let Some(report) = v.report.as_ref() else {
            continue;
        };
        for row in &report.rows {
            let labels = format!("venue=\"{}\",symbol=\"{}\"", row.venue.as_str(), row.symbol);
            push_raw(
                &mut out,
                "tickvault_messages_missing_total",
                &labels,
                row.stats.messages_missing as f64,
            );
            push_raw(
                &mut out,
                "tickvault_rows_dropped_total",
                &labels,
                row.stats.rows_dropped_by_backpressure as f64,
            );
            push_raw(
                &mut out,
                "tickvault_unverifiable_total",
                &labels,
                row.stats.unverifiable as f64,
            );
        }
    }

    ([("content-type", "text/plain; version=0.0.4")], out)
}

async fn report(State(ctx): State<Ctx>) -> impl IntoResponse {
    let now = ctx.clock.stamp().wall_nanos;
    let venues = ctx.health.snapshot();
    let mut out = String::from("tickvault\n=========\n\n");

    if venues.is_empty() {
        out.push_str("no venues have reported yet\n");
        return ([("content-type", "text/plain; charset=utf-8")], out);
    }

    out.push_str(&format!(
        "{:<12} {:>8} {:>10} {:>9} {:>9}  {}\n",
        "venue", "frames", "last frame", "reconn", "restarts", "state"
    ));
    for v in &venues {
        let quiet = match v.silent_for(now) {
            Some(q) => format!("{:.1}s ago", q as f64 / 1e9),
            None => "never".to_string(),
        };
        let state = match v.trouble(now, SILENCE_BUDGET_NANOS) {
            None => "ok".to_string(),
            Some(why) => why,
        };
        out.push_str(&format!(
            "{:<12} {:>8} {:>10} {:>9} {:>9}  {}\n",
            v.venue.as_str(),
            v.frames,
            quiet,
            v.reconnects,
            v.restarts,
            state
        ));
    }

    for v in &venues {
        if let Some(report) = v.report.as_ref().filter(|r| !r.rows.is_empty()) {
            out.push_str(&format!("\n{}\n{report}\n", v.venue.as_str()));
        }
    }
    ([("content-type", "text/plain; charset=utf-8")], out)
}

fn push_help(out: &mut String, name: &str, kind: &str, help: &str) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
}

fn push(out: &mut String, name: &str, v: &VenueHealth, value: f64) {
    push_raw(out, name, &format!("venue=\"{}\"", v.venue.as_str()), value);
}

fn push_raw(out: &mut String, name: &str, labels: &str, value: f64) {
    out.push_str(&format!("{name}{{{labels}}} {value}\n"));
}
