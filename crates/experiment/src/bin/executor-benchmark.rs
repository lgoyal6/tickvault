//! Reproducible executor benchmark for the Indicator Lab contract.
//!
//! The workload is intentionally synthetic: it measures executor semantics,
//! not market-data throughput. The task inputs and resource estimates are
//! fixed so serial, bounded-parallel, and cached runs can be compared on one
//! machine without claiming a production performance number.

use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tickvault_experiment::{
    digest,
    executor::{Executor, Task},
};

fn tasks() -> Vec<Task> {
    (0..8)
        .map(|n| Task {
            id: digest(format!("indicator-lab-benchmark-task-{n}").as_bytes()),
            estimated_memory_bytes: 1 << 20,
            input: json!({"workload": "named-synthetic", "task": n, "seed": 42}),
        })
        .collect()
}

fn run(workers: usize, tasks: &[Task]) -> tickvault_experiment::Result<serde_json::Value> {
    let root = tempdir()?;
    let executor = Executor::new(
        root.path().join("run"),
        root.path().join("cache"),
        workers,
        8 << 20,
    )?;
    let cancel = AtomicBool::new(false);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let first = executor.run(
        tasks.to_vec(),
        {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let calls = Arc::clone(&calls);
            move |task, _| {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                calls.fetch_add(1, Ordering::SeqCst);
                // Fixed work keeps the comparison stable while still exposing the
                // executor's worker bound.
                thread::sleep(Duration::from_millis(10));
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(task.input.clone())
            }
        },
        &cancel,
    )?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let cached_started = Instant::now();
    let cached = executor.run(tasks.to_vec(), |_, _| panic!("cache miss"), &cancel)?;
    let cached_ms = cached_started.elapsed().as_secs_f64() * 1000.0;
    Ok(json!({
        "workers": workers,
        "tasks": tasks.len(),
        "memory_budget_bytes": 8 << 20,
        "task_estimate_bytes": 1 << 20,
        "elapsed_ms": elapsed_ms,
        "cached_elapsed_ms": cached_ms,
        "peak_active_workers": peak.load(Ordering::SeqCst),
        "runner_calls": calls.load(Ordering::SeqCst),
        "first_run_succeeded": first.iter().all(|r| r.status == tickvault_experiment::executor::Status::Succeeded),
        "cached_run_hits": cached.iter().filter(|r| r.cached).count(),
        "cached_run_equivalent": cached.iter().all(|r| r.output.is_some()),
    }))
}

fn main() -> tickvault_experiment::Result<()> {
    let tasks = tasks();
    let output = json!({
        "workload": "named-synthetic-eight-tasks",
        "seed": 42,
        "runs": [run(1, &tasks)?, run(2, &tasks)?],
        "note": "Elapsed time and concurrency are measured on this host; memory is an admission estimate, not an OS RSS measurement.",
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
