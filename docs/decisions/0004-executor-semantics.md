# 0004: Executor semantics

Status: accepted, 2026-10-06

## Context

Experiments over several datasets are independent tasks. Running them one at a
time wastes cores; running them carelessly in parallel produces duplicate work,
lost results after a crash, and results that depend on scheduling order.

## Decision

Local execution only. A queue or distributed executor is out of scope until
these semantics have been stable for a while, and the executor's interface is
small on purpose (`Executor::run(tasks, runner, cancel)`) so that the research
contract never names a queue.

### Task state machine

```
pending -> running -> succeeded
                   -> failed{transient}  -> pending   (retry, bounded)
                   -> failed{permanent}
                   -> cancelled
running (found on restart) -> pending   (recovered, counted)
```

Every transition is written to `<run>/tasks/<task_id>.json` with
write-temp-then-rename and an fsync of the directory, so a crash leaves either
the old state or the new one, never a torn file.

### Rules

- **Identity.** A task's id is content derived (ADR 0003). Two tasks with the
  same id in one run are one task.
- **No duplicate execution.** A run holds an OS lock on its directory, so a
  second process running the same experiment fails fast instead of racing.
  Across different experiments that share a task, the cache entry is guarded
  by its own OS lock: the second process blocks, then finds the result and
  does not compute it. The kernel drops both locks when the holder dies, which
  is the reason they are OS locks and not pid files (the same reasoning as
  `store::lock`).
- **Content-addressed cache.** `<cache>/<task_id>.json`, written atomically
  after the task succeeds. Only a succeeded task is cached. A cancelled or
  failed task leaves no cache entry.
- **Bounded pool.** At most `resources.workers` tasks run at once, capped at
  the machine's available parallelism. Results are collected and sorted by
  task id, so the report does not depend on which worker finished first.
- **Memory admission.** Each task carries an estimate of its peak memory from
  the grid size, the depth and the feature count. A task is admitted only while
  the sum of running estimates fits `resources.memory_mb`; a task that alone
  exceeds it fails permanently without running. This is admission control on an
  estimate, not an operating system limit, and the report records the
  process's measured peak resident size next to the estimate so the two can be
  compared.
- **Retry only transient failures.** An I/O error of kind interrupted, timed
  out or would-block is transient and retried up to a bound with a fixed
  backoff. A verification failure, a leakage violation or malformed data is
  permanent and never retried, because retrying a deterministic failure only
  repeats it.
- **Cancellation.** A shared token is checked between grid steps. A cancelled
  task records `cancelled`, writes no cache entry, and the run reports partial
  results with an explicit status rather than as a shorter success.
- **Restart recovery.** On start, a run directory whose lock this process now
  holds cannot have a live writer, so every `running` task in it is stale: it
  goes back to `pending` and the recovery is logged. `succeeded` tasks are
  loaded from the cache, not recomputed.
- **Partial results.** The run summary lists every task with its final state.
  A report from a partial run says so in its first line.
