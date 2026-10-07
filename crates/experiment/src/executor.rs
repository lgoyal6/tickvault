//! Local bounded execution with durable states and per-content OS locks.
use crate::{Result, digest};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub estimated_memory_bytes: u64,
    pub input: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task: Task,
    pub status: Status,
    pub attempts: u32,
    pub recovered: bool,
    pub cached: bool,
    pub error: Option<String>,
    pub output: Option<Value>,
}
#[derive(Debug)]
pub enum Failure {
    Transient(String),
    Permanent(String),
    Cancelled,
}
#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry {
    task: Task,
    output: Value,
    checksum: String,
}
fn cache_checksum(task: &Task, output: &Value) -> Result<String> {
    Ok(digest(&serde_json::to_vec(&(task, output))?))
}
fn same_task(a: &Task, b: &Task) -> Result<bool> {
    Ok(serde_json::to_vec(a)? == serde_json::to_vec(b)?)
}
fn atomic_write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let directory = path.parent().ok_or("missing state parent")?;
    fs::create_dir_all(directory)?;
    let file = tempfile::NamedTempFile::new_in(directory)?;
    file.as_file().write_all(&serde_json::to_vec(value)?)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|e| e.error)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}
fn lock_file(path: &Path) -> Result<File> {
    Ok(File::options()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)?)
}

pub struct Executor {
    run: PathBuf,
    cache: PathBuf,
    workers: usize,
    memory_bytes: u64,
}
impl Executor {
    pub fn new(run: PathBuf, cache: PathBuf, workers: usize, memory_bytes: u64) -> Result<Self> {
        if workers == 0 || workers > 256 || memory_bytes == 0 {
            return Err("invalid executor resource limits".into());
        }
        Ok(Self {
            run,
            cache,
            workers: workers.min(thread::available_parallelism().map_or(1, usize::from)),
            memory_bytes,
        })
    }
    pub fn run<F>(
        &self,
        tasks: Vec<Task>,
        runner: F,
        cancel: &AtomicBool,
    ) -> Result<Vec<TaskRecord>>
    where
        F: Fn(&Task, &AtomicBool) -> std::result::Result<Value, Failure> + Sync,
    {
        fs::create_dir_all(self.run.join("tasks"))?;
        fs::create_dir_all(&self.cache)?;
        let run_lock = lock_file(&self.run.join(".lock"))?;
        run_lock
            .try_lock()
            .map_err(|_| "run already active or cannot lock")?;
        let mut unique = BTreeMap::new();
        // Validate the entire plan before writing any task state.
        for task in tasks {
            if task.id.len() != 64
                || !task
                    .id
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                || task.estimated_memory_bytes == 0
            {
                return Err("invalid task identity or memory estimate".into());
            }
            if let Some(existing) = unique.get(&task.id)
                && !same_task(existing, &task)?
            {
                return Err("same identity names conflicting tasks".into());
            }
            unique.insert(task.id.clone(), task);
        }
        let mut pending = VecDeque::new();
        let mut results = Vec::new();
        for (_, task) in unique {
            let path = self.state_path(&task.id);
            let previous = if path.exists() {
                Some(serde_json::from_slice::<TaskRecord>(&fs::read(&path)?)?)
            } else {
                None
            };
            if let Some(old) = &previous
                && !same_task(&old.task, &task)?
            {
                return Err("durable task identity collision".into());
            }
            let recovered = previous
                .as_ref()
                .is_some_and(|r| r.status == Status::Running || r.recovered);
            let mut record = TaskRecord {
                task,
                status: Status::Pending,
                attempts: 0,
                recovered,
                cached: false,
                error: None,
                output: None,
            };
            if record.task.estimated_memory_bytes > self.memory_bytes {
                record.status = Status::Failed;
                record.error = Some("task memory estimate exceeds admission budget".into());
                atomic_write(&path, &record)?;
                results.push(record);
            } else {
                atomic_write(&path, &record)?;
                pending.push_back(record);
            }
        }
        // Batches are deliberately simple: the total admitted estimate and live
        // worker count stay bounded, even if completion ordering differs.
        while !pending.is_empty() {
            let mut batch = Vec::new();
            let mut admitted = 0;
            while batch.len() < self.workers {
                let Some(next) = pending.front() else {
                    break;
                };
                if next.task.estimated_memory_bytes > self.memory_bytes - admitted {
                    break;
                }
                let next = pending.pop_front().unwrap();
                admitted += next.task.estimated_memory_bytes;
                batch.push(next);
            }
            let completed = thread::scope(|scope| {
                let handles: Vec<_> = batch
                    .into_iter()
                    .map(|record| {
                        let runner = &runner;
                        scope.spawn(move || self.execute(record, runner, cancel))
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join()
                            .map_err(|_| "executor worker panicked".into())
                            .and_then(|r| r)
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            results.extend(completed);
        }
        results.sort_by(|a, b| a.task.id.cmp(&b.task.id));
        atomic_write(&self.run.join("summary.json"), &results)?;
        Ok(results)
    }
    fn state_path(&self, id: &str) -> PathBuf {
        self.run.join("tasks").join(format!("{id}.json"))
    }
    fn execute<F>(
        &self,
        mut record: TaskRecord,
        runner: &F,
        cancel: &AtomicBool,
    ) -> Result<TaskRecord>
    where
        F: Fn(&Task, &AtomicBool) -> std::result::Result<Value, Failure>,
    {
        let state = self.state_path(&record.task.id);
        let cancelled = |record: &mut TaskRecord| {
            record.status = Status::Cancelled;
            record.error = Some("cancelled".into());
        };
        let lock = lock_file(&self.cache.join(format!("{}.lock", record.task.id)))?;
        loop {
            if cancel.load(Ordering::Acquire) {
                cancelled(&mut record);
                atomic_write(&state, &record)?;
                return Ok(record);
            }
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => thread::sleep(Duration::from_millis(25)),
                Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
            }
        }
        let cache = self.cache.join(format!("{}.json", record.task.id));
        if cache.exists() {
            let entry: CacheEntry = serde_json::from_slice(&fs::read(&cache)?)?;
            if !same_task(&entry.task, &record.task)?
                || entry.checksum != cache_checksum(&entry.task, &entry.output)?
            {
                return Err("cache integrity or identity failure".into());
            }
            record.status = Status::Succeeded;
            record.cached = true;
            record.output = Some(entry.output);
            atomic_write(&state, &record)?;
            return Ok(record);
        }
        loop {
            if cancel.load(Ordering::Acquire) {
                cancelled(&mut record);
                break;
            }
            record.attempts += 1;
            record.status = Status::Running;
            record.error = None;
            atomic_write(&state, &record)?;
            match runner(&record.task, cancel) {
                Ok(output) if !cancel.load(Ordering::Acquire) => {
                    let entry = CacheEntry {
                        checksum: cache_checksum(&record.task, &output)?,
                        task: record.task.clone(),
                        output: output.clone(),
                    };
                    atomic_write(&cache, &entry)?;
                    record.output = Some(output);
                    record.status = Status::Succeeded;
                    break;
                }
                Ok(_) | Err(Failure::Cancelled) => {
                    cancelled(&mut record);
                    break;
                }
                Err(Failure::Permanent(error)) => {
                    record.status = Status::Failed;
                    record.error = Some(error);
                    break;
                }
                Err(Failure::Transient(error)) => {
                    record.status = Status::Failed;
                    record.error = Some(error);
                    atomic_write(&state, &record)?;
                    if record.attempts >= 3 {
                        break;
                    }
                    record.status = Status::Pending;
                    atomic_write(&state, &record)?;
                    // Short cancellation-aware backoff. Retries are a runner's
                    // explicit classification, never inferred from message text.
                    for _ in 0..4 {
                        if cancel.load(Ordering::Acquire) {
                            break;
                        }
                        thread::sleep(Duration::from_millis(25));
                    }
                }
            }
        }
        atomic_write(&state, &record)?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    fn task(n: u8) -> Task {
        Task {
            id: digest(&[n]),
            estimated_memory_bytes: 10,
            input: serde_json::json!({"n": n}),
        }
    }
    #[test]
    fn bounded_execution_cache_and_restart_are_durable() {
        let dir = tempfile::tempdir().unwrap();
        let executor =
            Executor::new(dir.path().join("run"), dir.path().join("cache"), 2, 20).unwrap();
        let cancel = AtomicBool::new(false);
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let calls = AtomicUsize::new(0);
        let runner = |task: &Task, _: &AtomicBool| {
            let n = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(n, Ordering::SeqCst);
            calls.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(task.input.clone())
        };
        let tasks = vec![task(1), task(2), task(1)];
        let result = executor.run(tasks.clone(), runner, &cancel).unwrap();
        assert_eq!(result.len(), 2);
        assert!(peak.load(Ordering::SeqCst) <= 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let mut interrupted = result[0].clone();
        interrupted.status = Status::Running;
        atomic_write(&executor.state_path(&interrupted.task.id), &interrupted).unwrap();
        let result = executor
            .run(tasks, |_, _| panic!("cached work executed"), &cancel)
            .unwrap();
        assert!(result.iter().all(|r| r.cached));
        assert!(result.iter().any(|r| r.recovered));
    }
    #[test]
    fn retry_permanent_failure_memory_and_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let executor =
            Executor::new(dir.path().join("run"), dir.path().join("cache"), 2, 20).unwrap();
        let cancel = AtomicBool::new(false);
        let calls = AtomicUsize::new(0);
        let result = executor
            .run(
                vec![task(1)],
                |_, _| {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(Failure::Transient("interrupted".into()))
                    } else {
                        Ok(Value::Null)
                    }
                },
                &cancel,
            )
            .unwrap();
        assert_eq!(result[0].attempts, 2);
        let result = executor
            .run(
                vec![task(2)],
                |_, _| Err(Failure::Permanent("bad data".into())),
                &cancel,
            )
            .unwrap();
        assert_eq!(result[0].attempts, 1);
        assert_eq!(result[0].status, Status::Failed);
        let mut large = task(3);
        large.estimated_memory_bytes = 21;
        let result = executor
            .run(vec![large], |_, _| panic!("inadmissible task ran"), &cancel)
            .unwrap();
        assert_eq!(result[0].attempts, 0);
        cancel.store(true, Ordering::Release);
        let result = executor
            .run(vec![task(4)], |_, _| panic!("cancelled task ran"), &cancel)
            .unwrap();
        assert_eq!(result[0].status, Status::Cancelled);
        assert!(!executor.cache.join(format!("{}.json", task(4).id)).exists());
    }
    #[test]
    fn corrupted_cache_and_identity_collisions_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let executor =
            Executor::new(dir.path().join("run"), dir.path().join("cache"), 1, 20).unwrap();
        let cancel = AtomicBool::new(false);
        executor
            .run(vec![task(1)], |_, _| Ok(Value::Null), &cancel)
            .unwrap();
        let path = executor.cache.join(format!("{}.json", task(1).id));
        let mut entry: CacheEntry = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        entry.output = Value::Bool(true);
        fs::write(path, serde_json::to_vec(&entry).unwrap()).unwrap();
        assert!(
            executor
                .run(vec![task(1)], |_, _| Ok(Value::Null), &cancel)
                .is_err()
        );
        let mut collision = task(2);
        collision.input = Value::Null;
        assert!(
            executor
                .run(vec![task(2), collision], |_, _| Ok(Value::Null), &cancel)
                .is_err()
        );
    }
}
