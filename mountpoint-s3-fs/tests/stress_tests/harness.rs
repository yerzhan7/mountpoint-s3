//! Core harness for stress scenarios.
//!
//! Each scenario implements [`Scenario`]. Call [`run`] from a `#[test] #[ignore]` entry point.
//!
//! # Limitations (v1)
//!
//! - No cross-thread backtrace dumping on stall. If a worker stalls, the watchdog logs
//!   per-worker progress counters but does not capture backtraces of other threads.
//!   Use a debugger or core dumps if you need more diagnostic detail.
//! - All scenarios run sequentially; `cargo nextest run ... --test-threads=1` is recommended.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::common::fuse::{self, TestSession, TestSessionConfig};

/// Default scenario duration if `STRESS_DURATION_SECS` is unset.
const DEFAULT_DURATION_SECS: u64 = 60;

/// How long to wait for worker threads to join after signalling stop.
const JOIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Watchdog poll interval.
const WATCHDOG_POLL: Duration = Duration::from_secs(1);

/// Sentinel value meaning "no stall detected".
const NO_STALL: usize = usize::MAX;

/// A stress-test scenario. Implementations describe a load shape; the harness drives it.
pub trait Scenario: Send + Sync {
    /// Short name used for logging and as the S3 test prefix component.
    fn name(&self) -> &str;

    /// Maximum time any single worker may go without incrementing its progress counter
    /// before the watchdog declares it stalled.
    fn max_idle_duration(&self) -> Duration {
        Duration::from_secs(30)
    }

    /// Number of worker threads to spawn.
    fn num_workers(&self) -> usize;

    /// Session configuration for this scenario (memory limit, part size, etc.).
    fn session_config(&self) -> TestSessionConfig;

    /// One-time setup (e.g. upload fixtures). Runs before any worker starts.
    fn setup(&self, _session: &TestSession) {}

    /// Worker body. Must loop until `stop` is set, incrementing `progress` to signal liveness.
    /// Workers should not panic on ENOMEM from `open()`/`create()` — catch, briefly back off, retry.
    fn run_worker(&self, worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool);
}

/// Run the given scenario. Reads `STRESS_DURATION_SECS` from env; default 60s.
pub fn run<S: Scenario + 'static>(scenario: S) {
    let duration = read_duration_env();
    let scenario = Arc::new(scenario);

    tracing::info!(
        scenario = scenario.name(),
        duration_secs = duration.as_secs(),
        workers = scenario.num_workers(),
        max_idle_secs = scenario.max_idle_duration().as_secs(),
        "stress: starting"
    );

    let session = fuse::s3_session::new(scenario.name(), scenario.session_config());
    scenario.setup(&session);

    let num_workers = scenario.num_workers();
    let stop = Arc::new(AtomicBool::new(false));
    let progress: Vec<Arc<AtomicU64>> = (0..num_workers).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let stalled_worker = Arc::new(AtomicUsize::new(NO_STALL));

    let mount_path: std::path::PathBuf = session.mount_path().to_path_buf();
    let mut handles = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let scenario = scenario.clone();
        let stop = stop.clone();
        let progress = progress[worker_id].clone();
        let mount_path = mount_path.clone();
        handles.push(thread::spawn(move || {
            scenario.run_worker(worker_id, &mount_path, &progress, &stop);
        }));
    }

    let watchdog = spawn_watchdog(
        scenario.name().to_string(),
        scenario.max_idle_duration(),
        progress.clone(),
        stop.clone(),
        stalled_worker.clone(),
    );

    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if stalled_worker.load(Ordering::SeqCst) != NO_STALL {
            break;
        }
        thread::sleep(Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now())));
    }
    stop.store(true, Ordering::SeqCst);
    if let Some(wd) = watchdog {
        let _ = wd.join();
    }

    let join_deadline = Instant::now() + JOIN_TIMEOUT;
    for (id, handle) in handles.into_iter().enumerate() {
        if Instant::now() >= join_deadline {
            panic!("worker {id} did not finish within {JOIN_TIMEOUT:?} after stop");
        }
        handle.join().unwrap_or_else(|e| panic!("worker {id} panicked: {e:?}"));
    }

    for (id, counter) in progress.iter().enumerate() {
        tracing::info!(
            scenario = scenario.name(),
            worker = id,
            progress = counter.load(Ordering::Relaxed),
            "stress: final worker progress"
        );
    }

    drop(session);

    let stalled = stalled_worker.load(Ordering::SeqCst);
    if stalled != NO_STALL {
        panic!(
            "stress: scenario {:?} failed: worker {stalled} stalled for at least {:?}",
            scenario.name(),
            scenario.max_idle_duration()
        );
    }

    tracing::info!(scenario = scenario.name(), "stress: finished");
}

fn spawn_watchdog(
    scenario_name: String,
    max_idle: Duration,
    progress: Vec<Arc<AtomicU64>>,
    stop: Arc<AtomicBool>,
    stalled_worker: Arc<AtomicUsize>,
) -> Option<thread::JoinHandle<()>> {
    if std::env::var("STRESS_WATCHDOG_DISABLE").is_ok() {
        tracing::warn!(scenario = %scenario_name, "stress: watchdog disabled via STRESS_WATCHDOG_DISABLE");
        return None;
    }
    Some(thread::spawn(move || {
        let start = Instant::now();
        let mut last_progress: Vec<u64> = progress.iter().map(|p| p.load(Ordering::Relaxed)).collect();
        let mut last_advance: Vec<Instant> = vec![start; progress.len()];
        while !stop.load(Ordering::Relaxed) {
            thread::sleep(WATCHDOG_POLL);
            let now = Instant::now();
            for (id, counter) in progress.iter().enumerate() {
                let current = counter.load(Ordering::Relaxed);
                if current > last_progress[id] {
                    last_progress[id] = current;
                    last_advance[id] = now;
                } else if now.duration_since(last_advance[id]) >= max_idle {
                    let snapshot: Vec<(usize, u64)> =
                        progress.iter().enumerate().map(|(i, p)| (i, p.load(Ordering::Relaxed))).collect();
                    tracing::error!(
                        scenario = %scenario_name,
                        stalled_worker = id,
                        max_idle_secs = max_idle.as_secs(),
                        ?snapshot,
                        "stress: worker stalled — per-worker progress snapshot"
                    );
                    stalled_worker.store(id, Ordering::SeqCst);
                    stop.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    }))
}

fn read_duration_env() -> Duration {
    let secs = std::env::var("STRESS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watchdog_detects_stall_on_single_worker() {
        let progress = vec![Arc::new(AtomicU64::new(0))];
        let stop = Arc::new(AtomicBool::new(false));
        let stalled = Arc::new(AtomicUsize::new(NO_STALL));
        let wd = spawn_watchdog(
            "test".into(),
            Duration::from_millis(500),
            progress,
            stop.clone(),
            stalled.clone(),
        )
        .expect("watchdog disabled via env");
        wd.join().unwrap();
        assert_eq!(stalled.load(Ordering::SeqCst), 0);
        assert!(stop.load(Ordering::SeqCst));
    }

    #[test]
    fn watchdog_does_not_fire_when_workers_progress() {
        let progress = vec![Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))];
        let stop = Arc::new(AtomicBool::new(false));
        let stalled = Arc::new(AtomicUsize::new(NO_STALL));
        let wd = spawn_watchdog(
            "test".into(),
            Duration::from_millis(500),
            progress.clone(),
            stop.clone(),
            stalled.clone(),
        )
        .expect("watchdog disabled via env");

        // Drive progress on both workers for ~1.5s, then stop.
        let deadline = Instant::now() + Duration::from_millis(1500);
        while Instant::now() < deadline {
            for p in &progress {
                p.fetch_add(1, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(100));
        }
        stop.store(true, Ordering::SeqCst);
        wd.join().unwrap();
        assert_eq!(stalled.load(Ordering::SeqCst), NO_STALL);
    }

    #[test]
    fn watchdog_detects_stall_of_one_among_many() {
        let progress = vec![Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))];
        let stop = Arc::new(AtomicBool::new(false));
        let stalled = Arc::new(AtomicUsize::new(NO_STALL));
        let wd = spawn_watchdog(
            "test".into(),
            Duration::from_millis(500),
            progress.clone(),
            stop.clone(),
            stalled.clone(),
        )
        .expect("watchdog disabled via env");

        // Worker 0 advances, worker 1 stays idle.
        let deadline = Instant::now() + Duration::from_millis(1500);
        while Instant::now() < deadline && stalled.load(Ordering::SeqCst) == NO_STALL {
            progress[0].fetch_add(1, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(100));
        }
        stop.store(true, Ordering::SeqCst);
        wd.join().unwrap();
        assert_eq!(stalled.load(Ordering::SeqCst), 1, "worker 1 should have been flagged");
    }
}
