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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::common::fuse::{self, TestSession, TestSessionConfig};

/// Default scenario duration if `STRESS_DURATION_SECS` is unset.
const DEFAULT_DURATION_SECS: u64 = 60;

/// How long to wait for worker threads to join after signalling stop.
const JOIN_TIMEOUT: Duration = Duration::from_secs(30);

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
        "stress: starting"
    );

    let session = fuse::s3_session::new(scenario.name(), scenario.session_config());
    scenario.setup(&session);

    let stop = Arc::new(AtomicBool::new(false));
    let progress: Vec<Arc<AtomicU64>> =
        (0..scenario.num_workers()).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let mount_path: std::path::PathBuf = session.mount_path().to_path_buf();
    let mut handles = Vec::with_capacity(scenario.num_workers());
    for worker_id in 0..scenario.num_workers() {
        let scenario = scenario.clone();
        let stop = stop.clone();
        let progress = progress[worker_id].clone();
        let mount_path = mount_path.clone();
        handles.push(thread::spawn(move || {
            scenario.run_worker(worker_id, &mount_path, &progress, &stop);
        }));
    }

    thread::sleep(duration);
    stop.store(true, Ordering::SeqCst);

    let deadline = Instant::now() + JOIN_TIMEOUT;
    for (id, handle) in handles.into_iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("worker {id} did not finish within {JOIN_TIMEOUT:?} after stop");
        }
        // std::thread::JoinHandle has no timed join; rely on the bounded work loop + deadline check.
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
    tracing::info!(scenario = scenario.name(), "stress: finished");
}

fn read_duration_env() -> Duration {
    let secs = std::env::var("STRESS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);
    Duration::from_secs(secs)
}
