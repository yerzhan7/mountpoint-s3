//! Core harness for stress scenarios.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use mountpoint_s3_fs::s3::{Bucket, Prefix, S3Path};

use crate::common::fuse::{self, TestSession, TestSessionConfig};
use crate::common::stress_recorder;

mod invariants;
mod latency;
mod memory_monitor;
mod report;
mod watchdog;
use invariants::{
    assert_p100_latency, assert_peak_reserved_invariant, assert_peak_rss_invariant, assert_teardown_invariants,
};
pub use latency::{FileOp, FileOpLatencies};
use memory_monitor::spawn_memory_monitor;
use report::dump_summary;
use watchdog::{NO_STALL, spawn_watchdog};

/// Default scenario duration if `STRESS_DURATION_SECS` is unset.
const DEFAULT_DURATION_SECS: u64 = 30;

/// A stress-test scenario. Implementations describe a load shape; the harness drives it.
pub trait Scenario: Send + Sync {
    /// Short name used for logging and as the S3 test prefix component.
    fn name(&self) -> &str;

    /// Maximum time worker `worker_id` may go without incrementing its progress counter
    /// before the watchdog declares it stalled. The default is 20s for all workers.
    /// The API still supports per-worker thresholds for scenarios that need them.
    fn max_idle_duration(&self, _worker_id: usize) -> Duration {
        Duration::from_secs(20)
    }

    /// Number of worker threads to spawn.
    fn num_workers(&self) -> usize;

    /// Session configuration for this scenario (memory limit, part size, etc.).
    fn session_config(&self) -> TestSessionConfig;

    /// One-time setup (e.g. upload test objects). Runs before any worker starts.
    fn setup(&self, _session: &TestSession) {}

    /// Worker body. Must loop until `stop` is set, incrementing `progress` to signal liveness.
    /// Time file ops via `latencies.time(op, || ...)` so the harness can aggregate per-op
    /// latency histograms and assert a p100 ceiling at teardown.
    fn run_worker(
        &self,
        worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut FileOpLatencies,
        stop: &AtomicBool,
    );

    /// Maximum allowed p100 latency for `op`, aggregated across all workers. Default 20s for
    /// every op; scenarios may override per op if they have a different natural profile.
    fn max_latency(&self, _op: FileOp) -> Duration {
        Duration::from_secs(20)
    }
}

/// Return the `S3Path` every stress scenario mounts at: `<S3_BUCKET_NAME>/<S3_BUCKET_TEST_PREFIX>`
fn stress_mount_s3_path() -> S3Path {
    let bucket = crate::common::s3::get_test_bucket();
    let prefix = std::env::var("S3_BUCKET_TEST_PREFIX").unwrap_or_else(|_| String::from("mountpoint-test/"));
    assert!(prefix.ends_with('/'), "S3_BUCKET_TEST_PREFIX should end in '/'");
    S3Path::new(Bucket::new(bucket).unwrap(), Prefix::new(&prefix).unwrap())
}

/// Run the given scenario. Reads `STRESS_DURATION_SECS` from env; default 30s.
pub fn run<S: Scenario + 'static>(scenario: S) {
    // Make sure the snapshotting recorder is installed even if the common-module ctor has not
    // run yet (nextest uses a fresh process per test).
    stress_recorder::install();

    let duration = read_duration_env();
    let scenario = Arc::new(scenario);

    tracing::info!(
        scenario = scenario.name(),
        duration_secs = duration.as_secs(),
        workers = scenario.num_workers(),
        "stress: starting"
    );

    let session = {
        let mut config = scenario.session_config();
        config.filesystem_config.allow_delete = true;
        config.filesystem_config.allow_overwrite = true;
        let s3_path = stress_mount_s3_path();
        let region = crate::common::s3::get_test_region();
        let sdk_client = crate::common::tokio_block_on(async { crate::common::s3::get_test_sdk_client(&region).await });
        fuse::s3_session::new_with_test_client(config, sdk_client, s3_path)
    };
    scenario.setup(&session);

    let num_workers = scenario.num_workers();
    let stop = Arc::new(AtomicBool::new(false));
    let progress: Vec<Arc<AtomicU64>> = (0..num_workers).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let stalled_worker = Arc::new(AtomicUsize::new(NO_STALL));

    let max_idles: Vec<Duration> = (0..num_workers).map(|i| scenario.max_idle_duration(i)).collect();
    let max_join_wait = max_idles
        .iter()
        .copied()
        .max()
        .expect("scenario must declare at least one worker");

    let mount_path: std::path::PathBuf = session.mount_path().to_path_buf();
    let mut handles: Vec<thread::JoinHandle<FileOpLatencies>> = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let scenario = scenario.clone();
        let stop = stop.clone();
        let progress = progress[worker_id].clone();
        let mount_path = mount_path.clone();
        handles.push(thread::spawn(move || {
            let mut latencies = FileOpLatencies::new();
            scenario.run_worker(worker_id, &mount_path, &progress, &mut latencies, &stop);
            latencies
        }));
    }

    let watchdog = spawn_watchdog(
        scenario.name().to_string(),
        max_idles,
        progress.clone(),
        stop.clone(),
        stalled_worker.clone(),
    );

    let memory_monitor = spawn_memory_monitor(stop.clone(), Duration::from_millis(100));

    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if stalled_worker.load(Ordering::SeqCst) != NO_STALL {
            break;
        }
        thread::sleep(Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now())));
    }
    stop.store(true, Ordering::SeqCst);
    let _ = watchdog.join();
    let _ = memory_monitor.join();

    // Bounded join: workers observe `stop` between ops and should finish quickly. If any
    // are wedged in a kernel FUSE syscall they cannot observe `stop` and `JoinHandle` has
    // no timeout API, so we poll `is_finished()` up to a deadline. On timeout we drop
    // (unmount) the session to force EIO/EINTR on stuck syscalls, then panic with the
    // stuck worker IDs.
    let join_deadline = Instant::now() + max_join_wait;
    while !handles.iter().all(|h| h.is_finished()) {
        if Instant::now() >= join_deadline {
            let stuck: Vec<usize> = handles
                .iter()
                .enumerate()
                .filter_map(|(id, h)| (!h.is_finished()).then_some(id))
                .collect();
            drop(session);
            panic!("stress: workers {stuck:?} did not finish within {max_join_wait:?} after stop");
        }
        thread::sleep(Duration::from_millis(100));
    }

    let mut aggregate = FileOpLatencies::new();
    for (id, handle) in handles.into_iter().enumerate() {
        let rec = handle.join().unwrap_or_else(|e| panic!("worker {id} panicked: {e:?}"));
        aggregate.merge(&rec);
    }

    drop(session);

    let stalled = stalled_worker.load(Ordering::SeqCst);
    if stalled != NO_STALL {
        panic!(
            "stress: scenario {:?} failed: worker {stalled} stalled for at least {:?}",
            scenario.name(),
            scenario.max_idle_duration(stalled),
        );
    }

    dump_summary(scenario.name(), &aggregate);

    tracing::info!("");
    tracing::info!("=== STRESS [{}] INVARIANT ASSERTIONS ===", scenario.name());
    let mem_limit = scenario.session_config().filesystem_config.mem_limit as f64;
    assert_peak_reserved_invariant(scenario.name(), mem_limit);
    assert_peak_rss_invariant(scenario.name(), mem_limit);
    assert_teardown_invariants(scenario.name());
    assert_p100_latency(scenario.name(), &aggregate, |op| scenario.max_latency(op));
    tracing::info!("");

    tracing::info!(scenario = scenario.name(), "stress: finished");
}

fn read_duration_env() -> Duration {
    let secs = std::env::var("STRESS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);
    Duration::from_secs(secs)
}
