//! Core harness for stress scenarios.
//!
//! Each scenario implements [`Scenario`]. Call [`run`] from a `#[test] #[ignore]` entry point.

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
mod report;
mod rss_sampler;
mod watchdog;
pub use latency::{FileOp, FileOpLatencies};
use invariants::{
    assert_p100_latency, assert_peak_reserved_invariant, assert_peak_rss_invariant, assert_teardown_invariants,
};
use report::dump_summary;
use rss_sampler::spawn_rss_sampler;
use watchdog::{NO_STALL, spawn_watchdog};

/// Default scenario duration if `STRESS_DURATION_SECS` is unset.
const DEFAULT_DURATION_SECS: u64 = 30;

/// A stress-test scenario. Implementations describe a load shape; the harness drives it.
pub trait Scenario: Send + Sync {
    /// Short name used for logging and as the S3 test prefix component.
    fn name(&self) -> &str;

    /// Maximum time worker `worker_id` may go without incrementing its progress counter
    /// before the watchdog declares it stalled. The default is 10s for all workers.
    /// The API still supports per-worker thresholds for scenarios that need them.
    fn max_idle_duration(&self, _worker_id: usize) -> Duration {
        Duration::from_secs(10)
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

    /// Raw `mem_limit` used to derive per-metric ceilings. Defaults to
    /// `session_config().filesystem_config.mem_limit`. See [`assert_peak_reserved_invariant`].
    fn peak_reserved_ceiling_bytes(&self) -> f64 {
        self.session_config().filesystem_config.mem_limit as f64
    }

    /// Maximum allowed p100 latency for `op`, aggregated across all workers. Default 10s for
    /// every op; scenarios may override per op if they have a different natural profile.
    fn max_latency(&self, _op: FileOp) -> Duration {
        Duration::from_secs(10)
    }
}

/// Return the `S3Path` every stress scenario mounts at: `<S3_BUCKET_NAME>/<S3_BUCKET_TEST_PREFIX>`
/// (no per-scenario nonce). Shared test objects live at `shared-stress-test-objects/...` under
/// this mount; ephemeral writer keys live flat at the root under a per-run nonce.
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
        // Enable delete + overwrite so scenarios can best-effort clean up test objects
        config.filesystem_config.allow_delete = true;
        config.filesystem_config.allow_overwrite = true;
        // All stress scenarios mount at the root of `S3_BUCKET_TEST_PREFIX` so shared test
        // objects (at `shared-stress-test-objects/...`) and per-run ephemeral writer keys
        // (see `test_objects::ephemeral_key`) live in the same namespace. The per-run nonce
        // baked into ephemeral keys keeps concurrent runs from colliding.
        let s3_path = stress_mount_s3_path();
        let region = crate::common::s3::get_test_region();
        let sdk_client =
            crate::common::tokio_block_on(async { crate::common::s3::get_test_sdk_client(&region).await });
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

    let rss_sampler = spawn_rss_sampler(stop.clone(), Duration::from_millis(100));

    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if stalled_worker.load(Ordering::SeqCst) != NO_STALL {
            break;
        }
        thread::sleep(Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now())));
    }
    stop.store(true, Ordering::SeqCst);
    let _ = watchdog.join();
    let _ = rss_sampler.join();

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
        let rec = handle
            .join()
            .unwrap_or_else(|e| panic!("worker {id} panicked: {e:?}"));
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
    assert_peak_reserved_invariant(scenario.name(), scenario.peak_reserved_ceiling_bytes());
    assert_peak_rss_invariant(scenario.name(), scenario.peak_reserved_ceiling_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn all_workers_threshold(num_workers: usize, d: Duration) -> Vec<Duration> {
        vec![d; num_workers]
    }

    #[test]
    fn file_op_latencies_time_and_merge() {
        let mut a = FileOpLatencies::new();
        let r = a.time(FileOp::Open, || 42);
        assert_eq!(r, 42);
        assert_eq!(a.histogram(FileOp::Open).len(), 1);
        a.time(FileOp::Read, || ());
        let mut b = FileOpLatencies::new();
        b.time(FileOp::Open, || ());
        a.merge(&b);
        assert_eq!(a.histogram(FileOp::Open).len(), 2);
        assert_eq!(a.histogram(FileOp::Read).len(), 1);
    }

    #[test]
    #[should_panic(expected = "violated p100 latency ceiling")]
    fn assert_p100_latency_flags_outlier() {
        let mut agg = FileOpLatencies::new();
        // Record a 6s read sample (in µs).
        agg.histograms[FileOp::Read as usize].record(6_000_000).unwrap();
        assert_p100_latency("test", &agg, |_| Duration::from_secs(5));
    }

    #[test]
    fn assert_p100_latency_skips_empty_ops() {
        let agg = FileOpLatencies::new();
        assert_p100_latency("test", &agg, |_| Duration::from_secs(5));
    }

    #[test]
    #[should_panic(expected = "op=read")]
    fn assert_p100_latency_respects_per_op_ceilings() {
        // Open: 6s sample under a 10s ceiling → OK. Read: 2s sample over a 1s ceiling → violation.
        let mut agg = FileOpLatencies::new();
        agg.histograms[FileOp::Open as usize].record(6_000_000).unwrap();
        agg.histograms[FileOp::Read as usize].record(2_000_000).unwrap();
        assert_p100_latency("test", &agg, |op| match op {
            FileOp::Open => Duration::from_secs(10),
            FileOp::Read => Duration::from_secs(1),
            _ => Duration::from_secs(30),
        });
    }

    #[test]
    fn watchdog_detects_stall_on_single_worker() {
        let progress = vec![Arc::new(AtomicU64::new(0))];
        let stop = Arc::new(AtomicBool::new(false));
        let stalled = Arc::new(AtomicUsize::new(NO_STALL));
        let wd = spawn_watchdog(
            "test".into(),
            all_workers_threshold(1, Duration::from_millis(500)),
            progress,
            stop.clone(),
            stalled.clone(),
        )
        ;
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
            all_workers_threshold(2, Duration::from_millis(500)),
            progress.clone(),
            stop.clone(),
            stalled.clone(),
        )
        ;

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
            all_workers_threshold(2, Duration::from_millis(500)),
            progress.clone(),
            stop.clone(),
            stalled.clone(),
        )
        ;

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

    #[test]
    fn rss_sampler_records_gauge() {
        crate::common::stress_recorder::install();
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_rss_sampler(stop.clone(), Duration::from_millis(50));
        thread::sleep(Duration::from_millis(300));
        stop.store(true, Ordering::SeqCst);
        handle.join().unwrap();
        let recorder = crate::common::stress_recorder::recorder().expect("installed");
        let m = recorder
            .get("process.memory_usage", &[])
            .expect("gauge registered");
        assert!(m.gauge_history().len() >= 1);
        assert!(m.gauge() > 0.0);
    }

    #[test]
    fn watchdog_respects_per_worker_thresholds() {
        // Worker 0 has a 2s threshold (never reached in this test window) and never advances.
        // Worker 1 has a 300ms threshold and never advances — it should be flagged first.
        let progress = vec![Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))];
        let stop = Arc::new(AtomicBool::new(false));
        let stalled = Arc::new(AtomicUsize::new(NO_STALL));
        let thresholds = vec![Duration::from_secs(2), Duration::from_millis(300)];
        let wd = spawn_watchdog(
            "test".into(),
            thresholds,
            progress.clone(),
            stop.clone(),
            stalled.clone(),
        )
        ;
        wd.join().unwrap();
        assert_eq!(stalled.load(Ordering::SeqCst), 1, "worker 1 should be flagged first");
    }
}