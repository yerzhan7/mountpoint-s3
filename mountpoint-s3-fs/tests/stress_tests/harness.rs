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

use mountpoint_s3_fs::s3::S3Path;

use crate::common::fuse::{self, TestSession, TestSessionConfig};
use crate::common::stress_recorder;
use crate::common::test_recorder::TestRecorder;

/// Default scenario duration if `STRESS_DURATION_SECS` is unset.
const DEFAULT_DURATION_SECS: u64 = 30;

/// Default metrics-logger interval if `STRESS_METRICS_INTERVAL_SECS` is unset.
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 10;

/// How long to wait for worker threads to join after signalling stop.
const JOIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Watchdog poll interval.
const WATCHDOG_POLL: Duration = Duration::from_secs(1);

/// Sentinel value meaning "no stall detected".
const NO_STALL: usize = usize::MAX;

/// At teardown, total `mem.bytes_reserved` across areas must be at or below this many bytes.
const TEARDOWN_RESERVED_SLACK_BYTES: f64 = 1.0 * 1024.0 * 1024.0;

/// A stress-test scenario. Implementations describe a load shape; the harness drives it.
pub trait Scenario: Send + Sync {
    /// Short name used for logging and as the S3 test prefix component.
    fn name(&self) -> &str;

    /// Maximum time worker `worker_id` may go without incrementing its progress counter
    /// before the watchdog declares it stalled. The default is 30s; scenarios with
    /// heterogeneous worker classes (e.g. readers vs writers) should override this to
    /// return a tighter threshold for fast-moving workers.
    fn max_idle_duration(&self, _worker_id: usize) -> Duration {
        Duration::from_secs(30)
    }

    /// Number of worker threads to spawn.
    fn num_workers(&self) -> usize;

    /// Session configuration for this scenario (memory limit, part size, etc.).
    fn session_config(&self) -> TestSessionConfig;

    /// If `Some`, the harness mounts against this `S3Path` instead of a per-scenario nonced
    /// prefix. Scenarios that depend on shared stress test objects return
    /// [`crate::stress_tests::test_objects::shared_s3_path`] here.
    fn s3_path_override(&self) -> Option<S3Path> {
        None
    }

    /// One-time setup (e.g. upload test objects). Runs before any worker starts.
    fn setup(&self, _session: &TestSession) {}

    /// Worker body. Must loop until `stop` is set, incrementing `progress` to signal liveness.
    fn run_worker(&self, worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool);

    /// Expected peak `sum(mem.bytes_reserved)` ceiling in bytes. Defaults to
    /// `session_config().filesystem_config.mem_limit`.
    fn peak_reserved_ceiling_bytes(&self) -> f64 {
        self.session_config().filesystem_config.mem_limit as f64
    }
}

/// Run the given scenario. Reads `STRESS_DURATION_SECS` from env; default 30s.
pub fn run<S: Scenario + 'static>(scenario: S) {
    // Make sure the snapshotting recorder is installed even if the common-module ctor has not
    // run yet (nextest uses a fresh process per test).
    stress_recorder::install();

    let duration = read_duration_env();
    let metrics_interval = read_metrics_interval_env();
    let scenario = Arc::new(scenario);

    tracing::info!(
        scenario = scenario.name(),
        duration_secs = duration.as_secs(),
        workers = scenario.num_workers(),
        metrics_interval_secs = metrics_interval.as_secs(),
        "stress: starting"
    );

    let session = match scenario.s3_path_override() {
        Some(s3_path) => {
            let region = crate::common::s3::get_test_region();
            let sdk_client =
                crate::common::tokio_block_on(async { crate::common::s3::get_test_sdk_client(&region).await });
            fuse::s3_session::new_with_test_client(scenario.session_config(), sdk_client, s3_path)
        }
        None => fuse::s3_session::new(scenario.name(), scenario.session_config()),
    };
    scenario.setup(&session);

    let num_workers = scenario.num_workers();
    let stop = Arc::new(AtomicBool::new(false));
    let progress: Vec<Arc<AtomicU64>> = (0..num_workers).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let stalled_worker = Arc::new(AtomicUsize::new(NO_STALL));
    let peak_reserved = Arc::new(AtomicU64::new(0));

    let max_idles: Vec<Duration> = (0..num_workers).map(|i| scenario.max_idle_duration(i)).collect();

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
        max_idles,
        progress.clone(),
        stop.clone(),
        stalled_worker.clone(),
    );
    let metrics_logger = spawn_metrics_logger(
        scenario.name().to_string(),
        metrics_interval,
        stop.clone(),
        peak_reserved.clone(),
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
    let _ = metrics_logger.join();

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
            scenario.max_idle_duration(stalled),
        );
    }

    assert_peak_reserved_invariant(
        scenario.name(),
        peak_reserved.load(Ordering::Relaxed) as f64,
        scenario.peak_reserved_ceiling_bytes(),
    );
    assert_teardown_invariants(scenario.name());

    tracing::info!(scenario = scenario.name(), "stress: finished");
}

fn spawn_watchdog(
    scenario_name: String,
    max_idle_per_worker: Vec<Duration>,
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
                } else if now.duration_since(last_advance[id]) >= max_idle_per_worker[id] {
                    let snapshot: Vec<(usize, u64)> = progress
                        .iter()
                        .enumerate()
                        .map(|(i, p)| (i, p.load(Ordering::Relaxed)))
                        .collect();
                    tracing::error!(
                        scenario = %scenario_name,
                        stalled_worker = id,
                        max_idle_secs = max_idle_per_worker[id].as_secs(),
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

fn read_metrics_interval_env() -> Duration {
    let secs = std::env::var("STRESS_METRICS_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);
    Duration::from_secs(secs)
}

fn spawn_metrics_logger(
    scenario_name: String,
    interval: Duration,
    stop: Arc<AtomicBool>,
    peak_reserved: Arc<AtomicU64>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            // Sleep in small slices so we notice stop promptly.
            let mut remaining = interval;
            let slice = Duration::from_millis(200);
            while remaining > Duration::ZERO && !stop.load(Ordering::Relaxed) {
                let s = slice.min(remaining);
                thread::sleep(s);
                remaining = remaining.saturating_sub(s);
            }
            let total = log_mem_metrics(&scenario_name);
            update_peak_reserved(&peak_reserved, total);
        }
    })
}

fn update_peak_reserved(peak_reserved: &AtomicU64, sample: f64) {
    if !sample.is_finite() || sample < 0.0 {
        return;
    }
    let sample_u64 = sample.round() as u64;
    let mut current = peak_reserved.load(Ordering::Relaxed);
    while sample_u64 > current {
        match peak_reserved.compare_exchange_weak(current, sample_u64, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

/// Log the current `mem.bytes_reserved` gauges (per area) and return the summed total.
fn log_mem_metrics(scenario_name: &str) -> f64 {
    let Some(recorder) = stress_recorder::recorder() else {
        return 0.0;
    };
    let per_area = read_mem_bytes_reserved(recorder);
    let total: f64 = per_area.iter().map(|(_, v)| *v).sum();
    for (area, value) in &per_area {
        tracing::info!(
            scenario = scenario_name,
            metric = "mem.bytes_reserved",
            area = *area,
            value = *value,
            "stress: metric",
        );
    }
    total
}

fn assert_peak_reserved_invariant(scenario_name: &str, peak: f64, ceiling: f64) {
    tracing::info!(
        scenario = scenario_name,
        peak_reserved_bytes = peak,
        ceiling_bytes = ceiling,
        "stress: peak mem.bytes_reserved"
    );
    assert!(
        peak <= ceiling,
        "peak-reserved invariant violated: peak sum(mem.bytes_reserved) = {} bytes exceeds ceiling {} bytes",
        peak,
        ceiling,
    );
}

fn assert_teardown_invariants(scenario_name: &str) {
    let Some(recorder) = stress_recorder::recorder() else {
        tracing::warn!(
            scenario = scenario_name,
            "stress: no recorder installed, skipping teardown invariants"
        );
        return;
    };
    let per_area = read_mem_bytes_reserved(recorder);
    let total: f64 = per_area.iter().map(|(_, v)| *v).sum();
    tracing::info!(
        scenario = scenario_name,
        total_reserved_bytes = total,
        ?per_area,
        "stress: teardown mem.bytes_reserved"
    );
    assert!(
        total <= TEARDOWN_RESERVED_SLACK_BYTES,
        "teardown invariant violated: sum(mem.bytes_reserved) = {} bytes exceeds slack {} bytes (per-area: {:?})",
        total,
        TEARDOWN_RESERVED_SLACK_BYTES,
        per_area,
    );
}

/// Return `(area_label, gauge_value)` pairs for each `mem.bytes_reserved{area=...}` we know about.
fn read_mem_bytes_reserved(recorder: &TestRecorder) -> Vec<(&'static str, f64)> {
    ["upload", "prefetch"]
        .into_iter()
        .filter_map(|area| {
            recorder
                .get("mem.bytes_reserved", &[("area", area)])
                .map(|metric| (area, metric.gauge()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_workers_threshold(num_workers: usize, d: Duration) -> Vec<Duration> {
        vec![d; num_workers]
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
            all_workers_threshold(2, Duration::from_millis(500)),
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
            all_workers_threshold(2, Duration::from_millis(500)),
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
        .expect("watchdog disabled via env");
        wd.join().unwrap();
        assert_eq!(stalled.load(Ordering::SeqCst), 1, "worker 1 should be flagged first");
    }

    #[test]
    fn update_peak_reserved_tracks_running_max() {
        let peak = AtomicU64::new(0);
        update_peak_reserved(&peak, 100.0);
        assert_eq!(peak.load(Ordering::Relaxed), 100);
        update_peak_reserved(&peak, 50.0);
        assert_eq!(peak.load(Ordering::Relaxed), 100);
        update_peak_reserved(&peak, 500.0);
        assert_eq!(peak.load(Ordering::Relaxed), 500);
        update_peak_reserved(&peak, -5.0);
        assert_eq!(peak.load(Ordering::Relaxed), 500);
        update_peak_reserved(&peak, f64::NAN);
        assert_eq!(peak.load(Ordering::Relaxed), 500);
    }
}
