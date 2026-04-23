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
use crate::common::test_recorder::stress::{StressMetricSnapshot, StressTestRecorder};

use hdrhistogram::Histogram as HdrHistogram;

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
    /// before the watchdog declares it stalled. The default is 30s for all workers (closes
    /// of large write objects can legitimately take many seconds). The API still supports
    /// per-worker thresholds for scenarios that need them.
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
    /// Time file ops via `recorder.time(op, || ...)` so the harness can aggregate per-op
    /// latency histograms and assert a p100 ceiling at teardown.
    fn run_worker(
        &self,
        worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        recorder: &mut WorkerRecorder,
        stop: &AtomicBool,
    );

    /// Expected peak `sum(mem.bytes_reserved)` ceiling in bytes. Defaults to
    /// `session_config().filesystem_config.mem_limit`.
    fn peak_reserved_ceiling_bytes(&self) -> f64 {
        self.session_config().filesystem_config.mem_limit as f64
    }

    /// Maximum allowed p100 latency per worker op, aggregated across all workers. Default 30s.
    fn max_latency(&self) -> Duration {
        Duration::from_secs(30)
    }
}

/// File operations timed by [`WorkerRecorder`].
#[derive(Clone, Copy, Debug)]
pub enum Op {
    Open = 0,
    Read = 1,
    Write = 2,
    Close = 3,
}

impl Op {
    pub const ALL: [Op; 4] = [Op::Open, Op::Read, Op::Write, Op::Close];

    pub fn name(self) -> &'static str {
        match self {
            Op::Open => "open",
            Op::Read => "read",
            Op::Write => "write",
            Op::Close => "close",
        }
    }
}

/// Per-worker latency recorder. Each worker owns one; the harness merges them at teardown.
pub struct WorkerRecorder {
    histograms: [HdrHistogram<u64>; 4],
}

impl WorkerRecorder {
    pub fn new() -> Self {
        let mk = || HdrHistogram::<u64>::new_with_bounds(1, 600_000_000, 3).expect("HDR bounds valid");
        Self {
            histograms: [mk(), mk(), mk(), mk()],
        }
    }

    /// Time `f` and record its elapsed microseconds under `op`.
    pub fn time<R>(&mut self, op: Op, f: impl FnOnce() -> R) -> R {
        let start = Instant::now();
        let out = f();
        let elapsed_us = start.elapsed().as_micros();
        let h = &mut self.histograms[op as usize];
        let high = h.high();
        let v = elapsed_us.min(high as u128) as u64;
        h.record(v).ok();
        out
    }

    pub fn histogram(&self, op: Op) -> &HdrHistogram<u64> {
        &self.histograms[op as usize]
    }

    pub fn merge(&mut self, other: &WorkerRecorder) {
        for op in Op::ALL {
            // Safe: both histograms share the same bounds.
            let _ = self.histograms[op as usize].add(&other.histograms[op as usize]);
        }
    }
}

impl Default for WorkerRecorder {
    fn default() -> Self {
        Self::new()
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
    let mut handles: Vec<thread::JoinHandle<WorkerRecorder>> = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let scenario = scenario.clone();
        let stop = stop.clone();
        let progress = progress[worker_id].clone();
        let mount_path = mount_path.clone();
        handles.push(thread::spawn(move || {
            let mut recorder = WorkerRecorder::new();
            scenario.run_worker(worker_id, &mount_path, &progress, &mut recorder, &stop);
            recorder
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
    let mut aggregate = WorkerRecorder::new();
    for (id, handle) in handles.into_iter().enumerate() {
        if Instant::now() >= join_deadline {
            panic!("worker {id} did not finish within {JOIN_TIMEOUT:?} after stop");
        }
        let rec = handle
            .join()
            .unwrap_or_else(|e| panic!("worker {id} panicked: {e:?}"));
        aggregate.merge(&rec);
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

    dump_summary(scenario.name(), &aggregate);

    assert_peak_reserved_invariant(
        scenario.name(),
        peak_reserved.load(Ordering::Relaxed) as f64,
        scenario.peak_reserved_ceiling_bytes(),
    );
    assert_teardown_invariants(scenario.name());
    assert_p100_latency(scenario.name(), &aggregate, scenario.max_latency());

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
fn read_mem_bytes_reserved(recorder: &StressTestRecorder) -> Vec<(&'static str, f64)> {
    ["upload", "prefetch"]
        .into_iter()
        .filter_map(|area| {
            recorder
                .get("mem.bytes_reserved", &[("area", area)])
                .map(|metric| (area, metric.gauge()))
        })
        .collect()
}

/// Format a microsecond value as milliseconds with one decimal place.
fn us_to_ms_str(us: u64) -> String {
    format!("{:.1}", us as f64 / 1000.0)
}

/// Print a per-op worker latency table followed by the global StressTestRecorder snapshot.
fn dump_summary(scenario_name: &str, aggregate: &WorkerRecorder) {
    println!("=== stress [{scenario_name}] worker op latencies ===");
    for op in Op::ALL {
        let h = aggregate.histogram(op);
        let count = h.len();
        if count == 0 {
            println!("op={} count=0", op.name());
            continue;
        }
        println!(
            "op={} count={} p50={}ms p90={}ms p99={}ms p100={}ms",
            op.name(),
            count,
            us_to_ms_str(h.value_at_quantile(0.50)),
            us_to_ms_str(h.value_at_quantile(0.90)),
            us_to_ms_str(h.value_at_quantile(0.99)),
            us_to_ms_str(h.max()),
        );
    }

    println!("=== stress [{scenario_name}] global metrics ===");
    let Some(recorder) = stress_recorder::recorder() else {
        println!("(no global recorder installed)");
        return;
    };
    let mut snapshot = recorder.snapshot_all();
    snapshot.sort_by(|a, b| format!("{}", a.0).cmp(&format!("{}", b.0)));
    for (key, metric) in snapshot {
        match metric {
            StressMetricSnapshot::Histogram(h) => {
                let count = h.len();
                if count == 0 {
                    println!("hist {key}: count=0");
                } else {
                    println!(
                        "hist {key}: count={} p50={}ms p90={}ms p99={}ms p100={}ms",
                        count,
                        us_to_ms_str(h.value_at_quantile(0.50)),
                        us_to_ms_str(h.value_at_quantile(0.90)),
                        us_to_ms_str(h.value_at_quantile(0.99)),
                        us_to_ms_str(h.max()),
                    );
                }
            }
            StressMetricSnapshot::Counter(c) => println!("counter {key}: {c}"),
            StressMetricSnapshot::Gauge(g) => println!("gauge {key}: {g}"),
        }
    }
}

/// Assert that the merged per-op p100 latency is within `max_latency`. Ops with zero samples
/// are skipped (e.g. read-only scenarios never record a write sample).
fn assert_p100_latency(scenario_name: &str, aggregate: &WorkerRecorder, max_latency: Duration) {
    let max_us = max_latency.as_micros().min(u64::MAX as u128) as u64;
    let mut violations: Vec<String> = Vec::new();
    for op in Op::ALL {
        let h = aggregate.histogram(op);
        if h.len() == 0 {
            continue;
        }
        let max_observed = h.max();
        if max_observed > max_us {
            violations.push(format!(
                "op={} p100={}ms exceeds max_latency={}ms (count={})",
                op.name(),
                us_to_ms_str(max_observed),
                us_to_ms_str(max_us),
                h.len(),
            ));
        }
    }
    if !violations.is_empty() {
        panic!(
            "stress: scenario {:?} violated p100 latency ceiling:\n  {}",
            scenario_name,
            violations.join("\n  "),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_workers_threshold(num_workers: usize, d: Duration) -> Vec<Duration> {
        vec![d; num_workers]
    }

    #[test]
    fn worker_recorder_time_and_merge() {
        let mut a = WorkerRecorder::new();
        let r = a.time(Op::Open, || 42);
        assert_eq!(r, 42);
        assert_eq!(a.histogram(Op::Open).len(), 1);
        a.time(Op::Read, || ());
        let mut b = WorkerRecorder::new();
        b.time(Op::Open, || ());
        a.merge(&b);
        assert_eq!(a.histogram(Op::Open).len(), 2);
        assert_eq!(a.histogram(Op::Read).len(), 1);
    }

    #[test]
    #[should_panic(expected = "violated p100 latency ceiling")]
    fn assert_p100_latency_flags_outlier() {
        let mut agg = WorkerRecorder::new();
        // Record a 6s read sample (in µs).
        agg.histograms[Op::Read as usize].record(6_000_000).unwrap();
        assert_p100_latency("test", &agg, Duration::from_secs(5));
    }

    #[test]
    fn assert_p100_latency_skips_empty_ops() {
        let agg = WorkerRecorder::new();
        assert_p100_latency("test", &agg, Duration::from_secs(5));
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
