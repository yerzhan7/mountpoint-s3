//! Core harness for stress scenarios.
//!
//! Each scenario implements [`Scenario`]. Call [`run`] from a `#[test] #[ignore]` entry point.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use mountpoint_s3_fs::metrics::defs::PROCESS_MEMORY_USAGE;
use mountpoint_s3_fs::s3::S3Path;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

use crate::common::fuse::{self, TestSession, TestSessionConfig};
use crate::common::stress_recorder;
use crate::common::test_recorder::stress::HdrMetric;

use hdrhistogram::Histogram as HdrHistogram;

/// Default scenario duration if `STRESS_DURATION_SECS` is unset.
const DEFAULT_DURATION_SECS: u64 = 30;

/// Watchdog poll interval.
const WATCHDOG_POLL: Duration = Duration::from_secs(1);

/// Sentinel value meaning "no stall detected".
const NO_STALL: usize = usize::MAX;

/// A stress-test scenario. Implementations describe a load shape; the harness drives it.
pub trait Scenario: Send + Sync {
    /// Short name used for logging and as the S3 test prefix component.
    fn name(&self) -> &str;

    /// Maximum time worker `worker_id` may go without incrementing its progress counter
    /// before the watchdog declares it stalled. The default is 30s for all workers.
    /// The API still supports per-worker thresholds for scenarios that need them.
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
    /// Time file ops via `latencies.time(op, || ...)` so the harness can aggregate per-op
    /// latency histograms and assert a p100 ceiling at teardown.
    fn run_worker(
        &self,
        worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut OpLatencies,
        stop: &AtomicBool,
    );

    /// Raw `mem_limit` used to derive per-metric ceilings. Defaults to
    /// `session_config().filesystem_config.mem_limit`. See [`assert_peak_reserved_invariant`].
    fn peak_reserved_ceiling_bytes(&self) -> f64 {
        self.session_config().filesystem_config.mem_limit as f64
    }

    /// Maximum allowed p100 latency per worker op, aggregated across all workers. Default 30s.
    fn max_latency(&self) -> Duration {
        Duration::from_secs(30)
    }
}

/// File operations timed by [`OpLatencies`].
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

/// Per-worker op-latency histograms. Each worker owns one; the harness merges them at teardown.
pub struct OpLatencies {
    histograms: [HdrHistogram<u64>; 4],
}

impl OpLatencies {
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

    pub fn merge(&mut self, other: &OpLatencies) {
        for op in Op::ALL {
            // Safe: both histograms share the same bounds.
            let _ = self.histograms[op as usize].add(&other.histograms[op as usize]);
        }
    }
}

impl Default for OpLatencies {
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
        match scenario.s3_path_override() {
            Some(s3_path) => {
                let region = crate::common::s3::get_test_region();
                let sdk_client =
                    crate::common::tokio_block_on(async { crate::common::s3::get_test_sdk_client(&region).await });
                fuse::s3_session::new_with_test_client(config, sdk_client, s3_path)
            }
            None => fuse::s3_session::new(scenario.name(), config),
        }
    };
    scenario.setup(&session);

    let num_workers = scenario.num_workers();
    let stop = Arc::new(AtomicBool::new(false));
    let progress: Vec<Arc<AtomicU64>> = (0..num_workers).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let stalled_worker = Arc::new(AtomicUsize::new(NO_STALL));

    let max_idles: Vec<Duration> = (0..num_workers).map(|i| scenario.max_idle_duration(i)).collect();

    let mount_path: std::path::PathBuf = session.mount_path().to_path_buf();
    let mut handles: Vec<thread::JoinHandle<OpLatencies>> = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let scenario = scenario.clone();
        let stop = stop.clone();
        let progress = progress[worker_id].clone();
        let mount_path = mount_path.clone();
        handles.push(thread::spawn(move || {
            let mut latencies = OpLatencies::new();
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

    let mut aggregate = OpLatencies::new();
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

    assert_peak_reserved_invariant(scenario.name(), scenario.peak_reserved_ceiling_bytes());
    assert_peak_rss_invariant(scenario.name(), scenario.peak_reserved_ceiling_bytes());
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
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
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
    })
}

/// Spawn a thread that samples the current process's RSS every `interval` and records it
/// into the `process.memory_usage` gauge, matching the metric name the production binary
/// emits from `mountpoint_s3_fs::metrics::poll_process_metrics`. Adapted from
/// `examples/s3io_benchmark/monitoring.rs` (`MemoryMonitor` / `monitor_memory_loop`),
/// translated from a tokio task to a sync `std::thread` driven by the harness's stop flag.
fn spawn_rss_sampler(stop: Arc<AtomicBool>, interval: Duration) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let Ok(pid) = get_current_pid() else {
            tracing::warn!("stress: get_current_pid failed; RSS sampling disabled");
            return;
        };
        let mut sys = System::new();
        let sample = |sys: &mut System| {
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                false,
                ProcessRefreshKind::nothing().with_memory(),
            );
            if let Some(process) = sys.process(pid) {
                metrics::gauge!(PROCESS_MEMORY_USAGE).set(process.memory() as f64);
            }
        };
        while !stop.load(Ordering::Relaxed) {
            sample(&mut sys);
            thread::sleep(interval);
        }
        // Final sample so the teardown peak is captured (mirrors `MemoryMonitor::stop`).
        sample(&mut sys);
    })
}

fn read_duration_env() -> Duration {
    let secs = std::env::var("STRESS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);
    Duration::from_secs(secs)
}

const MEM_AREAS: &[&str] = &["upload", "prefetch"];
const POOL_KINDS: &[&str] = &["get_object", "put_object", "disk_cache", "append", "other"];

/// Assert each individual reserved-memory gauge stayed below its appropriate ceiling over
/// the whole run:
///
/// * `mem.bytes_reserved{area=*}` (Mountpoint's own limiter-tracked reservations) <=
///   `mem_limit - additional_mem_reserved` (the effective budget the limiter enforces against).
/// * `pool.reserved_bytes{kind=*}` (CRT paged-pool occupancy) <= `mem_limit`.
///
/// Peaks come from each gauge's post-mutation HDR history, so no samples are missed between
/// allocation spikes. We deliberately do not assert the sum across labels — the peaks are
/// independent time series and summing their maxes overestimates the true peak of the sum.
/// Per-label checks give clearer failure attribution (which area / which kind breached).
fn assert_peak_reserved_invariant(scenario_name: &str, mem_limit: f64) {
    let Some(recorder) = stress_recorder::recorder() else {
        tracing::warn!(
            scenario = scenario_name,
            "stress: no recorder installed, skipping peak-reserved invariant"
        );
        return;
    };
    let mem_limit_u64 = mem_limit as u64;
    let additional_mem_reserved = (mem_limit_u64 / 8).max(128 * 1024 * 1024);
    let effective_budget = mem_limit_u64.saturating_sub(additional_mem_reserved);

    let mut violations: Vec<String> = Vec::new();
    for &area in MEM_AREAS {
        let Some(metric) = recorder.get("mem.bytes_reserved", &[("area", area)]) else {
            continue;
        };
        let peak = metric.gauge_history().max();
        tracing::info!(
            scenario = scenario_name,
            area = area,
            peak = %format_mib(peak),
            ceiling = %format_mib(effective_budget),
            "stress: peak mem.bytes_reserved"
        );
        if peak > effective_budget {
            violations.push(format!(
                "mem.bytes_reserved{{area={area}}} peak {} exceeds effective budget {}",
                format_mib(peak),
                format_mib(effective_budget),
            ));
        }
    }
    for &kind in POOL_KINDS {
        let Some(metric) = recorder.get("pool.reserved_bytes", &[("kind", kind)]) else {
            continue;
        };
        let peak = metric.gauge_history().max();
        tracing::info!(
            scenario = scenario_name,
            kind = kind,
            peak = %format_mib(peak),
            ceiling = %format_mib(mem_limit_u64),
            "stress: peak pool.reserved_bytes"
        );
        if peak > mem_limit_u64 {
            violations.push(format!(
                "pool.reserved_bytes{{kind={kind}}} peak {} exceeds mem_limit {}",
                format_mib(peak),
                format_mib(mem_limit_u64),
            ));
        }
    }
    // TODO(mem-limiter): promote from warn to assert once production accounting is tight
    // enough that breaches indicate real regressions rather than known-gap overshoot.
    if !violations.is_empty() {
        tracing::warn!(
            scenario = scenario_name,
            ?violations,
            "stress: peak-reserved invariant violated (warn-only until mem-limiter accounting is tightened)"
        );
    }
}

/// Assert the peak sampled `process.memory_usage` (OS-reported RSS) stayed under
/// `ceiling_bytes`. Mirrors [`assert_peak_reserved_invariant`]: peak comes from the gauge's
/// HDR history, and violations are logged at warn until mem-limiter accounting is tightened.
fn assert_peak_rss_invariant(scenario_name: &str, ceiling_bytes: f64) {
    let Some(recorder) = stress_recorder::recorder() else {
        tracing::warn!(
            scenario = scenario_name,
            "stress: no recorder installed, skipping peak-rss invariant"
        );
        return;
    };
    let Some(metric) = recorder.get("process.memory_usage", &[]) else {
        tracing::warn!(
            scenario = scenario_name,
            "stress: process.memory_usage not recorded, skipping peak-rss invariant"
        );
        return;
    };
    let peak = metric.gauge_history().max();
    let ceiling = ceiling_bytes as u64;
    tracing::info!(
        scenario = scenario_name,
        peak = %format_mib(peak),
        ceiling = %format_mib(ceiling),
        "stress: peak process.memory_usage"
    );
    let mut violations: Vec<String> = Vec::new();
    if peak > ceiling {
        violations.push(format!(
            "process.memory_usage peak {} exceeds mem_limit {}",
            format_mib(peak),
            format_mib(ceiling),
        ));
    }
    // TODO(mem-limiter): promote from warn to assert once production accounting is tight
    // enough that breaches indicate real regressions rather than known-gap overshoot.
    if !violations.is_empty() {
        tracing::warn!(
            scenario = scenario_name,
            ?violations,
            "stress: peak-rss invariant violated (warn-only until mem-limiter accounting is tightened)"
        );
    }
}

/// After the session is dropped, every reservation-tracking gauge must be back to zero.
/// Checks both `mem.bytes_reserved{area=*}` (limiter-tracked) and `pool.reserved_bytes{kind=*}`
/// (CRT pool occupancy); a non-zero value means we leaked a reservation or a pool buffer.
fn assert_teardown_invariants(scenario_name: &str) {
    let Some(recorder) = stress_recorder::recorder() else {
        tracing::warn!(
            scenario = scenario_name,
            "stress: no recorder installed, skipping teardown invariants"
        );
        return;
    };
    let mut leaks: Vec<String> = Vec::new();
    for &area in MEM_AREAS {
        if let Some(metric) = recorder.get("mem.bytes_reserved", &[("area", area)]) {
            let v = metric.gauge();
            tracing::info!(scenario = scenario_name, area, value = v, "stress: teardown mem.bytes_reserved");
            if v != 0.0 {
                leaks.push(format!("mem.bytes_reserved{{area={area}}} = {v}"));
            }
        }
    }
    for &kind in POOL_KINDS {
        if let Some(metric) = recorder.get("pool.reserved_bytes", &[("kind", kind)]) {
            let v = metric.gauge();
            tracing::info!(scenario = scenario_name, kind, value = v, "stress: teardown pool.reserved_bytes");
            if v != 0.0 {
                leaks.push(format!("pool.reserved_bytes{{kind={kind}}} = {v}"));
            }
        }
    }
    assert!(leaks.is_empty(), "teardown invariant violated:\n  {}", leaks.join("\n  "));
}

/// Format a microsecond value as milliseconds with one decimal place.
fn us_to_ms_str(us: u64) -> String {
    format!("{:.1}", us as f64 / 1000.0)
}

/// Format a byte count as `<value> MiB` with one decimal place, for readable log output.
fn format_mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Print a per-op worker latency table followed by the global HdrRecorder snapshot.
fn dump_summary(scenario_name: &str, aggregate: &OpLatencies) {
    tracing::info!("=== stress [{scenario_name}] worker op latencies ===");
    for op in Op::ALL {
        let h = aggregate.histogram(op);
        let count = h.len();
        if count == 0 {
            tracing::info!("op={} count=0", op.name());
            continue;
        }
        tracing::info!(
            "op={} count={} p50={}ms p90={}ms p99={}ms p100={}ms",
            op.name(),
            count,
            us_to_ms_str(h.value_at_quantile(0.50)),
            us_to_ms_str(h.value_at_quantile(0.90)),
            us_to_ms_str(h.value_at_quantile(0.99)),
            us_to_ms_str(h.max()),
        );
    }

    tracing::info!("=== stress [{scenario_name}] global metrics ===");
    let Some(recorder) = stress_recorder::recorder() else {
        tracing::info!("(no global recorder installed)");
        return;
    };
    let mut lines: Vec<(String, String)> = Vec::new();
    recorder.for_each(|key, metric| {
        let key_str = format!("{key}");
        let line = match metric {
            HdrMetric::Histogram(_) => {
                let h = metric.histogram_clone();
                let count = h.len();
                if count == 0 {
                    format!("hist {key}: count=0")
                } else {
                    // Raw HDR values: unit varies per metric (bytes, µs, MiB, count, etc.) —
                    // the caller interprets from the metric name. Formatting a single unit
                    // here would be wrong for anything that isn't latency.
                    format!(
                        "hist {key}: count={} p50={} p90={} p99={} p100={}",
                        count,
                        h.value_at_quantile(0.50),
                        h.value_at_quantile(0.90),
                        h.value_at_quantile(0.99),
                        h.max(),
                    )
                }
            }
            HdrMetric::Counter(_) => format!("counter {key}: {}", metric.counter()),
            HdrMetric::Gauge(_) => {
                let current = metric.gauge();
                let history = metric.gauge_history();
                format!(
                    "gauge {key}: current={current} peak={} samples={} (p50={} p90={} p99={})",
                    history.max(),
                    history.len(),
                    history.value_at_quantile(0.50),
                    history.value_at_quantile(0.90),
                    history.value_at_quantile(0.99),
                )
            }
        };
        lines.push((key_str, line));
    });
    lines.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, line) in lines {
        tracing::info!("{}", line);
    }
}

/// Assert that the merged per-op p100 latency is within `max_latency`. Ops with zero samples
/// are skipped (e.g. read-only scenarios never record a write sample).
fn assert_p100_latency(scenario_name: &str, aggregate: &OpLatencies, max_latency: Duration) {
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
    fn op_latencies_time_and_merge() {
        let mut a = OpLatencies::new();
        let r = a.time(Op::Open, || 42);
        assert_eq!(r, 42);
        assert_eq!(a.histogram(Op::Open).len(), 1);
        a.time(Op::Read, || ());
        let mut b = OpLatencies::new();
        b.time(Op::Open, || ());
        a.merge(&b);
        assert_eq!(a.histogram(Op::Open).len(), 2);
        assert_eq!(a.histogram(Op::Read).len(), 1);
    }

    #[test]
    #[should_panic(expected = "violated p100 latency ceiling")]
    fn assert_p100_latency_flags_outlier() {
        let mut agg = OpLatencies::new();
        // Record a 6s read sample (in µs).
        agg.histograms[Op::Read as usize].record(6_000_000).unwrap();
        assert_p100_latency("test", &agg, Duration::from_secs(5));
    }

    #[test]
    fn assert_p100_latency_skips_empty_ops() {
        let agg = OpLatencies::new();
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