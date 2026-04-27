//! `idle_and_churn`: short-lived handles + idle-held handles under the 512 MiB memory limit.
//!
//! 48 churn workers open a random shared small object, read it fully, close; repeated.
//! 8 idle workers open a handle, read the small object fully (triggering a prefetcher
//! reservation), then hold the handle idle for 5-15s before closing and re-opening.
//!
//! Targets handle-lifecycle races and creates natural pruning candidates for future pruning
//! strategies.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;
use mountpoint_s3_fs::s3::S3Path;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, FileOp, FileOpLatencies, Scenario};
use crate::stress_tests::scenarios::common::read_to_eof_once;
use crate::stress_tests::test_objects::{self, SMALL_SET_COUNT, SMALL_SET_SIZE, small_object_key};

const NUM_CHURN_WORKERS: usize = 48;
const NUM_IDLE_WORKERS: usize = 8;

struct IdleAndChurn;

impl Scenario for IdleAndChurn {
    fn name(&self) -> &str {
        "idle_and_churn"
    }

    fn num_workers(&self) -> usize {
        NUM_CHURN_WORKERS + NUM_IDLE_WORKERS
    }

    fn session_config(&self) -> TestSessionConfig {
        TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT)
    }

    fn s3_path_override(&self) -> Option<S3Path> {
        Some(test_objects::shared_s3_path())
    }

    fn setup(&self, _session: &TestSession) {
        let objs: Vec<(String, usize)> = (0..SMALL_SET_COUNT)
            .map(|i| (small_object_key(i), SMALL_SET_SIZE))
            .collect();
        let refs: Vec<(&str, usize)> = objs.iter().map(|(k, s)| (k.as_str(), *s)).collect();
        test_objects::ensure_shared_objects(&refs);
    }

    fn run_worker(
        &self,
        worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut FileOpLatencies,
        stop: &AtomicBool,
    ) {
        let mut buf = vec![0u8; SMALL_SET_SIZE];
        let mut iter: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            iter += 1;
            let path = mount_path.join(small_object_key(pick_index(iter, worker_id)));
            if worker_id < NUM_CHURN_WORKERS {
                read_to_eof_once("idle_and_churn churn", &path, &mut buf, progress, latencies, stop);
            } else {
                idle_cycle(&path, &mut buf, iter, progress, latencies, stop);
            }
        }
    }
}

/// One idle-worker iteration: open, read the small object fully (so the prefetcher reserves
/// memory against this handle), hold idle for 5-15s, then close.
fn idle_cycle(
    path: &Path,
    buf: &mut [u8],
    iter: u64,
    progress: &AtomicU64,
    latencies: &mut FileOpLatencies,
    stop: &AtomicBool,
) {
    let mut file = latencies
        .time(FileOp::Open, || File::open(path))
        .unwrap_or_else(|e| panic!("idle_and_churn idle: open of {path:?} failed: {e:?}"));
    progress.fetch_add(1, Ordering::Relaxed);
    let n = latencies
        .time(FileOp::Read, || file.read(buf))
        .unwrap_or_else(|e| panic!("idle_and_churn idle: read of {path:?} failed: {e:?}"));
    progress.fetch_add(n as u64, Ordering::Relaxed);

    let deadline = Instant::now() + Duration::from_secs(5 + (iter % 11)); // 5..=15 s
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(500));
    }

    latencies.time(FileOp::CloseRead, || drop(file));
    progress.fetch_add(1, Ordering::Relaxed);
}

/// Deterministic pseudo-random pick across the shared small-object set.
fn pick_index(iter: u64, worker_id: usize) -> usize {
    (iter.wrapping_mul(2_654_435_761).wrapping_add(worker_id as u64) as usize) % SMALL_SET_COUNT
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn idle_and_churn() {
    harness::run(IdleAndChurn);
}
