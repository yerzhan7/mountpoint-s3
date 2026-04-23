//! `churn`: short-lived handles + idle-held handles under the 512 MiB memory limit. 48 workers
//! open a random shared test object, read it fully, close; repeated. 8 additional workers open
//! a handle, read 1 MiB (to trigger a prefetcher reservation), then stay idle for 5-15s before
//! closing and re-opening. Targets lifecycle-related races and creates natural pruning
//! candidates for the upcoming strategy-D pruner.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;
use mountpoint_s3_fs::s3::S3Path;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, Op, Scenario, WorkerRecorder};
use crate::stress_tests::test_objects::{self, SMALL_SET_COUNT, SMALL_SET_SIZE, small_object_key};

const NUM_SHORT_WORKERS: usize = 48;
const NUM_IDLE_WORKERS: usize = 8;
const IDLE_READ_BYTES: usize = 1024 * 1024; // 1 MiB

struct Churn;

impl Scenario for Churn {
    fn name(&self) -> &str {
        "churn"
    }

    fn num_workers(&self) -> usize {
        NUM_SHORT_WORKERS + NUM_IDLE_WORKERS
    }

    fn session_config(&self) -> TestSessionConfig {
        TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT)
    }

    fn s3_path_override(&self) -> Option<S3Path> {
        Some(test_objects::shared_s3_path())
    }

    fn setup(&self, _session: &TestSession) {
        test_objects::ensure_small_set();
    }

    fn run_worker(
        &self,
        worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        recorder: &mut WorkerRecorder,
        stop: &AtomicBool,
    ) {
        if worker_id < NUM_SHORT_WORKERS {
            short_loop(worker_id, mount_path, progress, recorder, stop);
        } else {
            idle_loop(worker_id - NUM_SHORT_WORKERS, mount_path, progress, recorder, stop);
        }
    }
}

fn short_loop(
    worker_id: usize,
    mount_path: &Path,
    progress: &AtomicU64,
    recorder: &mut WorkerRecorder,
    stop: &AtomicBool,
) {
    let mut buf = vec![0u8; SMALL_SET_SIZE];
    let mut iter: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        iter += 1;
        // Deterministic pseudo-random pick across the shared small-object set.
        let idx = (iter.wrapping_mul(2_654_435_761).wrapping_add(worker_id as u64) as usize) % SMALL_SET_COUNT;
        let path = mount_path.join(small_object_key(idx));
        let mut file = recorder
            .time(Op::Open, || File::open(&path))
            .unwrap_or_else(|e| {
                panic!("churn: worker {worker_id}: open of {path:?} failed: {e:?}");
            });
        progress.fetch_add(1, Ordering::Relaxed);
        let mut read = 0usize;
        while read < SMALL_SET_SIZE && !stop.load(Ordering::Relaxed) {
            let n = recorder
                .time(Op::Read, || file.read(&mut buf[read..]))
                .unwrap_or_else(|e| {
                    panic!("churn: worker {worker_id}: read of {path:?} failed: {e:?}");
                });
            if n == 0 {
                break;
            }
            read += n;
            progress.fetch_add(n as u64, Ordering::Relaxed);
        }
        recorder.time(Op::Close, || drop(file));
    }
}

/// Open a handle, read 1 MiB to trigger a prefetcher reservation, hold the handle idle for
/// 5-15s, then close and re-open. Progress is incremented on every open, every byte read, and
/// every close, so the watchdog sees forward progress even though we're mostly idle.
fn idle_loop(
    worker_id: usize,
    mount_path: &Path,
    progress: &AtomicU64,
    recorder: &mut WorkerRecorder,
    stop: &AtomicBool,
) {
    let mut buf = vec![0u8; IDLE_READ_BYTES];
    let mut iter: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        iter += 1;
        let idx = (iter.wrapping_mul(2_246_822_519).wrapping_add(worker_id as u64) as usize) % SMALL_SET_COUNT;
        let path = mount_path.join(small_object_key(idx));
        let mut file = recorder
            .time(Op::Open, || File::open(&path))
            .unwrap_or_else(|e| {
                panic!("churn: idle worker {worker_id}: open of {path:?} failed: {e:?}");
            });
        progress.fetch_add(1, Ordering::Relaxed);

        // Drive at least one read so the prefetcher reserves memory against this handle; the
        // handle then becomes a valid pruning candidate while it sits idle below.
        let mut read = 0usize;
        while read < IDLE_READ_BYTES.min(SMALL_SET_SIZE) && !stop.load(Ordering::Relaxed) {
            let n = recorder
                .time(Op::Read, || file.read(&mut buf[read..]))
                .unwrap_or_else(|e| {
                    panic!("churn: idle worker {worker_id}: read of {path:?} failed: {e:?}");
                });
            if n == 0 {
                break;
            }
            read += n;
            progress.fetch_add(n as u64, Ordering::Relaxed);
        }

        let idle_secs = 5 + (iter % 11); // 5..=15
        let idle_deadline = Instant::now() + Duration::from_secs(idle_secs);
        while Instant::now() < idle_deadline && !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(500));
        }
        recorder.time(Op::Close, || drop(file));
        progress.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn churn() {
    harness::run(Churn);
}
