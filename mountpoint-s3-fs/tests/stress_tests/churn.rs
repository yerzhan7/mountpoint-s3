//! `churn`: short-lived handles + idle-held handles under the 512 MiB memory limit. 48 workers
//! open a random fixture, read it fully, close; repeated. 8 additional workers open a handle
//! and stay idle for 5-15s before closing and re-opening. Targets lifecycle-related races and
//! creates natural pruning candidates.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, Scenario};

const NUM_SHORT_WORKERS: usize = 48;
const NUM_IDLE_WORKERS: usize = 8;
const NUM_FIXTURES: usize = 100;
const FIXTURE_SIZE: usize = 128 * 1024; // 128 KiB
const BACKOFF: Duration = Duration::from_millis(100);

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

    fn setup(&self, session: &TestSession) {
        tracing::info!(count = NUM_FIXTURES, size = FIXTURE_SIZE, "churn: uploading fixtures");
        let payload = vec![0x5Au8; FIXTURE_SIZE];
        for i in 0..NUM_FIXTURES {
            session.client().put_object(&fixture_key(i), &payload).unwrap();
        }
    }

    fn run_worker(&self, worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
        if worker_id < NUM_SHORT_WORKERS {
            short_loop(worker_id, mount_path, progress, stop);
        } else {
            idle_loop(worker_id - NUM_SHORT_WORKERS, mount_path, progress, stop);
        }
    }
}

fn fixture_key(i: usize) -> String {
    format!("churn/fixture_{i:04}.bin")
}

fn short_loop(worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
    let mut buf = vec![0u8; FIXTURE_SIZE];
    let mut iter: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        iter += 1;
        // Deterministic pseudo-random pick across fixtures.
        let idx = (iter.wrapping_mul(2_654_435_761).wrapping_add(worker_id as u64) as usize) % NUM_FIXTURES;
        let path = mount_path.join(fixture_key(idx));
        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(worker_id, ?e, "churn: open failed");
                std::thread::sleep(BACKOFF);
                continue;
            }
        };
        let mut read = 0usize;
        while read < FIXTURE_SIZE && !stop.load(Ordering::Relaxed) {
            match file.read(&mut buf[read..]) {
                Ok(0) => break,
                Ok(n) => {
                    read += n;
                    progress.fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!(worker_id, ?e, "churn: read failed");
                    break;
                }
            }
        }
    }
}

/// Open a handle, hold it idle for 5-15s, close, re-open. Progress is incremented once per
/// open/close cycle so the watchdog sees forward progress even though we're not reading.
fn idle_loop(worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
    let mut iter: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        iter += 1;
        let idx = (iter.wrapping_mul(2_246_822_519).wrapping_add(worker_id as u64) as usize) % NUM_FIXTURES;
        let path = mount_path.join(fixture_key(idx));
        let Ok(_file) = File::open(&path) else {
            std::thread::sleep(BACKOFF);
            continue;
        };

        let idle_secs = 5 + (iter % 11); // 5..=15
        let idle_deadline = Instant::now() + Duration::from_secs(idle_secs);
        while Instant::now() < idle_deadline && !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(500));
        }
        // Increment progress on close so the watchdog doesn't treat a 15s hold as a stall.
        // (scenario.max_idle_duration() defaults to 30s which covers the 15s upper bound.)
        progress.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn churn() {
    harness::run(Churn);
}
