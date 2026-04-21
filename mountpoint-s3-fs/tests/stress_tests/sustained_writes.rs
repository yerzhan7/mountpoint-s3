//! `sustained_writes`: 64 workers concurrently writing 50-200 MiB objects under the 512 MiB
//! memory limit. Exercises upload buffer allocation, CRT pool reservation under pressure,
//! and (once admission control lands) ENOMEM-on-open tolerance.

use std::fs::File;
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::TestSessionConfig;
use crate::stress_tests::harness::{self, Scenario};

const NUM_WORKERS: usize = 64;
const WRITE_CHUNK: usize = 1024 * 1024; // 1 MiB
const MIN_OBJECT_SIZE: usize = 50 * 1024 * 1024;
const MAX_OBJECT_SIZE: usize = 200 * 1024 * 1024;
const ENOMEM_BACKOFF: Duration = Duration::from_millis(100);

struct SustainedWrites;

impl Scenario for SustainedWrites {
    fn name(&self) -> &str {
        "sustained_writes"
    }

    fn num_workers(&self) -> usize {
        NUM_WORKERS
    }

    fn session_config(&self) -> TestSessionConfig {
        TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT)
    }

    fn run_worker(&self, worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
        let mut iter: u64 = 0;
        let chunk = vec![0xC3u8; WRITE_CHUNK];
        while !stop.load(Ordering::Relaxed) {
            iter += 1;
            let size = next_size(iter);
            let key = format!("w{worker_id:03}_i{iter:06}.bin");
            let path = mount_path.join(&key);

            let mut file = match File::create(&path) {
                Ok(f) => f,
                Err(e) if is_enomem(&e) => {
                    tracing::debug!(worker_id, ?e, "sustained_writes: ENOMEM on open, backing off");
                    std::thread::sleep(ENOMEM_BACKOFF);
                    continue;
                }
                Err(e) => {
                    tracing::warn!(worker_id, ?e, "sustained_writes: open failed, backing off");
                    std::thread::sleep(ENOMEM_BACKOFF);
                    continue;
                }
            };

            let mut written = 0usize;
            while written < size && !stop.load(Ordering::Relaxed) {
                let n = (size - written).min(WRITE_CHUNK);
                match file.write_all(&chunk[..n]) {
                    Ok(()) => {
                        written += n;
                        progress.fetch_add(n as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::warn!(worker_id, ?e, "sustained_writes: write failed");
                        break;
                    }
                }
            }
            drop(file);
            // Best-effort cleanup so we don't accumulate thousands of objects during long runs.
            let _ = std::fs::remove_file(&path);
        }
    }
}

fn is_enomem(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(libc::ENOMEM) || e.kind() == ErrorKind::OutOfMemory
}

/// Deterministic pseudo-random size in `[MIN_OBJECT_SIZE, MAX_OBJECT_SIZE]`.
fn next_size(seed: u64) -> usize {
    let range = MAX_OBJECT_SIZE - MIN_OBJECT_SIZE;
    MIN_OBJECT_SIZE + (seed.wrapping_mul(2_654_435_761) as usize) % (range + 1)
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn sustained_writes() {
    harness::run(SustainedWrites);
}
