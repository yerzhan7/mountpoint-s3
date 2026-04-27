//! `sustained_writes`: 48 workers concurrently writing 50-200 MiB objects under the 512 MiB
//! memory limit. Sized so the per-part upload reservations stay under the memory budget
//! (48 workers × 8 MiB part-size = 384 MiB < 512 MiB).

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::TestSessionConfig;
use crate::stress_tests::harness::{self, FileOp, Scenario, FileOpLatencies};

const NUM_WORKERS: usize = 48;
const WRITE_CHUNK: usize = 8 * 1024 * 1024; // 8 MiB — matches default part size
const MIN_OBJECT_SIZE: usize = 50 * 1024 * 1024;
const MAX_OBJECT_SIZE: usize = 200 * 1024 * 1024;

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

    fn run_worker(
        &self,
        worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut FileOpLatencies,
        stop: &AtomicBool,
    ) {
        let mut iter: u64 = 0;
        let chunk = vec![0xC3u8; WRITE_CHUNK];
        while !stop.load(Ordering::Relaxed) {
            iter += 1;
            let size = next_size(iter);
            let key = format!("w{worker_id:03}_i{iter:06}.bin");
            let path = mount_path.join(&key);

            let mut file = latencies
                .time(FileOp::Open, || File::create(&path))
                .unwrap_or_else(|e| {
                    panic!("sustained_writes: worker {worker_id}: create failed: {e:?}");
                });
            progress.fetch_add(1, Ordering::Relaxed);

            let mut written = 0usize;
            while written < size && !stop.load(Ordering::Relaxed) {
                let n = (size - written).min(WRITE_CHUNK);
                latencies
                    .time(FileOp::Write, || file.write_all(&chunk[..n]))
                    .unwrap_or_else(|e| {
                        panic!("sustained_writes: worker {worker_id}: write failed: {e:?}");
                    });
                written += n;
                progress.fetch_add(n as u64, Ordering::Relaxed);
            }
            latencies.time(FileOp::CloseWrite, || drop(file));
            progress.fetch_add(1, Ordering::Relaxed);
            // Best-effort cleanup so we don't accumulate thousands of objects during long runs.
            let _ = std::fs::remove_file(&path);
        }
    }
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
