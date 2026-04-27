//! `sustained_writes`: 48 workers concurrently writing 100 MiB objects under the 512 MiB
//! memory limit. Sized so the per-part upload reservations stay under the memory budget
//! (48 workers × 8 MiB part-size = 384 MiB < 512 MiB).

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::TestSessionConfig;
use crate::stress_tests::harness::{self, FileOp, FileOpLatencies, Scenario};
use crate::stress_tests::test_objects;

const NUM_WORKERS: usize = 48;
const WRITE_CHUNK: usize = 8 * 1024 * 1024; // 8 MiB — matches default part size
const OBJECT_SIZE: usize = 100 * 1024 * 1024; // 100 MiB

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
            // Flat, per-run-nonced key so writes never collide with shared test objects,
            // leftover objects from prior runs, or concurrent runs.
            let key = test_objects::ephemeral_key("sustained_writes", &format!("w{worker_id:03}_i{iter:06}.bin"));
            let path = mount_path.join(&key);

            let mut file = latencies
                .time(FileOp::Open, || File::create(&path))
                .unwrap_or_else(|e| {
                    panic!("sustained_writes: worker {worker_id}: create failed: {e:?}");
                });
            progress.fetch_add(1, Ordering::Relaxed);

            let mut written = 0usize;
            while written < OBJECT_SIZE && !stop.load(Ordering::Relaxed) {
                let n = (OBJECT_SIZE - written).min(WRITE_CHUNK);
                latencies
                    .time(FileOp::Write, || file.write_all(&chunk[..n]))
                    .unwrap_or_else(|e| {
                        panic!("sustained_writes: worker {worker_id}: write failed: {e:?}");
                    });
                written += n;
                progress.fetch_add(n as u64, Ordering::Relaxed);
            }
            // `sync_all` (not implicit `drop`) so MPU-complete errors surface — `Drop for File`
            // swallows the `close(2)` return value.
            latencies
                .time(FileOp::CloseWrite, || file.sync_all())
                .unwrap_or_else(|e| {
                    panic!("sustained_writes: worker {worker_id}: sync_all failed: {e:?}");
                });
            drop(file);
            progress.fetch_add(1, Ordering::Relaxed);
            // Best-effort cleanup so we don't accumulate thousands of objects during long runs.
            let _ = std::fs::remove_file(&path);
        }
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn sustained_writes() {
    harness::run(SustainedWrites);
}
