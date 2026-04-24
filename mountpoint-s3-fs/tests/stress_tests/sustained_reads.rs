//! `sustained_reads`: 32 workers concurrently reading a ~1 GiB shared test object front-to-back
//! under the 512 MiB memory limit. Exercises prefetch reservation, window growth, and pruning
//! interactions under sustained read pressure.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;
use mountpoint_s3_fs::s3::S3Path;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, Op, Scenario, OpLatencies};
use crate::stress_tests::test_objects::{self, READ_OBJECT_KEY};

const READ_CHUNK: usize = 8 * 1024 * 1024; // 8 MiB — matches default part size
const NUM_WORKERS: usize = 32;

struct SustainedReads;

impl Scenario for SustainedReads {
    fn name(&self) -> &str {
        "sustained_reads"
    }

    fn num_workers(&self) -> usize {
        NUM_WORKERS
    }

    fn session_config(&self) -> TestSessionConfig {
        TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT)
    }

    fn s3_path_override(&self) -> Option<S3Path> {
        Some(test_objects::shared_s3_path())
    }

    fn setup(&self, _session: &TestSession) {
        test_objects::ensure_read_object();
    }

    fn run_worker(
        &self,
        _worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut OpLatencies,
        stop: &AtomicBool,
    ) {
        let path = mount_path.join(READ_OBJECT_KEY);
        let mut buf = vec![0u8; READ_CHUNK];
        while !stop.load(Ordering::Relaxed) {
            let mut file = latencies
                .time(Op::Open, || File::open(&path))
                .unwrap_or_else(|e| {
                    panic!("sustained_reads: open of {path:?} failed: {e:?}");
                });
            // Count every successful open as progress too.
            progress.fetch_add(1, Ordering::Relaxed);
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let n = latencies
                    .time(Op::Read, || file.read(&mut buf))
                    .unwrap_or_else(|e| {
                        panic!("sustained_reads: read of {path:?} failed: {e:?}");
                    });
                if n == 0 {
                    break; // EOF — re-open
                }
                progress.fetch_add(n as u64, Ordering::Relaxed);
            }
            latencies.time(Op::Close, || drop(file));
        }
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn sustained_reads() {
    harness::run(SustainedReads);
}
