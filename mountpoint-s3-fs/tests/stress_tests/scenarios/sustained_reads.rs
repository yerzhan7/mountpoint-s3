//! `sustained_reads`: 32 workers concurrently reading a ~100 GiB shared test object front-to-back
//! under the 512 MiB memory limit. Exercises prefetch reservation, window growth, and pruning
//! interactions under sustained read pressure.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, FileOpLatencies, Scenario};
use crate::stress_tests::scenarios::common::read_to_eof_once;
use crate::stress_tests::test_objects::{self, LARGE_OBJECT_KEY, LARGE_OBJECT_SIZE, SHARED_MOUNT_PREFIX};

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

    fn setup(&self, _session: &TestSession) {
        test_objects::ensure_shared_objects(&[(LARGE_OBJECT_KEY, LARGE_OBJECT_SIZE)]);
    }

    fn run_worker(
        &self,
        _worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut FileOpLatencies,
        stop: &AtomicBool,
    ) {
        let path = mount_path.join(SHARED_MOUNT_PREFIX).join(LARGE_OBJECT_KEY);
        let mut buf = vec![0u8; READ_CHUNK];
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            read_to_eof_once("sustained_reads", &path, &mut buf, progress, latencies, stop);
        }
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn sustained_reads() {
    harness::run(SustainedReads);
}
