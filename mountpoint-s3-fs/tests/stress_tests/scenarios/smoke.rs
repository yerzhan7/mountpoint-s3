//! Smoke test: mount against real S3 with the 512 MiB memory limit and drive one worker
//! through the harness. Proves the module is wired up correctly.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::TestSessionConfig;
use crate::stress_tests::harness::{self, FileOp, Scenario, FileOpLatencies};

const KEY: &str = "smoke.bin";
const PAYLOAD_LEN: usize = 4096;

struct SmokeScenario;

impl Scenario for SmokeScenario {
    fn name(&self) -> &str {
        "stress_smoke"
    }

    fn num_workers(&self) -> usize {
        1
    }

    fn session_config(&self) -> TestSessionConfig {
        TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT)
    }

    fn setup(&self, session: &crate::common::fuse::TestSession) {
        let payload = vec![0xABu8; PAYLOAD_LEN];
        session.client().put_object(KEY, &payload).unwrap();
    }

    fn run_worker(
        &self,
        _worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut FileOpLatencies,
        stop: &AtomicBool,
    ) {
        let path = mount_path.join(KEY);
        while !stop.load(Ordering::Relaxed) {
            let mut buf = Vec::with_capacity(PAYLOAD_LEN);
            let mut f = latencies.time(FileOp::Open, || File::open(&path)).unwrap();
            let n = latencies.time(FileOp::Read, || f.read_to_end(&mut buf)).unwrap();
            latencies.time(FileOp::Close, || drop(f));
            progress.fetch_add(n as u64, Ordering::Relaxed);
        }
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn smoke() {
    harness::run(SmokeScenario);
}
