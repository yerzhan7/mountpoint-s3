//! `sustained_reads`: 32 workers concurrently reading a ~1 GiB object front-to-back under
//! the 512 MiB memory limit. Exercises prefetch reservation, window growth, and pruning
//! interactions under sustained read pressure.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, Scenario};

const FIXTURE_KEY: &str = "sustained_reads.bin";
const FIXTURE_SIZE: usize = 1024 * 1024 * 1024; // 1 GiB
const READ_CHUNK: usize = 1024 * 1024; // 1 MiB
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

    fn setup(&self, session: &TestSession) {
        // Cheap deterministic payload — we don't verify content.
        let payload = vec![0xA5u8; FIXTURE_SIZE];
        tracing::info!(
            size = FIXTURE_SIZE,
            key = FIXTURE_KEY,
            "sustained_reads: uploading fixture"
        );
        session.client().put_object(FIXTURE_KEY, &payload).unwrap();
    }

    fn run_worker(&self, _worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
        let path = mount_path.join(FIXTURE_KEY);
        let mut buf = vec![0u8; READ_CHUNK];
        while !stop.load(Ordering::Relaxed) {
            let mut file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(?e, "sustained_reads: open failed, retrying");
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
            };
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                match file.read(&mut buf) {
                    Ok(0) => break, // EOF — re-open below
                    Ok(n) => {
                        progress.fetch_add(n as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::warn!(?e, "sustained_reads: read failed, re-opening");
                        break;
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn sustained_reads() {
    harness::run(SustainedReads);
}
