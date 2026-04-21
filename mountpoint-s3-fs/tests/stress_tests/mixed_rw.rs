//! `mixed_rw`: 16 readers + 32 writers sharing the same session under the 512 MiB memory
//! limit. Targets the read/write starvation risk flagged by the Memory Limiter Plan — the
//! per-worker watchdog guarantees that both roles make forward progress.

use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, Scenario};

const NUM_READERS: usize = 16;
const NUM_WRITERS: usize = 32;
const FIXTURE_KEY: &str = "mixed_rw_fixture.bin";
const FIXTURE_SIZE: usize = 1024 * 1024 * 1024; // 1 GiB
const READ_CHUNK: usize = 1024 * 1024; // 1 MiB
const WRITE_CHUNK: usize = 1024 * 1024; // 1 MiB
const WRITE_OBJECT_SIZE: usize = 100 * 1024 * 1024; // 100 MiB
const ENOMEM_BACKOFF: Duration = Duration::from_millis(100);

struct MixedRw;

impl Scenario for MixedRw {
    fn name(&self) -> &str {
        "mixed_rw"
    }

    fn num_workers(&self) -> usize {
        NUM_READERS + NUM_WRITERS
    }

    fn session_config(&self) -> TestSessionConfig {
        TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT)
    }

    fn setup(&self, session: &TestSession) {
        let payload = vec![0xA5u8; FIXTURE_SIZE];
        tracing::info!(size = FIXTURE_SIZE, key = FIXTURE_KEY, "mixed_rw: uploading fixture");
        session.client().put_object(FIXTURE_KEY, &payload).unwrap();
    }

    fn run_worker(&self, worker_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
        if worker_id < NUM_READERS {
            reader_loop(worker_id, mount_path, progress, stop);
        } else {
            writer_loop(worker_id - NUM_READERS, mount_path, progress, stop);
        }
    }
}

fn reader_loop(reader_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
    let path = mount_path.join(FIXTURE_KEY);
    let mut buf = vec![0u8; READ_CHUNK];
    while !stop.load(Ordering::Relaxed) {
        let Ok(mut file) = File::open(&path) else {
            std::thread::sleep(ENOMEM_BACKOFF);
            continue;
        };
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    progress.fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!(reader_id, ?e, "mixed_rw: read failed, re-opening");
                    break;
                }
            }
        }
    }
}

fn writer_loop(writer_id: usize, mount_path: &Path, progress: &AtomicU64, stop: &AtomicBool) {
    let chunk = vec![0xC3u8; WRITE_CHUNK];
    let mut iter: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        iter += 1;
        let key = format!("mixed_w{writer_id:03}_i{iter:06}.bin");
        let path = mount_path.join(&key);

        let mut file = match File::create(&path) {
            Ok(f) => f,
            Err(e) if is_enomem(&e) => {
                tracing::debug!(writer_id, ?e, "mixed_rw: ENOMEM on open, backing off");
                std::thread::sleep(ENOMEM_BACKOFF);
                continue;
            }
            Err(e) => {
                tracing::warn!(writer_id, ?e, "mixed_rw: open failed, backing off");
                std::thread::sleep(ENOMEM_BACKOFF);
                continue;
            }
        };
        let mut written = 0usize;
        while written < WRITE_OBJECT_SIZE && !stop.load(Ordering::Relaxed) {
            let n = (WRITE_OBJECT_SIZE - written).min(WRITE_CHUNK);
            if let Err(e) = file.write_all(&chunk[..n]) {
                tracing::warn!(writer_id, ?e, "mixed_rw: write failed");
                break;
            }
            written += n;
            progress.fetch_add(n as u64, Ordering::Relaxed);
        }
        drop(file);
        let _ = std::fs::remove_file(&path);
    }
}

fn is_enomem(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(libc::ENOMEM) || e.kind() == ErrorKind::OutOfMemory
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn mixed_rw() {
    harness::run(MixedRw);
}
