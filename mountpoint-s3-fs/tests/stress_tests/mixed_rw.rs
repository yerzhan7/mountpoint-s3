//! `mixed_rw`: 16 readers + 24 writers sharing the same session under the 512 MiB memory
//! limit. Targets the read/write starvation risk flagged by the Memory Limiter Plan — the
//! per-worker watchdog guarantees that both roles make forward progress.
//!
//! Worker sizing: 24 writers × 8 MiB part-size = 192 MiB upload reservations, leaving ample
//! headroom (~320 MiB) for the 16 readers' prefetch windows.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;
use mountpoint_s3_fs::s3::S3Path;

use crate::common::fuse::{TestSession, TestSessionConfig};
use crate::stress_tests::harness::{self, Op, Scenario, WorkerRecorder};
use crate::stress_tests::test_objects::{self, READ_OBJECT_KEY};

const NUM_READERS: usize = 16;
const NUM_WRITERS: usize = 24;
const READ_CHUNK: usize = 8 * 1024 * 1024; // 8 MiB — matches default part size
const WRITE_CHUNK: usize = 8 * 1024 * 1024; // 8 MiB — matches default part size
const WRITE_OBJECT_SIZE: usize = 100 * 1024 * 1024; // 100 MiB

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

    fn s3_path_override(&self) -> Option<S3Path> {
        // Mount at the shared stress-fixtures prefix so readers can see the shared 1 GiB
        // test object. Writers must use their own key namespace so they cannot collide with
        // the shared read object.
        Some(test_objects::shared_s3_path())
    }

    fn setup(&self, _session: &TestSession) {
        test_objects::ensure_read_object();
    }

    fn run_worker(
        &self,
        worker_id: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        recorder: &mut WorkerRecorder,
        stop: &AtomicBool,
    ) {
        if worker_id < NUM_READERS {
            reader_loop(worker_id, mount_path, progress, recorder, stop);
        } else {
            writer_loop(worker_id - NUM_READERS, mount_path, progress, recorder, stop);
        }
    }
}

fn reader_loop(
    reader_id: usize,
    mount_path: &Path,
    progress: &AtomicU64,
    recorder: &mut WorkerRecorder,
    stop: &AtomicBool,
) {
    let path = mount_path.join(READ_OBJECT_KEY);
    let mut buf = vec![0u8; READ_CHUNK];
    while !stop.load(Ordering::Relaxed) {
        let mut file = recorder
            .time(Op::Open, || File::open(&path))
            .unwrap_or_else(|e| {
                panic!("mixed_rw: reader {reader_id}: open of {path:?} failed: {e:?}");
            });
        // Count every successful open as progress too.
        progress.fetch_add(1, Ordering::Relaxed);
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            let n = recorder
                .time(Op::Read, || file.read(&mut buf))
                .unwrap_or_else(|e| {
                    panic!("mixed_rw: reader {reader_id}: read of {path:?} failed: {e:?}");
                });
            if n == 0 {
                break;
            }
            progress.fetch_add(n as u64, Ordering::Relaxed);
        }
        recorder.time(Op::Close, || drop(file));
    }
}

fn writer_loop(
    writer_id: usize,
    mount_path: &Path,
    progress: &AtomicU64,
    recorder: &mut WorkerRecorder,
    stop: &AtomicBool,
) {
    let chunk = vec![0xC3u8; WRITE_CHUNK];
    let mut iter: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        iter += 1;
        // Namespace writer keys so they cannot collide with shared test objects.
        let key = format!("mixed_rw_ephemeral_w{writer_id:03}_i{iter:06}.bin");
        let path = mount_path.join(&key);

        let mut file = recorder
            .time(Op::Open, || File::create(&path))
            .unwrap_or_else(|e| {
                panic!("mixed_rw: writer {writer_id}: create failed: {e:?}");
            });
        progress.fetch_add(1, Ordering::Relaxed);

        let mut written = 0usize;
        while written < WRITE_OBJECT_SIZE && !stop.load(Ordering::Relaxed) {
            let n = (WRITE_OBJECT_SIZE - written).min(WRITE_CHUNK);
            recorder
                .time(Op::Write, || file.write_all(&chunk[..n]))
                .unwrap_or_else(|e| {
                    panic!("mixed_rw: writer {writer_id}: write failed: {e:?}");
                });
            written += n;
            progress.fetch_add(n as u64, Ordering::Relaxed);
        }
        recorder.time(Op::Close, || drop(file));
        progress.fetch_add(1, Ordering::Relaxed);
        let _ = std::fs::remove_file(&path);
    }
}

#[test]
#[ignore = "stress test; run with --run-ignored only"]
fn mixed_rw() {
    harness::run(MixedRw);
}
