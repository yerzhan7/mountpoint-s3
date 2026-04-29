//! Idle worker: open a handle to a key in a shared object pool, read it fully so the
//! prefetcher reserves memory against the handle, hold the handle idle for 5–15s,
//! close. Exercises the prefetcher retaining reservations behind idle handles.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::stress_tests::harness::{FileOp, FileOpLatencies, Worker};
use crate::stress_tests::test_objects::SHARED_OBJECTS_PREFIX;

use super::churn::pick_index;
use super::common::SharedObjectPool;

/// A worker that opens a key from `pool`, reads it fully (so the prefetcher reserves
/// memory against the handle), holds the handle idle for 5–15s, then closes and
/// re-opens.
pub struct Idle {
    pub pool: SharedObjectPool,
}

impl Worker for Idle {
    fn kind(&self) -> &'static str {
        "idle"
    }

    fn shared_objects(&self) -> Vec<(String, usize)> {
        self.pool.manifest()
    }

    fn run(
        &self,
        instance: usize,
        mount_path: &Path,
        progress: &AtomicU64,
        latencies: &mut FileOpLatencies,
        stop: &AtomicBool,
    ) {
        let mut buf = vec![0u8; self.pool.size];
        let mut iter: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            iter += 1;
            let path = mount_path
                .join(SHARED_OBJECTS_PREFIX)
                .join(self.pool.key(pick_index(iter, instance, self.pool.count)));
            idle_cycle(&path, &mut buf, iter, progress, latencies, stop);
        }
    }
}

/// One idle-worker iteration: open, read the object fully (so the prefetcher reserves
/// memory against this handle), hold idle for 5-15s, then close.
fn idle_cycle(
    path: &Path,
    buf: &mut [u8],
    iter: u64,
    progress: &AtomicU64,
    latencies: &mut FileOpLatencies,
    stop: &AtomicBool,
) {
    let mut file = latencies
        .time(FileOp::Open, || File::open(path))
        .unwrap_or_else(|e| panic!("idle: open of {path:?} failed: {e:?}"));
    progress.fetch_add(1, Ordering::Relaxed);
    let n = latencies
        .time(FileOp::Read, || file.read(buf))
        .unwrap_or_else(|e| panic!("idle: read of {path:?} failed: {e:?}"));
    progress.fetch_add(n as u64, Ordering::Relaxed);

    let deadline = Instant::now() + Duration::from_secs(5 + (iter % 11)); // 5..=15 s
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(500));
    }

    latencies.time(FileOp::CloseRead, || drop(file));
    progress.fetch_add(1, Ordering::Relaxed);
}
