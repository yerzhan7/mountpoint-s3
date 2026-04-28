//! Shared worker-loop helpers used by multiple stress scenarios.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::stress_tests::harness::{FileOp, FileOpLatencies};

/// Open `path`, read it front-to-back into `buf`, close. Increments `progress` on every
/// successful open and every byte read. Returns on `stop`. Panics (with `scope` in the
/// message) on any I/O error.
pub fn read_to_eof_once(
    scope: &str,
    path: &Path,
    buf: &mut [u8],
    progress: &AtomicU64,
    latencies: &mut FileOpLatencies,
    stop: &AtomicBool,
) {
    let mut file = latencies
        .time(FileOp::Open, || File::open(path))
        .unwrap_or_else(|e| panic!("{scope}: open of {path:?} failed: {e:?}"));
    progress.fetch_add(1, Ordering::Relaxed);
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let n = latencies
            .time(FileOp::Read, || file.read(buf))
            .unwrap_or_else(|e| panic!("{scope}: read of {path:?} failed: {e:?}"));
        if n == 0 {
            break;
        }
        progress.fetch_add(n as u64, Ordering::Relaxed);
    }
    latencies.time(FileOp::CloseRead, || drop(file));
}
