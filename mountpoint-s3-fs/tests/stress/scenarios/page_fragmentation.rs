//! `page_fragmentation`: a single worker that reproduces page-fragmentation amplification.
//!
//! A pool page holds 16 buffers and can only be trimmed when all 16 are free. The worker opens
//! 48 partial-write handles (filling 3 whole pages), closes all but one handle per page, then
//! holds those 3 survivors while reading a shared object in a loop. The result is 3 pages left
//! resident behind only 3 in-use buffers — a fragmentation tail that `trim()` cannot reclaim
//! because no page is fully empty. Runs entirely on one worker thread; the existing RSS/reserved
//! invariants observe the amplification at teardown.

use std::sync::Arc;

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::TestSessionConfig;
use crate::stress::harness::{self, Scenario, Worker, default_max_latency};
use crate::stress::workers::PageFragmenter;

#[test]
fn page_fragmentation() {
    let worker: Arc<dyn Worker> = Arc::new(PageFragmenter {
        scope: "page_fragmentation",
    });
    harness::run(Scenario {
        name: "page_fragmentation",
        session_config: TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT),
        workers: vec![worker],
        max_latency: default_max_latency,
    });
}
