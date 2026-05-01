//! `idle_and_churn`: 48 churn workers + 8 idle workers against a small-object pool under
//! the 512 MiB memory limit. Exercises short-lived handles concurrently with handles held
//! idle long enough to retain prefetcher reservations.

use std::iter::{chain, repeat_n};
use std::sync::Arc;

use mountpoint_s3_fs::mem_limiter::MINIMUM_MEM_LIMIT;

use crate::common::fuse::TestSessionConfig;
use crate::harness::{self, Scenario, Worker, default_max_latency};
use crate::workers::{Churn, Idle, SMALL_OBJECT_POOL};

const NUM_CHURN_WORKERS: usize = 48;
const NUM_IDLE_WORKERS: usize = 8;

#[test]
fn idle_and_churn() {
    let churn: Arc<dyn Worker> = Arc::new(Churn { pool: SMALL_OBJECT_POOL });
    let idle: Arc<dyn Worker> = Arc::new(Idle { pool: SMALL_OBJECT_POOL });
    let workers = chain(
        repeat_n(churn, NUM_CHURN_WORKERS),
        repeat_n(idle, NUM_IDLE_WORKERS),
    )
    .collect();
    harness::run(Scenario {
        name: "idle_and_churn",
        session_config: TestSessionConfig::default().with_mem_limit(MINIMUM_MEM_LIMIT),
        workers,
        max_latency: default_max_latency,
    });
}
