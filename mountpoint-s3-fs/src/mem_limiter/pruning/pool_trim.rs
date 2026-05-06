//! Strategy C — pool trim (spec §5.3).

use std::sync::Arc;

use super::strategy::{PruningResult, PruningStrategy};
use super::{AllocationQueueOps, PoolPruneOps};

/// Asks the pool to release idle (secondary) buffers. On success,
/// immediately wakes the allocation queue so pending requests can be
/// satisfied from the newly freed memory, and returns
/// [`PruningResult::Acted`]. Emits the `mem.pruning_trims` counter on
/// the `Acted` path.
pub(crate) struct PoolTrimStrategy {
    pool: Arc<dyn PoolPruneOps>,
    queue: Arc<dyn AllocationQueueOps>,
}

impl PoolTrimStrategy {
    pub(crate) fn new(pool: Arc<dyn PoolPruneOps>, queue: Arc<dyn AllocationQueueOps>) -> Self {
        Self { pool, queue }
    }
}

impl PruningStrategy for PoolTrimStrategy {
    fn run(&self) -> PruningResult {
        if self.pool.trim() {
            metrics::counter!("mem.pruning_trims").increment(1);
            self.queue.try_wake_pending();
            PruningResult::Acted
        } else {
            PruningResult::Skipped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem_limiter::pruning::{MockAllocationQueueOps, MockPoolPruneOps};

    #[test]
    fn acts_and_wakes_when_trim_succeeds() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_trim().times(1).return_const(true);
        let mut queue = MockAllocationQueueOps::new();
        queue.expect_try_wake_pending().times(1).return_const(());
        let strategy = PoolTrimStrategy::new(Arc::new(pool), Arc::new(queue));

        assert_eq!(strategy.run(), PruningResult::Acted);
    }

    #[test]
    fn skips_and_does_not_wake_when_trim_fails() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_trim().times(1).return_const(false);
        // No expectation on try_wake_pending — mockall panics on
        // unexpected calls, so this asserts it is not invoked.
        let queue = MockAllocationQueueOps::new();
        let strategy = PoolTrimStrategy::new(Arc::new(pool), Arc::new(queue));

        assert_eq!(strategy.run(), PruningResult::Skipped);
    }
}
