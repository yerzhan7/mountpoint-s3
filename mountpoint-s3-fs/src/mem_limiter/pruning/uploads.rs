//! Strategy A — wait for in-flight uploads (spec §5.3).

use std::sync::Arc;

use super::PoolPruneOps;
use super::strategy::{PruningResult, PruningStrategy};

/// Reports [`PruningResult::Acted`] while any upload buffer
/// (`BufferKind::PutObject` or `BufferKind::Append`) is reserved.
/// An in-flight upload will eventually complete and release its
/// buffer, so "waiting" is the correct pruning action here — spec §5.6
/// forbids the engine from cancelling uploads.
pub(crate) struct UploadsStrategy {
    pool: Arc<dyn PoolPruneOps>,
}

impl UploadsStrategy {
    pub(crate) fn new(pool: Arc<dyn PoolPruneOps>) -> Self {
        Self { pool }
    }
}

impl PruningStrategy for UploadsStrategy {
    fn run(&self) -> PruningResult {
        if self.pool.reserved_upload_bytes() > 0 {
            PruningResult::Acted
        } else {
            PruningResult::Skipped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem_limiter::pruning::MockPoolPruneOps;

    #[test]
    fn acts_when_upload_bytes_nonzero() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_reserved_upload_bytes().times(1).return_const(4096_u64);
        let strategy = UploadsStrategy::new(Arc::new(pool));

        assert_eq!(strategy.run(), PruningResult::Acted);
    }

    #[test]
    fn skips_when_zero() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_reserved_upload_bytes().times(1).return_const(0_u64);
        let strategy = UploadsStrategy::new(Arc::new(pool));

        assert_eq!(strategy.run(), PruningResult::Skipped);
    }
}
