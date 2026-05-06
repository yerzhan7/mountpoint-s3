//! Strategy B — wait for in-progress reads (spec §5.3).

use std::sync::Arc;

use super::HandleRegistryOps;
use super::strategy::{PruningResult, PruningStrategy};

/// Reports [`PruningResult::Acted`] while any handle is in a FUSE read
/// and owns a live `RequestTask`. The S3 response for that read will
/// release a buffer once it lands, so the engine should defer to that
/// release rather than disrupt an in-progress read — spec §5.6 forbids
/// pruning handles whose `active_read_range.is_some()`.
pub(crate) struct ActiveReadsStrategy {
    registry: Arc<dyn HandleRegistryOps>,
}

impl ActiveReadsStrategy {
    pub(crate) fn new(registry: Arc<dyn HandleRegistryOps>) -> Self {
        Self { registry }
    }
}

impl PruningStrategy for ActiveReadsStrategy {
    fn run(&self) -> PruningResult {
        if self.registry.any_active_read_with_task() {
            PruningResult::Acted
        } else {
            PruningResult::Skipped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem_limiter::pruning::MockHandleRegistryOps;

    #[test]
    fn acts_when_any_handle_has_active_read_and_task() {
        let mut registry = MockHandleRegistryOps::new();
        registry.expect_any_active_read_with_task().times(1).return_const(true);
        let strategy = ActiveReadsStrategy::new(Arc::new(registry));

        assert_eq!(strategy.run(), PruningResult::Acted);
    }

    #[test]
    fn skips_otherwise() {
        let mut registry = MockHandleRegistryOps::new();
        registry.expect_any_active_read_with_task().times(1).return_const(false);
        let strategy = ActiveReadsStrategy::new(Arc::new(registry));

        assert_eq!(strategy.run(), PruningResult::Skipped);
    }
}
