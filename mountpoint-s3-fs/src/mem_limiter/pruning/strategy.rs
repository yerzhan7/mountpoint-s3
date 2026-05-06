//! [`PruningStrategy`] trait and the shared [`PruningResult`] enum.
//!
//! Each concrete strategy lives in its own sibling module
//! ([`uploads`](super::uploads), [`active_reads`](super::active_reads),
//! [`pool_trim`](super::pool_trim)). The
//! [`PruningEngine`](super::PruningEngine) rotates through a vector of
//! trait objects, so adding a new strategy (WI-6 speculative-prefetch
//! reset, for example) means writing a new module and pushing one
//! entry onto the vec — no changes to the engine's control flow.

/// Result of a single pruning strategy invocation or a full pruning
/// round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PruningResult {
    /// The strategy took an action that will eventually release memory
    /// (for example, it observed an in-flight upload that must
    /// complete, or it freed buffers via
    /// [`PoolPruneOps::trim`](super::PoolPruneOps::trim)).
    Acted,
    /// The strategy had nothing to do and the caller should try the
    /// next one.
    Skipped,
}

/// A single buffer-pruning strategy.
///
/// Strategies are self-contained: each one holds the trait-object
/// references to whichever subsystem(s) it needs, so the engine can
/// treat them uniformly as `dyn PruningStrategy`.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait PruningStrategy: Send + Sync {
    /// Run the strategy once. Called from
    /// [`PruningEngine::run_pruning_round`](super::PruningEngine::run_pruning_round).
    fn run(&self) -> PruningResult;
}
