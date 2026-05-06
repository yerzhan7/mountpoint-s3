//! Buffer pruning engine (WI-5 of `specs-memory-limter-project/plan.md`).
//!
//! Under memory pressure the [`MemoryLimiter`](super::MemoryLimiter) must free
//! buffers before new allocations can proceed. This module implements a
//! coalesced, round-robin pruning engine that rotates through a set of
//! strategies, each of which either takes an action that will eventually
//! release memory or reports `Skipped` so the next strategy can be tried.
//!
//! # Current scope
//!
//! Only strategies A (uploads), B (active reads), and C (pool trim) from
//! `spec.md` §5.3 are implemented. Strategy D (speculative-prefetch reset) is
//! deferred to WI-6 — at that point the rotation modulus flips from `% 3` back
//! to `% 4`.
//!
//! # Integration status
//!
//! The engine is intentionally **not yet wired** into the live
//! [`MemoryLimiter`]. It depends on three pieces that are still in flight:
//!
//! * WI-2 — a unified handle registry with per-handle state (implements
//!   [`HandleRegistryOps`]).
//! * WI-3 — the allocation queue with `acquire_async` (implements
//!   [`AllocationQueueOps`]).
//! * WI-4 — pool-gated allocations; the pool also implements
//!   [`PoolPruneOps`].
//!
//! Once those land, a follow-up PR constructs a [`PruningEngine`] inside
//! `MemoryLimiter::new` and spawns [`PruningEngine::pruning_loop`] on the
//! tokio runtime. Every entry point defined here is therefore `pub(crate)`.
//!
//! # Testability
//!
//! All external effects go through trait seams that are `#[automock]`ed
//! under `cfg(test)`, so unit tests drive every code path without any real
//! pool, queue, or runtime. The coalescing window uses an injected
//! [`Clock`] so `tokio::time::pause` can advance time deterministically.
//!
//! [`MemoryLimiter`]: super::MemoryLimiter

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;

/// Result of a single pruning strategy or pruning round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PruningResult {
    /// The strategy took an action that will eventually release memory (for
    /// example, it observed an in-flight upload that must complete, or it
    /// freed buffers via [`PoolPruneOps::trim`]).
    Acted,
    /// The strategy had nothing to do and the caller should try the next one.
    Skipped,
}

/// Pool operations the pruning engine needs.
///
/// In production this is implemented by `PagedPool` (WI-4). Strategies A and
/// C consult it to (A) determine whether an upload is in flight and (C)
/// trigger buffer reclamation.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait PoolPruneOps: Send + Sync {
    /// Returns the total bytes reserved by upload buffers (the sum of
    /// `BufferKind::PutObject` and `BufferKind::Append` reservations).
    fn reserved_upload_bytes(&self) -> u64;

    /// Attempts to release unused secondary buffers. Returns `true` if at
    /// least one buffer was freed.
    fn trim(&self) -> bool;
}

/// Allocation-queue operations the pruning engine needs.
///
/// In production this is implemented by the `AllocationQueue` that WI-3
/// introduces. The engine consults the queue to short-circuit pruning when
/// no request is waiting, and notifies it after a successful trim so any
/// newly reservable requests can be woken.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait AllocationQueueOps: Send + Sync {
    /// Whether the pending-allocation queue is empty.
    fn is_empty(&self) -> bool;

    /// Attempt to satisfy queued allocations from now-available memory. If
    /// the queue is still non-empty afterwards, memory pressure remains.
    fn try_wake_pending(&self);

    /// Set or clear the memory-pressure signal. `false` is emitted when the
    /// queue has drained and no pruning work is pending.
    fn set_memory_pressure(&self, value: bool);
}

/// Handle-registry operations the pruning engine needs.
///
/// In production this is implemented by the registry that WI-2 introduces.
/// Strategy B consults it to determine whether any handle is currently in
/// an `active_read_range` with a `current_task` — in which case pruning
/// should defer to the in-flight response rather than acting itself.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait HandleRegistryOps: Send + Sync {
    /// Returns `true` if at least one handle is currently in the middle of a
    /// FUSE read and owns a live `RequestTask` backing it.
    fn any_active_read_with_task(&self) -> bool;
}

/// Abstraction over time so the coalescing window in
/// [`PruningEngine::pruning_loop`] can be driven deterministically by tests.
///
/// Production uses [`TokioClock`], which forwards to `tokio::time::sleep`.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait Clock: Send + Sync {
    async fn sleep(&self, duration: Duration);
}

/// Production [`Clock`] implementation backed by [`tokio::time::sleep`].
#[derive(Debug, Default)]
#[allow(dead_code)] // Constructed by follow-up PR (WI-4 integration).
pub(crate) struct TokioClock;

#[async_trait]
impl Clock for TokioClock {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Buffer pruning engine.
///
/// Holds trait-object references to its three cooperating subsystems plus a
/// [`Clock`], a round-robin strategy index, and a [`Notify`] used by
/// [`PruningEngine::trigger`] to wake [`PruningEngine::pruning_loop`].
///
/// See the module-level docs for scope, integration status, and the
/// algorithm used by [`PruningEngine::run_pruning_round`].
#[allow(dead_code)] // Constructed by follow-up PR (WI-4 integration).
pub(crate) struct PruningEngine {
    pool: Arc<dyn PoolPruneOps>,
    queue: Arc<dyn AllocationQueueOps>,
    registry: Arc<dyn HandleRegistryOps>,
    clock: Arc<dyn Clock>,
    next_strategy: AtomicUsize,
    notify: Notify,
}

impl PruningEngine {
    /// Create a new pruning engine. Does not start the background loop —
    /// callers must `tokio::spawn(engine.pruning_loop())` once ready.
    #[allow(dead_code)] // Constructed by follow-up PR (WI-4 integration).
    pub(crate) fn new(
        pool: Arc<dyn PoolPruneOps>,
        queue: Arc<dyn AllocationQueueOps>,
        registry: Arc<dyn HandleRegistryOps>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            pool,
            queue,
            registry,
            clock,
            next_strategy: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    /// Strategy A — uploads in flight.
    ///
    /// If any upload buffer is currently reserved (`PutObject` or `Append`)
    /// we return [`PruningResult::Acted`]: an in-flight upload will
    /// eventually complete and release that memory. Pruning must not
    /// cancel uploads (spec §5.6), so "waiting" is the correct action.
    fn strategy_uploads(&self) -> PruningResult {
        if self.pool.reserved_upload_bytes() > 0 {
            PruningResult::Acted
        } else {
            PruningResult::Skipped
        }
    }

    /// Strategy B — active reads.
    ///
    /// If any handle is currently in a FUSE read and owns a live
    /// `RequestTask`, the S3 response for that read will release a buffer
    /// when it lands. Return [`PruningResult::Acted`] to defer to that
    /// release rather than disrupting an in-progress read (spec §5.6:
    /// pruning never targets handles with `active_read_range.is_some()`).
    fn strategy_active_reads(&self) -> PruningResult {
        if self.registry.any_active_read_with_task() {
            PruningResult::Acted
        } else {
            PruningResult::Skipped
        }
    }

    /// Strategy C — pool trim.
    ///
    /// Ask the pool to release idle (secondary) buffers. On success,
    /// immediately give the allocation queue a chance to satisfy pending
    /// requests from the newly freed memory, and return
    /// [`PruningResult::Acted`]. Emits the `mem.pruning_trims` counter on
    /// the `Acted` path.
    fn strategy_pool_trim(&self) -> PruningResult {
        if self.pool.trim() {
            metrics::counter!("mem.pruning_trims").increment(1);
            self.queue.try_wake_pending();
            PruningResult::Acted
        } else {
            PruningResult::Skipped
        }
    }

    /// Execute one pruning round (spec §5.2).
    ///
    /// If the allocation queue is empty, the memory-pressure signal is
    /// cleared and the round is a no-op. Otherwise the engine rotates
    /// through the available strategies in round-robin order, starting
    /// from `next_strategy % 3`, and returns [`PruningResult::Acted`] on
    /// the first strategy that acts. `next_strategy` is advanced by one on
    /// every call so successive rounds pick a different starting point.
    ///
    /// Emits the `mem.pruning_rounds` counter once per invocation.
    pub(crate) fn run_pruning_round(&self) -> PruningResult {
        metrics::counter!("mem.pruning_rounds").increment(1);

        if self.queue.is_empty() {
            self.queue.set_memory_pressure(false);
            return PruningResult::Skipped;
        }

        const STRATEGY_COUNT: usize = 3;
        let start = self.next_strategy.fetch_add(1, Ordering::Relaxed) % STRATEGY_COUNT;
        for i in 0..STRATEGY_COUNT {
            // TODO(WI-6): add strategy_speculative_prefetch and flip the
            // rotation modulus back to 4.
            let result = match (start + i) % STRATEGY_COUNT {
                0 => self.strategy_uploads(),
                1 => self.strategy_active_reads(),
                2 => self.strategy_pool_trim(),
                _ => unreachable!("STRATEGY_COUNT guarantees index in range"),
            };
            if matches!(result, PruningResult::Acted) {
                return PruningResult::Acted;
            }
        }
        PruningResult::Skipped
    }

    /// Signal [`pruning_loop`](Self::pruning_loop) to run a pruning round
    /// after the coalescing window.
    ///
    /// Multiple `trigger` calls made before the loop wakes collapse into a
    /// single round (the underlying [`Notify`] stores at most one permit).
    /// Callers should invoke this from `acquire_async` on enqueue and from
    /// `try_wake_pending` when the queue remains non-empty (spec §5.4).
    #[allow(dead_code)] // Invoked by WI-3/WI-4 once wired in.
    pub(crate) fn trigger(&self) {
        self.notify.notify_one();
    }

    /// Background coalescing loop (spec §5.4).
    ///
    /// Waits for a [`trigger`](Self::trigger), sleeps for the 1 ms
    /// coalescing window via the injected [`Clock`], then runs exactly one
    /// pruning round. Intended to be spawned once via `tokio::spawn` at
    /// memory-limiter startup (not done in this PR — see the module docs).
    #[allow(dead_code)] // Spawned by WI-4 once MemoryLimiter integration lands.
    pub(crate) async fn pruning_loop(self: Arc<Self>) {
        loop {
            self.notify.notified().await;
            self.clock.sleep(Duration::from_millis(1)).await;
            self.run_pruning_round();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a [`PruningEngine`] from the four mocks. Returned separately so
    /// tests can set expectations on each mock before constructing the
    /// engine, then pass the engine into the call under test.
    fn build_engine(
        pool: MockPoolPruneOps,
        queue: MockAllocationQueueOps,
        registry: MockHandleRegistryOps,
        clock: MockClock,
    ) -> PruningEngine {
        PruningEngine::new(Arc::new(pool), Arc::new(queue), Arc::new(registry), Arc::new(clock))
    }

    /// Smoke test: the `#[automock]` macro generates `Mock*` types and each
    /// one has a `new()` constructor we can use from test code.
    #[test]
    fn mocks_are_constructible() {
        let _pool = MockPoolPruneOps::new();
        let _queue = MockAllocationQueueOps::new();
        let _registry = MockHandleRegistryOps::new();
        let _clock = MockClock::new();
    }

    /// The engine itself can be constructed from its trait-object deps. This
    /// exercises that the trait bounds line up and [`TokioClock`] is usable
    /// as a production impl.
    #[test]
    fn engine_is_constructible() {
        let pool: Arc<dyn PoolPruneOps> = Arc::new(MockPoolPruneOps::new());
        let queue: Arc<dyn AllocationQueueOps> = Arc::new(MockAllocationQueueOps::new());
        let registry: Arc<dyn HandleRegistryOps> = Arc::new(MockHandleRegistryOps::new());
        let clock: Arc<dyn Clock> = Arc::new(TokioClock);
        let _engine = PruningEngine::new(pool, queue, registry, clock);
    }

    // ── Strategy A — uploads ──────────────────────────────────────────────

    #[test]
    fn strategy_uploads_acts_when_upload_bytes_nonzero() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_reserved_upload_bytes().times(1).return_const(4096_u64);
        let engine = build_engine(
            pool,
            MockAllocationQueueOps::new(),
            MockHandleRegistryOps::new(),
            MockClock::new(),
        );

        assert_eq!(engine.strategy_uploads(), PruningResult::Acted);
    }

    #[test]
    fn strategy_uploads_skips_when_zero() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_reserved_upload_bytes().times(1).return_const(0_u64);
        let engine = build_engine(
            pool,
            MockAllocationQueueOps::new(),
            MockHandleRegistryOps::new(),
            MockClock::new(),
        );

        assert_eq!(engine.strategy_uploads(), PruningResult::Skipped);
    }

    // ── Strategy B — active reads ─────────────────────────────────────────

    #[test]
    fn strategy_active_reads_acts_when_any_handle_has_active_read_and_task() {
        let mut registry = MockHandleRegistryOps::new();
        registry.expect_any_active_read_with_task().times(1).return_const(true);
        let engine = build_engine(
            MockPoolPruneOps::new(),
            MockAllocationQueueOps::new(),
            registry,
            MockClock::new(),
        );

        assert_eq!(engine.strategy_active_reads(), PruningResult::Acted);
    }

    #[test]
    fn strategy_active_reads_skips_otherwise() {
        let mut registry = MockHandleRegistryOps::new();
        registry.expect_any_active_read_with_task().times(1).return_const(false);
        let engine = build_engine(
            MockPoolPruneOps::new(),
            MockAllocationQueueOps::new(),
            registry,
            MockClock::new(),
        );

        assert_eq!(engine.strategy_active_reads(), PruningResult::Skipped);
    }

    // ── Strategy C — pool trim ────────────────────────────────────────────

    #[test]
    fn strategy_pool_trim_acts_and_wakes_when_trim_succeeds() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_trim().times(1).return_const(true);
        let mut queue = MockAllocationQueueOps::new();
        queue.expect_try_wake_pending().times(1).return_const(());
        let engine = build_engine(pool, queue, MockHandleRegistryOps::new(), MockClock::new());

        assert_eq!(engine.strategy_pool_trim(), PruningResult::Acted);
    }

    #[test]
    fn strategy_pool_trim_skips_and_does_not_wake_when_trim_fails() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_trim().times(1).return_const(false);
        // No expectation set on try_wake_pending — mockall panics on
        // unexpected calls, so this also asserts it is not invoked.
        let queue = MockAllocationQueueOps::new();
        let engine = build_engine(pool, queue, MockHandleRegistryOps::new(), MockClock::new());

        assert_eq!(engine.strategy_pool_trim(), PruningResult::Skipped);
    }

    // ── run_pruning_round — rotation ──────────────────────────────────────

    #[test]
    fn round_returns_skipped_and_clears_pressure_when_queue_empty() {
        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(1).return_const(true);
        queue
            .expect_set_memory_pressure()
            .withf(|&v| !v)
            .times(1)
            .return_const(());
        let engine = build_engine(
            MockPoolPruneOps::new(),
            queue,
            MockHandleRegistryOps::new(),
            MockClock::new(),
        );

        assert_eq!(engine.run_pruning_round(), PruningResult::Skipped);
    }

    #[test]
    fn round_acts_on_first_strategy_when_it_returns_acted() {
        let mut pool = MockPoolPruneOps::new();
        // Strategy A is first (start=0 after fetch_add on a fresh engine):
        // reserved_upload_bytes nonzero -> Acted. Subsequent strategies
        // must not be called.
        pool.expect_reserved_upload_bytes().times(1).return_const(1024_u64);

        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(1).return_const(false);

        let engine = build_engine(pool, queue, MockHandleRegistryOps::new(), MockClock::new());

        assert_eq!(engine.run_pruning_round(), PruningResult::Acted);
    }

    #[test]
    fn round_returns_skipped_when_all_strategies_skip() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_reserved_upload_bytes().times(1).return_const(0_u64);
        pool.expect_trim().times(1).return_const(false);

        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(1).return_const(false);
        // try_wake_pending must not be called because trim returned false.

        let mut registry = MockHandleRegistryOps::new();
        registry.expect_any_active_read_with_task().times(1).return_const(false);

        let engine = build_engine(pool, queue, registry, MockClock::new());

        assert_eq!(engine.run_pruning_round(), PruningResult::Skipped);
    }

    /// Across three rounds where only `strategy_pool_trim` acts, the
    /// rotation index shifts so that the "first-tried" strategy changes
    /// each round. Because rounds short-circuit on the first `Acted`,
    /// strategies tried *after* `pool_trim` in a given round are not
    /// called. The resulting per-mock call counts uniquely identify the
    /// rotation:
    ///
    /// - round 1 (start=0): uploads → active_reads → trim ✓
    /// - round 2 (start=1): active_reads → trim ✓
    /// - round 3 (start=2): trim ✓
    ///
    /// Totals: uploads 1×, active_reads 2×, trim 3×.
    #[test]
    fn round_rotates_start_strategy_across_calls() {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_reserved_upload_bytes().times(1).return_const(0_u64);
        pool.expect_trim().times(3).return_const(true);

        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(3).return_const(false);
        queue.expect_try_wake_pending().times(3).return_const(());

        let mut registry = MockHandleRegistryOps::new();
        registry.expect_any_active_read_with_task().times(2).return_const(false);

        let engine = build_engine(pool, queue, registry, MockClock::new());

        for _ in 0..3 {
            assert_eq!(engine.run_pruning_round(), PruningResult::Acted);
        }
    }

    // ── trigger + pruning_loop — coalescing ──────────────────────────────

    /// Build a `PruningEngine` whose strategies observe a shared
    /// [`AtomicUsize`] counter that increments on every call to
    /// `pool.reserved_upload_bytes` — the first strategy tried after a
    /// round starts. Using a real [`TokioClock`] with paused tokio time
    /// lets us drive the 1 ms coalescing window deterministically.
    fn build_loop_engine(round_counter: Arc<std::sync::atomic::AtomicUsize>) -> Arc<PruningEngine> {
        let mut pool = MockPoolPruneOps::new();
        pool.expect_reserved_upload_bytes().returning(move || {
            round_counter.fetch_add(1, Ordering::SeqCst);
            1024 // Acted — first strategy wins, no further mock calls needed.
        });
        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().returning(|| false);
        let registry = MockHandleRegistryOps::new();

        Arc::new(PruningEngine::new(
            Arc::new(pool),
            Arc::new(queue),
            Arc::new(registry),
            Arc::new(TokioClock),
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn trigger_wakes_loop_and_runs_round_after_coalesce_window() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let engine = build_loop_engine(counter.clone());
        let handle = tokio::spawn(engine.clone().pruning_loop());

        // Yield so the spawned loop reaches `notified().await` before we
        // call trigger; this is the scenario where a trigger arrives while
        // the loop is idle.
        tokio::task::yield_now().await;
        engine.trigger();

        // Yield again so the loop consumes the permit, then enters sleep.
        // Sleep is paused so the round must NOT have run yet.
        tokio::task::yield_now().await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // Advance past the coalescing window; the round should now run.
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test(start_paused = true)]
    async fn multiple_triggers_within_window_coalesce_to_single_round() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let engine = build_loop_engine(counter.clone());
        let handle = tokio::spawn(engine.clone().pruning_loop());

        // Fire five triggers before the loop has a chance to run. Notify's
        // semantics collapse these into a single permit.
        for _ in 0..5 {
            engine.trigger();
        }

        // Let the loop run: consume permit → sleep → round.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Advance well beyond the window; no further rounds run because
        // the 4 extra triggers were coalesced into the one permit already
        // consumed.
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        handle.abort();
        let _ = handle.await;
    }
}
