//! Buffer pruning engine (WI-5 of `specs-memory-limter-project/plan.md`).
//!
//! Under memory pressure the [`MemoryLimiter`](super::MemoryLimiter) must free
//! buffers before new allocations can proceed. This module implements a
//! coalesced, round-robin pruning engine that rotates through a set of
//! [`PruningStrategy`] implementations, each of which either takes an
//! action that will eventually release memory ([`PruningResult::Acted`])
//! or reports [`PruningResult::Skipped`] so the next strategy can be
//! tried.
//!
//! # Current scope
//!
//! Only strategies A ([`UploadsStrategy`]), B ([`ActiveReadsStrategy`]),
//! and C ([`PoolTrimStrategy`]) from `spec.md` §5.3 are implemented.
//! Strategy D (speculative-prefetch reset) is deferred to WI-6; adding it
//! is a matter of creating a new module alongside the existing three and
//! pushing one entry onto the [`PruningEngine`]'s strategy vec. The
//! engine's round-robin logic is strategy-count agnostic.
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
//! Once those land, a follow-up PR constructs the concrete strategies,
//! builds a [`PruningEngine`] inside `MemoryLimiter::new`, and spawns
//! [`PruningEngine::pruning_loop`] onto the crate's existing
//! [`crate::async_util::Runtime`] (which wraps [`futures::task::Spawn`];
//! today that is a CRT event-loop-group based executor). No production
//! [`Clock`] implementation ships in this PR — a runtime-agnostic timer
//! (for example a `futures-timer`-style sleep or a thread-based wake) is
//! chosen as part of WI-4. Every entry point defined here is therefore
//! `pub(crate)`.
//!
//! # Testability
//!
//! All external effects go through trait seams that are `#[automock]`ed
//! under `cfg(test)`, so unit tests drive every code path without any
//! real pool, queue, or runtime. Engine rotation tests use the
//! automocked [`PruningStrategy`] directly, and the coalescing window
//! uses an injected [`Clock`] so `tokio::time::pause` can advance time
//! deterministically from inside the crate's (dev-only) tokio test
//! runtime.
//!
//! # Notifier choice
//!
//! The coalescing signal uses an `async_channel::bounded::<()>(1)` —
//! already a crate dependency — rather than `tokio::sync::Notify` so the
//! engine has no coupling to the tokio runtime. `try_send(())` on a full
//! channel silently drops, which matches `notify_one`'s single-permit
//! coalescing semantics; `recv().await` on the receiver matches
//! `notified().await`.
//!
//! [`MemoryLimiter`]: super::MemoryLimiter

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_channel::{Receiver, Sender, TrySendError};
use async_trait::async_trait;

mod active_reads;
mod pool_trim;
mod strategy;
mod uploads;

pub(crate) use active_reads::ActiveReadsStrategy;
pub(crate) use pool_trim::PoolTrimStrategy;
pub(crate) use strategy::{PruningResult, PruningStrategy};
pub(crate) use uploads::UploadsStrategy;

/// Pool operations the pruning engine needs.
///
/// In production this is implemented by `PagedPool` (WI-4). Consumed by
/// [`UploadsStrategy`] and [`PoolTrimStrategy`].
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
/// introduces. The engine consults the queue to short-circuit pruning
/// when no request is waiting, and [`PoolTrimStrategy`] notifies it
/// after a successful trim so newly reservable requests can be woken.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait AllocationQueueOps: Send + Sync {
    /// Whether the pending-allocation queue is empty.
    fn is_empty(&self) -> bool;

    /// Attempt to satisfy queued allocations from now-available memory.
    /// If the queue is still non-empty afterwards, memory pressure
    /// remains.
    fn try_wake_pending(&self);

    /// Set or clear the memory-pressure signal. `false` is emitted when
    /// the queue has drained and no pruning work is pending.
    fn set_memory_pressure(&self, value: bool);
}

/// Handle-registry operations the pruning engine needs.
///
/// In production this is implemented by the registry that WI-2
/// introduces. Consumed by [`ActiveReadsStrategy`].
#[cfg_attr(test, mockall::automock)]
pub(crate) trait HandleRegistryOps: Send + Sync {
    /// Returns `true` if at least one handle is currently in the middle
    /// of a FUSE read and owns a live `RequestTask` backing it.
    fn any_active_read_with_task(&self) -> bool;
}

/// Abstraction over time so the coalescing window in
/// [`PruningEngine::pruning_loop`] can be driven deterministically by
/// tests.
///
/// No production implementation ships in this PR — a runtime-agnostic
/// timer will be added alongside the WI-4 wiring of [`PruningEngine`]
/// into the live [`super::MemoryLimiter`]. Tests use an ephemeral
/// `TokioClock` gated under `#[cfg(test)]`.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait Clock: Send + Sync {
    async fn sleep(&self, duration: Duration);
}

/// Test-only [`Clock`] implementation backed by [`tokio::time::sleep`].
/// Lives under `#[cfg(test)]` because `tokio::time::sleep` requires a
/// tokio runtime, which mountpoint's non-test code does not provide (it
/// uses the CRT event-loop-group threads via
/// [`crate::async_util::Runtime`]).
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct TokioClock;

#[cfg(test)]
#[async_trait]
impl Clock for TokioClock {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Buffer pruning engine.
///
/// Holds a vector of [`PruningStrategy`] trait objects and two
/// round-meta dependencies — the allocation queue (drives the
/// short-circuit when no requests are waiting) and a [`Clock`] (drives
/// the coalescing window). A capacity-1 `async_channel` is used by
/// [`PruningEngine::trigger`] to wake
/// [`PruningEngine::pruning_loop`]. See the module-level docs for the
/// notifier rationale and the algorithm used by
/// [`PruningEngine::run_pruning_round`].
#[allow(dead_code)] // Constructed by follow-up PR (WI-4 integration).
pub(crate) struct PruningEngine {
    strategies: Vec<Arc<dyn PruningStrategy>>,
    queue: Arc<dyn AllocationQueueOps>,
    clock: Arc<dyn Clock>,
    next_strategy: AtomicUsize,
    notify_tx: Sender<()>,
    notify_rx: Receiver<()>,
}

impl PruningEngine {
    /// Create a new pruning engine. Does not start the background loop —
    /// callers must spawn [`Self::pruning_loop`] on whichever executor
    /// is in use (today: the crate's [`crate::async_util::Runtime`]).
    ///
    /// The order of `strategies` defines the round-robin rotation; the
    /// engine picks a different starting index on each round so that
    /// over time every strategy gets a fair turn at being tried first.
    #[allow(dead_code)] // Constructed by follow-up PR (WI-4 integration).
    pub(crate) fn new(
        strategies: Vec<Arc<dyn PruningStrategy>>,
        queue: Arc<dyn AllocationQueueOps>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (notify_tx, notify_rx) = async_channel::bounded(1);
        Self {
            strategies,
            queue,
            clock,
            next_strategy: AtomicUsize::new(0),
            notify_tx,
            notify_rx,
        }
    }

    /// Execute one pruning round (spec §5.2).
    ///
    /// If the allocation queue is empty, the memory-pressure signal is
    /// cleared and the round is a no-op. Otherwise the engine rotates
    /// through `self.strategies` in round-robin order, starting from
    /// `next_strategy % strategies.len()`, and returns
    /// [`PruningResult::Acted`] on the first strategy that acts.
    /// `next_strategy` is advanced by one on every call so successive
    /// rounds pick a different starting point.
    ///
    /// Emits the `mem.pruning_rounds` counter once per invocation.
    pub(crate) fn run_pruning_round(&self) -> PruningResult {
        metrics::counter!("mem.pruning_rounds").increment(1);

        if self.queue.is_empty() {
            self.queue.set_memory_pressure(false);
            return PruningResult::Skipped;
        }

        let n = self.strategies.len();
        if n == 0 {
            // Defensive: an engine with no strategies can never act. Not
            // expected to happen in production (WI-4 builds the vec
            // explicitly), but guards against a `% 0` panic below.
            return PruningResult::Skipped;
        }

        let start = self.next_strategy.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let idx = (start + i) % n;
            if matches!(self.strategies[idx].run(), PruningResult::Acted) {
                return PruningResult::Acted;
            }
        }
        PruningResult::Skipped
    }

    /// Signal [`pruning_loop`](Self::pruning_loop) to run a pruning
    /// round after the coalescing window.
    ///
    /// Multiple `trigger` calls made before the loop wakes collapse
    /// into a single round: the notifier is a capacity-1 channel and
    /// additional sends fail with [`TrySendError::Full`], which we
    /// silently drop. Callers should invoke this from `acquire_async`
    /// on enqueue and from `try_wake_pending` when the queue remains
    /// non-empty (spec §5.4).
    #[allow(dead_code)] // Invoked by WI-3/WI-4 once wired in.
    pub(crate) fn trigger(&self) {
        match self.notify_tx.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => {
                // Either the permit was stored, or a previous trigger is
                // already pending — coalesce by doing nothing.
            }
            Err(TrySendError::Closed(())) => {
                // The loop's receiver has been dropped — nothing to
                // signal.
            }
        }
    }

    /// Background coalescing loop (spec §5.4).
    ///
    /// Waits for a [`trigger`](Self::trigger), sleeps for the 1 ms
    /// coalescing window via the injected [`Clock`], then runs exactly
    /// one pruning round. Intended to be spawned once at memory-limiter
    /// startup (not done in this PR — see the module docs). Exits
    /// cleanly if the notifier channel is closed (e.g. the
    /// [`PruningEngine`] was dropped).
    #[allow(dead_code)] // Spawned by WI-4 once MemoryLimiter integration lands.
    pub(crate) async fn pruning_loop(self: Arc<Self>) {
        while self.notify_rx.recv().await.is_ok() {
            self.clock.sleep(Duration::from_millis(1)).await;
            self.run_pruning_round();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::strategy::MockPruningStrategy;
    use super::*;

    /// Build a [`PruningEngine`] from a vector of mock strategies plus
    /// mocks for queue and clock. Each test configures expectations on
    /// each mock before handing it in.
    fn build_engine(
        strategies: Vec<Arc<dyn PruningStrategy>>,
        queue: MockAllocationQueueOps,
        clock: MockClock,
    ) -> PruningEngine {
        PruningEngine::new(strategies, Arc::new(queue), Arc::new(clock))
    }

    /// Smoke test: the `#[automock]` macro generates `Mock*` types and
    /// each one has a `new()` constructor we can use from test code.
    #[test]
    fn mocks_are_constructible() {
        let _pool = MockPoolPruneOps::new();
        let _queue = MockAllocationQueueOps::new();
        let _registry = MockHandleRegistryOps::new();
        let _clock = MockClock::new();
        let _strategy = MockPruningStrategy::new();
    }

    /// The engine can be constructed from the concrete strategy types.
    /// Exercises the re-exports and the trait-object coercion at the
    /// call site a follow-up CR will use.
    #[test]
    fn engine_is_constructible_with_concrete_strategies() {
        let pool: Arc<dyn PoolPruneOps> = Arc::new(MockPoolPruneOps::new());
        let queue: Arc<dyn AllocationQueueOps> = Arc::new(MockAllocationQueueOps::new());
        let registry: Arc<dyn HandleRegistryOps> = Arc::new(MockHandleRegistryOps::new());
        let clock: Arc<dyn Clock> = Arc::new(TokioClock);

        let strategies: Vec<Arc<dyn PruningStrategy>> = vec![
            Arc::new(UploadsStrategy::new(pool.clone())),
            Arc::new(ActiveReadsStrategy::new(registry)),
            Arc::new(PoolTrimStrategy::new(pool, queue.clone())),
        ];

        let _engine = PruningEngine::new(strategies, queue, clock);
    }

    // ── run_pruning_round — rotation ─────────────────────────────────────

    #[test]
    fn round_returns_skipped_and_clears_pressure_when_queue_empty() {
        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(1).return_const(true);
        queue
            .expect_set_memory_pressure()
            .withf(|&v| !v)
            .times(1)
            .return_const(());

        // No strategies should be consulted — an empty vec would also
        // fit the short-circuit, but we include a strategy that would
        // panic if called to make the short-circuit assertion stronger.
        let mut s = MockPruningStrategy::new();
        s.expect_run().times(0);

        let engine = build_engine(vec![Arc::new(s)], queue, MockClock::new());
        assert_eq!(engine.run_pruning_round(), PruningResult::Skipped);
    }

    #[test]
    fn round_with_no_strategies_is_a_skipped_no_op() {
        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(1).return_const(false);

        let engine = build_engine(Vec::new(), queue, MockClock::new());
        assert_eq!(engine.run_pruning_round(), PruningResult::Skipped);
    }

    #[test]
    fn round_acts_on_first_strategy_when_it_returns_acted() {
        // Strategy 0 acts; strategies 1 and 2 must NOT be called because
        // the round short-circuits on the first Acted.
        let mut s0 = MockPruningStrategy::new();
        s0.expect_run().times(1).return_const(PruningResult::Acted);
        let mut s1 = MockPruningStrategy::new();
        s1.expect_run().times(0);
        let mut s2 = MockPruningStrategy::new();
        s2.expect_run().times(0);

        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(1).return_const(false);

        let engine = build_engine(vec![Arc::new(s0), Arc::new(s1), Arc::new(s2)], queue, MockClock::new());
        assert_eq!(engine.run_pruning_round(), PruningResult::Acted);
    }

    #[test]
    fn round_returns_skipped_when_all_strategies_skip() {
        let mut s0 = MockPruningStrategy::new();
        s0.expect_run().times(1).return_const(PruningResult::Skipped);
        let mut s1 = MockPruningStrategy::new();
        s1.expect_run().times(1).return_const(PruningResult::Skipped);
        let mut s2 = MockPruningStrategy::new();
        s2.expect_run().times(1).return_const(PruningResult::Skipped);

        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(1).return_const(false);

        let engine = build_engine(vec![Arc::new(s0), Arc::new(s1), Arc::new(s2)], queue, MockClock::new());
        assert_eq!(engine.run_pruning_round(), PruningResult::Skipped);
    }

    /// Across three rounds where strategies 0 and 1 `Skip` but strategy
    /// 2 `Act`s, the rotation index shifts so the "first-tried"
    /// strategy changes each round. Rounds short-circuit on the first
    /// `Acted`, so strategies tried *after* strategy 2 in a given round
    /// are not called. The resulting per-strategy call counts uniquely
    /// identify the rotation:
    ///
    /// - round 1 (start=0): s0 skip → s1 skip → s2 act ✓
    /// - round 2 (start=1): s1 skip → s2 act ✓
    /// - round 3 (start=2): s2 act ✓
    ///
    /// Totals: s0 1×, s1 2×, s2 3×.
    #[test]
    fn round_rotates_start_strategy_across_calls() {
        let mut s0 = MockPruningStrategy::new();
        s0.expect_run().times(1).return_const(PruningResult::Skipped);
        let mut s1 = MockPruningStrategy::new();
        s1.expect_run().times(2).return_const(PruningResult::Skipped);
        let mut s2 = MockPruningStrategy::new();
        s2.expect_run().times(3).return_const(PruningResult::Acted);

        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().times(3).return_const(false);

        let engine = build_engine(vec![Arc::new(s0), Arc::new(s1), Arc::new(s2)], queue, MockClock::new());

        for _ in 0..3 {
            assert_eq!(engine.run_pruning_round(), PruningResult::Acted);
        }
    }

    // ── trigger + pruning_loop — coalescing ──────────────────────────────

    /// Build an engine with a single mock strategy that increments a
    /// shared counter on each run. Uses a real [`TokioClock`] with
    /// paused tokio time so the 1 ms coalescing window can be driven
    /// deterministically.
    fn build_loop_engine(round_counter: Arc<std::sync::atomic::AtomicUsize>) -> Arc<PruningEngine> {
        let mut strategy = MockPruningStrategy::new();
        strategy.expect_run().returning(move || {
            round_counter.fetch_add(1, Ordering::SeqCst);
            PruningResult::Acted // No other strategies, so any value is fine.
        });

        let mut queue = MockAllocationQueueOps::new();
        queue.expect_is_empty().returning(|| false);

        Arc::new(PruningEngine::new(
            vec![Arc::new(strategy)],
            Arc::new(queue),
            Arc::new(TokioClock),
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn trigger_wakes_loop_and_runs_round_after_coalesce_window() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let engine = build_loop_engine(counter.clone());
        let handle = tokio::spawn(engine.clone().pruning_loop());

        // Yield so the spawned loop reaches `recv().await` before we
        // call trigger; this is the scenario where a trigger arrives
        // while the loop is idle.
        tokio::task::yield_now().await;
        engine.trigger();

        // Yield again so the loop consumes the permit, then enters
        // sleep. Sleep is paused so the round must NOT have run yet.
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

        // Fire five triggers before the loop has a chance to run. The
        // capacity-1 notifier channel collapses these into a single
        // pending permit (four `try_send`s fail with `Full` and are
        // dropped silently).
        for _ in 0..5 {
            engine.trigger();
        }

        // Let the loop run: consume permit → sleep → round.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Advance well beyond the window; no further rounds run because
        // the 4 extra triggers were coalesced into the one permit
        // already consumed.
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        handle.abort();
        let _ = handle.await;
    }
}
