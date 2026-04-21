# Memory Limiter Stress Tests

Long-running tests that sustain concurrent load against real S3 to shake out
deadlocks, per-worker starvation, and memory-accounting bugs in the memory
limiter and pruner.

## Scope

**What these tests assert (survival properties only):**

- No panics.
- No per-worker stall longer than `max_idle_duration` (default 30s). Every
  worker must advance its progress counter at least once per window.
- At teardown, `mem.bytes_reserved` across all areas returns to within
  1 MiB of zero.

**What these tests do _not_ check:**

- Specific pruning-strategy behaviour (e.g. "strategy D picked the LRU idle
  handle") — belongs in targeted integration tests in `tests/fuse_tests/`.
- ENOMEM on `open()` for writers beyond the admission-control limit — also an
  integration-test concern. Writer scenarios here _tolerate_ ENOMEM by backing
  off and retrying; they do not assert that it fires.
- Throughput or latency — belongs in the `benchmark/` crate.
- Data integrity — deferred; the fixture bytes are not verified on read.

## Platform

Linux x86_64 only in v1. Recommended runner class:
`[self-hosted, linux, x64, nvme-high-performance]` (the same class as
`.github/workflows/bench.yml`). Running on GitHub-hosted runners is likely to
be network-bound rather than memory-limited and may produce misleading results.

## Prerequisites

AWS credentials with read/write access to an S3 bucket, and the following
environment variables (same as the standard integration tests):

| Variable                  | Required | Description                                       |
|---------------------------|----------|---------------------------------------------------|
| `S3_BUCKET_NAME`          | yes      | Bucket to use for fixtures and ephemeral objects  |
| `S3_REGION`               | yes      | Region of the bucket                              |
| `S3_BUCKET_TEST_PREFIX`   | no       | Defaults to `mountpoint-test/`; must end in `/`   |
| `AWS_*` / profile         | yes      | Standard AWS SDK credential resolution            |

Optional stress-specific variables:

| Variable                         | Default | Purpose                                             |
|----------------------------------|---------|-----------------------------------------------------|
| `STRESS_DURATION_SECS`           | 60      | Per-scenario duration in seconds                    |
| `STRESS_METRICS_INTERVAL_SECS`   | 10      | How often the harness logs `mem.bytes_reserved`     |
| `STRESS_WATCHDOG_DISABLE`        | unset   | If set to any value, disables the stall watchdog    |

## How to run

A single scenario:

```
cargo nextest run \
    --features stress_tests,s3_tests \
    --package mountpoint-s3-fs \
    stress_tests::sustained_reads \
    -- --run-ignored only
```

All four scenarios sequentially:

```
cargo nextest run \
    --features stress_tests,s3_tests \
    --package mountpoint-s3-fs \
    'stress_tests::' \
    -- --run-ignored only --test-threads=1
```

A ~1h full run (four scenarios × 15 min each):

```
STRESS_DURATION_SECS=900 cargo nextest run \
    --features stress_tests,s3_tests \
    --package mountpoint-s3-fs \
    'stress_tests::' \
    -- --run-ignored only --test-threads=1
```

The `#[ignore]` attribute on each test is intentional: it keeps these tests
out of the default `cargo test` path. Always pass `--run-ignored only`.

## Scenarios

| Name               | Workers          | What it does                                                                 |
|--------------------|------------------|------------------------------------------------------------------------------|
| `smoke`            | 1                | Reads a 4 KiB object in a loop. Sanity-check that the harness works.         |
| `sustained_reads`  | 32               | Front-to-back-read a 1 GiB fixture, re-open on EOF.                          |
| `sustained_writes` | 64               | Write 50–200 MiB objects and delete them. Tolerates ENOMEM on open.          |
| `mixed_rw`         | 16 + 32          | 16 readers against a 1 GiB fixture and 32 writers sharing the same session.  |
| `churn`            | 48 + 8           | 48 short-lived readers over 100 small fixtures + 8 idle-held handles.        |

All scenarios set `mem_limit = MINIMUM_MEM_LIMIT` (512 MiB) via
`TestSessionConfig::with_mem_limit`.

## Adding a new scenario

1. Create `tests/stress_tests/my_scenario.rs`.
2. Implement `crate::stress_tests::harness::Scenario`: at minimum `name`,
   `num_workers`, `session_config`, and `run_worker`. Override
   `max_idle_duration` and `setup` only if the defaults don't fit.
3. Add a `#[test] #[ignore = "stress test; run with --run-ignored only"]`
   entry point that calls `harness::run(MyScenario)`.
4. Register the module in `tests/stress_tests/mod.rs`.
5. `cargo check --features stress_tests,s3_tests` — it should compile.
6. Run it locally at a short duration
   (`STRESS_DURATION_SECS=30 cargo nextest run ...`) before proposing for
   review.

Worker bodies should:

- Increment the progress counter on every unit of work. The watchdog needs at
  least one increment per `max_idle_duration`.
- Check `stop.load(Ordering::Relaxed)` frequently and return when set.
- Treat ENOMEM from `open()`/`create()` as a back-off-and-retry signal, not a
  panic. Use `std::io::Error::raw_os_error() == Some(libc::ENOMEM)` or
  `err.kind() == ErrorKind::OutOfMemory`. This is forward-compatibility with
  the admission-control work arriving in the memory limiter.

## Known limitations (v1)

- The watchdog logs per-worker progress counters on stall but does **not**
  capture backtraces of other threads. Use a debugger or core dumps if you
  need more detail.
- CI wiring is not done in v1. These tests are intended to be driven from a
  dev workstation or an on-demand self-hosted runner until stability is
  established.
- Only `mem.bytes_reserved` gauges for the `upload` and `prefetch` areas are
  read by the harness. Richer memory-limiter metrics (allocation queue depth,
  pruning counters) can be added to
  `tests/stress_tests/harness.rs::read_mem_bytes_reserved` once they are
  registered by the limiter.
