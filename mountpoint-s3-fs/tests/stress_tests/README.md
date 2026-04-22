# Memory Limiter Stress Tests

Long-running tests that sustain concurrent load against real S3 to shake out
deadlocks, per-worker starvation, and memory-accounting bugs in the memory
limiter and pruner.

## Scope

**What these tests assert (survival properties only):**

- No panics.
- No per-worker stall longer than each worker's `max_idle_duration`. Every
  worker must advance its progress counter at least once per window. Reader
  workers use a 5 s threshold; writer workers use a 30 s threshold (flushing
  hundreds of MiB on close can legitimately take several seconds).
- At teardown, `mem.bytes_reserved` across all areas returns to within
  1 MiB of zero.
- During the run, the peak of `sum(mem.bytes_reserved)` sampled by the metrics
  logger never exceeds the configured `mem_limit` (512 MiB for these tests).

**What these tests do _not_ check:**

- Specific pruning-strategy behaviour (e.g. "strategy D picked the LRU idle
  handle") — belongs in targeted integration tests in `tests/fuse_tests/`.
- Throughput or latency — belongs in the `benchmark/` crate.
- Data integrity — deferred; the test-object bytes are not verified on read.

Worker counts for write-heavy scenarios are sized so the per-part upload
reservations stay well within the configured memory budget at steady state
(at most ~48 concurrent writers × 8 MiB parts = 384 MiB against a 512 MiB
limit).

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
| `S3_BUCKET_NAME`          | yes      | Bucket to use for test objects and ephemeral keys |
| `S3_REGION`               | yes      | Region of the bucket                              |
| `S3_BUCKET_TEST_PREFIX`   | no       | Defaults to `mountpoint-test/`; must end in `/`   |
| `AWS_*` / profile         | yes      | Standard AWS SDK credential resolution            |

Optional stress-specific variables:

| Variable                         | Default | Purpose                                             |
|----------------------------------|---------|-----------------------------------------------------|
| `STRESS_DURATION_SECS`           | 30      | Per-scenario duration in seconds                    |
| `STRESS_METRICS_INTERVAL_SECS`   | 10      | How often the harness logs `mem.bytes_reserved`     |
| `STRESS_WATCHDOG_DISABLE`        | unset   | If set to any value, disables the stall watchdog    |

## Shared test objects

Several scenarios read a large (1 GiB) or small (128 KiB × 100) object from a
*shared*, non-nonced prefix so that consecutive runs don't pay the upload cost
every time:

    <S3_BUCKET_TEST_PREFIX>stress-fixtures/read_1gib.bin
    <S3_BUCKET_TEST_PREFIX>stress-fixtures/small_0000.bin
    ...
    <S3_BUCKET_TEST_PREFIX>stress-fixtures/small_0099.bin

Scenarios that need these objects mount directly at the
`<S3_BUCKET_TEST_PREFIX>stress-fixtures/` prefix (via
`Scenario::s3_path_override`) and upload-if-missing in `setup()` via a `HEAD`
then `PutObject`. These objects are **never deleted** by the harness. Writer
workers in `mixed_rw` namespace their ephemeral keys so they cannot collide
with the shared ones.

## How to run

The `stress_tests` feature transitively enables `s3_tests`, so no extra
feature flag is needed to pull in the real-S3 test session helpers.

A single scenario:

```
cargo nextest run \
    --features stress_tests \
    --package mountpoint-s3-fs \
    stress_tests::sustained_reads \
    --run-ignored only
```

All scenarios sequentially:

```
cargo nextest run \
    --features stress_tests \
    --package mountpoint-s3-fs \
    'stress_tests::' \
    --run-ignored only --test-threads=1
```

A ~1 h full run (four scenarios × 15 min each, matching the CI budget):

```
STRESS_DURATION_SECS=900 cargo nextest run \
    --features stress_tests \
    --package mountpoint-s3-fs \
    'stress_tests::' \
    --run-ignored only --test-threads=1
```

The `#[ignore]` attribute on each test is intentional: it keeps these tests
out of the default `cargo test` path. Always pass `--run-ignored only`.

## Scenarios

| Name               | Workers          | What it does                                                                       |
|--------------------|------------------|------------------------------------------------------------------------------------|
| `smoke`            | 1                | Reads a 4 KiB object in a loop. Sanity-check that the harness works.               |
| `sustained_reads`  | 32               | Front-to-back-read the shared 1 GiB test object, re-open on EOF.                   |
| `sustained_writes` | 48               | Write 50–200 MiB objects and delete them.                                          |
| `mixed_rw`         | 16 + 24          | 16 readers against the shared 1 GiB test object + 24 writers in the same session.  |
| `churn`            | 48 + 8           | 48 short-lived readers over 100 shared small test objects + 8 idle-held handles.   |

The idle workers in `churn` each perform a 1 MiB read after opening a handle
before going idle, so the handle has a prefetcher reservation registered with
the memory limiter and is a valid pruning candidate while it sits idle.

All scenarios set `mem_limit = MINIMUM_MEM_LIMIT` (512 MiB) via
`TestSessionConfig::with_mem_limit`.

## Adding a new scenario

1. Create `tests/stress_tests/my_scenario.rs`.
2. Implement `crate::stress_tests::harness::Scenario`: at minimum `name`,
   `num_workers`, `session_config`, and `run_worker`. Override
   `max_idle_duration`, `s3_path_override`, `peak_reserved_ceiling_bytes`,
   and `setup` only if the defaults don't fit.
3. If the scenario needs a shared test object, add a helper in
   `tests/stress_tests/test_objects.rs` (HEAD + `PutObject`-if-missing),
   return `Some(test_objects::shared_s3_path())` from `s3_path_override`, and
   upload-if-missing from `setup()`.
4. Add a `#[test] #[ignore = "stress test; run with --run-ignored only"]`
   entry point that calls `harness::run(MyScenario)`.
5. Register the module in `tests/stress_tests/mod.rs`.
6. `cargo check --features stress_tests` — it should compile.
7. Run it locally at a short duration
   (`STRESS_DURATION_SECS=30 cargo nextest run ...`) before proposing for
   review.

Worker bodies should:

- Increment the progress counter on every unit of work. The watchdog needs at
  least one increment per `max_idle_duration(worker_id)`.
- Writers: bump progress on every successful `File::create`, every
  `write_all` call, and on `drop(file)` so the long flush/close path still
  shows up as liveness.
- Readers / open-read-close loops: bump progress on every successful open in
  addition to the bytes read.
- Check `stop.load(Ordering::Relaxed)` frequently and return when set.
- **Not** tolerate errors from `File::open`/`File::create`/`read`/`write`.
  Scenarios here are sized so these calls should always succeed against a
  healthy session; any failure is a test-design or memory-limiter bug and
  should panic so the failure is visible.

## CI

`stress` is a label-gated PR workflow:

- `.github/workflows/stress.yml` — reusable, matrix over the four scenarios,
  runner `[self-hosted, linux, x64, nvme-high-performance]`, bucket
  `S3_BENCH_BUCKET_NAME`, prefix `S3_BUCKET_BENCH_PREFIX`,
  `STRESS_DURATION_SECS=900`. Approval gate via the `PR stress` environment.
- `.github/workflows/stress_pr.yml` — triggers `stress.yml` when a PR is
  labelled `stress`.

There is no scheduled / nightly run; attach the `stress` label to a PR to
exercise them.

## Known limitations (v1)

- The watchdog logs per-worker progress counters on stall but does **not**
  capture backtraces of other threads. Use a debugger or core dumps if you
  need more detail.
- Only `mem.bytes_reserved` gauges for the `upload` and `prefetch` areas are
  read by the harness. Richer memory-limiter metrics (allocation queue depth,
  pruning counters) can be added to
  `tests/stress_tests/harness.rs::read_mem_bytes_reserved` once they are
  registered by the limiter.
