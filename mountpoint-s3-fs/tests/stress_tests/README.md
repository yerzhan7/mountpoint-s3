# Memory Limiter Stress Tests

Long-running tests that sustain concurrent load against real S3 to shake out
deadlocks, per-worker starvation, per-op tail-latency, and memory-accounting
bugs in the memory limiter and pruner.

## Scope

**What these tests assert (survival properties only):**

- No panics.
- No per-worker stall longer than `Scenario::max_idle_duration(worker_id)`.
  The default is 30 s for every worker; the API still supports per-worker
  thresholds for scenarios that need them.
- At teardown, each reservation gauge is exactly zero — every reservation
  must be released once the mount is dropped. Checked per-label for:
  - `mem.bytes_reserved{area=upload|prefetch}` (Mountpoint's limiter).
  - `pool.reserved_bytes{kind=get_object|put_object|disk_cache|append|other}`
    (CRT paged-pool occupancy).
- During the run, each individual reservation gauge's peak stays under its
  ceiling. Per-label peaks come from the gauge's HDR history so no spike
  between samples can be missed:
  - `mem.bytes_reserved{area=*}` peak ≤ `mem_limit - additional_mem_reserved`
    (the effective budget the limiter enforces, ~384 MiB on a 512 MiB limit).
  - `pool.reserved_bytes{kind=*}` peak ≤ `mem_limit` (512 MiB).
  Peaks are not summed across labels — summing per-label peaks over-
  estimates the true peak of the sum, and per-label checks give sharper
  failure attribution.
- For each worker op (`open`, `read`, `write`, `close`) aggregated across
  every worker, the p100 latency is within `Scenario::max_latency()`. Default
  is 30 s; ops with zero samples are skipped.

**What these tests do _not_ check:**

- Specific pruning-strategy behaviour (e.g. "strategy D picked the LRU idle
  handle") — belongs in targeted integration tests in `tests/fuse_tests/`.
- Throughput — belongs in the `benchmark/` crate. Latencies are tracked here
  as a fairness signal, not a throughput benchmark.
- Data integrity — deferred; the test-object bytes are not verified on read.

Worker counts for write-heavy scenarios are sized so per-part upload
reservations stay well within the configured memory budget at steady state
(at most ~48 concurrent writers × 8 MiB parts = 384 MiB against a 512 MiB
limit).

## Platform

Linux x86_64 on our self-hosted runners; macOS via macFUSE works for local
smoke runs. Recommended CI runner class:
`[self-hosted, linux, x64, nvme-high-performance]` (same class as
`.github/workflows/bench.yml`). GitHub-hosted runners are likely to be
network-bound and produce misleading results.

## Prerequisites

AWS credentials with read/write access to an S3 bucket, and the following
environment variables (same as the standard integration tests):

| Variable                | Required | Description                                         |
|-------------------------|----------|-----------------------------------------------------|
| `S3_BUCKET_NAME`        | yes      | Bucket to use for test objects and ephemeral keys   |
| `S3_REGION`             | yes      | Region of the bucket                                |
| `S3_BUCKET_TEST_PREFIX` | no       | Defaults to `mountpoint-test/`; must end in `/`     |
| AWS credentials         | yes      | Standard AWS SDK resolution: `AWS_PROFILE`, static `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, instance role, etc. |

Optional stress-specific variables:

| Variable                       | Default | Purpose                                                                |
|--------------------------------|---------|------------------------------------------------------------------------|
| `STRESS_DURATION_SECS`         | 30      | Per-scenario duration in seconds                                       |
| `STRESS_WATCHDOG_DISABLE`      | unset   | If set to any value, disables the stall watchdog                       |

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
then `PutObject`. These objects are **never deleted** by the harness.

The harness enables `allow_delete` and `allow_overwrite` on the mount so
scenarios can best-effort `remove_file` their ephemeral objects through the
mount and so a new run does not hit `EPERM` on a key that a prior run wrote.
Writer scenarios still namespace their ephemeral keys with a per-run nonce
from `test_objects::ephemeral_run_id()` for readability, not correctness.

## How to run

The `stress_tests` feature transitively enables `s3_tests`, so no extra
feature flag is needed to pull in the real-S3 test session helpers.

A single scenario locally (macOS with `AWS_PROFILE`):

```
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CREDENTIAL_EXPIRATION
export AWS_PROFILE=personal AWS_REGION=us-east-1 S3_REGION=us-east-1
export S3_BUCKET_NAME=your-bench-bucket STRESS_DURATION_SECS=15

cargo test --release \
    --package mountpoint-s3-fs --test mod --features stress_tests \
    stress_tests::smoke::smoke -- --ignored --nocapture --test-threads=1
```

A single scenario via nextest:

```
cargo nextest run \
    --features stress_tests \
    --package mountpoint-s3-fs \
    stress_tests::sustained_reads \
    --run-ignored only \
    --success-output final --failure-output final
```

`--success-output final` and `--failure-output final` are required if you
want to see the end-of-run tracing output (worker op table and global
metrics dump) in nextest's default formatter, since nextest captures
stdout/stderr by default and only shows them in the summary when configured.

All scenarios sequentially:

```
cargo nextest run \
    --features stress_tests \
    --package mountpoint-s3-fs \
    'stress_tests::' \
    --run-ignored only --test-threads=1 \
    --success-output final --failure-output final
```

A ~20 min full run (four scenarios × 5 min each, matching the current CI budget):

```
STRESS_DURATION_SECS=300 cargo nextest run \
    --features stress_tests \
    --package mountpoint-s3-fs \
    'stress_tests::' \
    --run-ignored only --test-threads=1 \
    --success-output final --failure-output final
```

The `#[ignore]` attribute on each test is intentional: it keeps these tests
out of the default `cargo test` path. Always pass `--run-ignored only`.

## Scenarios

| Name               | Workers   | What it does                                                                       |
|--------------------|-----------|------------------------------------------------------------------------------------|
| `smoke`            | 1         | Reads a 4 KiB object in a loop. Sanity-check that the harness works.               |
| `sustained_reads`  | 32        | Front-to-back-read the shared 1 GiB test object, re-open on EOF.                   |
| `sustained_writes` | 48        | Write 50–200 MiB objects and delete them.                                          |
| `mixed_rw`         | 16 + 24   | 16 readers against the shared 1 GiB test object + 24 writers in the same session.  |
| `churn`            | 48 + 8    | 48 short-lived readers over 100 shared small test objects + 8 idle-held handles.   |

The idle workers in `churn` each perform a 1 MiB read after opening a handle
before going idle, so the handle has a prefetcher reservation registered with
the memory limiter and is a valid pruning candidate while it sits idle.

All scenarios set `mem_limit = MINIMUM_MEM_LIMIT` (512 MiB) via
`TestSessionConfig::with_mem_limit`.

## End-of-run output

At teardown the harness prints (via `tracing::info!`):

1. A **worker op latency table** built from merged per-worker
   `OpLatencies` HDR histograms — one line per op with count and
   p50/p90/p99/p100 in ms, e.g.

   ```
   === stress [sustained_reads] worker op latencies ===
   op=open count=96 p50=475.6ms p90=536.6ms p99=536.6ms p100=536.6ms
   op=read count=9664 p50=965.1ms p90=1264.6ms p99=1612.8ms p100=1946.6ms
   op=write count=0
   op=close count=64 p50=0.4ms p90=17.7ms p99=30.0ms p100=30.0ms
   ```

2. A **global metrics snapshot** from the `HdrRecorder` (HDR-backed
   replacement for `TestRecorder` that ingests every `metrics::` emission
   from production code during the run). Histograms are printed as raw HDR
   values (unit varies per metric — bytes / µs / MiB / count — infer from
   the key name); counters and gauges print directly. Gauge lines include
   both the current value and the true peak from the gauge's HDR history.

## Adding a new scenario

1. Create `tests/stress_tests/my_scenario.rs`.
2. Implement `crate::stress_tests::harness::Scenario`: at minimum `name`,
   `num_workers`, `session_config`, and `run_worker`. Override
   `max_idle_duration`, `max_latency`, `s3_path_override`,
   `peak_reserved_ceiling_bytes`, and `setup` only if the defaults don't fit.
3. `run_worker` has the signature

   ```rust
   fn run_worker(
       &self,
       worker_id: usize,
       mount_path: &Path,
       progress: &AtomicU64,
       latencies: &mut OpLatencies,
       stop: &AtomicBool,
   );
   ```

   Wrap every file-system operation in `latencies.time(Op::_, || ...)` so
   the harness can aggregate per-op latency distributions. For example:

   ```rust
   let mut file = latencies
       .time(Op::Open, || File::open(&path))
       .unwrap_or_else(|e| panic!("open failed: {e:?}"));
   let n = latencies
       .time(Op::Read, || file.read(&mut buf))
       .unwrap_or_else(|e| panic!("read failed: {e:?}"));
   latencies.time(Op::Close, || drop(file));
   ```

4. If the scenario needs a shared test object, add a helper in
   `tests/stress_tests/test_objects.rs` (HEAD + `PutObject`-if-missing),
   return `Some(test_objects::shared_s3_path())` from `s3_path_override`, and
   upload-if-missing from `setup()`.
5. Add a `#[test] #[ignore = "stress test; run with --run-ignored only"]`
   entry point that calls `harness::run(MyScenario)`.
6. Register the module in `tests/stress_tests/mod.rs`.
7. `cargo check --features stress_tests --tests` — it should compile.
8. Run it locally at a short duration
   (`STRESS_DURATION_SECS=30 cargo test --release ...`) before proposing for
   review.

Worker bodies should:

- Increment the progress counter on every unit of work. The watchdog needs
  at least one increment per `max_idle_duration(worker_id)`.
- Writers: bump progress on every successful `File::create`, every
  `write_all` call, and on `drop(file)` so the long flush/close path still
  shows up as liveness.
- Readers / open-read-close loops: bump progress on every successful open in
  addition to the bytes read.
- Time every op via `latencies.time(Op::_, || ...)`. The `p100` assertion is
  aggregated across workers per op.
- Check `stop.load(Ordering::Relaxed)` frequently and return when set.
- **Not** tolerate errors from `File::open`/`File::create`/`read`/`write`.
  Scenarios here are sized so these calls should always succeed against a
  healthy session; any failure is a test-design or memory-limiter bug and
  should panic so the failure is visible.

## CI

- `.github/workflows/stress.yml` — reusable, matrix over the four scenarios,
  runner `[self-hosted, linux, x64, nvme-high-performance]`, bucket
  `S3_BENCH_BUCKET_NAME`, prefix `S3_BUCKET_BENCH_PREFIX`,
  `STRESS_DURATION_SECS=300`. Approval gate via the `PR stress` environment
  for PR runs; no gate on push. Triggers: `workflow_call`,
  `push: [main]`, `workflow_dispatch`.
- `.github/workflows/stress_pr.yml` — triggers `stress.yml` when a PR is
  labelled `stress`.
- `RUST_LOG=info,awscrt=warn` is set at the workflow level so the end-of-run
  tracing dump is visible while keeping the CRT's TLS-init info line out of
  nextest's test-listing output (nextest's list parser rejects any line that
  doesn't end in `: test` or `: benchmark`).

The non-stress workflows on this fork are currently disabled to
`workflow_dispatch: only` for fork-development ergonomics — revert before
merging back upstream.

There is no scheduled / nightly run; the workflow triggers on push to `main`
and on manual dispatch. On upstream, the `stress` label on a PR remains the
primary trigger.

## Known limitations

- The watchdog logs per-worker progress counters on stall but does **not**
  capture backtraces of other threads. Use a debugger or core dumps if you
  need more detail.
- `sum(mem.bytes_reserved)` peak is computed as the sum of per-area gauge-
  history maxes. Because per-area peaks may occur at different instants,
  this is an upper bound on the true `sum` peak, which is exactly what we
  want for an "at or below ceiling" invariant — never false-passing a
  violation.
- Gauge history records per-mutation (not time-weighted) — the histogram's
  percentiles reflect the distribution of observed reservation sizes across
  all mutation events, not the distribution over wall-clock time.
