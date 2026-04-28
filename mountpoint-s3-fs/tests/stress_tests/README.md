# Stress Tests

Long-running tests that sustain concurrent load against real S3 to shake out
deadlocks, per-worker stalls, tail-latency regressions, and memory issues.

## What it asserts

- No file I/O errors.
- Aggregated per file I/O p100 latency is within `Scenario::max_latency(op)`.
- No per-worker stall longer than `Scenario::max_idle_duration(worker_id)`.
- At teardown, every reservation gauge is back to zero — for each label of
  `mem.bytes_reserved{area=*}` and `pool.reserved_bytes{kind=*}`.

## Environment variables

| Variable                | Required | Description                                         |
|-------------------------|----------|-----------------------------------------------------|
| `S3_BUCKET_NAME`        | yes      | Bucket to use for test objects and ephemeral keys   |
| `S3_REGION`             | yes      | Region of the bucket                                |
| `S3_BUCKET_TEST_PREFIX` | no       | Defaults to `mountpoint-test/`; must end in `/`     |
| AWS credentials         | yes      | Standard SDK resolution (`AWS_PROFILE`, static keys, instance role, etc.) |
| `STRESS_DURATION_SECS`  | no       | Per-scenario duration in seconds; default 30        |

## How to run

The `stress_tests` feature transitively enables `s3_tests`, so no extra
feature flag is needed. Tests are `#[ignore]` by default — always pass
`--run-ignored only`.

A single scenario:

```
cargo nextest run --release \
    --features stress_tests \
    --package mountpoint-s3-fs \
    'stress_tests::scenarios::sustained_reads' \
    --run-ignored only --success-output final --failure-output final
```

All scenarios sequentially:

```
cargo nextest run --release \
    --features stress_tests \
    --package mountpoint-s3-fs \
    'stress_tests::' \
    --run-ignored only --test-threads=1 \
    --success-output final --failure-output final
```

## Adding a new scenario

1. Create `tests/stress_tests/scenarios/my_scenario.rs`.
2. Implement `crate::stress_tests::harness::Scenario`: at minimum `name`,
   `num_workers`, `session_config`, and `run_worker`. Override
   `max_idle_duration`, `max_latency`, and `setup` only if the defaults
   don't fit.
3. In `run_worker`, wrap every file-system operation in
   `latencies.time(FileOp::_, || ...)` so the harness can aggregate per-op
   latency distributions. Use `FileOp::CloseRead` / `FileOp::CloseWrite`
   depending on the handle mode.
4. For shared read inputs, call
   `test_objects::ensure_shared_objects(&[(key, size)])` from `setup()` and
   open the file through the mount at
   `mount_path.join(test_objects::SHARED_OBJECTS_PREFIX).join(key)`. For
   ephemeral writes, use `test_objects::ephemeral_key(scenario, suffix)` to
   get a flat, per-run-nonced key that cannot collide with shared objects,
   prior runs, or concurrent runs.
5. Add a `#[test] #[ignore = "stress test; run with --run-ignored only"]`
   entry point that calls `harness::run(MyScenario)`.
6. Register the module in `tests/stress_tests/scenarios/mod.rs`.

Worker bodies must increment the `progress` counter frequently (the watchdog
polls it) and must check `stop.load(Ordering::Relaxed)` between ops. They
should panic on I/O errors — scenarios are sized so the operations always
succeed against a healthy session.
