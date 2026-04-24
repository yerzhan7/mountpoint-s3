//! Long-running stress tests for the memory limiter.
//!
//! See `README.md` in this directory for scope, runbook, and how to add new scenarios.
//!
//! All tests in this module are marked `#[ignore]` and are only compiled when the
//! `stress_tests` feature is enabled (which transitively enables `s3_tests`).

mod harness;
mod scenarios;
mod test_objects;
