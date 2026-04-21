//! Long-running stress tests for the memory limiter.
//!
//! See `README.md` in this directory for scope, runbook, and how to add new scenarios.
//!
//! All tests in this module are marked `#[ignore]` and are only compiled when the
//! `stress_tests` and `s3_tests` features are both enabled.

mod churn;
mod harness;
mod mixed_rw;
mod smoke;
mod sustained_reads;
mod sustained_writes;
