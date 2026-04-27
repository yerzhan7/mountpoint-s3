//! Stress-test scenarios. Each submodule implements one [`super::harness::Scenario`]
//! and exposes a `#[test] #[ignore]` entry point.

mod common;
mod idle_and_churn;
mod mixed_rw;
mod smoke;
mod sustained_reads;
mod sustained_writes;
