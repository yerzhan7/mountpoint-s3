//! Long-running stress tests.
#![cfg(feature = "stress_tests")]

#[path = "common/mod.rs"]
mod common;

#[path = "stress_tests/harness/mod.rs"]
mod harness;
#[path = "stress_tests/scenarios/mod.rs"]
mod scenarios;
#[path = "stress_tests/test_objects.rs"]
mod test_objects;
#[path = "stress_tests/workers/mod.rs"]
mod workers;
