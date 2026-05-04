//! Long-running stress tests.
#![cfg(feature = "stress_tests")]

#[path = "common/mod.rs"]
mod common;

#[path = "stress_tests"]
mod _stress_tests {
    pub mod harness;
    pub mod scenarios;
    pub mod test_objects;
    pub mod workers;
}
use _stress_tests::{harness, test_objects, workers};
