//! Settings for the experimental FUSE-over-io_uring transport.
//!
//! The transport is off by default and configured through environment variables, following the same
//! pattern as the other unstable FUSE knobs. It is read in two places that cannot easily share a
//! value: [`crate::fs::S3Filesystem::init`], which must request the capability and cap `max_write`
//! during FUSE_INIT, and [`crate::fuse::session::FuseSession`], which spawns the ring workers.

/// Payload buffer size to target when io_uring is enabled.
///
/// The kernel sizes ring entry payload buffers as
/// `max(8 KiB, max_write, max_pages * PAGE_SIZE)`, and every entry needs its own buffer. With one
/// entry per CPU per worker thread, the default 16 MiB `max_write` would need tens of gigabytes, so
/// we cap `max_write` at the largest READ the kernel will ever issue (`max_pages` is clamped to 256
/// pages, i.e. 1 MiB). Reads are unaffected; writes are split into more, smaller requests.
pub const DEFAULT_MAX_WRITE: u32 = 1024 * 1024;

const ENV_ENABLED: &str = "UNSTABLE_MOUNTPOINT_FUSE_IO_URING";
const ENV_THREADS_PER_QUEUE: &str = "UNSTABLE_MOUNTPOINT_FUSE_IO_URING_THREADS_PER_QUEUE";
const ENV_ENTRIES_PER_THREAD: &str = "UNSTABLE_MOUNTPOINT_FUSE_IO_URING_ENTRIES_PER_THREAD";
const ENV_PIN_THREADS: &str = "UNSTABLE_MOUNTPOINT_FUSE_IO_URING_PIN_THREADS";
const ENV_MAX_WRITE_KIB: &str = "UNSTABLE_MOUNTPOINT_FUSE_IO_URING_MAX_WRITE_KIB";

/// How the io_uring transport should be configured for this mount.
#[derive(Debug, Clone, Copy)]
pub struct UringSettings {
    pub enabled: bool,
    pub threads_per_queue: usize,
    pub entries_per_thread: usize,
    /// Pin each worker to the CPU whose queue it serves. Measured to be a large loss on buffered
    /// reads, see [`fuser::uring::UringConfig::pin_threads`].
    pub pin_threads: bool,
    /// `max_write` to negotiate in FUSE_INIT, which determines the ring payload buffer size.
    pub max_write: u32,
}

impl Default for UringSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            threads_per_queue: 2,
            entries_per_thread: 1,
            pin_threads: false,
            max_write: DEFAULT_MAX_WRITE,
        }
    }
}

impl UringSettings {
    /// Read the settings from the environment. Unparseable values fall back to the default and are
    /// reported, since a typo silently disabling the feature would be hard to notice.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        let enabled = bool_from_env(ENV_ENABLED).unwrap_or(defaults.enabled);
        if !enabled {
            return defaults;
        }
        Self {
            enabled,
            threads_per_queue: usize_from_env(ENV_THREADS_PER_QUEUE)
                .filter(|value| *value > 0)
                .unwrap_or(defaults.threads_per_queue),
            entries_per_thread: usize_from_env(ENV_ENTRIES_PER_THREAD)
                .filter(|value| *value > 0)
                .unwrap_or(defaults.entries_per_thread),
            pin_threads: bool_from_env(ENV_PIN_THREADS).unwrap_or(defaults.pin_threads),
            max_write: usize_from_env(ENV_MAX_WRITE_KIB)
                .filter(|value| *value > 0)
                .map(|kib| (kib * 1024) as u32)
                .unwrap_or(defaults.max_write),
        }
    }
}

fn bool_from_env(key: &str) -> Option<bool> {
    let value = std::env::var(key).ok()?;
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        other => {
            tracing::warn!("ignoring {key}={other}: expected a boolean");
            None
        }
    }
}

fn usize_from_env(key: &str) -> Option<usize> {
    let value = std::env::var(key).ok()?;
    match value.trim().parse() {
        Ok(parsed) => Some(parsed),
        Err(_) => {
            tracing::warn!("ignoring {key}={value}: expected a positive integer");
            None
        }
    }
}
