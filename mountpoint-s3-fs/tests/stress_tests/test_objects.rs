//! Shared, reusable stress test objects.
//!
//! These objects live at a stable, non-nonced S3 prefix
//! (`<S3_BUCKET_TEST_PREFIX>shared-stress-test-objects/`) and are uploaded on demand via
//! Mountpoint's own [`Uploader`] if missing or the wrong size. They are never deleted —
//! multiple stress runs reuse the same objects to avoid paying the upload cost on every run.
//!
//! Scenarios mount at the `<S3_BUCKET_TEST_PREFIX>` root and read shared objects via the
//! `shared-stress-test-objects/<key>` relative path under the mount.

use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use mountpoint_s3_client::S3CrtClient;
use mountpoint_s3_client::config::S3ClientConfig;
use mountpoint_s3_fs::Runtime;
use mountpoint_s3_fs::mem_limiter::{MINIMUM_MEM_LIMIT, MemoryLimiter};
use mountpoint_s3_fs::memory::PagedPool;
use mountpoint_s3_fs::upload::{Uploader, UploaderConfig};

use crate::common::s3::{get_test_bucket, get_test_endpoint_config, get_test_region, get_test_sdk_client};
use crate::common::tokio_block_on;

/// Stable suffix appended to `S3_BUCKET_TEST_PREFIX` for shared stress test objects.
const SHARED_PREFIX_SUFFIX: &str = "shared-stress-test-objects/";

/// Path (relative to the mount) under which shared test objects are visible. Scenarios join
/// this with a key from [`LARGE_OBJECT_KEY`] / [`small_object_key`] to open a shared object
/// through the mount.
pub const SHARED_MOUNT_PREFIX: &str = SHARED_PREFIX_SUFFIX;

/// Key (inside the shared prefix) for the large read object used by `sustained_reads`
/// and `mixed_rw`.
pub const LARGE_OBJECT_KEY: &str = "read_100gib.bin";

/// Size of the shared large read object (100 GiB).
pub const LARGE_OBJECT_SIZE: usize = 100 * 1024 * 1024 * 1024;

/// Number of small shared objects used by `churn`.
pub const SMALL_SET_COUNT: usize = 100;

/// Size of each small shared object.
pub const SMALL_SET_SIZE: usize = 128 * 1024;

/// Key format for entries in the shared small-object set used by `churn`.
pub fn small_object_key(index: usize) -> String {
    format!("small_{index:04}.bin")
}

/// A process-wide, per-run nonce for ephemeral writer keys.
///
/// Writer scenarios must namespace their ephemeral output so it cannot collide with either
/// the shared test objects, leftover objects from prior runs, or other concurrent runs. Use
/// [`ephemeral_key`] to construct a namespaced flat key (no '/' → no `mkdir` through the
/// mount required).
pub fn ephemeral_run_id() -> &'static str {
    use std::sync::OnceLock;
    static RUN_ID: OnceLock<String> = OnceLock::new();
    RUN_ID.get_or_init(|| {
        // Seed a short, collision-resistant-enough id from the system clock + pid.
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{:016x}-{}", now_ns, std::process::id())
    })
}

/// Build a flat (no `/`) ephemeral key for writer scenarios, namespaced by the per-run nonce
/// and scenario name so concurrent runs, iterations, and scenarios cannot collide, and so
/// writers don't need to `mkdir` intermediate prefixes through the mount.
pub fn ephemeral_key(scenario: &str, suffix: &str) -> String {
    format!("ephemeral_{}_{scenario}_{suffix}", ephemeral_run_id())
}

/// Return the shared prefix (e.g. `mountpoint-test/shared-stress-test-objects/`).
fn shared_prefix_string() -> String {
    let base = std::env::var("S3_BUCKET_TEST_PREFIX").unwrap_or_else(|_| String::from("mountpoint-test/"));
    assert!(base.ends_with('/'), "S3_BUCKET_TEST_PREFIX should end in '/'");
    format!("{base}{SHARED_PREFIX_SUFFIX}")
}

/// Upload the given `(key, size)` shared test objects if they are not already present at the
/// expected size. All objects are filled with [`TEST_OBJECT_FILL_BYTE`] and share a single
/// `Uploader<S3CrtClient>` whose part size is sized off the largest object in the list, so
/// the caller gets one CRT client setup per call regardless of how many objects are uploaded.
pub fn ensure_shared_objects(objects: &[(&str, usize)]) {
    if objects.is_empty() {
        return;
    }
    let max_size = objects.iter().map(|(_, s)| *s).max().unwrap_or(0);
    let (uploader, bucket, part_size) = build_test_object_uploader(max_size);
    for (key, size) in objects {
        upload_test_object_with_uploader(&uploader, &bucket, part_size, key, *size);
    }
}

async fn head_object_size(client: &Client, bucket: &str, key: &str) -> Option<usize> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(head) => Some(head.content_length().expect("HEAD response missing content_length") as usize),
        Err(e) => {
            let service_err = e.into_service_error();
            if matches!(service_err, HeadObjectError::NotFound(_)) {
                None
            } else {
                panic!("HEAD failed for s3://{bucket}/{key}: {service_err:?}");
            }
        }
    }
}

const DEFAULT_WRITE_PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_PARTS: u64 = 10_000;
const TEST_OBJECT_FILL_BYTE: u8 = 0xA5;

/// Memory budget for the test object uploader: 95% of total system memory, floored at
/// `MINIMUM_MEM_LIMIT`. Matches the pattern used by Mountpoint.
fn compute_test_object_mem_budget() -> u64 {
    use sysinfo::{RefreshKind, System};
    let sys = System::new_with_specifics(RefreshKind::everything());
    let ninety_five_pct = ((sys.total_memory() as f64) * 0.95) as u64;
    ninety_five_pct.max(MINIMUM_MEM_LIMIT)
}

/// Build an `Uploader<S3CrtClient>` for shared test object uploads. Part size is raised above
/// `DEFAULT_WRITE_PART_SIZE` if needed so `max_object_size` fits within the 10,000-part MPU
/// cap. Returns the uploader, the test bucket name, and the chosen part size (so the write
/// loop can reuse a single part-sized buffer).
///
/// Built with its own `PagedPool` and `MemoryLimiter` so test object upload is independent of
/// whatever memory limit the scenario under test has configured.
fn build_test_object_uploader(max_object_size: usize) -> (Uploader<S3CrtClient>, String, usize) {
    let bucket = get_test_bucket();
    let min_part_size = (max_object_size as u64).div_ceil(MAX_PARTS) as usize;
    let part_size = DEFAULT_WRITE_PART_SIZE.max(min_part_size);

    let pool = PagedPool::new_with_candidate_sizes([part_size]);
    let mem_limiter = Arc::new(MemoryLimiter::new(pool.clone(), compute_test_object_mem_budget()));

    let client_config = S3ClientConfig::default()
        .part_size(part_size)
        .endpoint_config(get_test_endpoint_config())
        .memory_pool(pool.clone());
    let client = S3CrtClient::new(client_config).expect("failed to build S3CrtClient for test object upload");
    let runtime = Runtime::new(client.event_loop_group());
    let uploader = Uploader::new(client, runtime, pool, mem_limiter, UploaderConfig::new(part_size));

    (uploader, bucket, part_size)
}

/// HEAD-skip-if-size-matches, else stream-upload a shared test object of `size` bytes (all
/// [`TEST_OBJECT_FILL_BYTE`]) at `<shared prefix>/<key>` via Mountpoint's own [`Uploader`].
fn upload_test_object_with_uploader(
    uploader: &Uploader<S3CrtClient>,
    bucket: &str,
    part_size: usize,
    key: &str,
    size: usize,
) {
    let full_key = format!("{}{}", shared_prefix_string(), key);

    // HEAD via a fresh SDK client: cheap per call, and uses the same credential chain as the
    // rest of the test helpers.
    let region = get_test_region();
    let present = tokio_block_on(async {
        let client: Client = get_test_sdk_client(&region).await;
        head_object_size(&client, bucket, &full_key).await == Some(size)
    });
    if present {
        tracing::debug!(bucket, key = %full_key, "stress: shared test object already present");
        return;
    }

    tracing::info!(
        bucket,
        key = %full_key,
        size,
        part_size,
        "stress: uploading shared test object"
    );

    tokio_block_on(async {
        let mut request = uploader
            .start_atomic_upload(bucket.to_string(), full_key.clone())
            .unwrap_or_else(|e| panic!("failed to start MPU for s3://{bucket}/{full_key}: {e:?}"));

        let buf = vec![TEST_OBJECT_FILL_BYTE; part_size];
        let mut offset = 0u64;
        while offset < size as u64 {
            let remaining = size as u64 - offset;
            let chunk = remaining.min(part_size as u64) as usize;
            let written = request
                .write(offset as i64, &buf[..chunk])
                .await
                .unwrap_or_else(|e| panic!("write failed at offset {offset} for s3://{bucket}/{full_key}: {e:?}"));
            offset += written as u64;
        }

        request
            .complete()
            .await
            .unwrap_or_else(|e| panic!("failed to complete MPU for s3://{bucket}/{full_key}: {e:?}"));
    });
}
