//! Shared, reusable stress test objects.
//!
//! These objects live at a stable, non-nonced S3 prefix (`<S3_BUCKET_TEST_PREFIX>stress-fixtures/`)
//! and are uploaded on demand (HEAD, then `PutObject` only if missing). They are never deleted —
//! multiple stress runs reuse the same objects to avoid paying the upload cost on every run.
//!
//! Scenarios that need a shared test object mount directly at the `stress-fixtures/` prefix
//! by overriding [`crate::stress_tests::harness::Scenario::s3_path_override`] with
//! [`shared_s3_path`].

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use mountpoint_s3_fs::s3::{Bucket, Prefix, S3Path};

use crate::common::s3::{get_test_bucket, get_test_region, get_test_sdk_client};
use crate::common::tokio_block_on;

/// Stable suffix appended to `S3_BUCKET_TEST_PREFIX` for shared stress test objects.
const SHARED_PREFIX_SUFFIX: &str = "stress-fixtures/";

/// Key (inside the shared prefix) for the 1 GiB read object used by `sustained_reads`
/// and `mixed_rw`.
pub const READ_OBJECT_KEY: &str = "read_1gib.bin";

/// Size of the shared 1 GiB read object.
pub const READ_OBJECT_SIZE: usize = 1024 * 1024 * 1024;

/// Number of small shared objects used by `churn`.
pub const SMALL_SET_COUNT: usize = 100;

/// Size of each small shared object.
pub const SMALL_SET_SIZE: usize = 128 * 1024;

/// Key format for entries in the shared small-object set used by `churn`.
pub fn small_object_key(index: usize) -> String {
    format!("small_{index:04}.bin")
}

/// Return the shared prefix (e.g. `mountpoint-test/stress-fixtures/`).
fn shared_prefix_string() -> String {
    let base = std::env::var("S3_BUCKET_TEST_PREFIX").unwrap_or_else(|_| String::from("mountpoint-test/"));
    assert!(base.ends_with('/'), "S3_BUCKET_TEST_PREFIX should end in '/'");
    format!("{base}{SHARED_PREFIX_SUFFIX}")
}

/// An `S3Path` pointing at the shared stress-fixtures prefix.
///
/// Scenarios that mount at this path can read the shared objects by their short key
/// (`READ_OBJECT_KEY`, `small_object_key(i)`), and should namespace any ephemeral writes
/// so they cannot collide with the shared keys.
pub fn shared_s3_path() -> S3Path {
    let bucket = get_test_bucket();
    let prefix = shared_prefix_string();
    S3Path::new(Bucket::new(bucket).unwrap(), Prefix::new(&prefix).unwrap())
}

/// Upload the 1 GiB shared read object if it is not already present.
pub fn ensure_read_object() {
    let payload = vec![0xA5u8; READ_OBJECT_SIZE];
    ensure_object(READ_OBJECT_KEY, &payload);
}

/// Upload the 100 small shared objects if they are not already present.
pub fn ensure_small_set() {
    let payload = vec![0x5Au8; SMALL_SET_SIZE];
    for i in 0..SMALL_SET_COUNT {
        ensure_object(&small_object_key(i), &payload);
    }
}

fn ensure_object(key: &str, payload: &[u8]) {
    let bucket = get_test_bucket();
    let region = get_test_region();
    let full_key = format!("{}{}", shared_prefix_string(), key);

    tokio_block_on(async {
        let client: Client = get_test_sdk_client(&region).await;
        if object_exists(&client, &bucket, &full_key).await {
            tracing::debug!(bucket, key = %full_key, "stress: shared test object already present");
            return;
        }
        tracing::info!(
            bucket,
            key = %full_key,
            size = payload.len(),
            "stress: uploading shared test object"
        );
        client
            .put_object()
            .bucket(&bucket)
            .key(&full_key)
            .body(ByteStream::from(payload.to_vec()))
            .send()
            .await
            .expect("failed to upload shared stress test object");
    });
}

async fn object_exists(client: &Client, bucket: &str, key: &str) -> bool {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => true,
        Err(e) => {
            let service_err = e.into_service_error();
            if matches!(service_err, HeadObjectError::NotFound(_)) {
                false
            } else {
                panic!("HEAD failed for s3://{bucket}/{key}: {service_err:?}");
            }
        }
    }
}
