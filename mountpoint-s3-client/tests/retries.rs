//! Tests for retries, against a local HTTP server that throttles every request.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::join_all;
use http_body_util::Full;
use hyper::Response;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use mountpoint_s3_client::config::{
    AddressingStyle, Allocator, EndpointConfig, S3ClientAuthConfig, S3ClientConfig, Uri,
};
use mountpoint_s3_client::types::HeadObjectParams;
use mountpoint_s3_client::{ObjectClient, S3CrtClient};
use test_case::test_case;
use tokio::net::TcpListener;

const REQUESTS: usize = 150;

/// Spawn an HTTP listener on a random loopback port that responds to every request with a 503.
/// Returns its bound address and the number of requests it has received.
async fn spawn_throttling_server() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let requests = Arc::new(AtomicUsize::new(0));

    let requests_for_server = requests.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let requests = requests_for_server.clone();
            tokio::spawn(async move {
                let service = service_fn(move |_req| {
                    requests.fetch_add(1, Ordering::SeqCst);
                    async { Response::builder().status(503).body(Full::new(Bytes::new())) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    (addr, requests)
}

fn make_client(endpoint: SocketAddr, max_attempts: usize) -> S3CrtClient {
    let endpoint_uri = Uri::new_from_str(&Allocator::default(), format!("http://{endpoint}")).expect("parse uri");
    let endpoint_config = EndpointConfig::new("us-east-1")
        .addressing_style(AddressingStyle::Path)
        .endpoint(endpoint_uri);
    let client_config = S3ClientConfig::new()
        .endpoint_config(endpoint_config)
        .auth_config(S3ClientAuthConfig::NoSigning)
        .max_attempts(NonZeroUsize::new(max_attempts).unwrap());
    S3CrtClient::new(client_config).expect("build S3CrtClient")
}

/// Send many requests that all fail, and count their retries. With two attempts, each request is
/// retried at most once, within 500ms. The retry quota starts with 500 tokens and a retry after a
/// 503 costs 5, so it allows about 100 retries while every request fails.
#[test_case(1, 0..=0; "one attempt")]
#[test_case(2, 1..=REQUESTS - 1; "two attempts")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_retries_of_throttled_requests(max_attempts: usize, expected_retries: RangeInclusive<usize>) {
    let (addr, received) = spawn_throttling_server().await;
    let client = make_client(addr, max_attempts);

    let results = join_all((0..REQUESTS).map(|i| {
        let client = &client;
        async move {
            client
                .head_object("test-bucket", &format!("key{i}"), &HeadObjectParams::new())
                .await
        }
    }))
    .await;
    for result in results {
        result.expect_err("every request is throttled");
    }

    let retries = received.load(Ordering::SeqCst) - REQUESTS;
    assert!(
        expected_retries.contains(&retries),
        "expected {expected_retries:?} retries, but {retries} were sent"
    );
}
