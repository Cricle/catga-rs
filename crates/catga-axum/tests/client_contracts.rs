//! Contract tests for the outgoing HTTP clients: correlation/trace header
//! propagation and cluster forwarding with bounded response decoding.

#[path = "common/server.rs"]
mod server;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{Json, Router, routing::post};
use catga_axum::{CORRELATION_ID_HEADER, CorrelationHttpClient, HttpClusterForwarder};
use catga_core::{
    CatgaResult, ClusterForwarder, ErrorCode, Message, Request, scope_correlation_value,
};
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetBalance {
    account: u64,
}
impl Message for GetBalance {}
impl Request for GetBalance {
    type Response = u64;
}

#[tokio::test]
async fn correlation_client_stamps_ambient_headers_and_preserves_explicit_ones() {
    let client = CorrelationHttpClient::new(reqwest::Client::new());

    let request = scope_correlation_value("corr-7".into(), async {
        client
            .post("http://localhost/orders", HeaderMap::new())
            .build()
    })
    .await
    .expect("request builds");
    assert_eq!(request.headers()[CORRELATION_ID_HEADER], "corr-7");

    // An explicit caller header wins over the ambient scope.
    let mut headers = HeaderMap::new();
    headers.insert(
        CORRELATION_ID_HEADER,
        "explicit".parse().expect("valid header value"),
    );
    let request = scope_correlation_value("corr-ambient".into(), async {
        client
            .request(reqwest::Method::POST, "http://localhost/orders", headers)
            .build()
    })
    .await
    .expect("request builds");
    assert_eq!(request.headers()[CORRELATION_ID_HEADER], "explicit");

    // Outside any scope no header is invented.
    let request = client
        .post("http://localhost/orders", HeaderMap::new())
        .build()
        .expect("request builds");
    assert!(request.headers().get(CORRELATION_ID_HEADER).is_none());
}

async fn leader_server(
    response_status: StatusCode,
    body: String,
) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().route(
        "/api/catga/forward/GetBalance",
        post(move || {
            let body = body.clone();
            async move { (response_status, body) }
        }),
    );
    server::spawn_app(app).await
}

#[tokio::test]
async fn forwarder_decodes_typed_responses_from_the_leader() {
    let (base, server) = leader_server(StatusCode::OK, "900".to_string()).await;
    let forwarder = HttpClusterForwarder::new(reqwest::Client::new());

    let balance = forwarder
        .forward(GetBalance { account: 1 }, &base)
        .await
        .expect("forward succeeds");
    assert_eq!(balance, 900);

    server.abort();
}

#[tokio::test]
async fn forwarder_maps_failures_to_transient_errors() {
    // Non-success leader status.
    let (base, server) = leader_server(StatusCode::SERVICE_UNAVAILABLE, String::new()).await;
    let forwarder = HttpClusterForwarder::new(reqwest::Client::new());
    let error = forwarder
        .forward(GetBalance { account: 1 }, &base)
        .await
        .expect_err("non-success status must fail");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("503"));
    server.abort();

    // A body that is not the typed JSON response.
    let (base, server) = leader_server(StatusCode::OK, "not json".to_string()).await;
    let error = forwarder
        .forward(GetBalance { account: 1 }, &base)
        .await
        .expect_err("invalid JSON must fail");
    assert_eq!(error.code(), ErrorCode::Transient);
    server.abort();

    // An unreachable leader.
    let error = forwarder
        .forward(GetBalance { account: 1 }, "http://127.0.0.1:1")
        .await
        .expect_err("connect failure must fail");
    assert_eq!(error.code(), ErrorCode::Transient);
}

#[tokio::test]
async fn forwarder_enforces_the_response_body_limit_while_streaming() {
    let oversized = "9".repeat(64 * 1024);
    let (base, server) = leader_server(StatusCode::OK, format!("\"{oversized}\"")).await;
    let forwarder = HttpClusterForwarder::with_response_limit(
        reqwest::Client::new(),
        NonZeroUsize::new(1024).expect("nonzero"),
    );
    let error = forwarder
        .forward(GetBalance { account: 1 }, &base)
        .await
        .expect_err("oversized body must fail");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("exceeds the configured limit"));
    server.abort();
}

#[tokio::test]
async fn forwarder_supports_custom_path_prefixes() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&hits);
    let app = Router::new().route(
        "/cluster/lead/GetBalance",
        post(move |Json(request): Json<GetBalance>| {
            let counted = Arc::clone(&counted);
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Json(request.account * 2)
            }
        }),
    );
    let (base, server) = server::spawn_app(app).await;

    let forwarder =
        HttpClusterForwarder::new(reqwest::Client::new()).with_path_prefix("/cluster/lead");
    let balance: CatgaResult<u64> = forwarder.forward(GetBalance { account: 5 }, &base).await;
    assert_eq!(balance.expect("forward succeeds"), 10);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // A trailing slash on the leader endpoint does not double the path slash.
    let balance = forwarder
        .forward(GetBalance { account: 6 }, &format!("{base}/"))
        .await
        .expect("forward succeeds");
    assert_eq!(balance, 12);

    server.abort();
}
