//! Contract tests for the outgoing HTTP clients: correlation/trace header
//! propagation, Raft transport failure classification, and cluster forwarding
//! with bounded response decoding.

#[path = "common/server.rs"]
mod server;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{Json, Router, routing::post};
use catga_axum::{
    CORRELATION_ID_HEADER, CorrelationHttpClient, HttpClusterForwarder, HttpRaftTransport,
};
use catga_cluster::{ClusterForwarder, RaftMember, RaftMessage, RaftTransport};
use catga_core::{
    CatgaResult, ErrorCode, Message, MessageTypeId, Request, scope_correlation_value,
};
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetBalance {
    account: u64,
}
impl Message for GetBalance {}
struct GetBalanceTypeId;
impl MessageTypeId for GetBalanceTypeId {
    const NAME: &'static str = "GetBalance";
}
impl Request for GetBalance {
    type Response = u64;
    type TypeId = GetBalanceTypeId;
}

fn transport(member_endpoint: &str) -> HttpRaftTransport {
    HttpRaftTransport::new(
        reqwest::Client::new(),
        vec![
            RaftMember::new(1, "http://node-1"),
            RaftMember::new(2, member_endpoint),
        ],
    )
}

fn frame_to(id: u64) -> RaftMessage {
    RaftMessage {
        to: id,
        ..Default::default()
    }
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

#[tokio::test]
async fn raft_transport_reports_unknown_peers_as_fatal() {
    let transport = transport("http://127.0.0.1:1");
    let error = transport
        .send(frame_to(9))
        .await
        .expect_err("unknown peer must fail");
    assert!(!error.is_retryable());
    assert!(error.to_string().contains("unknown Raft peer 9"));

    // Removing a member drops its route; re-adding restores delivery.
    transport.remove_member(2);
    let error = transport
        .send(frame_to(2))
        .await
        .expect_err("removed peer must fail");
    assert!(!error.is_retryable());
}

#[tokio::test]
async fn raft_transport_classifies_connect_failures_as_retryable() {
    // Port 1 is reserved and refuses connections on loopback.
    let transport = transport("http://127.0.0.1:1");
    let error = transport
        .send(frame_to(2))
        .await
        .expect_err("closed port must fail");
    assert!(error.is_retryable(), "connect failures are retryable");
}

#[tokio::test]
async fn raft_transport_maps_http_statuses_to_retryability() {
    for (status, retryable) in [
        (StatusCode::OK, true),
        (StatusCode::NO_CONTENT, true),
        (StatusCode::REQUEST_TIMEOUT, true),
        (StatusCode::TOO_EARLY, true),
        (StatusCode::TOO_MANY_REQUESTS, true),
        (StatusCode::BAD_GATEWAY, true),
        (StatusCode::SERVICE_UNAVAILABLE, true),
        (StatusCode::GATEWAY_TIMEOUT, true),
        (StatusCode::BAD_REQUEST, false),
        (StatusCode::FORBIDDEN, false),
        (StatusCode::INTERNAL_SERVER_ERROR, false),
    ] {
        let app = Router::new().route("/api/catga/raft", post(move || async move { status }));
        let (base, server) = server::spawn_app(app).await;
        let transport = transport(&base);
        let result = transport.send(frame_to(2)).await;
        if status.is_success() {
            result.unwrap_or_else(|error| panic!("{status} must succeed, got {error}"));
        } else {
            let error = result.expect_err("non-success status must fail");
            assert_eq!(
                error.is_retryable(),
                retryable,
                "status {status} retryability"
            );
        }
        server.abort();
    }
}

#[tokio::test]
async fn raft_transport_bounds_sends_with_the_request_timeout() {
    let app = Router::new().route(
        "/api/catga/raft",
        post(|| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            StatusCode::OK
        }),
    );
    let (base, server) = server::spawn_app(app).await;
    let transport = transport(&base).with_request_timeout(Duration::from_millis(50));

    let error = transport
        .send(frame_to(2))
        .await
        .expect_err("a stalled peer must time out");
    assert!(error.is_retryable(), "timeouts are retryable backpressure");

    server.abort();
}

#[tokio::test]
async fn raft_transport_attaches_the_peer_identity_header() {
    let observed = Arc::new(std::sync::Mutex::new(None::<String>));
    let captured = Arc::clone(&observed);
    let app = Router::new().route(
        "/api/catga/raft",
        post(move |headers: HeaderMap| {
            let captured = Arc::clone(&captured);
            async move {
                *captured.lock().expect("lock") = headers
                    .get("x-catga-peer")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                StatusCode::OK
            }
        }),
    );
    let (base, server) = server::spawn_app(app).await;

    let transport = transport(&base).with_peer_identity("node-1");
    transport.send(frame_to(2)).await.expect("send succeeds");
    assert_eq!(
        observed.lock().expect("lock").as_deref(),
        Some("node-1"),
        "the configured identity must reach the peer"
    );

    // Member updates re-route subsequent sends.
    transport.update_member(2, Arc::from("http://127.0.0.1:1"));
    let error = transport
        .send(frame_to(2))
        .await
        .expect_err("updated route must apply");
    assert!(error.is_retryable());

    server.abort();
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
