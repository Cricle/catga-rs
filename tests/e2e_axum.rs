//! E2E tests for catga-axum HTTP client and server integration.
//!
//! These tests verify end-to-end HTTP client/server communication, correlation header
//! propagation, and middleware chain behavior using actual network connections.

use std::{
    future::IntoFuture,
    num::NonZeroUsize,
    sync::atomic::{AtomicU32, Ordering},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode},
    middleware,
    response::IntoResponse,
    routing::post,
};
use catga_axum::{
    CORRELATION_ID_HEADER, CatgaHttpError, CorrelationHttpClient, HttpClusterForwarder,
    HttpRaftTransport, IntoCatgaHttpResponse, RAFT_MESSAGE_PATH, catga_routes,
    correlation_middleware, endpoint_panic_middleware, event_route, leader_forward_route,
    mediator_route, raft_message_route, raft_peer_identity_middleware,
};
use catga_cluster::{
    ClusterForwarder, RaftMember, RaftMessage, RaftTransport, StaticRaftInboundPolicy,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, EventHandler, Handler, Mediator, Registry, Request,
    scope_correlation_id,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Deserialize, Serialize)]
struct E2eRequest {
    value: u32,
}

impl catga_core::Message for E2eRequest {}

impl Request for E2eRequest {
    type Response = u32;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct E2eHandler;

#[async_trait]
impl Handler<E2eRequest> for E2eHandler {
    async fn handle(&self, request: E2eRequest) -> CatgaResult<u32> {
        Ok(request.value + 1)
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct E2eEvent(u32);

impl catga_core::Message for E2eEvent {}

impl Event for E2eEvent {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct E2eEventHandler(std::sync::Arc<AtomicU32>);

#[async_trait]
impl EventHandler<E2eEvent> for E2eEventHandler {
    async fn handle(&self, event: E2eEvent) -> CatgaResult<()> {
        self.0.store(event.0, Ordering::Relaxed);
        Ok(())
    }
}

/// E2E test: HTTP server starts and accepts connections on a real TCP socket.
#[tokio::test]
async fn e2e_http_server_binds_and_accepts_connections() {
    let mut registry = Registry::new();
    registry
        .register_request::<E2eRequest, _>(E2eHandler)
        .unwrap();
    let app =
        mediator_route::<E2eRequest>("/api/test", std::sync::Arc::new(Mediator::new(registry)))
            .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    // Make an actual HTTP request over TCP
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/api/test"))
        .json(&E2eRequest { value: 41 })
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<u32>().await.unwrap(), 42);
}

/// E2E test: CorrelationHttpClient propagates correlation headers across actual HTTP requests.
#[tokio::test]
async fn e2e_correlation_http_client_propagates_headers() {
    let observed_correlation = std::sync::Arc::new(AsyncMutex::new(None));
    let app = Router::new().route(
        "/observe",
        post({
            let observed_correlation = std::sync::Arc::clone(&observed_correlation);
            move |headers: HeaderMap| async move {
                *observed_correlation.lock().await = headers
                    .get(CORRELATION_ID_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                StatusCode::NO_CONTENT
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let endpoint = format!("http://127.0.0.1:{port}/observe");

    // Use scope_correlation_id to set ambient correlation
    let result = scope_correlation_id(12345, async {
        CorrelationHttpClient::new(reqwest::Client::new())
            .post(&endpoint, HeaderMap::new())
            .send()
            .await
    })
    .await
    .unwrap();

    server.abort();

    assert_eq!(result.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        *observed_correlation.lock().await,
        Some("12345".to_string())
    );
}

/// E2E test: HttpClusterForwarder forwards requests to leader endpoint.
#[tokio::test]
async fn e2e_http_cluster_forwarder_forwards_to_leader() {
    let app = leader_forward_route::<E2eRequest>({
        let mut registry = Registry::new();
        registry
            .register_request::<E2eRequest, _>(E2eHandler)
            .unwrap();
        std::sync::Arc::new(Mediator::new(registry))
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let endpoint = format!("http://127.0.0.1:{port}");
    let result = HttpClusterForwarder::new(reqwest::Client::new())
        .forward(E2eRequest { value: 41 }, &endpoint)
        .await
        .unwrap();

    server.abort();

    assert_eq!(result, 42);
}

/// E2E test: Event route publishes events and they are received by handlers.
#[tokio::test]
async fn e2e_event_route_publishes_and_handles_events() {
    let captured = std::sync::Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry.register_event::<E2eEvent, _>(E2eEventHandler(std::sync::Arc::clone(&captured)));
    let app = event_route::<E2eEvent>("/api/events", std::sync::Arc::new(Mediator::new(registry)))
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/api/events"))
        .json(&E2eEvent(42))
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(captured.load(Ordering::Relaxed), 42);
}

/// E2E test: Multiple routes work together in a merged router.
#[tokio::test]
async fn e2e_merged_router_handles_multiple_routes() {
    let captured = std::sync::Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<E2eRequest, _>(E2eHandler)
        .unwrap();
    registry.register_event::<E2eEvent, _>(E2eEventHandler(std::sync::Arc::clone(&captured)));
    let app = catga_routes! {
        mediator = std::sync::Arc::new(Mediator::new(registry));
        requests {
            "/api/forward" => E2eRequest,
        }
        events {
            "/api/event" => E2eEvent,
        }
    }
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();

    // Test request route
    let request_response = client
        .post(format!("http://127.0.0.1:{port}/api/forward"))
        .json(&E2eRequest { value: 100 })
        .send()
        .await
        .unwrap();
    assert_eq!(request_response.json::<u32>().await.unwrap(), 101);

    // Test event route
    let event_response = client
        .post(format!("http://127.0.0.1:{port}/api/event"))
        .json(&E2eEvent(99))
        .send()
        .await
        .unwrap();
    assert_eq!(event_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(captured.load(Ordering::Relaxed), 99);

    server.abort();
}

/// E2E test: HttpClusterForwarder respects response size limits.
#[tokio::test]
async fn e2e_http_cluster_forwarder_enforces_response_limit() {
    let large_body = "x".repeat(256);
    let app = Router::new().route(
        "/api/catga/forward/E2eRequest",
        post(move |_: ()| async move { large_body.clone() }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let endpoint = format!("http://127.0.0.1:{port}");
    let error = HttpClusterForwarder::with_response_limit(
        reqwest::Client::new(),
        NonZeroUsize::new(128).expect("non-zero"),
    )
    .forward(E2eRequest { value: 0 }, &endpoint)
    .await
    .expect_err("oversized response should be rejected");

    server.abort();

    assert_eq!(error.code(), ErrorCode::Transient);
}

async fn validate_handler(Json(payload): Json<E2eRequest>) -> impl IntoResponse {
    if payload.value == 0 {
        CatgaHttpError::from(CatgaError::new(
            ErrorCode::Validation,
            "value must be non-zero",
        ))
        .into_response()
    } else {
        let response: CatgaResult<u32> = Ok(payload.value + 1);
        response.into_catga_response(StatusCode::OK)
    }
}

/// E2E test: CatgaHttpError maps to correct HTTP status codes.
#[tokio::test]
async fn e2e_catga_error_maps_to_correct_http_status() {
    let app = Router::new().route("/validate", post(validate_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();

    // Test validation error returns UNPROCESSABLE_ENTITY
    let error_response = client
        .post(format!("http://127.0.0.1:{port}/validate"))
        .json(&E2eRequest { value: 0 })
        .send()
        .await
        .unwrap();
    assert_eq!(error_response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // Test success returns OK
    let success_response = client
        .post(format!("http://127.0.0.1:{port}/validate"))
        .json(&E2eRequest { value: 10 })
        .send()
        .await
        .unwrap();
    assert_eq!(success_response.status(), StatusCode::OK);
    assert_eq!(success_response.json::<u32>().await.unwrap(), 11);

    server.abort();
}

/// E2E test: correlation_middleware echoes correlation header in response.
#[tokio::test]
async fn e2e_correlation_middleware_echoes_header_in_response() {
    async fn endpoint() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/test", post(endpoint))
        .layer(middleware::from_fn(correlation_middleware));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/test"))
        .header(CORRELATION_ID_HEADER, "test-correlation-123")
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("test-correlation-123")
    );
}

/// E2E test: endpoint_panic_middleware catches panics and returns stable error.
#[tokio::test]
async fn e2e_endpoint_panic_middleware_returns_stable_error() {
    async fn panicking_endpoint() -> StatusCode {
        panic!("intentional test panic");
    }

    let app = Router::new()
        .route("/panic", post(panicking_endpoint))
        .layer(middleware::from_fn(endpoint_panic_middleware));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/panic"))
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "internal");
    assert_eq!(body["message"], "endpoint handler panicked");
}

/// E2E test: a peer that accepts TCP but never responds becomes retryable transport
/// backpressure once a request timeout is configured, instead of stalling the Raft loop.
#[tokio::test]
async fn e2e_http_raft_transport_request_timeout_is_retryable() {
    let app = Router::new().route(
        RAFT_MESSAGE_PATH,
        post(|| async {
            // A hung peer: the connection is accepted but no response ever arrives.
            std::future::pending::<()>().await;
            StatusCode::NO_CONTENT
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let transport = HttpRaftTransport::new(reqwest::Client::new(), [RaftMember::new(1, endpoint)])
        .with_request_timeout(std::time::Duration::from_millis(100));
    let message = RaftMessage {
        from: 2,
        to: 1,
        ..RaftMessage::default()
    };

    let error = transport
        .send(message)
        .await
        .expect_err("a hung peer must time out");
    server.abort();

    assert!(
        error.is_retryable(),
        "a request timeout must surface as retryable backpressure: {error}"
    );
}

/// E2E test: the built-in Raft client authenticates against the built-in server route when
/// the peer identity header is plumbed through raft_peer_identity_middleware.
#[tokio::test]
async fn e2e_raft_peer_identity_round_trip_between_transport_and_route() {
    let (inbox, mut receiver) = tokio::sync::mpsc::channel(1);
    let policy = StaticRaftInboundPolicy::new(1, [(2, "node-2")]).expect("valid static policy");
    let app =
        raft_message_route(inbox, policy).layer(middleware::from_fn(raft_peer_identity_middleware));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let message = RaftMessage {
        from: 2,
        to: 1,
        ..RaftMessage::default()
    };

    // The matching identity authenticates and the frame reaches the runtime inbox.
    let transport = HttpRaftTransport::new(
        reqwest::Client::new(),
        [RaftMember::new(1, endpoint.clone())],
    )
    .with_peer_identity("node-2");
    transport
        .send(message.clone())
        .await
        .expect("authenticated peer send must succeed");
    let received = receiver.recv().await.expect("inbox must receive the frame");
    assert_eq!(received, message);

    // Negative: without an identity header the route rejects the frame as unauthenticated.
    let anonymous = HttpRaftTransport::new(
        reqwest::Client::new(),
        [RaftMember::new(1, endpoint.clone())],
    );
    let error = anonymous
        .send(message.clone())
        .await
        .expect_err("a missing identity must be rejected");
    assert!(!error.is_retryable(), "401 must be fatal: {error}");

    // Negative: an identity that does not match the claimed sender is forbidden.
    let impostor = HttpRaftTransport::new(reqwest::Client::new(), [RaftMember::new(1, endpoint)])
        .with_peer_identity("node-3");
    let error = impostor
        .send(message)
        .await
        .expect_err("a mismatched identity must be rejected");
    assert!(!error.is_retryable(), "403 must be fatal: {error}");

    server.abort();
}

// ---------------------------------------------------------------------------
// Mutual-TLS Raft peer authentication
//
// These tests run real TLS handshakes on 127.0.0.1 with certificates generated
// into a temporary directory. The server derives RaftPeerIdentity from the
// VERIFIED client certificate chain; the policy then binds it to Raft member 2.
// ---------------------------------------------------------------------------

mod mtls_raft {
    use std::{
        net::TcpListener as StdTcpListener,
        path::{Path, PathBuf},
        time::Duration,
    };

    use catga_axum::{
        DevCertificateAuthority, HttpRaftTransport, MtlsAcceptor, mtls_peer_identity_middleware,
        mtls_reqwest_client, raft_message_route, serve_mtls,
    };
    use catga_cluster::{RaftMember, RaftMessage, RaftTransport, StaticRaftInboundPolicy};
    use tempfile::TempDir;
    use tokio::{sync::mpsc, task::JoinHandle};

    use super::middleware;

    const NODE_2_IDENTITY: &str = "spiffe://cluster/node-2";

    struct MtlsTestPki {
        _dir: TempDir,
        ca_cert: PathBuf,
        node1_cert: PathBuf,
        node1_key: PathBuf,
        node2_cert: PathBuf,
        node2_key: PathBuf,
        node3_cert: PathBuf,
        node3_key: PathBuf,
    }

    fn write_pem(dir: &Path, name: &str, contents: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write PEM fixture");
        path
    }

    fn mtls_test_pki() -> MtlsTestPki {
        let ca = DevCertificateAuthority::generate().expect("generate dev CA");
        let dir = tempfile::tempdir().expect("create certificate tempdir");
        let ca_cert = write_pem(dir.path(), "ca.pem", ca.cert_pem());
        let node = |name: &str| {
            let identity = ca
                .issue_node_identity("cluster", name)
                .expect("issue node identity");
            (
                write_pem(
                    dir.path(),
                    &format!("{name}.pem"),
                    identity.certificate_chain_pem(),
                ),
                write_pem(
                    dir.path(),
                    &format!("{name}-key.pem"),
                    identity.private_key_pem(),
                ),
            )
        };
        let (node1_cert, node1_key) = node("node-1");
        let (node2_cert, node2_key) = node("node-2");
        let (node3_cert, node3_key) = node("node-3");
        MtlsTestPki {
            _dir: dir,
            ca_cert,
            node1_cert,
            node1_key,
            node2_cert,
            node2_key,
            node3_cert,
            node3_key,
        }
    }

    /// Starts node 1 with mTLS ingress and a policy binding Raft member 2 to the
    /// node-2 SPIFFE identity. Returns the HTTPS endpoint, the Raft inbox, and the server task.
    async fn start_mtls_raft_node1(
        pki: &MtlsTestPki,
    ) -> (String, mpsc::Receiver<RaftMessage>, JoinHandle<()>) {
        let (inbox, receiver) = mpsc::channel(1);
        let policy =
            StaticRaftInboundPolicy::new(1, [(2, NODE_2_IDENTITY)]).expect("valid static policy");
        let app = raft_message_route(inbox, policy)
            .layer(middleware::from_fn(mtls_peer_identity_middleware));
        let acceptor = MtlsAcceptor::from_pem_files(&pki.node1_cert, &pki.node1_key, &pki.ca_cert)
            .expect("build mTLS acceptor");
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let endpoint = format!(
            "https://{}",
            listener.local_addr().expect("listener address")
        );
        let server = tokio::spawn(async move {
            serve_mtls(listener, acceptor, app)
                .await
                .expect("mTLS server must run");
        });
        (endpoint, receiver, server)
    }

    fn mtls_transport(client: reqwest::Client, endpoint: String) -> HttpRaftTransport {
        HttpRaftTransport::new(client, [RaftMember::new(1, endpoint)])
            .with_request_timeout(Duration::from_secs(10))
    }

    /// E2E: a client certificate whose SAN URI is bound to Raft member 2 authenticates, and the
    /// protobuf frame lands in the runtime inbox.
    #[tokio::test]
    async fn e2e_mtls_client_certificate_authenticates_raft_peer() {
        let pki = mtls_test_pki();
        let (endpoint, mut receiver, server) = start_mtls_raft_node1(&pki).await;

        let client = mtls_reqwest_client(&pki.node2_cert, &pki.node2_key, &pki.ca_cert)
            .expect("build mTLS reqwest client");
        let transport = mtls_transport(client, endpoint);
        let message = RaftMessage {
            from: 2,
            to: 1,
            ..RaftMessage::default()
        };

        transport
            .send(message.clone())
            .await
            .expect("certificate-authenticated peer send must succeed");
        let received = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("inbox must receive the frame promptly")
            .expect("inbox must stay open");
        assert_eq!(received, message);

        server.abort();
    }

    /// E2E: a valid certificate whose identity is not bound to the claimed sender is forbidden.
    #[tokio::test]
    async fn e2e_mtls_unbound_certificate_identity_is_forbidden() {
        let pki = mtls_test_pki();
        let (endpoint, _receiver, server) = start_mtls_raft_node1(&pki).await;

        let client = mtls_reqwest_client(&pki.node3_cert, &pki.node3_key, &pki.ca_cert)
            .expect("build mTLS reqwest client");
        let transport = mtls_transport(client, endpoint);
        let message = RaftMessage {
            from: 3,
            to: 1,
            ..RaftMessage::default()
        };

        let error = transport
            .send(message)
            .await
            .expect_err("an unbound certificate identity must be rejected");
        assert!(!error.is_retryable(), "403 must be fatal: {error}");

        server.abort();
    }

    /// E2E: a client without a certificate never completes the mTLS handshake, so no Raft frame
    /// can reach the route at all.
    #[tokio::test]
    async fn e2e_mtls_missing_client_certificate_is_rejected() {
        let pki = mtls_test_pki();
        let (endpoint, _receiver, server) = start_mtls_raft_node1(&pki).await;

        let ca_pem = std::fs::read(&pki.ca_cert).expect("read CA certificate");
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .tls_certs_only([reqwest::Certificate::from_pem(&ca_pem).expect("parse CA cert")])
            .build()
            .expect("build cert-less reqwest client");
        let transport = mtls_transport(client, endpoint);
        let message = RaftMessage {
            from: 2,
            to: 1,
            ..RaftMessage::default()
        };

        transport
            .send(message)
            .await
            .expect_err("the TLS handshake must fail without a client certificate");

        server.abort();
    }
}
