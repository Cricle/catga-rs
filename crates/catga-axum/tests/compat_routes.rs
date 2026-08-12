//! Contract tests for the typed route builders in `compat`: Raft ingress status
//! matrix, mediator and event routes, leader forwarding routes, and the
//! self-asserted peer-identity middleware.

#[path = "common/server.rs"]
mod server;

use std::sync::Arc;

use axum::{Router, middleware};
use catga_axum::{
    EndpointMethod, HttpClusterForwarder, event_route, event_route_with_method,
    leader_forward_route, leader_forward_route_at, mediator_route, mediator_route_with_method,
    raft_message_route, raft_peer_identity_middleware,
};
use catga_cluster::{
    ClusterForwarder, RaftInboundPolicy, RaftInboundRejection, RaftMessage, RaftPeerIdentity,
    StaticRaftInboundPolicy,
};
use catga_core::{
    CatgaError, ErrorCode, Event, Mediator, Message, MessageTypeId, Registry, Request,
    current_transport_context, event_handler, request_handler,
};
use http::StatusCode;
use protobuf::Message as ProtobufMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Add {
    value: u64,
}
impl Message for Add {}
struct AddTypeId;
impl MessageTypeId for AddTypeId {
    const NAME: &'static str = "Add";
}
impl Request for Add {
    type Response = u64;
    type TypeId = AddTypeId;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Fail;
impl Message for Fail {}
struct FailTypeId;
impl MessageTypeId for FailTypeId {
    const NAME: &'static str = "Fail";
}
impl Request for Fail {
    type Response = u64;
    type TypeId = FailTypeId;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Added;
impl Message for Added {}
struct AddedTypeId;
impl MessageTypeId for AddedTypeId {
    const NAME: &'static str = "Added";
}
impl Event for Added {
    type TypeId = AddedTypeId;
}

/// An inbound policy that accepts every frame; used to isolate route mechanics
/// from authorization outcomes.
struct PermitAll;

impl RaftInboundPolicy for PermitAll {
    fn authorize(
        &self,
        _peer: Option<&RaftPeerIdentity>,
        _message: &RaftMessage,
    ) -> Result<(), RaftInboundRejection> {
        Ok(())
    }
}

fn add_mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry
        .register_request::<Add, _>(request_handler(|add: Add| async move { Ok(add.value + 1) }))
        .expect("handler registers");
    registry.register_event::<Added, _>(event_handler(|_: Added| async { Ok(()) }));
    Arc::new(Mediator::new(registry))
}

fn frame(from: u64, to: u64) -> Vec<u8> {
    RaftMessage {
        from,
        to,
        ..Default::default()
    }
    .write_to_bytes()
    .expect("frame must serialize")
}

fn protobuf_post(client: &reqwest::Client, url: String, body: Vec<u8>) -> reqwest::RequestBuilder {
    client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
        .body(body)
}

fn static_policy() -> StaticRaftInboundPolicy {
    StaticRaftInboundPolicy::new(1, [(2, "peer-two")]).expect("valid policy")
}

#[tokio::test]
async fn raft_message_route_enforces_content_type_and_framing() {
    let (inbox, _receiver) = mpsc::channel(4);
    let app = raft_message_route(inbox, PermitAll);
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/catga/raft");

    // A non-protobuf content type is rejected before parsing.
    let response = client
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(frame(2, 1))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

    // Media-type parameters and case differences are still accepted.
    let response = client
        .post(&url)
        .header(
            reqwest::header::CONTENT_TYPE,
            "Application/X-Protobuf; charset=binary",
        )
        .body(frame(2, 1))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // A malformed protobuf frame is a client error.
    let response = protobuf_post(&client, url, vec![0xFF, 0xFF, 0xFF])
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    server.abort();
}

#[tokio::test]
async fn raft_message_route_authorizes_the_verified_identity() {
    let (inbox, mut receiver) = mpsc::channel(4);
    let app = raft_message_route(inbox, static_policy())
        .layer(middleware::from_fn(raft_peer_identity_middleware));
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/catga/raft");

    // No identity at all is unauthenticated.
    let response = protobuf_post(&client, url.clone(), frame(2, 1))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // An identity that does not match the claimed sender is forbidden.
    let response = protobuf_post(&client, url.clone(), frame(2, 1))
        .header("x-catga-peer", "peer-three")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // A frame addressed to another node is forbidden even for a known peer.
    let response = protobuf_post(&client, url.clone(), frame(2, 9))
        .header("x-catga-peer", "peer-two")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // The bound identity delivers the frame into the runtime inbox.
    let response = protobuf_post(&client, url.clone(), frame(2, 1))
        .header("x-catga-peer", "peer-two")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let delivered = receiver.recv().await.expect("frame must arrive");
    assert_eq!((delivered.from, delivered.to), (2, 1));

    server.abort();
}

#[tokio::test]
async fn raft_message_route_applies_bounded_backpressure() {
    let client = reqwest::Client::new();

    // A full bounded inbox answers 429 rather than queueing unbounded work.
    let (full_inbox, full_receiver) = mpsc::channel(1);
    full_inbox
        .try_send(RaftMessage::default())
        .expect("prefill fits");
    let (full_base, full_server) =
        server::spawn_app(raft_message_route(full_inbox, PermitAll)).await;
    let response = protobuf_post(&client, format!("{full_base}/api/catga/raft"), frame(2, 1))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    drop(full_receiver);
    full_server.abort();

    // A closed inbox reports the stopped runtime as unavailable.
    let (closed_inbox, closed_receiver) = mpsc::channel::<RaftMessage>(1);
    drop(closed_receiver);
    let (closed_base, closed_server) =
        server::spawn_app(raft_message_route(closed_inbox, PermitAll)).await;
    let response = protobuf_post(
        &client,
        format!("{closed_base}/api/catga/raft"),
        frame(2, 1),
    )
    .send()
    .await
    .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    closed_server.abort();
}

#[tokio::test]
async fn mediator_routes_dispatch_typed_requests_and_map_failures() {
    let mut registry = Registry::new();
    registry
        .register_request::<Add, _>(request_handler(|add: Add| async move { Ok(add.value + 1) }))
        .expect("handler registers");
    registry
        .register_request::<Fail, _>(request_handler(|_: Fail| async {
            Err(CatgaError::new(ErrorCode::Conflict, "rejected"))
        }))
        .expect("handler registers");
    let mediator = Arc::new(Mediator::new(registry));

    let app = Router::new()
        .merge(mediator_route::<Add>("/api/add", Arc::clone(&mediator)).expect("route"))
        .merge(
            mediator_route_with_method::<Add>(
                EndpointMethod::Put,
                "/api/add-put",
                Arc::clone(&mediator),
            )
            .expect("route"),
        )
        .merge(mediator_route::<Fail>("/api/fail", Arc::clone(&mediator)).expect("route"));
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/api/add"))
        .json(&Add { value: 41 })
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "42");

    // The same typed endpoint registered for another verb dispatches too.
    let response = client
        .put(format!("{base}/api/add-put"))
        .json(&Add { value: 1 })
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "2");

    // Handler failures flow through the Catga error-to-status mapping.
    let response = client
        .post(format!("{base}/api/fail"))
        .json(&Fail)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = response.text().await.expect("body");
    assert!(body.contains("\"code\":\"conflict\""), "got {body}");

    server.abort();
}

#[test]
fn typed_route_registration_rejects_relative_paths() {
    let mediator = add_mediator();
    assert!(mediator_route::<Add>("api/add", Arc::clone(&mediator)).is_err());
    assert!(mediator_route::<Add>("/", Arc::clone(&mediator)).is_err());
    assert!(event_route::<Added>("api/added", Arc::clone(&mediator)).is_err());
    assert!(
        event_route_with_method::<Added>(EndpointMethod::Get, "api/added", Arc::clone(&mediator))
            .is_err()
    );
}

#[tokio::test]
async fn event_routes_publish_and_scope_inbound_trace_context() {
    let mediator = add_mediator();
    let app = Router::new()
        .merge(event_route::<Added>("/api/added", Arc::clone(&mediator)).expect("route"))
        .merge(
            event_route_with_method::<Added>(
                EndpointMethod::Put,
                "/api/added-put",
                Arc::clone(&mediator),
            )
            .expect("route"),
        );
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/api/added"))
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .json(&Added)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = client
        .put(format!("{base}/api/added-put"))
        .header("traceparent", "not-valid")
        .json(&Added)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    server.abort();
}

#[tokio::test]
async fn leader_forward_routes_serve_the_http_cluster_forwarder() {
    let mediator = add_mediator();
    let app = Router::new()
        .merge(leader_forward_route::<Add>(Arc::clone(&mediator)))
        .merge(leader_forward_route_at::<Add>(
            "/custom/forward",
            Arc::clone(&mediator),
        ));
    let (base, server) = server::spawn_app(app).await;

    let forwarder = HttpClusterForwarder::new(reqwest::Client::new());
    let added = forwarder
        .forward(Add { value: 10 }, &base)
        .await
        .expect("forward succeeds");
    assert_eq!(added, 11);

    let custom = HttpClusterForwarder::new(reqwest::Client::new())
        .with_path_builder(|leader, _| format!("{leader}/custom/forward"));
    let added = custom
        .forward(Add { value: 20 }, &base)
        .await
        .expect("forward succeeds");
    assert_eq!(added, 21);

    server.abort();
}

#[tokio::test]
async fn mediator_routes_run_inside_the_inbound_trace_scope() {
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Scoped;
    impl Message for Scoped {}
    struct ScopedTypeId;
    impl MessageTypeId for ScopedTypeId {
        const NAME: &'static str = "Scoped";
    }
    impl Request for Scoped {
        type Response = bool;
        type TypeId = ScopedTypeId;
    }

    let mut registry = Registry::new();
    registry
        .register_request::<Scoped, _>(request_handler(|_: Scoped| async {
            Ok(current_transport_context().is_some())
        }))
        .expect("handler registers");
    let mediator = Arc::new(Mediator::new(registry));
    let app = mediator_route::<Scoped>("/api/scoped", mediator).expect("route");
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    // A valid inbound traceparent is scoped through mediator dispatch.
    let response = client
        .post(format!("{base}/api/scoped"))
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .json(&Scoped)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.text().await.expect("body"), "true");

    // Without one the handler runs unscoped.
    let response = client
        .post(format!("{base}/api/scoped"))
        .json(&Scoped)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.text().await.expect("body"), "false");

    server.abort();
}
