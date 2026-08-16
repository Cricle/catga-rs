//! Integration tests for `src/transport/grpc.rs` (`GrpcTransport` / `PeerClient`).
//!
//! Coverage notes:
//! - `PeerClient::do_send` does not issue a real RPC yet (proto definitions
//!   are pending), so a successful send only proves channel establishment.
//! - Happy-path tests bind an ephemeral TCP listener (port 0) and never use
//!   fixed ports. tonic's `connect()` completes after the TCP handshake
//!   (the HTTP/2 client preface is written without waiting for the server),
//!   so a plain bound listener is sufficient.
//! - Error-path tests use an endpoint string that fails URI parsing, which is
//!   deterministic and performs no network I/O.
//! - The backpressure *rejection* path requires saturating in-flight sends
//!   against a hanging connection; the controller itself is covered by
//!   `module_tests.rs`, so here we only verify that in-flight accounting
//!   returns to zero after sends complete.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use catga_raft::transport::grpc::PeerClient;
use catga_raft::{
    BincodeCodec, CatgaRaftError, CircuitBreakerConfig, CircuitBreakerState, GrpcTransport,
    RaftTransport,
};

/// An endpoint string that never parses as a valid URI (contains spaces),
/// guaranteeing a fast, deterministic `Transport("invalid endpoint")`
/// failure without any network I/O.
const UNREACHABLE_ENDPOINT: &str = "http://not a valid endpoint";

/// Start a real gRPC raft server on an ephemeral port; the returned endpoint
/// has no scheme so callers can exercise `add_peer` normalisation.
async fn live_grpc_server() -> (tokio::task::JoinHandle<()>, String) {
    use catga_raft::transport::{RaftGrpcService, serve_with_bound_addr};
    let service = RaftGrpcService::new(|_msg| Ok(()));
    let (bound, handle) = serve_with_bound_addr("127.0.0.1:0".parse().unwrap(), service)
        .await
        .expect("start gRPC server");
    (handle, bound.to_string())
}

/// Encode a default raft message so the server can decode the payload.
fn encoded_raft_message() -> Bytes {
    use protobuf::Message as _;
    let msg = raft::prelude::Message::default();
    Bytes::from(msg.write_to_bytes().expect("encode raft message"))
}

// ============================================================================
// GrpcTransport construction / defaults
// ============================================================================

#[test]
fn grpc_transport_construction_and_defaults() {
    let transport = GrpcTransport::new(7);
    assert_eq!(transport.local_node_id(), 7);
    assert_eq!(transport.peer_count(), 0);
    assert!(transport.peer_ids().is_empty());
    assert!(!transport.has_peer(2));

    let defaulted: GrpcTransport = Default::default();
    assert_eq!(defaulted.local_node_id(), 0);
    assert_eq!(defaulted.peer_count(), 0);

    let debug = format!("{:?}", transport);
    assert!(debug.contains("GrpcTransport"));
    assert!(debug.contains("local_node_id"));
    assert!(debug.contains("peer_count"));
}

#[test]
fn grpc_transport_alias_and_codec_constructor() {
    // `RaftTransport` is a backwards-compatible alias for `GrpcTransport`.
    let aliased: RaftTransport = GrpcTransport::new(1);
    assert_eq!(aliased.local_node_id(), 1);

    let with_codec = GrpcTransport::new_with_codec(4, BincodeCodec);
    assert_eq!(with_codec.local_node_id(), 4);
    assert_eq!(with_codec.peer_count(), 0);
}

// ============================================================================
// Peer management
// ============================================================================

#[tokio::test]
async fn grpc_transport_add_remove_peer_lifecycle() {
    let transport = GrpcTransport::new(1);

    transport
        .add_peer(2, "127.0.0.1:9001".to_string())
        .await
        .unwrap();
    assert!(transport.has_peer(2));
    assert_eq!(transport.peer_count(), 1);
    assert_eq!(transport.peer_ids(), vec![2]);

    // Re-adding an existing peer replaces it; the count must not grow.
    transport
        .add_peer(2, "127.0.0.1:9002".to_string())
        .await
        .unwrap();
    assert_eq!(transport.peer_count(), 1);

    transport.remove_peer(2).unwrap();
    assert!(!transport.has_peer(2));
    assert_eq!(transport.peer_count(), 0);

    // Removing an unknown peer is an error.
    let err = transport.remove_peer(2).unwrap_err();
    assert!(matches!(err, CatgaRaftError::NodeNotFound(2)));
}

// ============================================================================
// send / send_vec
// ============================================================================

#[tokio::test]
async fn grpc_transport_send_to_self_is_noop() {
    let transport = GrpcTransport::new(5);
    // Sends addressed to the local node id are skipped and always succeed,
    // even though no peer is registered.
    transport
        .send(5, Bytes::from_static(b"hello"))
        .await
        .unwrap();
    transport.send_vec(5, b"hello".to_vec()).await.unwrap();
}

#[tokio::test]
async fn grpc_transport_send_to_unknown_peer_fails() {
    let transport = GrpcTransport::new(1);

    let err = transport
        .send(99, Bytes::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(matches!(err, CatgaRaftError::NodeNotFound(99)));

    let err = transport.send_vec(42, vec![1]).await.unwrap_err();
    assert!(matches!(err, CatgaRaftError::NodeNotFound(42)));
}

#[tokio::test]
async fn grpc_transport_send_happy_path_over_real_socket() {
    let (_server, addr) = live_grpc_server().await;
    let transport = GrpcTransport::new(1);

    // Address without scheme: add_peer must normalise it with `http://`.
    transport.add_peer(2, addr).await.unwrap();

    let payload = encoded_raft_message();
    transport.send(2, payload.clone()).await.unwrap();
    transport.send_vec(2, payload.to_vec()).await.unwrap();
    transport.broadcast(payload).await.unwrap();
}

// ============================================================================
// broadcast
// ============================================================================

#[tokio::test]
async fn grpc_transport_broadcast_aggregates_peer_errors() {
    let transport = GrpcTransport::new(1);
    transport
        .add_peer(2, UNREACHABLE_ENDPOINT.to_string())
        .await
        .unwrap();
    transport
        .add_peer(3, UNREACHABLE_ENDPOINT.to_string())
        .await
        .unwrap();

    let err = transport
        .broadcast(Bytes::from_static(b"msg"))
        .await
        .unwrap_err();
    match err {
        CatgaRaftError::Transport(msg) => {
            assert!(
                msg.contains("broadcast failed to 2 peers"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected Transport error, got {other:?}"),
    }

    // The fire-and-forget variant swallows the same errors without panicking.
    transport
        .broadcast_unchecked(Bytes::from_static(b"msg"))
        .await;
}

#[tokio::test]
async fn grpc_transport_broadcast_skips_local_node_entry() {
    let transport = GrpcTransport::new(9);
    // Even if the local node id is registered as a peer with a broken
    // endpoint, broadcast must filter it out and succeed.
    transport
        .add_peer(9, UNREACHABLE_ENDPOINT.to_string())
        .await
        .unwrap();
    transport
        .broadcast(Bytes::from_static(b"msg"))
        .await
        .unwrap();
}

// ============================================================================
// send_many
// ============================================================================

#[tokio::test]
async fn grpc_transport_send_many_variants() {
    let transport = GrpcTransport::new(1);

    // An empty map short-circuits to Ok.
    transport.send_many(HashMap::new()).await.unwrap();

    // Unknown peers are reported in a Transport summary error.
    let mut missing = HashMap::new();
    missing.insert(99u64, Bytes::from_static(b"x"));
    let err = transport.send_many(missing).await.unwrap_err();
    match err {
        CatgaRaftError::Transport(msg) => {
            assert!(
                msg.contains("1 sends failed"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected Transport error, got {other:?}"),
    }

    // Peer id 0 is treated as "self" by send_many and skipped.
    let mut to_self = HashMap::new();
    to_self.insert(0u64, Bytes::from_static(b"x"));
    transport.send_many(to_self).await.unwrap();

    // A registered, reachable peer succeeds.
    let (_server, addr) = live_grpc_server().await;
    transport.add_peer(2, addr).await.unwrap();
    let mut live = HashMap::new();
    live.insert(2u64, encoded_raft_message());
    transport.send_many(live).await.unwrap();
}

// ============================================================================
// PeerClient
// ============================================================================

#[test]
fn grpc_peer_client_initial_state() {
    // Construction is lazy: no connection is attempted here.
    let client = PeerClient::new(3, "http://127.0.0.1:1".to_string());
    assert_eq!(client.circuit_breaker_state(), CircuitBreakerState::Closed);
    assert_eq!(client.inflight_count(), 0);
}

#[tokio::test]
async fn grpc_peer_client_failures_open_default_circuit_breaker() {
    let client = PeerClient::new(3, UNREACHABLE_ENDPOINT.to_string());

    // The default failure threshold is 5: each send fails with an invalid
    // endpoint error and records a failure in the circuit breaker.
    for _ in 0..5 {
        let err = client.send(Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, CatgaRaftError::Transport(_)));
        // Resources (permit + inflight) are released even on failure.
        assert_eq!(client.inflight_count(), 0);
    }

    assert_eq!(client.circuit_breaker_state(), CircuitBreakerState::Open);

    // Once the breaker is open, subsequent sends fail fast.
    let err = client.send(Bytes::from_static(b"x")).await.unwrap_err();
    assert!(matches!(err, CatgaRaftError::CircuitBreakerOpen));
}

#[tokio::test]
async fn grpc_peer_client_with_config_custom_threshold() {
    let config = CircuitBreakerConfig::default().with_failure_threshold(2);
    let client = PeerClient::with_config(
        4,
        UNREACHABLE_ENDPOINT.to_string(),
        2,                        // pool_size
        16,                       // max_pending
        32,                       // inflight_limit
        8,                        // batch_size
        Duration::from_millis(1), // flush_interval
        config,
    );

    for _ in 0..2 {
        assert!(matches!(
            client.send(Bytes::from_static(b"x")).await,
            Err(CatgaRaftError::Transport(_))
        ));
    }

    // Custom threshold of 2 is reached, so the circuit must now be open.
    assert_eq!(client.circuit_breaker_state(), CircuitBreakerState::Open);
    assert!(matches!(
        client.send(Bytes::from_static(b"x")).await,
        Err(CatgaRaftError::CircuitBreakerOpen)
    ));
}

#[tokio::test]
async fn grpc_peer_client_send_batch_edge_cases() {
    let client = PeerClient::new(5, UNREACHABLE_ENDPOINT.to_string());

    // An empty batch succeeds without touching the network or the breaker.
    client.send_batch(Vec::new()).await.unwrap();
    assert_eq!(client.circuit_breaker_state(), CircuitBreakerState::Closed);
    assert_eq!(client.inflight_count(), 0);

    // A non-empty batch propagates the underlying send error.
    let err = client
        .send_batch(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")])
        .await
        .unwrap_err();
    assert!(matches!(err, CatgaRaftError::Transport(_)));
}

#[tokio::test]
async fn grpc_peer_client_send_happy_path_over_real_socket() {
    let (_server, addr) = live_grpc_server().await;
    let client = PeerClient::new(2, format!("http://{addr}"));

    client.send(encoded_raft_message()).await.unwrap();
    client
        .send_batch(vec![encoded_raft_message(), encoded_raft_message()])
        .await
        .unwrap();

    assert_eq!(client.circuit_breaker_state(), CircuitBreakerState::Closed);
    assert_eq!(client.inflight_count(), 0);
}
