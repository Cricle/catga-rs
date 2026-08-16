//! Tests for the send-path performance hardening.
//!
//! Covers the two send-path optimizations:
//! 1. `GrpcTransport::send_grouped` fans out to all peers inside a single
//!    future (`futures::future::join_all`) instead of spawning one task per
//!    peer. Error aggregation must stay identical: per-peer failures are
//!    counted into a summary `Transport` error while successful peers still
//!    deliver their batches.
//! 2. `ConnectionPool::get_channel` serves a warm pool through a read lock
//!    plus an atomic round-robin cursor; concurrent warm calls must never
//!    grow the pool or fail.
//!
//! All servers bind ephemeral ports (`127.0.0.1:0`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use catga_raft::transport::connection::ConnectionPool;
use catga_raft::transport::server::{RaftGrpcService, serve_with_bound_addr};
use catga_raft::{CatgaRaftError, GrpcTransport};
use raft::prelude::MessageType;

/// Build a distinct raft message for delivery assertions.
fn message(
    msg_type: MessageType,
    from: u64,
    to: u64,
    term: u64,
    index: u64,
) -> raft::prelude::Message {
    raft::prelude::Message {
        msg_type,
        from,
        to,
        term,
        index,
        ..Default::default()
    }
}

/// Serialize a raft message with the same codec the wire contract mandates.
fn encode(msg: &raft::prelude::Message) -> Bytes {
    Bytes::from(
        <raft::prelude::Message as protobuf::Message>::write_to_bytes(msg)
            .expect("valid raft message serializes"),
    )
}

/// Identity fields used to compare delivery order.
fn key(msg: &raft::prelude::Message) -> (MessageType, u64, u64, u64, u64) {
    (msg.msg_type, msg.from, msg.to, msg.term, msg.index)
}

/// Start a transport server whose callback records every received message
/// into a shared sink.
async fn start_server() -> (
    SocketAddr,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<raft::prelude::Message>>>,
) {
    let received = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);
    let service = RaftGrpcService::new(move |msg| {
        sink.lock().expect("sink lock").push(msg);
        Ok(())
    });
    let (addr, handle) = serve_with_bound_addr(SocketAddr::from(([127, 0, 0, 1], 0)), service)
        .await
        .expect("serve on ephemeral port");
    (addr, handle, received)
}

// ============================================================================
// send_grouped: single-future fan-out with identical error aggregation
// ============================================================================

#[tokio::test]
async fn send_grouped_partial_failure_still_delivers_successes() {
    let (addr, handle, received) = start_server().await;

    let transport = GrpcTransport::new(1);
    transport.add_peer(2, addr.to_string()).await.unwrap();

    let sent = [
        message(MessageType::MsgAppend, 1, 2, 1, 5),
        message(MessageType::MsgHeartbeat, 1, 2, 2, 0),
    ];
    let mut grouped = HashMap::new();
    grouped.insert(2, sent.iter().map(encode).collect::<Vec<_>>());
    // Peer 99 was never registered: it must fail without sinking the batch
    // destined for the healthy peer.
    grouped.insert(
        99,
        vec![encode(&message(MessageType::MsgHeartbeat, 1, 99, 1, 0))],
    );

    let err = transport
        .send_grouped(grouped)
        .await
        .expect_err("one failed peer must yield a summary error");
    match err {
        CatgaRaftError::Transport(msg) => {
            assert!(
                msg.contains("1 sends failed in send_grouped"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected Transport error, got: {other:?}"),
    }

    // The healthy peer's batch must have been delivered in order despite the
    // concurrent failure.
    let got = received.lock().expect("sink lock");
    assert_eq!(got.len(), sent.len(), "healthy peer must receive its batch");
    let got_keys: Vec<_> = got.iter().map(key).collect();
    let sent_keys: Vec<_> = sent.iter().map(key).collect();
    assert_eq!(got_keys, sent_keys, "messages must arrive in send order");

    handle.abort();
}

#[tokio::test]
async fn send_grouped_counts_every_failed_peer_in_summary() {
    let transport = GrpcTransport::new(1);

    let mut grouped = HashMap::new();
    // Unknown peer: NodeNotFound mapped into the summary.
    grouped.insert(
        7,
        vec![encode(&message(MessageType::MsgHeartbeat, 1, 7, 1, 0))],
    );
    // Registered but unreachable peer: invalid endpoint fails fast and
    // deterministically.
    transport
        .add_peer(8, "http://not a valid endpoint".to_string())
        .await
        .unwrap();
    grouped.insert(
        8,
        vec![encode(&message(MessageType::MsgHeartbeat, 1, 8, 1, 0))],
    );

    let err = transport
        .send_grouped(grouped)
        .await
        .expect_err("two failed peers must yield a summary error");
    match err {
        CatgaRaftError::Transport(msg) => {
            assert!(
                msg.contains("2 sends failed in send_grouped"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected Transport error, got: {other:?}"),
    }
}

#[tokio::test]
async fn send_grouped_self_skip_counts_as_success_in_mixed_group() {
    let (addr, handle, received) = start_server().await;

    // Register the live server under id 0 as well: the self-skip must drop
    // that batch while the other peer still succeeds, yielding overall Ok.
    let transport = GrpcTransport::new(1);
    transport.add_peer(0, addr.to_string()).await.unwrap();
    transport.add_peer(2, addr.to_string()).await.unwrap();

    let to_two = [message(MessageType::MsgAppend, 1, 2, 1, 5)];
    let mut grouped = HashMap::new();
    grouped.insert(
        0,
        vec![encode(&message(MessageType::MsgHeartbeat, 1, 0, 1, 0))],
    );
    grouped.insert(2, to_two.iter().map(encode).collect::<Vec<_>>());

    transport.send_grouped(grouped).await.unwrap();

    let got = received.lock().expect("sink lock");
    assert_eq!(
        got.len(),
        to_two.len(),
        "only peer 2's batch may arrive; peer 0 is skipped"
    );
    assert_eq!(
        got.iter().map(key).collect::<Vec<_>>(),
        to_two.iter().map(key).collect::<Vec<_>>()
    );

    handle.abort();
}

// ============================================================================
// ConnectionPool: warm-path read lock + atomic round-robin
// ============================================================================

/// Bind an ephemeral TCP listener and spawn an accept loop that drops
/// accepted sockets (tonic's `connect` only needs the TCP handshake).
async fn start_dummy_listener() -> (tokio::task::JoinHandle<()>, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let handle =
        tokio::spawn(async move { while let Ok((_socket, _peer)) = listener.accept().await {} });
    (handle, format!("http://{}", addr))
}

#[tokio::test]
async fn conn_pool_warm_path_serves_concurrent_reads_without_growing() {
    let (listener, endpoint) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint, 4);

    // Prime the pool so every subsequent call hits the warm fast path.
    let _channel = pool.get_channel().await.expect("prime get_channel");
    assert_eq!(pool.connection_count(), 1);

    // Hammer the warm path concurrently: the read-lock fast path must serve
    // every call and must never create additional connections.
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let p = pool.clone();
        tasks.push(tokio::spawn(async move { p.get_channel().await }));
    }
    for t in tasks {
        t.await
            .expect("task join")
            .expect("warm get_channel succeeds");
    }

    assert_eq!(
        pool.connection_count(),
        1,
        "warm-path reads must not grow the pool"
    );
    assert!(!pool.is_empty());
    assert!(!pool.is_full());

    listener.abort();
}

#[tokio::test]
async fn conn_pool_warm_path_survives_clear_and_refill() {
    let (listener, endpoint) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint, 4);
    let _channel = pool.get_channel().await.expect("prime get_channel");

    // Sequential warm calls advance the atomic cursor without reconnecting.
    for _ in 0..8 {
        let _channel = pool.get_channel().await.expect("warm get_channel");
    }
    assert_eq!(pool.connection_count(), 1);

    // clear resets the cursor; the pool must reconnect cleanly afterwards.
    pool.clear();
    assert!(pool.is_empty());
    let _channel = pool.get_channel().await.expect("get_channel after clear");
    assert_eq!(pool.connection_count(), 1);

    listener.abort();
}
