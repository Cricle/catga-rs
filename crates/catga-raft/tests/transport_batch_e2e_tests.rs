//! End-to-end tests for grouped batch sending over the gRPC raft transport.
//!
//! Covers `GrpcTransport::send_grouped` against the real server
//! (`transport/server.rs`) bound to an ephemeral port:
//! - all messages of a peer arrive via one StepBatch RPC, in order
//! - groups fan out to multiple peers concurrently
//! - an empty map succeeds without touching the network
//! - an unknown peer yields a Transport error
//! - peer id 0 (the local node itself) is skipped

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
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
    let mut msg = raft::prelude::Message::default();
    msg.msg_type = msg_type;
    msg.from = from;
    msg.to = to;
    msg.term = term;
    msg.index = index;
    msg
}

/// Serialize a raft message with the same codec the wire contract mandates.
fn encode(msg: &raft::prelude::Message) -> Bytes {
    Bytes::from(
        <raft::prelude::Message as protobuf::Message>::write_to_bytes(msg)
            .expect("valid raft message serializes"),
    )
}

/// Identity fields used to compare delivery order without depending on
/// protobuf internals of the decoded message.
fn key(msg: &raft::prelude::Message) -> (MessageType, u64, u64, u64, u64) {
    (msg.msg_type, msg.from, msg.to, msg.term, msg.index)
}

/// Start a transport server whose callback records every received message
/// into a shared sink. Returns the bound address, the server handle and the
/// sink.
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

#[tokio::test]
async fn send_grouped_delivers_all_messages_of_a_peer_in_order() {
    let (addr, handle, received) = start_server().await;

    let transport = GrpcTransport::new(1);
    transport.add_peer(2, addr.to_string()).await.unwrap();

    let sent = vec![
        message(MessageType::MsgAppend, 1, 2, 1, 5),
        message(MessageType::MsgAppend, 1, 2, 1, 8),
        message(MessageType::MsgHeartbeat, 1, 2, 2, 0),
        message(MessageType::MsgAppendResponse, 3, 2, 1, 8),
    ];
    let mut grouped = HashMap::new();
    grouped.insert(2, sent.iter().map(encode).collect::<Vec<_>>());

    transport.send_grouped(grouped).await.unwrap();

    let got = received.lock().expect("sink lock");
    assert_eq!(got.len(), sent.len(), "every message must arrive");
    let got_keys: Vec<_> = got.iter().map(key).collect();
    let sent_keys: Vec<_> = sent.iter().map(key).collect();
    assert_eq!(got_keys, sent_keys, "messages must arrive in send order");

    handle.abort();
}

#[tokio::test]
async fn send_grouped_fans_out_to_multiple_peers() {
    let (addr_a, handle_a, received_a) = start_server().await;
    let (addr_b, handle_b, received_b) = start_server().await;

    let transport = GrpcTransport::new(1);
    transport.add_peer(2, addr_a.to_string()).await.unwrap();
    transport.add_peer(3, addr_b.to_string()).await.unwrap();

    let to_a = vec![
        message(MessageType::MsgAppend, 1, 2, 1, 5),
        message(MessageType::MsgHeartbeat, 1, 2, 1, 0),
    ];
    let to_b = vec![message(MessageType::MsgRequestVote, 1, 3, 2, 9)];
    let mut grouped = HashMap::new();
    grouped.insert(2, to_a.iter().map(encode).collect::<Vec<_>>());
    grouped.insert(3, to_b.iter().map(encode).collect::<Vec<_>>());

    transport.send_grouped(grouped).await.unwrap();

    let got_a = received_a.lock().expect("sink lock");
    let got_b = received_b.lock().expect("sink lock");
    assert_eq!(
        got_a.iter().map(key).collect::<Vec<_>>(),
        to_a.iter().map(key).collect::<Vec<_>>()
    );
    assert_eq!(
        got_b.iter().map(key).collect::<Vec<_>>(),
        to_b.iter().map(key).collect::<Vec<_>>()
    );

    handle_a.abort();
    handle_b.abort();
}

#[tokio::test]
async fn send_grouped_empty_map_is_ok() {
    let transport = GrpcTransport::new(1);
    transport.send_grouped(HashMap::new()).await.unwrap();
}

#[tokio::test]
async fn send_grouped_unknown_peer_yields_transport_error() {
    let transport = GrpcTransport::new(1);

    let mut grouped = HashMap::new();
    grouped.insert(
        7,
        vec![encode(&message(MessageType::MsgHeartbeat, 1, 7, 1, 0))],
    );

    let err = transport
        .send_grouped(grouped)
        .await
        .expect_err("unknown peer must fail");
    assert!(
        matches!(err, CatgaRaftError::Transport(_)),
        "expected Transport error, got: {}",
        err
    );
}

#[tokio::test]
async fn send_grouped_skips_self_peer_zero() {
    let (addr, handle, received) = start_server().await;

    // Register the server under id 0 as well: if the self-skip regressed,
    // the batch would be delivered there instead of being dropped.
    let transport = GrpcTransport::new(1);
    transport.add_peer(0, addr.to_string()).await.unwrap();

    let mut grouped = HashMap::new();
    grouped.insert(
        0,
        vec![encode(&message(MessageType::MsgHeartbeat, 1, 0, 1, 0))],
    );

    transport.send_grouped(grouped).await.unwrap();
    assert!(
        received.lock().expect("sink lock").is_empty(),
        "messages addressed to peer 0 must be skipped"
    );

    handle.abort();
}
