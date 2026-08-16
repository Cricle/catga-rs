//! End-to-end tests for the gRPC raft transport.
//!
//! Covers the real server (`transport/server.rs`) bound to an ephemeral port
//! and the real client path (`transport/grpc.rs`), exchanging
//! protobuf-encoded `raft::prelude::Message` payloads:
//! - `Step` delivers a decoded message to the service callback
//! - `StepBatch` delivers every payload of a batch in order
//! - undecodable payloads are rejected with `INVALID_ARGUMENT`
//! - callback errors surface as `INTERNAL` statuses

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use catga_raft::transport::grpc::PeerClient;
use catga_raft::transport::proto_gen::pb;
use catga_raft::transport::server::{RaftGrpcService, serve_with_bound_addr};
use catga_raft::{CatgaRaftError, GrpcTransport};
use crossbeam::channel::Receiver;
use raft::prelude::MessageType;

/// Build a heartbeat message as used in the wire-format contract.
fn heartbeat(from: u64, to: u64, term: u64) -> raft::prelude::Message {
    let mut msg = raft::prelude::Message::default();
    msg.msg_type = MessageType::MsgHeartbeat;
    msg.from = from;
    msg.to = to;
    msg.term = term;
    msg
}

/// Serialize a raft message with the same codec the wire contract mandates.
fn encode(msg: &raft::prelude::Message) -> Vec<u8> {
    <raft::prelude::Message as protobuf::Message>::write_to_bytes(msg)
        .expect("valid raft message serializes")
}

/// Start a transport server whose callback records every received message on
/// an unbounded channel. Returns the bound address, the server handle and the
/// receiving end of the callback channel.
async fn start_server() -> (
    SocketAddr,
    tokio::task::JoinHandle<()>,
    Receiver<raft::prelude::Message>,
) {
    let (tx, rx) = crossbeam::channel::unbounded();
    let service = RaftGrpcService::new(move |msg| {
        tx.send(msg)
            .map_err(|e| CatgaRaftError::Transport(e.to_string()))
    });
    let (addr, handle) = serve_with_bound_addr(SocketAddr::from(([127, 0, 0, 1], 0)), service)
        .await
        .expect("serve on ephemeral port");
    (addr, handle, rx)
}

/// Open a raw tonic channel to the test server.
async fn connect(addr: SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect to test server")
}

/// Receive one message from the callback channel with a bounded wait.
fn recv(rx: &Receiver<raft::prelude::Message>) -> raft::prelude::Message {
    rx.recv_timeout(Duration::from_secs(5))
        .expect("message delivered to step callback")
}

#[tokio::test]
async fn step_delivers_message_to_callback() {
    let (addr, handle, rx) = start_server().await;

    // Full client path: GrpcTransport -> PeerClient -> Step RPC.
    let transport = GrpcTransport::new(1);
    transport.add_peer(2, addr.to_string()).await.unwrap();
    transport
        .send(2, Bytes::from(encode(&heartbeat(2, 1, 1))))
        .await
        .unwrap();

    let received = recv(&rx);
    assert_eq!(received.msg_type, MessageType::MsgHeartbeat);
    assert_eq!(received.from, 2);
    assert_eq!(received.to, 1);
    assert_eq!(received.term, 1);

    handle.abort();
}

#[tokio::test]
async fn step_batch_delivers_all_messages_in_order() {
    let (addr, handle, rx) = start_server().await;

    let client = PeerClient::new(3, format!("http://{}", addr));
    client
        .send_batch(vec![
            Bytes::from(encode(&heartbeat(2, 1, 1))),
            Bytes::from(encode(&heartbeat(3, 1, 2))),
        ])
        .await
        .unwrap();

    let first = recv(&rx);
    let second = recv(&rx);
    assert_eq!(first.msg_type, MessageType::MsgHeartbeat);
    assert_eq!((first.from, first.to, first.term), (2, 1, 1));
    assert_eq!((second.from, second.to, second.term), (3, 1, 2));

    handle.abort();
}

#[tokio::test]
async fn step_rejects_undecodable_payload_with_invalid_argument() {
    let (addr, handle, rx) = start_server().await;

    let mut raw = pb::raft_client::RaftClient::new(connect(addr).await);
    // Wire type 7 does not exist, so this can never parse as a Message.
    let status = raw
        .step(pb::RaftMessage {
            payload: Bytes::from_static(&[0xFF]),
        })
        .await
        .expect_err("garbage payload must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(rx.is_empty(), "callback must not see garbage payloads");

    handle.abort();
}

#[tokio::test]
async fn step_maps_callback_errors_to_internal_status() {
    let service = RaftGrpcService::new(|_| Err(CatgaRaftError::Transport("step boom".to_string())));
    let (addr, handle) = serve_with_bound_addr(SocketAddr::from(([127, 0, 0, 1], 0)), service)
        .await
        .expect("serve on ephemeral port");

    let mut raw = pb::raft_client::RaftClient::new(connect(addr).await);
    let status = raw
        .step(pb::RaftMessage {
            payload: Bytes::from(encode(&heartbeat(2, 1, 1))),
        })
        .await
        .expect_err("callback failure must surface as a status");
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(
        status.message().contains("step boom"),
        "status should carry the callback error, got: {}",
        status.message()
    );

    handle.abort();
}

/// Guard against accidental self-delivery regressions in the service: a
/// failing callback must not swallow the rest of a batch silently.
#[tokio::test]
async fn batch_rejects_when_payload_undecodable() {
    let (addr, handle, rx) = start_server().await;

    let mut raw = pb::raft_client::RaftClient::new(connect(addr).await);
    let status = raw
        .step_batch(pb::RaftMessageBatch {
            payloads: vec![
                Bytes::from(encode(&heartbeat(2, 1, 1))),
                Bytes::from_static(&[0xFF]),
            ],
        })
        .await
        .expect_err("batch with a garbage payload must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // The valid payload before the bad one was still stepped.
    let received = recv(&rx);
    assert_eq!((received.from, received.term), (2, 1));

    handle.abort();
}
