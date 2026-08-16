//! Integration tests for `catga_raft::transport::trait_` (the `Transport` trait).
//!
//! The `Transport` trait is publicly reachable both as
//! `catga_raft::transport::Transport` (re-export) and
//! `catga_raft::transport::trait_::Transport` (original path). The crate does
//! not ship a `Transport` impl (the `GrpcTransport` type exposes equivalent
//! inherent methods instead), so these tests exercise the trait contract
//! itself through small reference implementations defined here:
//!
//! - construction/defaults of an implementor (empty peer set, local id)
//! - peer lifecycle (`add_peer`, `remove_peer`, `has_peer`, `peer_ids`,
//!   `peer_count`)
//! - happy paths for `send_message`, `broadcast`, `send_many`
//! - the DEFAULT trait methods `send_snapshot` / `send_vote_request`, which
//!   must delegate to `send_message` and propagate its errors
//! - honoring an overridden `send_snapshot`
//! - error cases (`NodeNotFound`, failing peers, partial `send_many` failure)
//! - the `Send + Sync` supertraits and generic use via `T: Transport` bounds
//!
//! All futures used here are `std::future::ready`, so no real networking or
//! sleeping is involved and every test completes instantly.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use bytes::Bytes;
use catga_raft::transport::Transport;
use catga_raft::transport::trait_::Transport as TraitModuleTransport;
use catga_raft::{CatgaRaftError, CatgaRaftResult};

// ============================================================================
// Reference implementation of the public `Transport` trait
// ============================================================================

/// One successfully delivered message.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SentRecord {
    peer_id: u64,
    payload: Vec<u8>,
}

/// A deterministic in-memory `Transport` used to pin down the trait contract.
#[derive(Default)]
struct MockTransport {
    local_id: u64,
    peers: Mutex<BTreeMap<u64, String>>,
    sent: Mutex<Vec<SentRecord>>,
    fail_peers: Mutex<HashSet<u64>>,
}

impl MockTransport {
    fn new(local_id: u64) -> Self {
        Self {
            local_id,
            ..Default::default()
        }
    }

    fn records(&self) -> Vec<SentRecord> {
        self.sent.lock().unwrap().clone()
    }

    /// Mark a peer as unreachable so every delivery to it fails.
    fn fail_peer(&self, peer_id: u64) {
        self.fail_peers.lock().unwrap().insert(peer_id);
    }

    /// Synchronous core of delivery, shared by all trait methods.
    fn try_deliver(&self, peer_id: u64, payload: Bytes) -> CatgaRaftResult<()> {
        if !self.peers.lock().unwrap().contains_key(&peer_id) {
            return Err(CatgaRaftError::NodeNotFound(peer_id));
        }
        if self.fail_peers.lock().unwrap().contains(&peer_id) {
            return Err(CatgaRaftError::Transport(format!(
                "peer {peer_id} unreachable"
            )));
        }
        self.sent.lock().unwrap().push(SentRecord {
            peer_id,
            payload: payload.to_vec(),
        });
        Ok(())
    }
}

impl Transport for MockTransport {
    fn send_message(
        &self,
        peer_id: u64,
        message: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        std::future::ready(self.try_deliver(peer_id, message))
    }

    // `send_snapshot` and `send_vote_request` intentionally NOT overridden:
    // we want the trait's default implementations under test.

    fn broadcast(
        &self,
        message: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        let peer_ids: Vec<u64> = self
            .peers
            .lock()
            .unwrap()
            .keys()
            .copied()
            .filter(|id| *id != self.local_id)
            .collect();
        let mut result: CatgaRaftResult<()> = Ok(());
        for id in peer_ids {
            if let Err(e) = self.try_deliver(id, message.clone()) {
                result = Err(e);
                break;
            }
        }
        std::future::ready(result)
    }

    fn send_many(
        &self,
        messages: HashMap<u64, Bytes>,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        let mut failures = 0usize;
        for (peer_id, message) in messages {
            if self.try_deliver(peer_id, message).is_err() {
                failures += 1;
            }
        }
        std::future::ready(if failures == 0 {
            Ok(())
        } else {
            Err(CatgaRaftError::Transport(format!(
                "{failures} sends failed in send_many"
            )))
        })
    }

    fn add_peer(
        &self,
        peer_id: u64,
        addr: String,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        self.peers.lock().unwrap().insert(peer_id, addr);
        std::future::ready(Ok(()))
    }

    fn remove_peer(
        &self,
        peer_id: u64,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        let removed = self.peers.lock().unwrap().remove(&peer_id).is_some();
        std::future::ready(if removed {
            Ok(())
        } else {
            Err(CatgaRaftError::NodeNotFound(peer_id))
        })
    }

    fn local_node_id(&self) -> u64 {
        self.local_id
    }

    fn has_peer(&self, peer_id: u64) -> bool {
        self.peers.lock().unwrap().contains_key(&peer_id)
    }

    fn peer_ids(&self) -> Vec<u64> {
        self.peers.lock().unwrap().keys().copied().collect()
    }

    fn peer_count(&self) -> usize {
        self.peers.lock().unwrap().len()
    }
}

/// Transport that overrides `send_snapshot` to prove overrides are honored
/// instead of the default delegation to `send_message`.
#[derive(Default)]
struct SnapshotOverrideTransport {
    inner: MockTransport,
    snapshot_payloads: Mutex<Vec<Vec<u8>>>,
}

impl Transport for SnapshotOverrideTransport {
    fn send_message(
        &self,
        peer_id: u64,
        message: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        self.inner.send_message(peer_id, message)
    }

    fn send_snapshot(
        &self,
        _peer_id: u64,
        snapshot: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        self.snapshot_payloads
            .lock()
            .unwrap()
            .push(snapshot.to_vec());
        std::future::ready(Ok(()))
    }

    fn broadcast(
        &self,
        message: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        self.inner.broadcast(message)
    }

    fn send_many(
        &self,
        messages: HashMap<u64, Bytes>,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        self.inner.send_many(messages)
    }

    fn add_peer(
        &self,
        peer_id: u64,
        addr: String,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        self.inner.add_peer(peer_id, addr)
    }

    fn remove_peer(
        &self,
        peer_id: u64,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        self.inner.remove_peer(peer_id)
    }

    fn local_node_id(&self) -> u64 {
        self.inner.local_node_id()
    }

    fn has_peer(&self, peer_id: u64) -> bool {
        self.inner.has_peer(peer_id)
    }

    fn peer_ids(&self) -> Vec<u64> {
        self.inner.peer_ids()
    }

    fn peer_count(&self) -> usize {
        self.inner.peer_count()
    }
}

/// Generic helper proving the trait can be used behind a `T: Transport` bound
/// (the trait's stated purpose: a unified interface for any implementation).
async fn deliver_via_trait<T: Transport>(
    transport: &T,
    peer_id: u64,
    payload: Bytes,
) -> CatgaRaftResult<()> {
    transport.send_message(peer_id, payload).await
}

fn assert_send_sync<T: Send + Sync>() {}

// ============================================================================
// Construction / defaults
// ============================================================================

#[test]
fn transport_trait_new_transport_has_empty_peer_set() {
    let transport = MockTransport::new(7);
    assert_eq!(transport.local_node_id(), 7);
    assert_eq!(transport.peer_count(), 0);
    assert!(transport.peer_ids().is_empty());
    assert!(!transport.has_peer(2));
    assert!(transport.records().is_empty());
}

// ============================================================================
// Peer lifecycle
// ============================================================================

#[tokio::test]
async fn transport_trait_add_and_remove_peer_lifecycle() {
    let transport = MockTransport::new(1);

    transport
        .add_peer(2, "127.0.0.1:0".to_string())
        .await
        .unwrap();
    transport
        .add_peer(3, "127.0.0.1:0".to_string())
        .await
        .unwrap();

    assert!(transport.has_peer(2));
    assert!(transport.has_peer(3));
    assert_eq!(transport.peer_count(), 2);
    // BTreeMap-backed: ids come back sorted.
    assert_eq!(transport.peer_ids(), vec![2, 3]);

    // Re-adding an existing peer updates it in place; count must not grow.
    transport
        .add_peer(2, "127.0.0.1:1".to_string())
        .await
        .unwrap();
    assert_eq!(transport.peer_count(), 2);

    transport.remove_peer(2).await.unwrap();
    assert!(!transport.has_peer(2));
    assert_eq!(transport.peer_ids(), vec![3]);
    assert_eq!(transport.peer_count(), 1);
}

#[tokio::test]
async fn transport_trait_remove_unknown_peer_is_node_not_found() {
    let transport = MockTransport::new(1);
    let err = transport.remove_peer(99).await.unwrap_err();
    assert!(matches!(err, CatgaRaftError::NodeNotFound(99)));
}

// ============================================================================
// send_message happy path / error cases
// ============================================================================

#[tokio::test]
async fn transport_trait_send_message_records_payload_for_known_peer() {
    let transport = MockTransport::new(1);
    transport
        .add_peer(2, "127.0.0.1:0".to_string())
        .await
        .unwrap();

    // Goes through a generic `T: Transport` helper, exercising the trait bound.
    deliver_via_trait(&transport, 2, Bytes::from_static(b"hello peer 2"))
        .await
        .unwrap();

    assert_eq!(
        transport.records(),
        vec![SentRecord {
            peer_id: 2,
            payload: b"hello peer 2".to_vec(),
        }]
    );
}

#[tokio::test]
async fn transport_trait_send_message_unknown_peer_returns_node_not_found() {
    let transport = MockTransport::new(1);
    let err = transport
        .send_message(42, Bytes::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(matches!(err, CatgaRaftError::NodeNotFound(42)));
    assert!(transport.records().is_empty());
}

// ============================================================================
// Default trait methods: send_snapshot / send_vote_request
// ============================================================================

#[tokio::test]
async fn transport_trait_default_send_snapshot_delegates_to_send_message() {
    let transport = MockTransport::new(1);
    transport
        .add_peer(2, "127.0.0.1:0".to_string())
        .await
        .unwrap();

    transport
        .send_snapshot(2, Bytes::from_static(b"snapshot-bytes"))
        .await
        .unwrap();

    // The default impl routes the snapshot through send_message, so it must
    // appear in the regular message log exactly once with the same payload.
    assert_eq!(
        transport.records(),
        vec![SentRecord {
            peer_id: 2,
            payload: b"snapshot-bytes".to_vec(),
        }]
    );
}

#[tokio::test]
async fn transport_trait_default_send_vote_request_delegates_to_send_message() {
    let transport = MockTransport::new(1);
    transport
        .add_peer(3, "127.0.0.1:0".to_string())
        .await
        .unwrap();

    transport
        .send_vote_request(3, Bytes::from_static(b"vote-req"))
        .await
        .unwrap();

    assert_eq!(
        transport.records(),
        vec![SentRecord {
            peer_id: 3,
            payload: b"vote-req".to_vec(),
        }]
    );
}

#[tokio::test]
async fn transport_trait_default_impls_propagate_send_errors() {
    let transport = MockTransport::new(1);
    transport
        .add_peer(2, "127.0.0.1:0".to_string())
        .await
        .unwrap();
    transport.fail_peer(2);

    let snapshot_err = transport
        .send_snapshot(2, Bytes::from_static(b"snap"))
        .await
        .unwrap_err();
    assert!(matches!(snapshot_err, CatgaRaftError::Transport(_)));

    let vote_err = transport
        .send_vote_request(2, Bytes::from_static(b"vote"))
        .await
        .unwrap_err();
    assert!(matches!(vote_err, CatgaRaftError::Transport(_)));

    // Failed deliveries must not be recorded as sent.
    assert!(transport.records().is_empty());
}

#[tokio::test]
async fn transport_trait_overridden_send_snapshot_bypasses_default() {
    let transport = SnapshotOverrideTransport::default();
    transport
        .add_peer(2, "127.0.0.1:0".to_string())
        .await
        .unwrap();

    transport
        .send_snapshot(2, Bytes::from_static(b"big-snapshot"))
        .await
        .unwrap();

    // Override records the snapshot separately and must NOT fall back to
    // the default send_message delegation.
    assert_eq!(
        *transport.snapshot_payloads.lock().unwrap(),
        vec![b"big-snapshot".to_vec()]
    );
    assert!(transport.inner.records().is_empty());
}

// ============================================================================
// broadcast
// ============================================================================

#[tokio::test]
async fn transport_trait_broadcast_delivers_to_every_peer() {
    let transport = MockTransport::new(1);
    for id in [2u64, 3, 4] {
        transport
            .add_peer(id, "127.0.0.1:0".to_string())
            .await
            .unwrap();
    }

    transport
        .broadcast(Bytes::from_static(b"heartbeat"))
        .await
        .unwrap();

    assert_eq!(
        transport.records(),
        vec![
            SentRecord {
                peer_id: 2,
                payload: b"heartbeat".to_vec()
            },
            SentRecord {
                peer_id: 3,
                payload: b"heartbeat".to_vec()
            },
            SentRecord {
                peer_id: 4,
                payload: b"heartbeat".to_vec()
            },
        ]
    );
}

#[tokio::test]
async fn transport_trait_broadcast_with_no_peers_is_ok() {
    let transport = MockTransport::new(1);
    transport
        .broadcast(Bytes::from_static(b"nobody home"))
        .await
        .unwrap();
    assert!(transport.records().is_empty());
}

// ============================================================================
// send_many
// ============================================================================

#[tokio::test]
async fn transport_trait_send_many_routes_distinct_payloads_per_peer() {
    let transport = MockTransport::new(1);
    for id in [2u64, 3] {
        transport
            .add_peer(id, "127.0.0.1:0".to_string())
            .await
            .unwrap();
    }

    let mut messages = HashMap::new();
    messages.insert(2u64, Bytes::from_static(b"for-two"));
    messages.insert(3u64, Bytes::from_static(b"for-three"));
    transport.send_many(messages).await.unwrap();

    // HashMap iteration order is nondeterministic; compare as a sorted set.
    let mut records = transport.records();
    records.sort_by_key(|r| r.peer_id);
    assert_eq!(
        records,
        vec![
            SentRecord {
                peer_id: 2,
                payload: b"for-two".to_vec()
            },
            SentRecord {
                peer_id: 3,
                payload: b"for-three".to_vec()
            },
        ]
    );
}

#[tokio::test]
async fn transport_trait_send_many_empty_map_is_ok() {
    let transport = MockTransport::new(1);
    transport.send_many(HashMap::new()).await.unwrap();
    assert!(transport.records().is_empty());
}

#[tokio::test]
async fn transport_trait_send_many_reports_failure_for_unknown_peer() {
    let transport = MockTransport::new(1);
    transport
        .add_peer(2, "127.0.0.1:0".to_string())
        .await
        .unwrap();

    let mut messages = HashMap::new();
    messages.insert(2u64, Bytes::from_static(b"ok"));
    messages.insert(99u64, Bytes::from_static(b"unknown"));

    let err = transport.send_many(messages).await.unwrap_err();
    assert!(matches!(err, CatgaRaftError::Transport(_)));

    // The valid peer still received its message despite the partial failure.
    assert_eq!(
        transport.records(),
        vec![SentRecord {
            peer_id: 2,
            payload: b"ok".to_vec()
        }]
    );
}

// ============================================================================
// Supertraits: Transport requires Send + Sync implementors
// ============================================================================

#[test]
fn transport_trait_implementor_satisfies_send_sync_supertraits() {
    // Compile-time proof that implementors can be Send + Sync.
    assert_send_sync::<MockTransport>();
    assert_send_sync::<SnapshotOverrideTransport>();

    // Runtime proof: a Transport can be moved across OS threads.
    let transport = std::sync::Arc::new(MockTransport::new(1));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(transport.add_peer(2, "127.0.0.1:0".to_string()))
        .unwrap();

    let moved = std::sync::Arc::clone(&transport);
    let handle = std::thread::spawn(move || (moved.local_node_id(), moved.peer_count()));
    assert_eq!(handle.join().unwrap(), (1, 1));
}

// Compile-time proof that the original module path (`transport::trait_`) and
// the re-export (`transport::Transport`) name the same public trait: an impl
// written against the re-exported path satisfies the `trait_`-path bound.
#[allow(dead_code)]
fn _trait_paths_are_interchangeable<T: TraitModuleTransport>(t: &T) -> u64 {
    let via_reexport: fn(&MockTransport) -> u64 = Transport::local_node_id;
    let _ = via_reexport;
    t.local_node_id()
}
