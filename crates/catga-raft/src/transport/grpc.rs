//! gRPC Transport implementation for Raft cluster communication.
//!
//! This module provides the main `GrpcTransport` implementation that uses
//! tonic for high-performance gRPC communication between Raft cluster nodes.
//!
//! # Features
//!
//! - Connection pooling per peer (4 channels by default)
//! - Backpressure control via semaphore
//! - Circuit breaker for fault isolation
//! - Message batching and aggregation
//! - Concurrent broadcast and send_many operations

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::Semaphore;

use crate::{CatgaRaftError, CatgaRaftResult};

use super::{
    backpressure::BackpressureController, batch::BatchSender, breaker::CircuitBreaker,
    codec::RaftCodec, connection::ConnectionPool, proto_gen::pb,
};

/// Maximum concurrent requests per peer (backpressure limit)
const MAX_PENDING_PER_CONN: usize = 128;

/// Default connection pool size per peer
const DEFAULT_POOL_SIZE: usize = 4;

/// Default in-flight message limit per peer
const DEFAULT_INFLIGHT_LIMIT: usize = 256;

/// Default batch size for message aggregation
const DEFAULT_BATCH_SIZE: usize = 64;

/// Default batch flush interval in milliseconds
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1;

/// Client handle for sending Raft messages to a specific peer.
///
/// Combines connection pooling, backpressure control, circuit breaker,
/// and batch sending for robust message delivery.
#[derive(Clone)]
#[allow(dead_code)]
pub struct PeerClient {
    peer_id: u64,
    pool: ConnectionPool,
    /// Semaphore to limit concurrent requests to this peer
    semaphore: Arc<Semaphore>,
    /// Circuit breaker for fault isolation
    circuit_breaker: Arc<CircuitBreaker>,
    /// Batch sender for message aggregation
    batch_sender: Arc<BatchSender>,
    /// Backpressure controller for in-flight tracking
    backpressure: Arc<BackpressureController>,
}

impl PeerClient {
    /// Create a new PeerClient for the given endpoint.
    pub fn new(peer_id: u64, endpoint: String) -> Self {
        Self {
            peer_id,
            pool: ConnectionPool::new(endpoint, DEFAULT_POOL_SIZE),
            semaphore: Arc::new(Semaphore::new(MAX_PENDING_PER_CONN)),
            circuit_breaker: Arc::new(CircuitBreaker::new(Default::default())),
            batch_sender: Arc::new(BatchSender::new(
                Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS),
                DEFAULT_BATCH_SIZE,
            )),
            backpressure: Arc::new(BackpressureController::new(DEFAULT_INFLIGHT_LIMIT)),
        }
    }

    /// Create a new PeerClient with custom configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn with_config(
        peer_id: u64,
        endpoint: String,
        pool_size: usize,
        max_pending: usize,
        inflight_limit: usize,
        batch_size: usize,
        flush_interval: Duration,
        circuit_breaker_config: super::breaker::CircuitBreakerConfig,
    ) -> Self {
        Self {
            peer_id,
            pool: ConnectionPool::new(endpoint, pool_size),
            semaphore: Arc::new(Semaphore::new(max_pending)),
            circuit_breaker: Arc::new(CircuitBreaker::new(circuit_breaker_config)),
            batch_sender: Arc::new(BatchSender::new(flush_interval, batch_size)),
            backpressure: Arc::new(BackpressureController::new(inflight_limit)),
        }
    }

    /// Send a Raft message to this peer in a single Step RPC.
    ///
    /// Applies circuit breaker and backpressure control around the call.
    pub async fn send(&self, msg: Bytes) -> CatgaRaftResult<()> {
        self.guarded(self.do_send(msg)).await
    }

    /// Send a batch of Raft messages to this peer in a single StepBatch RPC.
    ///
    /// Applies circuit breaker and backpressure control once for the whole
    /// batch. An empty batch succeeds without touching the network or the
    /// breaker.
    pub async fn send_batch(&self, messages: Vec<Bytes>) -> CatgaRaftResult<()> {
        if messages.is_empty() {
            return Ok(());
        }
        self.guarded(self.do_send_batch(messages)).await
    }

    /// Run one RPC under circuit breaker and backpressure control.
    async fn guarded<T>(&self, op: impl Future<Output = CatgaRaftResult<T>>) -> CatgaRaftResult<T> {
        // Check circuit breaker
        if !self.circuit_breaker.is_allowed() {
            return Err(CatgaRaftError::CircuitBreakerOpen);
        }

        // Acquire semaphore permit (backpressure)
        let permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| CatgaRaftError::Transport("peer overloaded".into()))?;

        // Track in-flight
        let current_inflight = self.backpressure.increment_inflight(self.peer_id);
        if current_inflight > self.backpressure.current_limit() {
            self.backpressure.decrement_inflight(self.peer_id);
            drop(permit);
            return Err(CatgaRaftError::Backpressure);
        }

        let result = op.await;

        // Record success/failure in circuit breaker. Backpressure is the
        // exception: it signals our own load-shedding, not peer health.
        // Counting it would let local throttling open the breaker, silently
        // dropping every message to the peer and turning overload into a
        // partition.
        match &result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(CatgaRaftError::Backpressure) => {}
            Err(_) => self.circuit_breaker.record_failure(),
        }

        // Release resources
        self.backpressure.decrement_inflight(self.peer_id);
        drop(permit);

        result
    }

    /// Internal send implementation: one Step RPC over a pooled channel.
    async fn do_send(&self, msg: Bytes) -> CatgaRaftResult<()> {
        let channel = self.pool.get_channel().await?;
        let mut client = pb::raft_client::RaftClient::new(channel);

        let msg_size = msg.len();
        // Zero-copy: the generated `payload` field is `bytes::Bytes`.
        client
            .step(pb::RaftMessage { payload: msg })
            .await
            .map_err(|e| {
                CatgaRaftError::Transport(format!(
                    "step RPC to {} failed: {}",
                    self.pool.endpoint(),
                    e
                ))
            })?;

        tracing::trace!(
            endpoint = %self.pool.endpoint(),
            msg_size,
            "raft message sent"
        );

        Ok(())
    }

    /// Internal batch implementation: one StepBatch RPC over a pooled channel.
    async fn do_send_batch(&self, messages: Vec<Bytes>) -> CatgaRaftResult<()> {
        let channel = self.pool.get_channel().await?;
        let mut client = pb::raft_client::RaftClient::new(channel);

        let batch_size = messages.len();
        let total_bytes: usize = messages.iter().map(|m| m.len()).sum();
        // Zero-copy: the generated `payloads` field is `Vec<bytes::Bytes>`.
        client
            .step_batch(pb::RaftMessageBatch { payloads: messages })
            .await
            .map_err(|e| {
                CatgaRaftError::Transport(format!(
                    "step_batch RPC to {} failed: {}",
                    self.pool.endpoint(),
                    e
                ))
            })?;

        tracing::trace!(
            endpoint = %self.pool.endpoint(),
            batch_size,
            total_bytes,
            "raft message batch sent"
        );

        Ok(())
    }

    /// Get the circuit breaker state.
    pub fn circuit_breaker_state(&self) -> super::breaker::CircuitBreakerState {
        self.circuit_breaker.state()
    }

    /// Get current in-flight count.
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.backpressure.total_inflight()
    }
}

/// gRPC Transport for Raft cluster communication.
///
/// Manages connections to all peers in the Raft cluster and provides
/// reliable message delivery with connection pooling, backpressure,
/// circuit breaker, and message batching.
#[derive(Clone)]
pub struct GrpcTransport {
    peers: Arc<RwLock<HashMap<u64, PeerClient>>>,
    local_node_id: u64,
}

impl GrpcTransport {
    /// Create a new GrpcTransport for the given local node ID with default codec.
    pub fn new(local_node_id: u64) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            local_node_id,
        }
    }

    /// Create a new GrpcTransport with a specific codec.
    #[allow(dead_code)]
    pub fn new_with_codec(local_node_id: u64, _codec: impl RaftCodec) -> Self {
        Self::new(local_node_id)
    }

    /// Get the local node ID.
    pub fn local_node_id(&self) -> u64 {
        self.local_node_id
    }

    /// Add a new peer to the transport or update an existing peer's address.
    ///
    /// If the peer already exists, this will replace its connection pool
    /// with a new one pointing to the updated address.
    pub async fn add_peer(&self, peer_id: u64, addr: String) -> CatgaRaftResult<()> {
        // Ensure address has proper scheme
        let addr = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else {
            format!("http://{}", addr)
        };

        let addr_for_log = addr.clone();
        let mut peers = self.peers.write();
        peers.insert(peer_id, PeerClient::new(peer_id, addr));

        tracing::info!(
            peer_id,
            addr = %addr_for_log,
            "peer added to transport"
        );

        Ok(())
    }

    /// Remove a peer from the transport.
    ///
    /// This will close all connections to the peer.
    pub fn remove_peer(&self, peer_id: u64) -> CatgaRaftResult<()> {
        let mut peers = self.peers.write();
        if peers.remove(&peer_id).is_some() {
            tracing::info!(peer_id, "peer removed from transport");
            Ok(())
        } else {
            Err(CatgaRaftError::NodeNotFound(peer_id))
        }
    }

    /// Check if a peer exists in the transport.
    pub fn has_peer(&self, peer_id: u64) -> bool {
        self.peers.read().contains_key(&peer_id)
    }

    /// Get the number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// Get all peer IDs.
    pub fn peer_ids(&self) -> Vec<u64> {
        self.peers.read().keys().copied().collect()
    }

    /// Send a Raft message to a specific peer.
    ///
    /// Returns an error if the peer is not found or if the message
    /// fails to be sent.
    pub async fn send(&self, peer_id: u64, msg: Bytes) -> CatgaRaftResult<()> {
        // Skip sending to self
        if peer_id == self.local_node_id {
            tracing::trace!("skipped send to self");
            return Ok(());
        }

        // Clone the client and drop the guard before awaiting: parking_lot
        // guards are not Send, so holding one across the await would make
        // the whole future unusable inside tokio::spawn.
        let peer = self
            .peers
            .read()
            .get(&peer_id)
            .cloned()
            .ok_or(CatgaRaftError::NodeNotFound(peer_id))?;

        peer.send(msg).await
    }

    /// Send a Raft message to a specific peer, cloning the message data for each.
    ///
    /// Convenience method that converts `Vec<u8>` to `Bytes`.
    pub async fn send_vec(&self, peer_id: u64, msg: Vec<u8>) -> CatgaRaftResult<()> {
        self.send(peer_id, Bytes::from(msg)).await
    }

    /// Broadcast a Raft message to all connected peers concurrently.
    ///
    /// Errors from individual peers are collected and returned as a summary.
    /// If any peer fails, returns an error containing the failed peer IDs.
    pub async fn broadcast(&self, msg: Bytes) -> CatgaRaftResult<()> {
        let peer_ids: Vec<u64> = {
            let peers = self.peers.read();
            peers
                .keys()
                .filter(|&&id| id != self.local_node_id)
                .copied()
                .collect()
        };

        if peer_ids.is_empty() {
            return Ok(());
        }

        let mut errors = Vec::new();

        // Send to all peers concurrently using futures::future::join_all
        let mut results = Vec::new();
        for &peer_id in &peer_ids {
            let msg_clone = msg.clone();
            results.push(self.send(peer_id, msg_clone));
        }
        let joined = futures::future::join_all(results).await;

        for (i, result) in joined.into_iter().enumerate() {
            if let Err(e) = result {
                tracing::warn!(
                    peer_id = peer_ids[i],
                    error = %e,
                    "failed to broadcast to peer"
                );
                errors.push((peer_ids[i], e));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            let error_count = errors.len();
            let first_error = errors.swap_remove(0).1;
            tracing::error!(
                failed_count = error_count,
                total_peers = peer_ids.len(),
                "broadcast partially failed"
            );
            Err(CatgaRaftError::Transport(format!(
                "broadcast failed to {} peers: {}",
                error_count, first_error
            )))
        }
    }

    /// Broadcast a Raft message to all peers except the local node.
    ///
    /// Fire-and-forget variant that logs errors but does not return them.
    pub async fn broadcast_unchecked(&self, msg: Bytes) {
        let _ = self.broadcast(msg).await;
    }

    /// Send messages to multiple peers concurrently.
    ///
    /// Unlike broadcast, this sends different messages to each peer.
    /// The `messages` map should contain (peer_id -> message) pairs.
    pub async fn send_many(&self, messages: HashMap<u64, Bytes>) -> CatgaRaftResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut errors = Vec::new();

        let mut handles = Vec::new();
        let mut ids = Vec::new();
        for (peer_id, msg) in messages {
            ids.push(peer_id);
            let peers = Arc::clone(&self.peers);
            handles.push(tokio::spawn(async move {
                // Check for self-send
                if peer_id == 0 {
                    // local_node_id not accessible here, skip 0
                    return Ok(());
                }

                let peer = peers.read().get(&peer_id).cloned();
                match peer {
                    Some(peer) => peer.send(msg).await,
                    None => Err(CatgaRaftError::NodeNotFound(peer_id)),
                }
            }));
        }

        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(peer_id = ids[i], error = %e, "send_many failed for peer");
                    errors.push(e);
                }
                Err(e) => {
                    tracing::warn!(peer_id = ids[i], error = %e, "send_many task failed");
                    errors.push(CatgaRaftError::Transport(e.to_string()));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(CatgaRaftError::Transport(format!(
                "{} sends failed in send_many",
                errors.len()
            )))
        }
    }

    /// Send message groups to multiple peers concurrently, one StepBatch RPC
    /// per peer.
    ///
    /// The `messages` map contains (peer_id -> messages) pairs; every
    /// message of a peer is delivered in order within its single batch RPC.
    /// Errors from individual peers are collected and returned as a summary,
    /// matching `send_many` semantics.
    ///
    /// All peer sends run inside this one future via
    /// `futures::future::join_all` instead of one spawned task per peer:
    /// the owner calls this from an already-spawned task per ready, so the
    /// old per-peer spawn created ~N extra tasks per call site (~50 at
    /// 50 nodes). `join_all` drives every batch RPC concurrently on the
    /// current task without the spawn storm.
    pub async fn send_grouped(&self, messages: HashMap<u64, Vec<Bytes>>) -> CatgaRaftResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let sends = messages.into_iter().map(|(peer_id, msgs)| {
            let peers = Arc::clone(&self.peers);
            async move {
                // Peer id 0 is the local node itself; nothing to send.
                if peer_id == 0 {
                    return (peer_id, Ok(()));
                }

                // Clone the client and drop the guard before awaiting:
                // parking_lot guards are not Send, so holding one across
                // the await would make the future unusable in tokio::spawn.
                let peer = peers.read().get(&peer_id).cloned();
                let result = match peer {
                    Some(peer) => peer.send_batch(msgs).await,
                    None => Err(CatgaRaftError::NodeNotFound(peer_id)),
                };
                (peer_id, result)
            }
        });
        let joined = futures::future::join_all(sends).await;

        let mut failed = 0usize;
        for (peer_id, result) in joined {
            if let Err(e) = result {
                tracing::warn!(peer_id, error = %e, "send_grouped failed for peer");
                failed += 1;
            }
        }

        if failed == 0 {
            Ok(())
        } else {
            Err(CatgaRaftError::Transport(format!(
                "{} sends failed in send_grouped",
                failed
            )))
        }
    }
}

impl Default for GrpcTransport {
    fn default() -> Self {
        Self::new(0)
    }
}

impl std::fmt::Debug for GrpcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTransport")
            .field("local_node_id", &self.local_node_id)
            .field("peer_count", &self.peer_count())
            .finish()
    }
}

// Type alias for backwards compatibility
#[allow(dead_code)]
pub type RaftTransport = GrpcTransport;

#[cfg(feature = "use-bincode")]
mod codec_redefinitions {
    use super::*;

    /// Bincode codec implementation.
    pub struct BincodeCodec;

    impl super::RaftCodec for BincodeCodec {
        fn encode<T: serde::Serialize>(&self, value: &T) -> CatgaRaftResult<Vec<u8>> {
            bincode::serde::encode_to_vec(value, bincode::config::standard())
                .map_err(|e| CatgaRaftError::Codec(e.to_string()))
        }

        fn decode<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> CatgaRaftResult<T> {
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map(|(value, _)| value)
                .map_err(|e| CatgaRaftError::Codec(e.to_string()))
        }
    }
}

#[cfg(not(feature = "use-bincode"))]
mod codec_redefinitions {
    use super::*;

    /// Prost codec implementation (using JSON for simplicity without protobuf).
    pub struct BincodeCodec;

    impl super::RaftCodec for BincodeCodec {
        fn encode<T: serde::Serialize>(&self, value: &T) -> CatgaRaftResult<Vec<u8>> {
            serde_json::to_vec(value).map_err(|e| CatgaRaftError::Codec(e.to_string()))
        }

        fn decode<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> CatgaRaftResult<T> {
            serde_json::from_slice(data).map_err(|e| CatgaRaftError::Codec(e.to_string()))
        }
    }
}

pub use codec_redefinitions::BincodeCodec;
