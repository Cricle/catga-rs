//! Transport trait definition for Raft message passing.
//!
//! This module defines the core `Transport` trait that all transport implementations
//! must implement. It provides a unified interface for sending Raft messages,
//! snapshots, and vote requests between cluster nodes.

use bytes::Bytes;
use std::collections::HashMap;

use crate::CatgaRaftResult;

/// Trait for Raft cluster transport layer.
///
/// Implementors must be thread-safe (Send + Sync) as they may be accessed
/// from multiple threads concurrently.
pub trait Transport: Send + Sync {
    /// Send a Raft message to a specific peer.
    ///
    /// # Arguments
    /// * `peer_id` - The ID of the target peer
    /// * `message` - The encoded Raft message bytes
    ///
    /// # Returns
    /// * `Ok(())` - Message sent successfully
    /// * `Err(CatgaRaftError)` - Failed to send message
    fn send_message(
        &self,
        peer_id: u64,
        message: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send;

    /// Send a snapshot to a specific peer.
    ///
    /// # Arguments
    /// * `peer_id` - The ID of the target peer
    /// * `snapshot` - The encoded snapshot data
    ///
    /// # Returns
    /// * `Ok(())` - Snapshot sent successfully
    /// * `Err(CatgaRaftError)` - Failed to send snapshot
    fn send_snapshot(
        &self,
        peer_id: u64,
        snapshot: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        async move {
            // Default implementation: treat snapshot as regular message
            // Subclasses can override for streaming support
            self.send_message(peer_id, snapshot).await
        }
    }

    /// Send a vote request to a specific peer.
    ///
    /// # Arguments
    /// * `peer_id` - The ID of the target peer
    /// * `request` - The encoded vote request bytes
    ///
    /// # Returns
    /// * `Ok(())` - Vote request sent successfully
    /// * `Err(CatgaRaftError)` - Failed to send vote request
    fn send_vote_request(
        &self,
        peer_id: u64,
        request: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send {
        async move {
            // Default implementation: treat vote request as regular message
            self.send_message(peer_id, request).await
        }
    }

    /// Broadcast a message to all connected peers.
    ///
    /// # Arguments
    /// * `message` - The encoded Raft message bytes
    ///
    /// # Returns
    /// * `Ok(())` - All messages sent successfully
    /// * `Err(CatgaRaftError)` - Partial or complete failure
    fn broadcast(
        &self,
        message: Bytes,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send;

    /// Send different messages to multiple peers concurrently.
    ///
    /// # Arguments
    /// * `messages` - A map of peer_id -> message bytes
    ///
    /// # Returns
    /// * `Ok(())` - All messages sent successfully
    /// * `Err(CatgaRaftError)` - Partial or complete failure
    fn send_many(
        &self,
        messages: HashMap<u64, Bytes>,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send;

    /// Add a new peer or update an existing peer's address.
    ///
    /// # Arguments
    /// * `peer_id` - The ID of the peer
    /// * `addr` - The peer's network address
    fn add_peer(
        &self,
        peer_id: u64,
        addr: String,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send;

    /// Remove a peer from the transport.
    ///
    /// # Arguments
    /// * `peer_id` - The ID of the peer to remove
    fn remove_peer(
        &self,
        peer_id: u64,
    ) -> impl std::future::Future<Output = CatgaRaftResult<()>> + Send;

    /// Get the local node ID.
    fn local_node_id(&self) -> u64;

    /// Check if a peer exists in the transport.
    fn has_peer(&self, peer_id: u64) -> bool;

    /// Get all peer IDs.
    fn peer_ids(&self) -> Vec<u64>;

    /// Get the number of connected peers.
    fn peer_count(&self) -> usize;
}
