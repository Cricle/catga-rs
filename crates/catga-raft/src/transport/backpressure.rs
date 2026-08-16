//! Backpressure controller implementation for flow control.
//!
//! This module provides the `BackpressureController` type that tracks
//! in-flight messages and applies backpressure when limits are exceeded.
//!
//! # Design
//!
//! - Per-peer in-flight message tracking
//! - Dynamic limit adjustment
//! - Configurable default limits
//! - Thread-safe operations

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

/// Default in-flight message limit per peer.
pub const DEFAULT_INFLIGHT_LIMIT: usize = 256;

/// Minimum in-flight limit.
pub const MIN_INFLIGHT_LIMIT: usize = 1;

/// Maximum in-flight limit.
pub const MAX_INFLIGHT_LIMIT: usize = 65536;

/// Per-peer in-flight state.
struct PeerInflight {
    /// Current in-flight count.
    count: AtomicUsize,
    /// Limit for this peer.
    limit: AtomicUsize,
}

impl PeerInflight {
    fn new(limit: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            limit: AtomicUsize::new(limit),
        }
    }

    fn increment(&self) -> usize {
        self.count.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn decrement(&self) -> usize {
        self.count.fetch_sub(1, Ordering::AcqRel).saturating_sub(1)
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    fn set_limit(&self, new_limit: usize) {
        self.limit.store(new_limit, Ordering::Release);
    }

    fn can_send(&self) -> bool {
        self.count.load(Ordering::Acquire) < self.limit.load(Ordering::Acquire)
    }
}

/// Backpressure controller for managing in-flight message limits.
///
/// Tracks the number of in-flight messages per peer and provides
/// backpressure signals when limits are exceeded.
pub struct BackpressureController {
    /// Per-peer in-flight state.
    peers: Arc<RwLock<HashMap<u64, Arc<PeerInflight>>>>,
    /// Default in-flight limit for new peers.
    default_limit: usize,
    /// Total in-flight across all peers.
    total_inflight: AtomicUsize,
}

impl BackpressureController {
    /// Create a new backpressure controller with default settings.
    pub fn new(default_limit: usize) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            default_limit: default_limit.clamp(MIN_INFLIGHT_LIMIT, MAX_INFLIGHT_LIMIT),
            total_inflight: AtomicUsize::new(0),
        }
    }

    /// Create a new backpressure controller with custom default limit.
    pub fn with_default_limit(limit: usize) -> Self {
        Self::new(limit)
    }

    /// Get or create peer state for the given peer ID.
    fn get_or_create_peer(&self, peer_id: u64) -> Arc<PeerInflight> {
        let peers = self.peers.read();
        if let Some(peer) = peers.get(&peer_id) {
            return Arc::clone(peer);
        }
        drop(peers);

        let mut peers = self.peers.write();
        // Double-check after acquiring write lock
        if let Some(peer) = peers.get(&peer_id) {
            return Arc::clone(peer);
        }

        let peer = Arc::new(PeerInflight::new(self.default_limit));
        peers.insert(peer_id, Arc::clone(&peer));
        Arc::clone(&peer)
    }

    /// Check if a message can be sent to the given peer.
    ///
    /// Returns `true` if the peer has capacity for more in-flight messages.
    pub fn can_send(&self, peer_id: u64) -> bool {
        let peer = self.get_or_create_peer(peer_id);
        peer.can_send()
    }

    /// Check if a message can be sent with the current in-flight count.
    ///
    /// This method allows checking before actually sending, useful for
    /// decision-making in higher layers.
    pub fn can_send_with_count(&self, peer_id: u64, current_inflight: usize) -> bool {
        let peer = self.get_or_create_peer(peer_id);
        current_inflight < peer.limit()
    }

    /// Record that a message is being sent to the given peer.
    ///
    /// Returns the new in-flight count.
    pub fn on_send(&self, peer_id: u64) -> usize {
        let peer = self.get_or_create_peer(peer_id);
        let new_count = peer.increment();
        self.total_inflight.fetch_add(1, Ordering::AcqRel);
        tracing::trace!(
            peer_id,
            count = new_count,
            limit = peer.limit(),
            "message sent"
        );
        new_count
    }

    /// Record that a message has been completed (success or failure).
    ///
    /// Returns the new in-flight count.
    pub fn on_complete(&self, peer_id: u64) -> usize {
        let peers = self.peers.read();
        if let Some(peer) = peers.get(&peer_id) {
            let new_count = peer.decrement();
            self.total_inflight.fetch_sub(1, Ordering::AcqRel);
            tracing::trace!(peer_id, count = new_count, "message completed");
            new_count
        } else {
            0
        }
    }

    /// Increment in-flight count for a peer.
    ///
    /// Returns the new count after incrementing.
    pub fn increment_inflight(&self, peer_id: u64) -> usize {
        self.on_send(peer_id)
    }

    /// Decrement in-flight count for a peer.
    ///
    /// Returns the new count after decrementing.
    pub fn decrement_inflight(&self, peer_id: u64) -> usize {
        self.on_complete(peer_id)
    }

    /// Get the current in-flight count for a peer.
    pub fn current_inflight(&self, peer_id: u64) -> usize {
        let peers = self.peers.read();
        peers.get(&peer_id).map(|p| p.count()).unwrap_or(0)
    }

    /// Get the total in-flight count across all peers.
    pub fn total_inflight(&self) -> usize {
        self.total_inflight.load(Ordering::Acquire)
    }

    /// Get the limit for a peer.
    pub fn limit(&self, peer_id: u64) -> usize {
        let peers = self.peers.read();
        peers
            .get(&peer_id)
            .map(|p| p.limit())
            .unwrap_or(self.default_limit)
    }

    /// Get the current limit for this controller.
    pub fn current_limit(&self) -> usize {
        self.default_limit
    }

    /// Adjust the limit for a specific peer.
    ///
    /// This allows dynamic adjustment based on peer performance or
    /// network conditions.
    pub fn adjust_limit(&self, peer_id: u64, new_limit: usize) {
        let peer = self.get_or_create_peer(peer_id);
        let clamped = new_limit.clamp(MIN_INFLIGHT_LIMIT, MAX_INFLIGHT_LIMIT);
        peer.set_limit(clamped);
        tracing::info!(
            peer_id,
            old_limit = peer.limit(),
            new_limit = clamped,
            "peer limit adjusted"
        );
    }

    /// Adjust the default limit for new peers.
    pub fn adjust_default_limit(&self, new_limit: usize) {
        let clamped = new_limit.clamp(MIN_INFLIGHT_LIMIT, MAX_INFLIGHT_LIMIT);
        tracing::info!(
            old_limit = self.default_limit,
            new_limit = clamped,
            "default limit adjusted"
        );
        // Note: This only affects new peers; existing peers keep their limits
    }

    /// Remove a peer from the controller.
    ///
    /// This will remove the peer's in-flight tracking state.
    pub fn remove_peer(&self, peer_id: u64) {
        let mut peers = self.peers.write();
        if let Some(peer) = peers.remove(&peer_id) {
            let count = peer.count();
            if count > 0 {
                self.total_inflight.fetch_sub(count, Ordering::AcqRel);
            }
            tracing::debug!(peer_id, "peer removed from backpressure controller");
        }
    }

    /// Get statistics for all peers.
    pub fn stats(&self) -> HashMap<u64, (usize, usize)> {
        let peers = self.peers.read();
        peers
            .iter()
            .map(|(&id, p)| (id, (p.count(), p.limit())))
            .collect()
    }

    /// Get utilization percentage for a peer.
    ///
    /// Returns a value between 0.0 and 1.0.
    pub fn utilization(&self, peer_id: u64) -> f64 {
        let peers = self.peers.read();
        if let Some(peer) = peers.get(&peer_id) {
            let count = peer.count() as f64;
            let limit = peer.limit() as f64;
            if limit > 0.0 {
                return count / limit;
            }
        }
        0.0
    }

    /// Get average utilization across all peers.
    pub fn average_utilization(&self) -> f64 {
        let peers = self.peers.read();
        if peers.is_empty() {
            return 0.0;
        }

        let total: f64 = peers
            .values()
            .map(|p| {
                let count = p.count() as f64;
                let limit = p.limit() as f64;
                if limit > 0.0 { count / limit } else { 0.0 }
            })
            .sum();

        total / peers.len() as f64
    }
}

impl Default for BackpressureController {
    fn default() -> Self {
        Self::new(DEFAULT_INFLIGHT_LIMIT)
    }
}

impl std::fmt::Debug for BackpressureController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpressureController")
            .field("default_limit", &self.default_limit)
            .field("total_inflight", &self.total_inflight)
            .field("peer_count", &self.peers.read().len())
            .finish()
    }
}
