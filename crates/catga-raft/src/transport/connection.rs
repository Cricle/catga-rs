//! Connection pool implementation for managing gRPC channels.
//!
//! This module provides the `ConnectionPool` type that manages multiple
//! gRPC channels per peer for improved throughput and resource utilization.
//!
//! # Design
//!
//! - Each peer maintains a pool of 4 channels by default
//! - Round-robin selection distributes load across channels
//! - Lazy connection establishment
//! - Automatic reconnection on failure

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::{CatgaRaftError, CatgaRaftResult};

/// Default connection pool size per peer.
pub const DEFAULT_POOL_SIZE: usize = 4;

/// Maximum connection pool size per peer.
pub const MAX_POOL_SIZE: usize = 16;

/// Connection pool entry containing multiple channels for a single peer.
///
/// This pool manages multiple gRPC channels to a single peer endpoint,
/// providing better throughput through parallel connections and improved
/// fault tolerance.
///
/// The hot path (`get_channel` on a warm pool) takes only a *read* lock on
/// the channel vector plus one atomic increment for round-robin; the write
/// lock is taken solely while the pool is being grown (cold start) or
/// cleared.
#[derive(Clone)]
pub struct ConnectionPool {
    channels: Arc<RwLock<Vec<Channel>>>,
    endpoint: Arc<str>,
    pool_size: usize,
    /// Lock-free round-robin cursor shared by all pool clones.
    next_index: Arc<AtomicUsize>,
}

impl ConnectionPool {
    /// Create a new connection pool for the given endpoint.
    ///
    /// # Arguments
    /// * `endpoint` - The gRPC server endpoint URL
    /// * `pool_size` - Maximum number of channels to maintain
    pub fn new(endpoint: String, pool_size: usize) -> Self {
        let pool_size = pool_size.clamp(1, MAX_POOL_SIZE);
        Self {
            channels: Arc::new(RwLock::new(Vec::with_capacity(pool_size))),
            endpoint: Arc::from(endpoint.into_boxed_str()),
            pool_size,
            next_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        self.endpoint.as_ref()
    }

    /// Get a channel from the pool.
    ///
    /// If an idle channel exists, it is returned. Otherwise, a new channel
    /// is created if the pool hasn't reached its maximum size.
    ///
    /// Uses round-robin selection to distribute load across channels.
    ///
    /// Warm-pool cost is one shared read lock plus one atomic increment;
    /// the write lock is only taken on the cold path while growing the pool.
    pub async fn get_channel(&self) -> CatgaRaftResult<Channel> {
        // Fast path: serve an existing channel round-robin under a read
        // lock. `fetch_add` wraps on overflow, which is harmless for the
        // modulo below (and unreachable in practice).
        {
            let channels = self.channels.read();
            if !channels.is_empty() {
                let next = self.next_index.fetch_add(1, Ordering::Relaxed);
                return Ok(channels[next % channels.len()].clone());
            }
        }

        // No idle channels, create a new one if under limit
        let channel = self.connect().await?;

        let mut channels = self.channels.write();
        if channels.len() < self.pool_size {
            channels.push(channel.clone());
        }
        Ok(channel)
    }

    /// Connect to the endpoint and return a new channel.
    async fn connect(&self) -> CatgaRaftResult<Channel> {
        let endpoint_str = self.endpoint.as_ref();

        let channel = tonic::transport::Endpoint::from_shared(endpoint_str.to_string())
            .map_err(|e| CatgaRaftError::Transport(format!("invalid endpoint: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| {
                CatgaRaftError::Transport(format!("failed to connect to {}: {}", endpoint_str, e))
            })?;

        tracing::debug!(endpoint = %endpoint_str, "new connection established");
        Ok(channel)
    }

    /// Return a channel to the pool (for connection reuse).
    ///
    /// Note: In the current implementation, channels are reused automatically
    /// via round-robin. This method exists for future extension where
    /// explicit connection return might be needed.
    pub fn return_channel(&self, _channel: Channel) {
        // Current implementation doesn't track individual channels
        // Round-robin handles distribution automatically
    }

    /// Get the current number of connections in the pool.
    pub fn connection_count(&self) -> usize {
        self.channels.read().len()
    }

    /// Get the configured pool size limit.
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Clear all connections from the pool.
    ///
    /// This can be used to force reconnection, e.g., after a network change.
    pub fn clear(&self) {
        let mut channels = self.channels.write();
        channels.clear();
        self.next_index.store(0, Ordering::Relaxed);
        tracing::debug!(endpoint = %self.endpoint.as_ref(), "connection pool cleared");
    }

    /// Check if the pool is at capacity.
    pub fn is_full(&self) -> bool {
        self.channels.read().len() >= self.pool_size
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.channels.read().is_empty()
    }
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("endpoint", &self.endpoint)
            .field("pool_size", &self.pool_size)
            .field("active_connections", &self.connection_count())
            .finish()
    }
}
