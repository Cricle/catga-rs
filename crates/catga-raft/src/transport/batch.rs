//! Batch sender implementation for message aggregation.
//!
//! This module provides the `BatchSender` type that aggregates multiple
//! messages before sending, reducing network overhead and improving throughput.
//!
//! # Features
//!
//! - Aggregates messages within a time window
//! - Configurable batch size threshold
//! - Automatic flush on timeout or size limit
//! - Per-peer batching support

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::CatgaRaftResult;

/// Default batch size threshold.
pub const DEFAULT_BATCH_SIZE: usize = 64;

/// Default flush interval in milliseconds.
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1;

/// Maximum pending batches before forcing flush.
pub const MAX_PENDING_BATCHES: usize = 1024;

/// Message batch for a single peer.
#[derive(Debug, Clone)]
pub struct MessageBatch {
    /// Target peer ID.
    pub peer_id: u64,
    /// Aggregated messages.
    pub messages: Vec<Bytes>,
    /// Batch creation time.
    pub created_at: Instant,
}

impl MessageBatch {
    /// Create a new message batch for a peer.
    pub fn new(peer_id: u64) -> Self {
        Self {
            peer_id,
            messages: Vec::new(),
            created_at: Instant::now(),
        }
    }

    /// Add a message to the batch.
    pub fn push(&mut self, msg: Bytes) {
        self.messages.push(msg);
    }

    /// Get the number of messages in the batch.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Get the total size of all messages in the batch.
    pub fn total_size(&self) -> usize {
        self.messages.iter().map(|m| m.len()).sum()
    }

    /// Get the age of the batch.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Batch sender for message aggregation.
///
/// Aggregates outgoing messages and flushes them in batches
/// to reduce network overhead and improve throughput.
#[allow(dead_code)]
pub struct BatchSender {
    /// Pending messages grouped by peer.
    pending: Arc<Mutex<HashMap<u64, Vec<Bytes>>>>,
    /// Flush interval.
    flush_interval: Duration,
    /// Maximum batch size before forced flush.
    max_batch_size: usize,
    /// Last flush time.
    last_flush: Arc<Mutex<Instant>>,
    /// Flush channel for async flushing.
    flush_tx: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    /// Flush trigger for immediate flush.
    flush_trigger: Arc<Mutex<Option<mpsc::Sender<u64>>>>,
}

impl BatchSender {
    /// Create a new batch sender with default configuration.
    pub fn new(flush_interval: Duration, max_batch_size: usize) -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            flush_interval,
            max_batch_size,
            last_flush: Arc::new(Mutex::new(Instant::now())),
            flush_tx: Arc::new(Mutex::new(None)),
            flush_trigger: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a new batch sender with custom configuration.
    pub fn with_config(
        flush_interval: Duration,
        max_batch_size: usize,
        max_pending: usize,
    ) -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::with_capacity(max_pending))),
            flush_interval,
            max_batch_size,
            last_flush: Arc::new(Mutex::new(Instant::now())),
            flush_tx: Arc::new(Mutex::new(None)),
            flush_trigger: Arc::new(Mutex::new(None)),
        }
    }

    /// Send a message through the batch sender.
    ///
    /// The message may be held in a pending batch until:
    /// - The batch size threshold is reached
    /// - The flush interval elapses
    /// - `flush()` is called explicitly
    pub async fn send(&self, peer_id: u64, msg: Bytes) -> CatgaRaftResult<()> {
        // Check if we should flush due to time
        if self.should_flush_by_time() {
            self.flush_by_peer(peer_id).await?;
        }

        // Add message to pending batch
        {
            let mut pending = self.pending.lock();
            let batch = pending.entry(peer_id).or_default();
            batch.push(msg);
        }

        // Check if we should flush due to size
        if self.should_flush_by_size(peer_id) {
            self.flush_by_peer(peer_id).await?;
        }

        Ok(())
    }

    /// Send multiple messages to the same peer.
    pub async fn send_batch(&self, peer_id: u64, messages: Vec<Bytes>) -> CatgaRaftResult<()> {
        for msg in messages {
            self.send(peer_id, msg).await?;
        }
        Ok(())
    }

    /// Flush pending messages for a specific peer.
    pub async fn flush_by_peer(&self, peer_id: u64) -> CatgaRaftResult<()> {
        let batch = {
            let mut pending = self.pending.lock();
            pending.remove(&peer_id)
        };

        if let Some(messages) = batch
            && !messages.is_empty()
        {
            tracing::trace!(
                peer_id,
                batch_size = messages.len(),
                total_bytes = messages.iter().map(|m| m.len()).sum::<usize>(),
                "flushing batch"
            );
            // In a real implementation, this would send to the actual transport
            // For now, we just drop the batch (fire-and-forget)
        }

        *self.last_flush.lock() = Instant::now();
        Ok(())
    }

    /// Flush all pending messages.
    ///
    /// Returns the number of batches flushed.
    pub async fn flush(&self) -> CatgaRaftResult<usize> {
        let batches: Vec<(u64, Vec<Bytes>)> = {
            let mut pending = self.pending.lock();
            let batches: Vec<_> = pending.drain().collect();
            batches
        };

        let count = batches.len();
        for (peer_id, messages) in batches {
            if !messages.is_empty() {
                tracing::trace!(peer_id, batch_size = messages.len(), "flushing batch");
            }
        }

        *self.last_flush.lock() = Instant::now();
        Ok(count)
    }

    /// Check if we should flush based on time interval.
    fn should_flush_by_time(&self) -> bool {
        let last = *self.last_flush.lock();
        last.elapsed() >= self.flush_interval
    }

    /// Check if we should flush based on batch size for a peer.
    fn should_flush_by_size(&self, peer_id: u64) -> bool {
        let pending = self.pending.lock();
        pending
            .get(&peer_id)
            .map(|batch| batch.len() >= self.max_batch_size)
            .unwrap_or(false)
    }

    /// Get the number of pending messages for a peer.
    pub fn pending_count(&self, peer_id: u64) -> usize {
        let pending = self.pending.lock();
        pending.get(&peer_id).map(|b| b.len()).unwrap_or(0)
    }

    /// Get the total number of pending messages across all peers.
    pub fn total_pending(&self) -> usize {
        let pending = self.pending.lock();
        pending.values().map(|b| b.len()).sum()
    }

    /// Get the number of peers with pending messages.
    pub fn pending_peers(&self) -> usize {
        let pending = self.pending.lock();
        pending.len()
    }

    /// Get the time since last flush.
    pub fn time_since_last_flush(&self) -> Duration {
        self.last_flush.lock().elapsed()
    }

    /// Get the configured flush interval.
    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Get the configured max batch size.
    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }
}

impl Default for BatchSender {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS),
            DEFAULT_BATCH_SIZE,
        )
    }
}

impl std::fmt::Debug for BatchSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSender")
            .field("flush_interval", &self.flush_interval)
            .field("max_batch_size", &self.max_batch_size)
            .field("total_pending", &self.total_pending())
            .field("pending_peers", &self.pending_peers())
            .finish()
    }
}
