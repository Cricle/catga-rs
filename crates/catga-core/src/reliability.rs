use std::sync::Arc;

use async_trait::async_trait;

use crate::{CatgaResult, Envelope};

/// State shared by inbox and idempotency records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessingState {
    /// The key has not been claimed by a processor.
    Pending,
    /// One processor currently owns the key.
    Claimed,
    /// Processing completed and may expose a cached result.
    Completed,
    /// Processing failed and may be claimed again.
    Failed,
}

/// Stores exclusive processing state for received transport messages.
#[async_trait]
pub trait InboxStore: Send + Sync {
    /// Atomically acquires a message unless it is already claimed or completed.
    async fn try_claim(&self, message_id: u64) -> CatgaResult<bool>;

    /// Marks a claimed message completed, retaining an optional serialized result.
    async fn complete(&self, message_id: u64, result: Option<Arc<[u8]>>) -> CatgaResult<()>;

    /// Marks a claimed message failed so a later attempt may claim it.
    async fn fail(&self, message_id: u64) -> CatgaResult<()>;

    /// Returns the current state when this process has retained the message.
    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>>;

    /// Returns the cached result for a completed message without copying its bytes.
    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>>;
}

/// Stores exclusive processing state for caller-provided idempotency keys.
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Atomically claims a key unless it is already claimed or completed.
    async fn try_claim(&self, key: &str) -> CatgaResult<bool>;

    /// Marks a claimed key completed, retaining an optional serialized result.
    async fn complete(&self, key: &str, result: Option<Arc<[u8]>>) -> CatgaResult<()>;

    /// Marks a claimed key failed so a later attempt may claim it.
    async fn fail(&self, key: &str) -> CatgaResult<()>;

    /// Returns the current state when this process has retained the key.
    async fn state(&self, key: &str) -> CatgaResult<Option<ProcessingState>>;

    /// Returns the cached result for a completed key without copying its bytes.
    async fn result(&self, key: &str) -> CatgaResult<Option<Arc<[u8]>>>;
}

/// A failed delivery retained for inspection or manual recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetter {
    envelope: Envelope,
    reason: Box<str>,
    attempts: u32,
}

impl DeadLetter {
    /// Creates a dead letter from the failed envelope, error text, and total attempts.
    pub fn new(envelope: Envelope, reason: impl Into<Box<str>>, attempts: u32) -> Self {
        Self {
            envelope,
            reason: reason.into(),
            attempts,
        }
    }

    /// Returns the failed envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Returns the terminal failure reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns the number of processing attempts already made.
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }
}

/// Persists terminal message failures for inspection and recovery.
#[async_trait]
pub trait DeadLetterStore: Send + Sync {
    /// Adds a terminal failure to the queue.
    async fn enqueue(&self, letter: DeadLetter) -> CatgaResult<()>;

    /// Returns up to `limit` retained failures in queue order.
    async fn list(&self, limit: usize) -> CatgaResult<Vec<DeadLetter>>;
}
