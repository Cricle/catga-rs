use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::{CatgaError, CatgaResult, Envelope, ErrorCode};

/// Default exclusive-processing lease for one inbox claim.
///
/// The five-minute value matches the upstream inbox behavior. Backends retain
/// the lease with the claim so a process crash cannot leave a message blocked
/// forever, while callers with known handler bounds can request a different
/// duration through [`InboxStore::try_claim_for`].
pub const DEFAULT_INBOX_CLAIM_LEASE: Duration = Duration::from_secs(5 * 60);

/// Default duration that completed idempotency records remain available as cached results.
pub const DEFAULT_IDEMPOTENCY_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// Maximum records one retention cleanup operation may inspect or remove.
pub const MAX_RETENTION_CLEANUP_LIMIT: usize = 1_024;

/// Validates a completed-record retention duration.
pub fn validate_completed_retention(retention: Duration) -> CatgaResult<()> {
    if retention.is_zero() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "completed record retention must be greater than zero",
        ));
    }
    Ok(())
}

/// Validates a bounded retention cleanup request.
pub fn validate_retention_cleanup_limit(limit: usize) -> CatgaResult<()> {
    if limit > MAX_RETENTION_CLEANUP_LIMIT {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "retention cleanup limit exceeds the configured memory budget",
        ));
    }
    Ok(())
}

/// Validates one requested inbox claim lease.
///
/// A zero lease could be reclaimed before the caller begins processing, so it
/// returns [`ErrorCode::Validation`] rather than silently creating a race.
pub fn validate_inbox_claim_lease(lease: Duration) -> CatgaResult<()> {
    if lease.is_zero() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "inbox claim lease must be greater than zero",
        ));
    }
    Ok(())
}

/// Returns the UTC epoch-millisecond deadline for a validated inbox lease.
///
/// The helper centralizes conversion bounds for in-memory, Redis, and NATS
/// stores. A clock before the Unix epoch is an operational failure; a lease
/// that cannot fit the portable millisecond representation is invalid input.
pub fn inbox_claim_expires_at(lease: Duration) -> CatgaResult<u64> {
    validate_inbox_claim_lease(lease)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        CatgaError::new(ErrorCode::Internal, "system clock precedes the Unix epoch")
    })?;
    let now = u64::try_from(now.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "system clock exceeds the supported millisecond range",
        )
    })?;
    let lease = u64::try_from(lease.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "inbox claim lease exceeds the supported millisecond range",
        )
    })?;
    now.checked_add(lease).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Validation,
            "inbox claim deadline exceeds the supported millisecond range",
        )
    })
}

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

    /// Atomically acquires a message for `lease` unless it is claimed or completed.
    ///
    /// Implementations retain the deadline with the claim and may reclaim an
    /// expired claim. The default preserves compatibility for third-party
    /// stores that only implement [`Self::try_claim`]; built-in durable stores
    /// override it with lease-aware transitions.
    async fn try_claim_for(&self, message_id: u64, lease: Duration) -> CatgaResult<bool> {
        validate_inbox_claim_lease(lease)?;
        self.try_claim(message_id).await
    }

    /// Marks a claimed message completed, retaining an optional serialized result.
    async fn complete(&self, message_id: u64, result: Option<Arc<[u8]>>) -> CatgaResult<()>;

    /// Marks a claimed message failed so a later attempt may claim it.
    async fn fail(&self, message_id: u64) -> CatgaResult<()>;

    /// Returns the current state when this process has retained the message.
    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>>;

    /// Returns the cached result for a completed message without copying its bytes.
    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>>;

    /// Removes up to `limit` completed records older than `retention`.
    async fn cleanup_completed(&self, _retention: Duration, limit: usize) -> CatgaResult<usize> {
        validate_retention_cleanup_limit(limit)?;
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "inbox retention cleanup is not supported by this store",
        ))
    }
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

    /// Removes up to `limit` completed records exceeding this store's configured retention.
    async fn cleanup_completed(&self, limit: usize) -> CatgaResult<usize> {
        validate_retention_cleanup_limit(limit)?;
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "idempotency retention cleanup is not supported by this store",
        ))
    }
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
