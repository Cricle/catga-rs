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
/// Maximum UTF-8 byte length of one retained dead-letter error description.
pub const MAX_DEAD_LETTER_DESCRIPTION_BYTES: usize = 1_024;
/// Maximum UTF-8 byte length of one retained dead-letter processing stage.
pub const MAX_DEAD_LETTER_STAGE_BYTES: usize = 64;

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
    diagnostics: DeadLetterDiagnostics,
}

/// Stable, bounded context captured when processing permanently fails.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterDiagnostics {
    failed_at_unix_ms: u64,
    error_code: ErrorCode,
    stage: Box<str>,
}

impl DeadLetterDiagnostics {
    /// Captures the current UTC failure time, error category, and processing stage.
    pub fn new(error_code: ErrorCode, stage: impl Into<Box<str>>) -> CatgaResult<Self> {
        let failed_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "system clock precedes the Unix epoch")
            })?
            .as_millis();
        let failed_at_unix_ms = u64::try_from(failed_at_unix_ms).map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "system clock exceeds the supported millisecond range",
            )
        })?;
        Self::try_at(failed_at_unix_ms, error_code, stage)
    }

    /// Creates diagnostics with an explicit UTC epoch-millisecond failure time.
    pub fn try_at(
        failed_at_unix_ms: u64,
        error_code: ErrorCode,
        stage: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let stage = stage.into();
        if stage.is_empty()
            || stage.len() > MAX_DEAD_LETTER_STAGE_BYTES
            || !stage
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "dead-letter stage must be a bounded ASCII identifier",
            ));
        }
        Ok(Self {
            failed_at_unix_ms,
            error_code,
            stage,
        })
    }

    /// Returns the UTC epoch-millisecond failure time.
    pub const fn failed_at_unix_ms(&self) -> u64 {
        self.failed_at_unix_ms
    }

    /// Returns the stable category of the terminal failure.
    pub const fn error_code(&self) -> ErrorCode {
        self.error_code
    }

    /// Returns the bounded processing stage that produced the dead letter.
    pub fn stage(&self) -> &str {
        &self.stage
    }
}

impl DeadLetter {
    /// Creates a compatibility dead letter from the failed envelope, error text, and total attempts.
    ///
    /// This legacy constructor retains a bounded description and marks diagnostics as
    /// `legacy` with an [`ErrorCode::Internal`] category. New failure paths should use
    /// [`Self::from_failure`] or [`Self::try_with_diagnostics`].
    pub fn new(envelope: Envelope, reason: impl Into<Box<str>>, attempts: u32) -> Self {
        Self {
            envelope,
            reason: truncate_description(reason.into()),
            attempts,
            diagnostics: DeadLetterDiagnostics {
                failed_at_unix_ms: 0,
                error_code: ErrorCode::Internal,
                stage: "legacy".into(),
            },
        }
    }

    /// Creates a dead letter with validated, structured failure diagnostics.
    pub fn try_with_diagnostics(
        envelope: Envelope,
        reason: impl Into<Box<str>>,
        attempts: u32,
        diagnostics: DeadLetterDiagnostics,
    ) -> CatgaResult<Self> {
        let reason = reason.into();
        if reason.len() > MAX_DEAD_LETTER_DESCRIPTION_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "dead-letter error description exceeds the configured memory budget",
            ));
        }
        Ok(Self {
            envelope,
            reason,
            attempts,
            diagnostics,
        })
    }

    /// Creates a bounded diagnostic dead letter from a framework failure.
    pub fn from_failure(
        envelope: Envelope,
        error: &CatgaError,
        attempts: u32,
        stage: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let diagnostics = DeadLetterDiagnostics::new(error.code(), stage)?;
        Ok(Self {
            envelope,
            reason: truncate_description(error.message().into()),
            attempts,
            diagnostics,
        })
    }

    /// Returns the failed envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Returns the bounded terminal failure description.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns the number of processing attempts already made.
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Returns stable failure diagnostics retained with this record.
    pub const fn diagnostics(&self) -> &DeadLetterDiagnostics {
        &self.diagnostics
    }
}

fn truncate_description(reason: Box<str>) -> Box<str> {
    if reason.len() <= MAX_DEAD_LETTER_DESCRIPTION_BYTES {
        return reason;
    }
    let mut end = MAX_DEAD_LETTER_DESCRIPTION_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].into()
}

/// Persists terminal message failures for inspection and recovery.
#[async_trait]
pub trait DeadLetterStore: Send + Sync {
    /// Adds a terminal failure to the queue.
    async fn enqueue(&self, letter: DeadLetter) -> CatgaResult<()>;

    /// Returns up to `limit` retained failures in queue order.
    async fn list(&self, limit: usize) -> CatgaResult<Vec<DeadLetter>>;
}

#[cfg(test)]
mod tests {
    use crate::{
        DeadLetter, DeadLetterDiagnostics, Envelope, ErrorCode, MAX_DEAD_LETTER_DESCRIPTION_BYTES,
        MessageMetadata,
    };

    fn envelope() -> Envelope {
        Envelope::new(
            7,
            "tests.dead-letter",
            vec![1, 2, 3],
            MessageMetadata::new(7, None),
        )
    }

    #[test]
    fn diagnostics_retain_bounded_failure_context() -> crate::CatgaResult<()> {
        let diagnostics = DeadLetterDiagnostics::new(ErrorCode::Timeout, "consumer.handle")?;
        let letter = DeadLetter::try_with_diagnostics(envelope(), "timed out", 3, diagnostics)?;

        assert_eq!(letter.diagnostics().error_code(), ErrorCode::Timeout);
        assert_eq!(letter.diagnostics().stage(), "consumer.handle");
        assert_eq!(letter.reason(), "timed out");
        assert!(letter.diagnostics().failed_at_unix_ms() > 0);
        Ok(())
    }

    #[test]
    fn diagnostics_reject_an_unbounded_error_description() -> crate::CatgaResult<()> {
        let diagnostics = DeadLetterDiagnostics::new(ErrorCode::Internal, "pipeline")?;
        let description = "x".repeat(MAX_DEAD_LETTER_DESCRIPTION_BYTES + 1);

        let result = DeadLetter::try_with_diagnostics(envelope(), description, 1, diagnostics);
        assert_eq!(
            result.err().map(|error| error.code()),
            Some(ErrorCode::Validation)
        );
        Ok(())
    }
}
