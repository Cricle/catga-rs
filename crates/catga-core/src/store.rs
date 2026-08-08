use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::{CatgaResult, MessageMetadata};

/// Maximum number of application headers carried by one [`Envelope`].
///
/// This cap bounds memory retained by untrusted remote metadata while leaving
/// room for the small routing and tenancy contexts used by transport adapters.
pub const MAX_ENVELOPE_HEADERS: usize = 64;

/// Maximum combined UTF-8 byte length of all envelope header keys and values.
///
/// The limit applies before a header set is attached or decoded, preventing a
/// compact envelope from allocating an unbounded metadata dictionary.
pub const MAX_ENVELOPE_HEADER_BYTES: usize = 8 * 1024;

/// One immutable application header attached to an [`Envelope`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvelopeHeader {
    key: Box<str>,
    value: Box<str>,
}

impl EnvelopeHeader {
    /// Returns the stable header key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the header value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Validated immutable application headers for an [`Envelope`].
///
/// The container stores its entries in an `Arc` slice. Cloning headers or an
/// envelope that contains them only increments the shared reference count;
/// no key or value string is copied. Construct values with [`Self::try_new`]
/// so duplicate keys and resource limits are rejected before transport work.
///
/// ```
/// use catga_core::EnvelopeHeaders;
///
/// let headers = EnvelopeHeaders::try_new([
///     ("tenant", "acme"),
///     ("region", "eu-west"),
/// ]).expect("valid headers");
/// assert_eq!(headers.get("tenant"), Some("acme"));
/// assert_eq!(headers.len(), 2);
/// assert!(headers.get("missing").is_none());
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EnvelopeHeaders(Arc<[EnvelopeHeader]>);

impl EnvelopeHeaders {
    /// Validates and stores application headers in their supplied order.
    ///
    /// Keys must not be blank and must be unique. At most
    /// [`MAX_ENVELOPE_HEADERS`] headers and
    /// [`MAX_ENVELOPE_HEADER_BYTES`] combined key/value bytes are accepted.
    /// Invalid input returns [`crate::ErrorCode::Validation`].
    pub fn try_new<I, K, V>(headers: I) -> CatgaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Box<str>>,
        V: Into<Box<str>>,
    {
        let mut entries = Vec::new();
        let mut total_bytes = 0usize;

        for (key, value) in headers {
            if entries.len() == MAX_ENVELOPE_HEADERS {
                return Err(crate::CatgaError::new(
                    crate::ErrorCode::Validation,
                    "envelope header count exceeds the configured limit",
                ));
            }

            let key = key.into();
            let value = value.into();
            if key.trim().is_empty() {
                return Err(crate::CatgaError::new(
                    crate::ErrorCode::Validation,
                    "envelope header key must not be empty or whitespace-only",
                ));
            }
            if entries
                .iter()
                .any(|header: &EnvelopeHeader| header.key == key)
            {
                return Err(crate::CatgaError::new(
                    crate::ErrorCode::Validation,
                    "envelope header keys must be unique",
                ));
            }

            let entry_bytes = key.len().checked_add(value.len()).ok_or_else(|| {
                crate::CatgaError::new(
                    crate::ErrorCode::Validation,
                    "envelope header bytes exceed the configured limit",
                )
            })?;
            total_bytes = total_bytes.checked_add(entry_bytes).ok_or_else(|| {
                crate::CatgaError::new(
                    crate::ErrorCode::Validation,
                    "envelope header bytes exceed the configured limit",
                )
            })?;
            if total_bytes > MAX_ENVELOPE_HEADER_BYTES {
                return Err(crate::CatgaError::new(
                    crate::ErrorCode::Validation,
                    "envelope header bytes exceed the configured limit",
                ));
            }

            entries.push(EnvelopeHeader { key, value });
        }

        Ok(Self(Arc::from(entries.into_boxed_slice())))
    }

    /// Returns the value for `key`, if this immutable set contains it.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|header| header.key.as_ref() == key)
            .map(EnvelopeHeader::value)
    }

    /// Iterates key/value pairs in their original deterministic order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &str)> + DoubleEndedIterator {
        self.0.iter().map(|header| (header.key(), header.value()))
    }

    /// Returns whether this set contains no headers.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of retained header pairs.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Merges `overrides` into this set while retaining deterministic order.
    ///
    /// Existing keys retain their position and receive the override value;
    /// keys absent from this set are appended in the override's order. The
    /// resulting set is validated against the same count and byte limits as
    /// [`Self::try_new`], so combining two valid untrusted header sets cannot
    /// exceed the envelope transport budget.
    pub fn merge_overrides(&self, overrides: &Self) -> CatgaResult<Self> {
        if self.is_empty() {
            return Ok(overrides.clone());
        }
        if overrides.is_empty() {
            return Ok(self.clone());
        }

        let mut entries = self.0.to_vec();
        for override_header in overrides.0.iter() {
            if let Some(existing) = entries
                .iter_mut()
                .find(|header| header.key == override_header.key)
            {
                *existing = override_header.clone();
            } else {
                entries.push(override_header.clone());
            }
        }

        Self::try_new(entries.into_iter().map(|header| (header.key, header.value)))
    }
}

/// A serialized message ready for durable delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    id: u64,
    message_type: Box<str>,
    payload: Vec<u8>,
    metadata: MessageMetadata,
    sent_at_unix_ms: Option<u64>,
    schema_version: u32,
    reply_to: Option<Box<str>>,
    headers: Option<EnvelopeHeaders>,
}

impl Envelope {
    /// Creates an envelope from its identity, type, serialized payload, and metadata.
    pub fn new(
        id: u64,
        message_type: impl Into<Box<str>>,
        payload: Vec<u8>,
        metadata: MessageMetadata,
    ) -> Self {
        Self::versioned(id, message_type, payload, metadata, 1)
    }

    /// Creates an envelope with an explicit event schema version.
    pub fn versioned(
        id: u64,
        message_type: impl Into<Box<str>>,
        payload: Vec<u8>,
        metadata: MessageMetadata,
        schema_version: u32,
    ) -> Self {
        Self {
            id,
            message_type: message_type.into(),
            payload,
            metadata,
            sent_at_unix_ms: current_unix_ms(),
            schema_version,
            reply_to: None,
            headers: None,
        }
    }

    /// Returns the durable message identifier.
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Returns the serialized message type name.
    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    /// Returns the serialized payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the transport metadata.
    pub const fn metadata(&self) -> MessageMetadata {
        self.metadata
    }

    /// Returns the UTC epoch-millisecond timestamp captured when this envelope was created.
    ///
    /// Historical wire payloads written before timestamp support return `None` rather than an
    /// inferred value. Use [`Self::sent_at`] when a [`SystemTime`] is more convenient.
    pub const fn sent_at_unix_ms(&self) -> Option<u64> {
        self.sent_at_unix_ms
    }

    /// Returns the captured transport creation time as a wall-clock value.
    ///
    /// A timestamp outside the platform's representable [`SystemTime`] range is exposed as
    /// `None`; the exact wire millisecond value remains available through
    /// [`Self::sent_at_unix_ms`].
    pub fn sent_at(&self) -> Option<SystemTime> {
        self.sent_at_unix_ms
            .and_then(|milliseconds| UNIX_EPOCH.checked_add(Duration::from_millis(milliseconds)))
    }

    /// Returns the schema version used to serialize this event payload.
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Returns the optional backend-specific destination for a correlated reply.
    pub fn reply_to(&self) -> Option<&str> {
        self.reply_to.as_deref()
    }

    /// Adds an optional reply destination without changing compact message metadata.
    pub fn with_reply_to(mut self, reply_to: impl Into<Box<str>>) -> Self {
        self.reply_to = Some(reply_to.into());
        self
    }

    /// Attaches validated application headers to this envelope.
    ///
    /// Empty header sets retain no allocation on the envelope. Nonempty sets
    /// are immutable and shared by clone through [`EnvelopeHeaders`].
    pub fn with_headers(mut self, headers: EnvelopeHeaders) -> Self {
        self.headers = (!headers.is_empty()).then_some(headers);
        self
    }

    /// Replaces the envelope's transport creation time with an exact UTC wall-clock value.
    ///
    /// Times before the Unix epoch or beyond the wire format's millisecond range return
    /// [`crate::ErrorCode::Validation`]. This is useful for deterministic replay and tests.
    pub fn with_sent_at(mut self, sent_at: SystemTime) -> CatgaResult<Self> {
        let elapsed = sent_at.duration_since(UNIX_EPOCH).map_err(|_| {
            crate::CatgaError::new(
                crate::ErrorCode::Validation,
                "envelope sent timestamp precedes the Unix epoch",
            )
        })?;
        let milliseconds = u64::try_from(elapsed.as_millis()).map_err(|_| {
            crate::CatgaError::new(
                crate::ErrorCode::Validation,
                "envelope sent timestamp exceeds the supported range",
            )
        })?;
        self.sent_at_unix_ms = Some(milliseconds);
        Ok(self)
    }

    /// Replaces the optional raw UTC epoch-millisecond transport creation time.
    ///
    /// This lossless builder is intended for codecs and deterministic replay. Use
    /// [`Self::with_sent_at`] for wall-clock validation at application boundaries.
    pub const fn with_sent_at_unix_ms(mut self, sent_at_unix_ms: Option<u64>) -> Self {
        self.sent_at_unix_ms = sent_at_unix_ms;
        self
    }

    /// Returns the value for one application header without allocating.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.as_ref().and_then(|headers| headers.get(key))
    }

    /// Iterates application headers in deterministic insertion order.
    ///
    /// Envelopes without headers return an empty iterator and retain no header
    /// container allocation.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .as_ref()
            .into_iter()
            .flat_map(EnvelopeHeaders::iter)
    }

    pub(crate) fn shared_headers(&self) -> Option<EnvelopeHeaders> {
        self.headers.clone()
    }

    /// Replaces transport metadata while retaining the envelope payload and identity.
    pub fn with_metadata(mut self, metadata: MessageMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

fn current_unix_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
}

/// The lifecycle state of a durable outbox message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxState {
    /// The message has not yet been claimed for delivery.
    Pending,
    /// One worker owns the delivery attempt.
    Claimed,
    /// Delivery exhausted its configured retry limit and requires inspection.
    Failed,
    /// Delivery was acknowledged and the record remains available for inspection.
    Published,
}

/// Default number of failed delivery attempts retained before an outbox message is terminal.
///
/// This matches the upstream outbox's `MaxRetries` default. Stores preserve
/// the value with each message, allowing callers to choose a different policy
/// for exceptional messages without a process-wide mutable setting.
pub const DEFAULT_OUTBOX_MAX_RETRIES: u32 = 3;

/// Maximum UTF-8 byte length retained for one outbox delivery failure reason.
///
/// The cap bounds durable memory and backend record size when a transport or
/// remote server includes untrusted diagnostic data in an error message.
pub const MAX_OUTBOX_FAILURE_ERROR_BYTES: usize = 1024;

/// Maximum messages one [`OutboxStore::claim`] invocation may retain or return.
///
/// Stores validate this public budget before allocating a claim vector, heap,
/// or backend query result. Callers needing a larger drain perform multiple
/// bounded claims, keeping worker memory and contention predictable.
pub const MAX_OUTBOX_CLAIM_LIMIT: usize = 1024;

/// Default exclusive-delivery lease applied by [`OutboxStore::claim`].
///
/// A bounded lease lets another worker recover publication after a process
/// exits between claiming a record and acknowledging it.
pub const DEFAULT_OUTBOX_CLAIM_LEASE: Duration = Duration::from_secs(5 * 60);

/// Longest exclusive-delivery lease accepted by [`OutboxStore::claim_for`].
///
/// This cap prevents one malformed configuration from making an abandoned
/// outbox record unavailable for an unbounded recovery interval.
pub const MAX_OUTBOX_CLAIM_LEASE: Duration = Duration::from_secs(60 * 60);

/// Validates a durable outbox message identifier before persistence work begins.
///
/// Identifier zero is reserved as an unset value. Rejecting it at every
/// [`OutboxStore::enqueue`] boundary preserves the source outbox contract and
/// prevents an invalid message from becoming a durable backend key.
pub fn validate_outbox_message_id(id: u64) -> CatgaResult<()> {
    if id == 0 {
        return Err(crate::CatgaError::new(
            crate::ErrorCode::Validation,
            "outbox message identifier must be greater than zero",
        ));
    }
    Ok(())
}

/// Validates one requested outbox claim budget before any allocation or I/O.
///
/// A limit of zero is valid and produces an empty claim. Values above
/// [`MAX_OUTBOX_CLAIM_LIMIT`] return [`crate::ErrorCode::Validation`] instead
/// of being silently truncated to a different batch size.
pub fn validate_outbox_claim_limit(limit: usize) -> CatgaResult<()> {
    if limit > MAX_OUTBOX_CLAIM_LIMIT {
        return Err(crate::CatgaError::new(
            crate::ErrorCode::Validation,
            "outbox claim limit exceeds the configured memory budget",
        ));
    }
    Ok(())
}

/// Validates one bounded exclusive-delivery lease.
pub fn validate_outbox_claim_lease(lease: Duration) -> CatgaResult<()> {
    if lease.as_millis() == 0 || lease > MAX_OUTBOX_CLAIM_LEASE {
        return Err(crate::CatgaError::new(
            crate::ErrorCode::Validation,
            "outbox claim lease must be at least one millisecond and no longer than the configured maximum",
        ));
    }
    Ok(())
}

/// Returns the UTC epoch-millisecond deadline for a validated outbox lease.
pub fn outbox_claim_expires_at(lease: Duration) -> CatgaResult<u64> {
    validate_outbox_claim_lease(lease)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        crate::CatgaError::new(
            crate::ErrorCode::Internal,
            "system clock precedes the Unix epoch",
        )
    })?;
    let now = u64::try_from(now.as_millis()).map_err(|_| {
        crate::CatgaError::new(
            crate::ErrorCode::Internal,
            "system clock exceeds the supported millisecond range",
        )
    })?;
    let lease = u64::try_from(lease.as_millis()).map_err(|_| {
        crate::CatgaError::new(
            crate::ErrorCode::Validation,
            "outbox claim lease exceeds the supported millisecond range",
        )
    })?;
    now.checked_add(lease).ok_or_else(|| {
        crate::CatgaError::new(
            crate::ErrorCode::Validation,
            "outbox claim deadline exceeds the supported millisecond range",
        )
    })
}

/// A message persisted until a transport acknowledges delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxMessage {
    envelope: Envelope,
    state: OutboxState,
    owner: Option<Box<str>>,
    retry_count: u32,
    max_retries: u32,
    last_error: Option<Box<str>>,
    published_at_unix_ms: Option<u64>,
    claimed_until_unix_ms: Option<u64>,
    claim_token: Option<Box<str>>,
}

impl OutboxMessage {
    /// Creates a pending outbox message.
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            state: OutboxState::Pending,
            owner: None,
            retry_count: 0,
            max_retries: DEFAULT_OUTBOX_MAX_RETRIES,
            last_error: None,
            published_at_unix_ms: None,
            claimed_until_unix_ms: None,
            claim_token: None,
        }
    }

    /// Creates a pending message that cannot be claimed before `not_before`.
    pub fn scheduled(envelope: Envelope, not_before: SystemTime) -> CatgaResult<Self> {
        let metadata = envelope.metadata().with_not_before(not_before)?;
        Ok(Self::new(envelope.with_metadata(metadata)))
    }

    /// Returns the durable message identifier.
    pub const fn id(&self) -> u64 {
        self.envelope.id()
    }

    /// Returns the current delivery state.
    pub const fn state(&self) -> OutboxState {
        self.state
    }

    /// Returns the worker that currently owns delivery.
    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }

    /// Returns how many failed publication attempts have been retained.
    pub const fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Returns the number of failed attempts permitted before terminal failure.
    pub const fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Returns the most recent bounded publication failure reason, if any.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Returns when an acknowledged message entered retained published history.
    pub const fn published_at_unix_ms(&self) -> Option<u64> {
        self.published_at_unix_ms
    }

    /// Returns the deadline of the current exclusive-delivery lease.
    pub const fn claimed_until_unix_ms(&self) -> Option<u64> {
        self.claimed_until_unix_ms
    }

    /// Returns the opaque token for the current exclusive-delivery attempt.
    ///
    /// Callers must present this exact value when completing the claim. Tokens
    /// change on every successful claim, including a recovery by the same
    /// owner, so an expired worker cannot complete a newer delivery attempt.
    pub fn claim_token(&self) -> Option<&str> {
        self.claim_token.as_deref()
    }

    /// Replaces this message's nonzero terminal failure limit.
    ///
    /// A zero limit is rejected because no delivery attempt could then be
    /// made. Use a limit of one to retain exactly one failed attempt before
    /// the message becomes [`OutboxState::Failed`].
    pub fn with_max_retries(mut self, max_retries: u32) -> CatgaResult<Self> {
        if max_retries == 0 {
            return Err(crate::CatgaError::new(
                crate::ErrorCode::Validation,
                "outbox maximum retries must be greater than zero",
            ));
        }
        self.max_retries = max_retries;
        Ok(self)
    }

    /// Restores durable retry history while retaining this message's pending state.
    ///
    /// Persistence adapters use this when reconstructing a claimed record.
    /// The error text is normalized to the same bound as a newly recorded
    /// failure, preventing a legacy backend record from bypassing the limit.
    pub fn with_retry_history(mut self, retry_count: u32, last_error: Option<&str>) -> Self {
        self.retry_count = retry_count;
        self.last_error = last_error.map(Self::bounded_failure_reason);
        self
    }

    /// Returns the optional delivery boundary retained with the envelope.
    pub fn not_before(&self) -> Option<SystemTime> {
        self.envelope.metadata().not_before()
    }

    /// Returns the persisted UTC epoch-millisecond delivery boundary.
    pub const fn not_before_unix_ms(&self) -> Option<u64> {
        self.envelope.metadata().not_before_unix_ms()
    }

    /// Returns whether this message may be claimed at `now`.
    pub fn is_due_at(&self, now: SystemTime) -> bool {
        self.envelope.metadata().is_due_at(now)
    }

    /// Returns whether this record may be atomically claimed at `now_unix_ms`.
    ///
    /// Claims restored from historical records without a deadline are treated
    /// as expired so an upgrade can recover previously stranded deliveries.
    pub fn is_claimable_at(&self, now_unix_ms: u64) -> bool {
        self.state == OutboxState::Pending
            || (self.state == OutboxState::Claimed
                && self
                    .claimed_until_unix_ms
                    .is_none_or(|deadline| deadline <= now_unix_ms))
    }

    /// Records the worker that exclusively owns the next delivery attempt.
    pub fn claim(&mut self, owner: impl Into<Box<str>>) {
        if matches!(self.state, OutboxState::Failed | OutboxState::Published) {
            return;
        }
        self.state = OutboxState::Claimed;
        self.owner = Some(owner.into());
        self.claimed_until_unix_ms = None;
        self.claim_token = None;
    }

    /// Records an exclusive owner and its recovery deadline.
    pub fn claim_until(&mut self, owner: impl Into<Box<str>>, expires_at_unix_ms: u64) {
        self.claim(owner);
        if self.state == OutboxState::Claimed {
            self.claimed_until_unix_ms = Some(expires_at_unix_ms);
        }
    }

    /// Records an exclusive owner, opaque claim token, and recovery deadline.
    pub fn claim_until_with_token(
        &mut self,
        owner: impl Into<Box<str>>,
        claim_token: impl Into<Box<str>>,
        expires_at_unix_ms: u64,
    ) {
        self.claim(owner);
        if self.state == OutboxState::Claimed {
            self.claim_token = Some(claim_token.into());
            self.claimed_until_unix_ms = Some(expires_at_unix_ms);
        }
    }

    /// Marks an owned delivery as acknowledged and retained for inspection.
    pub fn mark_published(&mut self, published_at_unix_ms: u64) {
        if self.state == OutboxState::Claimed {
            self.state = OutboxState::Published;
            self.owner = None;
            self.published_at_unix_ms = Some(published_at_unix_ms);
            self.claimed_until_unix_ms = None;
            self.claim_token = None;
        }
    }

    /// Returns this message to the pending state after a failed delivery attempt.
    pub fn release(&mut self) {
        if self.state == OutboxState::Claimed {
            self.state = OutboxState::Pending;
            self.owner = None;
            self.claimed_until_unix_ms = None;
            self.claim_token = None;
        }
    }

    /// Records one failed owned delivery and selects its next terminal state.
    ///
    /// The reason is copied into at most [`MAX_OUTBOX_FAILURE_ERROR_BYTES`]
    /// bytes without splitting a UTF-8 code point. Stores must call this only
    /// after atomically verifying the caller owns a claimed message.
    pub fn record_failure(&mut self, reason: &str) {
        self.retry_count = self.retry_count.saturating_add(1);
        self.last_error = Some(Self::bounded_failure_reason(reason));
        self.owner = None;
        self.claimed_until_unix_ms = None;
        self.claim_token = None;
        self.state = if self.retry_count >= self.max_retries {
            OutboxState::Failed
        } else {
            OutboxState::Pending
        };
    }

    /// Copies a failure reason into the durable outbox error budget.
    ///
    /// Adapters use this before passing a reason to an external store so the
    /// bound applies to transient command buffers as well as persisted records.
    pub fn bounded_failure_reason(reason: &str) -> Box<str> {
        if reason.len() <= MAX_OUTBOX_FAILURE_ERROR_BYTES {
            return reason.into();
        }

        let mut end = MAX_OUTBOX_FAILURE_ERROR_BYTES;
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason[..end].into()
    }

    /// Returns the serialized envelope that must be published.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }
}

/// Persists outbound messages until their transport delivery is acknowledged.
///
/// Completion operations require the opaque claim token returned on each
/// [`Self::claim`] or [`Self::claim_for`] result. Callers migrating from the
/// prior owner-only completion API must retain that token beside the message
/// identifier and pass it to [`Self::ack`], [`Self::release`], or
/// [`Self::record_failure`]. There is intentionally no owner-only completion
/// compatibility path because it cannot fence a stale recovery by the same
/// worker identity.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Adds a nonzero-identified message in the pending state.
    ///
    /// Implementations return [`crate::ErrorCode::Conflict`] for a duplicate
    /// durable identifier and retain the originally enqueued message unchanged.
    /// A zero identifier returns [`crate::ErrorCode::Validation`] before the
    /// implementation allocates, encodes, or performs backend I/O.
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()>;

    /// Atomically claims up to `limit` pending or expired messages for a worker.
    ///
    /// A zero limit returns no messages. Implementations reject values above
    /// [`MAX_OUTBOX_CLAIM_LIMIT`] before allocating or querying their backend.
    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>>;

    /// Atomically claims work with an explicit exclusive-delivery lease.
    ///
    /// Built-in stores retain `lease` with each claim and allow another owner
    /// to recover it after the deadline. The default preserves source
    /// compatibility for third-party stores while validating the caller's
    /// request; durable adapters should override it with lease-aware CAS.
    async fn claim_for(
        &self,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> CatgaResult<Vec<OutboxMessage>> {
        validate_outbox_claim_lease(lease)?;
        self.claim(owner, limit).await
    }

    /// Marks a message published only when `owner` and `claim_token` still own it.
    ///
    /// A message remains available through [`Self::list_published`] until its
    /// configured retention cleanup removes it. The token comes from the
    /// claimed [`OutboxMessage`] and fences stale delivery attempts, including
    /// a reclaimed attempt using the same owner string.
    async fn ack(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()>;

    /// Returns a message to pending only when `owner` and `claim_token` still own it.
    async fn release(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()>;

    /// Records a failed delivery attempt only when `owner` and `claim_token` still own `id`.
    ///
    /// Implementations increment the persisted retry count, retain a bounded
    /// last error, clear the owner, and either return the message to pending or
    /// move it to [`OutboxState::Failed`] at its per-message retry limit. A
    /// stale ownership or token makes no change.
    async fn record_failure(
        &self,
        owner: &str,
        id: u64,
        claim_token: &str,
        reason: &str,
    ) -> CatgaResult<()>;

    /// Removes a message only while no worker owns its delivery attempt.
    ///
    /// Returns `false` when the message does not exist or has already been
    /// claimed for delivery.
    async fn cancel(&self, id: u64) -> CatgaResult<bool>;

    /// Returns up to `limit` acknowledged messages retained for inspection.
    async fn list_published(&self, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        validate_outbox_claim_limit(limit)?;
        Err(crate::CatgaError::new(
            crate::ErrorCode::Unsupported,
            "published outbox history is not supported by this store",
        ))
    }

    /// Removes up to `limit` acknowledged records older than `retention`.
    async fn cleanup_published(&self, _retention: Duration, limit: usize) -> CatgaResult<usize> {
        crate::validate_retention_cleanup_limit(limit)?;
        Err(crate::CatgaError::new(
            crate::ErrorCode::Unsupported,
            "published outbox cleanup is not supported by this store",
        ))
    }
}

