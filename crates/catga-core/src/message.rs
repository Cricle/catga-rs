use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{CatgaError, CatgaResult, ErrorCode};

/// A value that can be handled or transported by Catga.
pub trait Message: Send + Sync + 'static {
    /// Returns the stable Rust type name used by the default registry.
    fn message_type(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Returns the schema version written to typed transport envelopes.
    ///
    /// Messages without an explicit evolution contract retain version one for
    /// wire compatibility. Versioned messages override this method or use the
    /// corresponding derive configuration.
    fn schema_version(&self) -> u32 {
        1
    }

    /// Returns the priority requested for typed transport delivery.
    ///
    /// Messages without an explicit priority retain normal delivery ordering.
    /// Implementations may override this method when priority depends on the
    /// message value; `#[derive(Message)]` supports a static priority
    /// declaration without a runtime allocation.
    fn priority(&self) -> MessagePriority {
        MessagePriority::Normal
    }

    /// Visits explicitly opted-in values for structured tracing.
    ///
    /// The default implementation exports no application data. Deriving [`Message`] supports
    /// `#[catga(trace_tag)]` and `#[catga(trace_tag = "name")]` on named fields, plus
    /// `#[catga(trace_tags(prefix = "name.", include = ["field"], exclude = ["field"]))]`
    /// for type-level bulk selection. This gives applications a compile-time,
    /// privacy-preserving equivalent of Catga's activity-tag provider without reflection or a
    /// per-message tag allocation.
    fn visit_trace_tags(&self, _: &mut dyn FnMut(&str, &dyn std::fmt::Display)) {}
}

/// A message that produces a typed response.
pub trait Request: Message {
    /// The value returned by the matching request handler.
    type Response: Send + 'static;

    /// Visits explicitly opted-in values from a successful response for structured tracing.
    ///
    /// The default implementation exports no response data. Override this method to expose only
    /// stable, non-sensitive values that are useful when diagnosing a request, for example a
    /// version or a bounded business status. Catga invokes it only after a successful dispatch
    /// and only while debug tracing for [`crate::TRACING_TARGET`] is enabled. Values are emitted
    /// as structured tracing events, never as metrics labels.
    ///
    /// Unlike [`Message::visit_trace_tags`], response tagging is declared on the request because
    /// the response type is an associated type of [`Request`]. This keeps the opt-in local to the
    /// request/response contract and lets ordinary response types remain dependency-free.
    fn visit_response_trace_tags(
        _: &Self::Response,
        _: &mut dyn FnMut(&str, &dyn std::fmt::Display),
    ) {
    }
}

/// A message that has no response value.
pub trait Command: Message {}

/// A message delivered to zero or more subscribers.
pub trait Event: Message + Clone {
    /// Returns explicit marker categories accepted by this event.
    ///
    /// State machines use these markers for deliberately declared category transitions. The
    /// default is a shared empty slice, so events that do not opt in perform no allocation or
    /// dynamic type discovery. Categories are not inheritance: an event implementation lists
    /// each marker it wants to expose.
    fn categories(&self) -> &'static [std::any::TypeId] {
        &[]
    }
}

/// A message that declares when its durable outbox delivery may begin.
///
/// [`Self::scheduled_at`] takes precedence over [`Self::delay`], matching the source contract.
/// The declaration alone never creates a timer, sleeps, or changes direct transport delivery.
/// Call a durable scheduled-outbox API to persist the resolved deadline and make recovery across
/// process restarts possible.
pub trait DelayedMessage: Message {
    /// Returns the absolute delivery boundary when one is declared.
    fn scheduled_at(&self) -> Option<SystemTime> {
        None
    }

    /// Returns the relative delivery delay when no absolute boundary is declared.
    fn delay(&self) -> Option<Duration> {
        None
    }

    /// Resolves this message's portable durable delivery boundary against `now`.
    ///
    /// An absolute boundary wins over a relative delay. The result must fit Catga's UTC
    /// epoch-millisecond outbox representation; otherwise this returns [`ErrorCode::Validation`]
    /// before payload serialization or store I/O.
    fn deliver_at(&self, now: SystemTime) -> CatgaResult<SystemTime> {
        let deliver_at = match self.scheduled_at() {
            Some(scheduled_at) => scheduled_at,
            None => match self.delay() {
                Some(delay) => now.checked_add(delay).ok_or_else(|| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "delayed message deadline exceeds the system time range",
                    )
                })?,
                None => now,
            },
        };
        let elapsed = deliver_at.duration_since(UNIX_EPOCH).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "delayed message deadline precedes the Unix epoch",
            )
        })?;
        u64::try_from(elapsed.as_millis()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "delayed message deadline exceeds the supported millisecond range",
            )
        })?;
        Ok(deliver_at)
    }
}

/// A typed request that also declares a durable delivery boundary.
///
/// Every value implementing both [`Request`] and [`DelayedMessage`] receives this marker
/// automatically; applications do not write a second implementation.
pub trait DelayedRequest: Request + DelayedMessage {}

impl<T> DelayedRequest for T where T: Request + DelayedMessage {}

/// An event that also declares a durable delivery boundary.
///
/// Every value implementing both [`Event`] and [`DelayedMessage`] receives this marker
/// automatically; applications do not write a second implementation.
pub trait DelayedEvent: Event + DelayedMessage {}

impl<T> DelayedEvent for T where T: Event + DelayedMessage {}

/// Supplies an optional stable shard key for automatic request batching.
///
/// Returning `None` places the request in the behavior's default shard.
/// Implementations return owned text because a batch actor retains the key
/// after the caller's request has been moved into its queue.
pub trait BatchKeyProvider {
    /// Returns the shard key for this request, when it has one.
    fn batch_key(&self) -> Option<Box<str>>;
}

/// Supplies compile-time batch runtime limits for one request type.
///
/// `#[derive(Message)]` can implement this with `#[catga(batch(...))]`, so
/// applications do not need a global batching-options registry.
pub trait BatchOptionsProvider {
    /// Returns the validated runtime limits declared by the message type.
    fn batch_options() -> crate::BatchOptions;
}

/// Delivery guarantee requested for a transported message.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[repr(u8)]
pub enum QualityOfService {
    /// The transport may drop a message rather than retrying it.
    AtMostOnce = 0,
    /// The transport retries delivery and consumers may see duplicates.
    #[default]
    AtLeastOnce = 1,
    /// Delivery requires deduplication by the receiving application.
    ExactlyOnce = 2,
}

impl QualityOfService {
    /// Returns the stable telemetry tag for this level.
    pub const fn as_tag(self) -> &'static str {
        match self {
            Self::AtMostOnce => "AtMostOnce",
            Self::AtLeastOnce => "AtLeastOnce",
            Self::ExactlyOnce => "ExactlyOnce",
        }
    }

    /// Returns whether this level needs a backend acknowledgement.
    pub const fn requires_ack(self) -> bool {
        matches!(self, Self::AtLeastOnce | Self::ExactlyOnce)
    }

    /// Returns whether consumers must deduplicate repeated message identities.
    pub const fn requires_deduplication(self) -> bool {
        matches!(self, Self::ExactlyOnce)
    }
}

/// Chooses whether a sender waits for delivery or relies on durable retry.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[repr(u8)]
pub enum DeliveryMode {
    /// Wait for the current operation's result.
    #[default]
    WaitForResult = 0,
    /// Persist for asynchronous retry by a background worker.
    AsyncRetry = 1,
}

/// Relative message importance for transports that support priority queues.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[repr(u8)]
pub enum MessagePriority {
    /// Deferrable work.
    Low = 0,
    /// Default application work.
    #[default]
    Normal = 1,
    /// Important work that should precede normal traffic.
    High = 2,
    /// Time-sensitive work.
    Critical = 3,
}

/// Identifiers propagated with a message through a distributed operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageMetadata {
    message_id: u64,
    correlation_id: Option<u64>,
    quality_of_service: QualityOfService,
    delivery_mode: DeliveryMode,
    priority: MessagePriority,
    not_before_unix_ms: Option<u64>,
}

impl MessageMetadata {
    /// Creates metadata for a message and its optional causal root.
    pub const fn new(message_id: u64, correlation_id: Option<u64>) -> Self {
        Self {
            message_id,
            correlation_id,
            quality_of_service: QualityOfService::AtLeastOnce,
            delivery_mode: DeliveryMode::WaitForResult,
            priority: MessagePriority::Normal,
            not_before_unix_ms: None,
        }
    }

    /// Returns the unique message identifier.
    pub const fn message_id(self) -> u64 {
        self.message_id
    }

    /// Returns the optional distributed correlation identifier.
    pub const fn correlation_id(self) -> Option<u64> {
        self.correlation_id
    }

    /// Returns the requested transport delivery guarantee.
    pub const fn quality_of_service(self) -> QualityOfService {
        self.quality_of_service
    }

    /// Returns whether the sender waits or relies on durable retry.
    pub const fn delivery_mode(self) -> DeliveryMode {
        self.delivery_mode
    }

    /// Returns the requested transport priority.
    pub const fn priority(self) -> MessagePriority {
        self.priority
    }

    /// Returns the optional UTC epoch-millisecond delivery boundary.
    pub const fn not_before_unix_ms(self) -> Option<u64> {
        self.not_before_unix_ms
    }

    /// Returns the optional wall-clock delivery boundary.
    pub fn not_before(self) -> Option<SystemTime> {
        self.not_before_unix_ms
            .and_then(|milliseconds| UNIX_EPOCH.checked_add(Duration::from_millis(milliseconds)))
    }

    /// Returns whether this metadata permits delivery at `now`.
    pub fn is_due_at(self, now: SystemTime) -> bool {
        let Some(not_before) = self.not_before_unix_ms else {
            return true;
        };
        now.duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
            .is_some_and(|milliseconds| milliseconds >= not_before)
    }

    /// Replaces the requested delivery guarantee.
    pub const fn with_quality_of_service(mut self, quality_of_service: QualityOfService) -> Self {
        self.quality_of_service = quality_of_service;
        self
    }

    /// Replaces the sender completion mode.
    pub const fn with_delivery_mode(mut self, delivery_mode: DeliveryMode) -> Self {
        self.delivery_mode = delivery_mode;
        self
    }

    /// Replaces the requested transport priority.
    pub const fn with_priority(mut self, priority: MessagePriority) -> Self {
        self.priority = priority;
        self
    }

    /// Replaces the optional UTC epoch-millisecond delivery boundary.
    pub const fn with_not_before_unix_ms(mut self, not_before_unix_ms: Option<u64>) -> Self {
        self.not_before_unix_ms = not_before_unix_ms;
        self
    }

    /// Replaces the optional wall-clock delivery boundary.
    pub fn with_not_before(self, not_before: SystemTime) -> CatgaResult<Self> {
        let elapsed = not_before.duration_since(UNIX_EPOCH).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "scheduled delivery time precedes the Unix epoch",
            )
        })?;
        let milliseconds = u64::try_from(elapsed.as_millis()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "scheduled delivery time exceeds the supported range",
            )
        })?;
        Ok(self.with_not_before_unix_ms(Some(milliseconds)))
    }
}
