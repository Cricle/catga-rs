use std::num::NonZeroUsize;

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// The default number of JetStream deliveries requested by one NATS pull operation.
pub const DEFAULT_NATS_PULL_BATCH_SIZE: usize = 64;

/// Controls bounded JetStream pull buffering for [`crate::NatsTransport`].
///
/// The default requests 64 deliveries per broker pull and retains the batch stream inside the
/// transport until every returned delivery has been handed to the caller. This reduces request
/// round trips for serial consumers without changing acknowledgement ownership. Use
/// [`Self::with_pull_batch_size`] to choose a smaller memory bound or a larger throughput bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NatsReceiveOptions {
    pull_batch_size: NonZeroUsize,
}

impl Default for NatsReceiveOptions {
    fn default() -> Self {
        Self {
            pull_batch_size: NonZeroUsize::new(DEFAULT_NATS_PULL_BATCH_SIZE)
                .expect("the default NATS pull batch size is nonzero"),
        }
    }
}

impl NatsReceiveOptions {
    /// Returns the maximum number of deliveries requested from JetStream per pull operation.
    pub const fn pull_batch_size(self) -> NonZeroUsize {
        self.pull_batch_size
    }

    /// Overrides the maximum number of deliveries requested from JetStream per pull operation.
    ///
    /// A positive value is required so every receive operation can make progress. The option is
    /// applied to the configured transport consumer and every provisioned destination consumer.
    pub fn with_pull_batch_size(mut self, pull_batch_size: usize) -> CatgaResult<Self> {
        self.pull_batch_size = NonZeroUsize::new(pull_batch_size).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "NATS pull batch size must be greater than zero",
            )
        })?;
        Ok(self)
    }
}

/// JetStream resources used by one Catga transport instance.
///
/// The four names form the durable delivery identity: keep them stable when
/// restarting a worker that should resume the same stream and consumer.
///
/// ```
/// use catga_nats::NatsConfig;
///
/// let config = NatsConfig {
///     server: "nats://127.0.0.1:4222".into(),
///     stream: "orders".into(),
///     subject: "orders.created".into(),
///     consumer: "orders-worker".into(),
/// };
/// assert_eq!(&*config.stream, "orders");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsConfig {
    /// NATS server URL.
    pub server: Box<str>,
    /// Durable JetStream stream name.
    pub stream: Box<str>,
    /// Subject used to publish envelopes.
    pub subject: Box<str>,
    /// Durable pull consumer name.
    pub consumer: Box<str>,
}

/// JetStream stream resources used by a publish-only NATS client.
///
/// Unlike [`NatsConfig`], this configuration has no consumer name. Constructing a
/// [`crate::NatsPublisher`] provisions only the stream and never leaves an idle durable consumer
/// behind on a publisher-only deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsPublisherConfig {
    /// NATS server URL.
    pub server: Box<str>,
    /// JetStream stream name that retains publications.
    pub stream: Box<str>,
    /// Subject used to publish envelopes into the stream.
    pub subject: Box<str>,
}

/// Core NATS resources used by one ephemeral Pub/Sub transport instance.
///
/// Unlike [`NatsConfig`], this configuration creates no JetStream resources. Publications are
/// visible only to subscribers connected at the time the NATS server processes them.
///
/// ```
/// use catga_nats::NatsPubSubConfig;
///
/// let config = NatsPubSubConfig {
///     server: "nats://127.0.0.1:4222".into(),
///     subject: "orders.notifications".into(),
/// };
/// assert_eq!(&*config.subject, "orders.notifications");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsPubSubConfig {
    /// NATS server URL.
    pub server: Box<str>,
    /// Core NATS subject used for both publication and subscription.
    pub subject: Box<str>,
}

/// Explicit JetStream resources backing one named durable destination.
///
/// Destination resources are supplied by the application rather than derived from an arbitrary
/// destination name.  This keeps stream retention, subject ownership, and durable consumer
/// identity reviewable in deployment configuration.
///
/// ```
/// use catga_nats::NatsDestinationConfig;
///
/// let destination = NatsDestinationConfig {
///     stream: "orders".into(),
///     subject: "orders.created".into(),
///     consumer: "orders-worker".into(),
/// };
/// assert_eq!(&*destination.consumer, "orders-worker");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsDestinationConfig {
    /// Durable JetStream stream name that captures [`Self::subject`].
    pub stream: Box<str>,
    /// JetStream subject used to publish destination envelopes.
    pub subject: Box<str>,
    /// Durable pull consumer used to receive destination envelopes.
    pub consumer: Box<str>,
}
