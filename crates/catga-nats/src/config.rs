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
