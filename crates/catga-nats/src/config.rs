use std::{num::NonZeroUsize, time::Duration};

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

/// Determines how JetStream retains consumer progress.
///
/// [`Self::Durable`] is the default and resumes the named consumer after a worker restart.
/// [`Self::Ephemeral`] creates a new pull consumer for the current transport instance; use it
/// for replay jobs or high-churn workers whose cursor must not survive process shutdown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NatsConsumerMode {
    /// Persist the configured consumer name and its acknowledged cursor.
    #[default]
    Durable,
    /// Create a server-named consumer without a durable cursor.
    Ephemeral,
}

/// Lifecycle settings for a JetStream pull consumer.
///
/// The default preserves the original durable-consumer behavior. An inactivity threshold is sent
/// to JetStream only when the application configures one; otherwise the server chooses its own
/// default. This prevents the transport from silently imposing a retention policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NatsConsumerOptions {
    mode: NatsConsumerMode,
    inactive_threshold: Option<Duration>,
}

impl NatsConsumerOptions {
    /// Returns lifecycle settings for a durable consumer.
    pub const fn durable() -> Self {
        Self {
            mode: NatsConsumerMode::Durable,
            inactive_threshold: None,
        }
    }

    /// Returns lifecycle settings for a server-named ephemeral consumer.
    pub const fn ephemeral() -> Self {
        Self {
            mode: NatsConsumerMode::Ephemeral,
            inactive_threshold: None,
        }
    }

    /// Returns whether the consumer cursor survives a transport restart.
    pub const fn mode(self) -> NatsConsumerMode {
        self.mode
    }

    /// Returns the caller-selected inactivity cleanup threshold, if any.
    pub const fn inactive_threshold(self) -> Option<Duration> {
        self.inactive_threshold
    }

    /// Sets the broker cleanup threshold for an inactive consumer.
    pub const fn with_inactive_threshold(mut self, inactive_threshold: Duration) -> Self {
        self.inactive_threshold = Some(inactive_threshold);
        self
    }
}

/// Aggregates bounded receive buffering and consumer lifecycle settings.
///
/// Construct this only when an application needs to override both defaults. The focused
/// `*_with_receive_options` constructors remain available for pull-buffer tuning alone.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NatsTransportOptions {
    receive: NatsReceiveOptions,
    consumer: NatsConsumerOptions,
}

impl NatsTransportOptions {
    /// Returns bounded pull-buffer settings.
    pub const fn receive(self) -> NatsReceiveOptions {
        self.receive
    }

    /// Returns JetStream consumer lifecycle settings.
    pub const fn consumer(self) -> NatsConsumerOptions {
        self.consumer
    }

    /// Replaces bounded pull-buffer settings.
    pub const fn with_receive(mut self, receive: NatsReceiveOptions) -> Self {
        self.receive = receive;
        self
    }

    /// Replaces JetStream consumer lifecycle settings.
    pub const fn with_consumer(mut self, consumer: NatsConsumerOptions) -> Self {
        self.consumer = consumer;
        self
    }
}

/// JetStream resources used by one Catga transport instance.
///
/// In the default durable mode, the four names form the delivery identity: keep them stable when
/// restarting a worker that should resume the same stream and consumer. When using
/// [`NatsConsumerOptions::ephemeral`], `consumer` is retained only for source compatibility and
/// validation; JetStream assigns a transient consumer name instead.
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
    /// Pull consumer name used by the default durable mode.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receive_and_consumer_options_keep_safe_defaults_and_validate_overrides() {
        let receive = NatsReceiveOptions::default();
        assert_eq!(
            receive.pull_batch_size().get(),
            DEFAULT_NATS_PULL_BATCH_SIZE
        );
        assert_eq!(
            receive
                .with_pull_batch_size(128)
                .expect("positive batch size")
                .pull_batch_size()
                .get(),
            128
        );
        assert_eq!(
            receive
                .with_pull_batch_size(0)
                .expect_err("zero batch size rejected")
                .code(),
            ErrorCode::Validation
        );

        let durable = NatsConsumerOptions::durable();
        assert_eq!(durable.mode(), NatsConsumerMode::Durable);
        assert_eq!(durable.inactive_threshold(), None);
        let ephemeral =
            NatsConsumerOptions::ephemeral().with_inactive_threshold(Duration::from_secs(30));
        assert_eq!(ephemeral.mode(), NatsConsumerMode::Ephemeral);
        assert_eq!(
            ephemeral.inactive_threshold(),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn transport_options_combine_receive_and_lifecycle_configuration() {
        let receive = NatsReceiveOptions::default()
            .with_pull_batch_size(4)
            .expect("positive batch size");
        let consumer = NatsConsumerOptions::ephemeral();
        let options = NatsTransportOptions::default()
            .with_receive(receive)
            .with_consumer(consumer);
        assert_eq!(options.receive(), receive);
        assert_eq!(options.consumer(), consumer);
        assert_eq!(
            NatsTransportOptions::default(),
            NatsTransportOptions::default()
        );
    }
}
