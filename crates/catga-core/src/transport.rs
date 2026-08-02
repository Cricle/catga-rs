use async_trait::async_trait;
use futures::{StreamExt, stream};
use std::time::Duration;

use crate::{CatgaError, CatgaResult, Command, Envelope, ErrorCode, Event, Request};

/// Default maximum number of simultaneously publishing envelopes in a transport batch.
///
/// This matches the upstream batch chunk size while keeping peak task and message state bounded.
pub const DEFAULT_TRANSPORT_BATCH_CONCURRENCY: usize = 100;

/// A validated name for a durable, point-to-point transport destination.
///
/// Destinations are intentionally separate from transport topics: publishing an envelope is a
/// backend's configured topic operation, whereas sending to a destination is a durable queue
/// operation.  Construct values with [`Self::parse`] so invalid names become
/// [`ErrorCode::Validation`] instead of an unchecked backend request.
///
/// ```
/// use catga_core::Destination;
///
/// let dest = Destination::parse("order-queue").expect("valid name");
/// assert_eq!(dest.as_str(), "order-queue");
/// assert!(Destination::parse("  ").is_err());
/// ```
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Destination(Box<str>);

impl Destination {
    /// Validates and stores a nonblank destination name.
    ///
    /// Names are kept exactly as supplied after validation, allowing adapters to apply their
    /// own valid naming rules while rejecting empty and whitespace-only names consistently.
    pub fn parse(name: impl Into<Box<str>>) -> CatgaResult<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "transport destination must not be empty or whitespace-only",
            ));
        }
        Ok(Self(name))
    }

    /// Returns the backend-neutral destination name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the destination and returns its compact owned representation.
    pub fn into_boxed_str(self) -> Box<str> {
        self.0
    }
}

impl std::fmt::Display for Destination {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A message received from a transport and awaiting acknowledgement.
pub struct Delivery {
    envelope: Envelope,
    acknowledger: Option<Box<dyn Acknowledger>>,
    attempts: u32,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Delivery")
            .field("envelope", &self.envelope)
            .field("requires_ack", &self.acknowledger.is_some())
            .field("attempts", &self.attempts)
            .finish()
    }
}

/// Performs the backend-specific acknowledgement for one delivery.
#[async_trait]
pub trait Acknowledger: Send {
    /// Commits successful processing exactly once.
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()>;

    /// Requests redelivery after unsuccessful processing.
    ///
    /// Backends without a native negative acknowledgement return
    /// [`ErrorCode::Unsupported`] rather than silently losing the delivery.
    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "transport does not support negative acknowledgement",
        ))
    }
}

impl Delivery {
    /// Creates a delivery around a received envelope.
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            acknowledger: None,
            attempts: 1,
        }
    }

    /// Creates a delivery that owns its backend-specific acknowledgement token.
    pub fn with_acknowledger(envelope: Envelope, acknowledger: Box<dyn Acknowledger>) -> Self {
        Self {
            envelope,
            acknowledger: Some(acknowledger),
            attempts: 1,
        }
    }

    /// Records the total number of backend delivery attempts for this value.
    ///
    /// Backends report their first delivery as one. A supplied zero is
    /// normalized to one because a received value always represents at least
    /// one attempt; this lets adapters use optional native metadata without
    /// creating an invalid delivery state.
    pub fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts.max(1);
        self
    }

    /// Returns the delivered envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Returns the total number of backend delivery attempts observed so far.
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Replaces the envelope while retaining the original acknowledgement token.
    pub fn map_envelope(
        self,
        mapper: impl FnOnce(Envelope) -> CatgaResult<Envelope>,
    ) -> CatgaResult<Self> {
        let Self {
            envelope,
            acknowledger,
            attempts,
        } = self;
        Ok(Self {
            envelope: mapper(envelope)?,
            acknowledger,
            attempts,
        })
    }

    /// Consumes the delivery and commits its backend acknowledgement when required.
    pub async fn acknowledge(mut self) -> CatgaResult<()> {
        match self.acknowledger.take() {
            Some(acknowledger) => acknowledger.acknowledge().await,
            None => Ok(()),
        }
    }

    /// Consumes the delivery and requests its redelivery from the backend.
    pub async fn negative_acknowledge(mut self) -> CatgaResult<()> {
        match self.acknowledger.take() {
            Some(acknowledger) => acknowledger.negative_acknowledge().await,
            None => Ok(()),
        }
    }

    /// Shorthand for [`Self::negative_acknowledge`].
    pub async fn nack(self) -> CatgaResult<()> {
        self.negative_acknowledge().await
    }
}

/// Sends envelopes and receives acknowledged deliveries.
#[async_trait]
pub trait MessageTransport: Send + Sync {
    /// Publishes an envelope, applying the transport's configured backpressure.
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()>;

    /// Publishes a caller-owned batch with the default bounded concurrency limit.
    ///
    /// The input is moved rather than cloned. Every envelope is attempted, and the first observed
    /// failure is returned only after all started work has completed.
    async fn publish_batch(&self, envelopes: Vec<Envelope>) -> CatgaResult<()> {
        self.publish_batch_with_concurrency(envelopes, DEFAULT_TRANSPORT_BATCH_CONCURRENCY)
            .await
    }

    /// Publishes a caller-owned batch with at most `concurrency_limit` active publish futures.
    ///
    /// This is streaming rather than task-collecting: memory used for pending futures is
    /// `O(concurrency_limit)`, independent of batch length. A zero limit is rejected with
    /// [`ErrorCode::Validation`]. Every input is attempted even when another publish fails; the
    /// first observed failure is returned after the batch drains.
    async fn publish_batch_with_concurrency(
        &self,
        envelopes: Vec<Envelope>,
        concurrency_limit: usize,
    ) -> CatgaResult<()> {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "transport batch concurrency limit must be greater than zero",
            ));
        }

        let mut publishes = stream::iter(envelopes)
            .map(|envelope| self.publish(envelope))
            .buffer_unordered(concurrency_limit);
        let mut first_error = None;
        while let Some(result) = publishes.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Receives the next delivery for the configured consumer.
    async fn receive(&self) -> CatgaResult<Delivery>;

    /// Acknowledges successful processing of a delivery.
    async fn ack(&self, delivery: Delivery) -> CatgaResult<()> {
        delivery.acknowledge().await
    }

    /// Requests redelivery of an unsuccessfully handled delivery.
    async fn nack(&self, delivery: Delivery) -> CatgaResult<()> {
        delivery.nack().await
    }
}

/// Sends to and receives from explicitly named durable destinations.
///
/// This contract extends [`MessageTransport`] instead of changing `publish` semantics.  An
/// adapter must document the durable resource behind each destination and return an error when
/// it is not provisioned; it must not silently fall back to best-effort Pub/Sub.
#[async_trait]
pub trait DestinationTransport: MessageTransport {
    /// Sends one caller-owned envelope to `destination`.
    ///
    /// Implementations apply their normal backpressure and return [`ErrorCode::Unavailable`] if
    /// the transport has stopped accepting new work.
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()>;

    /// Sends a caller-owned batch using the default bounded concurrency limit.
    async fn send_batch_to(
        &self,
        destination: &Destination,
        envelopes: Vec<Envelope>,
    ) -> CatgaResult<()> {
        self.send_batch_to_with_concurrency(
            destination,
            envelopes,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Sends a caller-owned batch with at most `concurrency_limit` active send futures.
    ///
    /// Every envelope is attempted before the first observed error is returned.  Pending future
    /// memory is `O(concurrency_limit)`; a zero limit returns [`ErrorCode::Validation`].
    async fn send_batch_to_with_concurrency(
        &self,
        destination: &Destination,
        envelopes: Vec<Envelope>,
        concurrency_limit: usize,
    ) -> CatgaResult<()> {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "destination transport batch concurrency limit must be greater than zero",
            ));
        }

        let mut sends = stream::iter(envelopes)
            .map(|envelope| self.send_to(destination, envelope))
            .buffer_unordered(concurrency_limit);
        let mut first_error = None;
        while let Some(result) = sends.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Receives the next acknowledged delivery from `destination`.
    ///
    /// The returned [`Delivery`] retains the backend acknowledgement token.  Call
    /// [`MessageTransport::ack`] after successful processing; dropping an unacknowledged
    /// delivery leaves durable adapters free to redeliver it.
    async fn receive_from(&self, destination: &Destination) -> CatgaResult<Delivery>;

    /// Provisions a named destination before it is used for send or receive.
    ///
    /// Transports that require explicit provisioning (such as the in-memory transport) override
    /// this. Transports with implicit destination creation keep the default no-op.
    fn declare_destination(&self, destination: &Destination) -> CatgaResult<()> {
        let _ = destination;
        Ok(())
    }
}

/// Typed message transport — unified interface for Request/Command/Event.
///
/// Implementors provide one concrete type (e.g., NatsTransport, RedisTransport, LocalTransport)
/// that satisfies all methods. Users pass `impl Transport` to handlers that need it.
///
/// # Example
///
/// ```ignore
/// async fn handler(transport: &impl Transport) -> CatgaResult<()> {
///     transport.send(GetUser { id: 42 }).await?;
///     transport.send_command(UpdateCache).await?;
///     transport.publish(UserLoggedIn { user_id }).await?;
///     Ok(())
/// }
/// ```
#[allow(async_fn_in_trait)]
pub trait Transport: Send + Sync {
    /// Sends a request and waits for its typed response.
    async fn send<R: Request>(&self, request: R) -> CatgaResult<R::Response>;

    /// Sends a command (fire-and-forget) and waits for acknowledgement.
    async fn send_command<C: Command>(&self, command: C) -> CatgaResult<()>;

    /// Publishes an event to all subscribers.
    async fn publish<E: Event>(&self, event: E) -> CatgaResult<()>;

    /// Sends a request after a delay.
    async fn send_delayed<R: Request>(
        &self,
        request: R,
        delay: Duration,
    ) -> CatgaResult<R::Response>;

    /// Sends a command after a delay.
    async fn send_command_delayed<C: Command>(
        &self,
        command: C,
        delay: Duration,
    ) -> CatgaResult<()>;

    /// Publishes an event after a delay.
    async fn publish_delayed<E: Event>(
        &self,
        event: E,
        delay: Duration,
    ) -> CatgaResult<()>;
}
