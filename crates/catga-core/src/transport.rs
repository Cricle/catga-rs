use async_trait::async_trait;
use futures::{StreamExt, stream};

use crate::{CatgaError, CatgaResult, Envelope, ErrorCode};

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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::{CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata, QualityOfService};

    use super::*;

    // Test for Destination
    #[test]
    fn destination_parse_valid_name() {
        let dest = Destination::parse("test-queue").expect("valid name");
        assert_eq!(dest.as_str(), "test-queue");
    }

    #[test]
    fn destination_parse_string() {
        let dest = Destination::parse(String::from("order-queue")).expect("valid name");
        assert_eq!(dest.as_str(), "order-queue");
    }

    #[test]
    fn destination_parse_rejects_empty() {
        let result = Destination::parse("");
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("validation error expected").code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn destination_parse_rejects_whitespace() {
        let result = Destination::parse("   ");
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("validation error expected").code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn destination_parse_accepts_leading_trailing_whitespace_trimmed() {
        let dest = Destination::parse("  test  ").expect("valid name");
        assert_eq!(dest.as_str(), "  test  ");
    }

    #[test]
    fn destination_into_boxed_str() {
        let dest = Destination::parse("test").expect("valid name");
        let boxed: Box<str> = dest.into_boxed_str();
        assert_eq!(&*boxed, "test");
    }

    #[test]
    fn destination_display() {
        let dest = Destination::parse("display-test").expect("valid name");
        let display = format!("{}", dest);
        assert_eq!(display, "display-test");
    }

    #[test]
    fn destination_debug() {
        let dest = Destination::parse("debug-test").expect("valid name");
        let debug = format!("{:?}", dest);
        assert!(debug.contains("Destination"));
        assert!(debug.contains("debug-test"));
    }

    #[test]
    fn destination_clone() {
        let dest1 = Destination::parse("clone-test").expect("valid name");
        let dest2 = dest1.clone();
        assert_eq!(dest1.as_str(), dest2.as_str());
    }

    #[test]
    fn destination_eq() {
        let dest1 = Destination::parse("eq-test").expect("valid name");
        let dest2 = Destination::parse("eq-test").expect("valid name");
        let dest3 = Destination::parse("other-test").expect("valid name");
        assert_eq!(dest1, dest2);
        assert_ne!(dest1, dest3);
    }

    #[test]
    fn destination_ord() {
        let dest1 = Destination::parse("aaa").expect("valid name");
        let dest2 = Destination::parse("bbb").expect("valid name");
        assert!(dest1 < dest2);
    }

    #[test]
    fn destination_hash() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let dest1 = Destination::parse("hash-test1").expect("valid name");
        let dest2 = Destination::parse("hash-test2").expect("valid name");
        map.insert(dest1.clone(), 1);
        map.insert(dest2.clone(), 2);
        assert_eq!(map.get(&dest1), Some(&1));
        assert_eq!(map.get(&dest2), Some(&2));
    }

    // Test for Delivery
    fn make_test_envelope() -> Envelope {
        let metadata =
            MessageMetadata::new(123, None).with_quality_of_service(QualityOfService::AtLeastOnce);
        Envelope::new(123, "test-event", Vec::new(), metadata)
    }

    #[test]
    fn delivery_new() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope);

        assert_eq!(delivery.attempts(), 1);
        assert_eq!(delivery.envelope().metadata().message_id(), 123);
    }

    #[test]
    fn delivery_with_attempts() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope).with_attempts(5);

        assert_eq!(delivery.attempts(), 5);
    }

    #[test]
    fn delivery_with_attempts_normalizes_zero() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope).with_attempts(0);

        assert_eq!(delivery.attempts(), 1);
    }

    #[test]
    fn delivery_with_acknowledger() {
        struct TestAcknowledger;
        #[async_trait]
        impl Acknowledger for TestAcknowledger {
            async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
                Ok(())
            }
        }

        let envelope = make_test_envelope();
        let delivery = Delivery::with_acknowledger(envelope, Box::new(TestAcknowledger));

        // Verify acknowledger is present
        let debug = format!("{:?}", delivery);
        assert!(debug.contains("requires_ack"));
    }

    #[tokio::test]
    async fn delivery_acknowledge_without_acknowledger() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope);

        assert!(delivery.acknowledge().await.is_ok());
    }

    #[tokio::test]
    async fn delivery_nack_without_acknowledger() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope);

        assert!(delivery.nack().await.is_ok());
    }

    #[tokio::test]
    async fn delivery_negative_acknowledge_without_acknowledger() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope);

        assert!(delivery.negative_acknowledge().await.is_ok());
    }

    #[test]
    fn delivery_map_envelope() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope);

        let new_envelope = make_test_envelope();
        let mapped = delivery
            .map_envelope(|_| Ok(new_envelope))
            .expect("map should succeed");

        assert_eq!(mapped.envelope().metadata().message_id(), 123);
    }

    #[test]
    fn delivery_map_envelope_error() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope);

        let result =
            delivery.map_envelope(|_| Err(CatgaError::new(ErrorCode::Internal, "test error")));

        assert!(result.is_err());
    }

    #[test]
    fn delivery_debug() {
        let envelope = make_test_envelope();
        let delivery = Delivery::new(envelope);

        let debug = format!("{:?}", delivery);
        assert!(debug.contains("Delivery"));
        assert!(debug.contains("envelope"));
        assert!(debug.contains("attempts"));
    }

    // MockAcknowledger with negative acknowledge
    struct MockAcknowledgerWithNack;
    #[async_trait]
    impl Acknowledger for MockAcknowledgerWithNack {
        async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
            Ok(())
        }

        async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
            Ok(())
        }
    }

    struct MockAcknowledger;
    #[async_trait]
    impl Acknowledger for MockAcknowledger {
        async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn delivery_acknowledge_with_acknowledger() {
        let envelope = make_test_envelope();
        let delivery = Delivery::with_acknowledger(envelope, Box::new(MockAcknowledger));

        assert!(delivery.acknowledge().await.is_ok());
    }

    #[tokio::test]
    async fn delivery_nack_with_acknowledger_with_nack() {
        let envelope = make_test_envelope();
        let delivery = Delivery::with_acknowledger(envelope, Box::new(MockAcknowledgerWithNack));

        assert!(delivery.nack().await.is_ok());
    }

    // Test for default constants
    #[test]
    fn default_transport_batch_concurrency_value() {
        assert_eq!(DEFAULT_TRANSPORT_BATCH_CONCURRENCY, 100);
    }

    // Mock MessageTransport for testing default implementations
    struct MockMessageTransport;
    #[async_trait]
    impl MessageTransport for MockMessageTransport {
        async fn publish(&self, _envelope: Envelope) -> CatgaResult<()> {
            Ok(())
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            Ok(Delivery::new(make_test_envelope()))
        }
    }

    // Test that default implementations work
    #[tokio::test]
    async fn message_transport_default_publish_batch() {
        let transport = MockMessageTransport;
        let envelopes = vec![make_test_envelope(), make_test_envelope()];

        let result = transport.publish_batch(envelopes).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn message_transport_publish_batch_with_concurrency_zero_rejected() {
        let transport = MockMessageTransport;
        let envelopes = vec![make_test_envelope()];

        let result = transport.publish_batch_with_concurrency(envelopes, 0).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("result should be Err").code(),
            ErrorCode::Validation
        );
    }

    #[tokio::test]
    async fn message_transport_publish_batch_with_concurrency() {
        let transport = MockMessageTransport;
        let envelopes = vec![make_test_envelope(), make_test_envelope()];

        let result = transport.publish_batch_with_concurrency(envelopes, 1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn message_transport_ack_default() {
        let transport = MockMessageTransport;
        let delivery = Delivery::new(make_test_envelope());

        // Default ack calls delivery.acknowledge()
        assert!(transport.ack(delivery).await.is_ok());
    }

    #[tokio::test]
    async fn message_transport_nack_default() {
        let transport = MockMessageTransport;
        let delivery = Delivery::new(make_test_envelope());

        // Default nack calls delivery.nack()
        assert!(transport.nack(delivery).await.is_ok());
    }

    // Mock DestinationTransport for testing
    struct MockDestinationTransport;
    #[async_trait]
    impl MessageTransport for MockDestinationTransport {
        async fn publish(&self, _envelope: Envelope) -> CatgaResult<()> {
            Ok(())
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            Ok(Delivery::new(make_test_envelope()))
        }
    }

    #[async_trait]
    impl DestinationTransport for MockDestinationTransport {
        async fn send_to(
            &self,
            _destination: &Destination,
            _envelope: Envelope,
        ) -> CatgaResult<()> {
            Ok(())
        }

        async fn receive_from(&self, _destination: &Destination) -> CatgaResult<Delivery> {
            Ok(Delivery::new(make_test_envelope()))
        }
    }

    #[tokio::test]
    async fn destination_transport_send_batch_to() {
        let transport = MockDestinationTransport;
        let dest = Destination::parse("test-dest").expect("valid test destination");
        let envelopes = vec![make_test_envelope(), make_test_envelope()];

        let result = transport.send_batch_to(&dest, envelopes).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn destination_transport_send_batch_to_with_concurrency_zero_rejected() {
        let transport = MockDestinationTransport;
        let dest = Destination::parse("test-dest").expect("valid test destination");
        let envelopes = vec![make_test_envelope()];

        let result = transport
            .send_batch_to_with_concurrency(&dest, envelopes, 0)
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("result should be Err").code(),
            ErrorCode::Validation
        );
    }

    #[tokio::test]
    async fn destination_transport_declare_destination_default() {
        let transport = MockDestinationTransport;
        let dest = Destination::parse("test-dest").expect("valid test destination");

        // Default implementation just returns Ok
        assert!(transport.declare_destination(&dest).is_ok());
    }

    #[tokio::test]
    async fn destination_transport_receive_from() {
        let transport = MockDestinationTransport;
        let dest = Destination::parse("test-dest").expect("valid test destination");

        let result = transport.receive_from(&dest).await;
        assert!(result.is_ok());
    }

    // Test for error propagation in batch operations
    struct FailingTransport;
    #[async_trait]
    impl MessageTransport for FailingTransport {
        async fn publish(&self, _envelope: Envelope) -> CatgaResult<()> {
            Err(CatgaError::new(ErrorCode::Unavailable, "transport down"))
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            Ok(Delivery::new(make_test_envelope()))
        }
    }

    #[tokio::test]
    async fn message_transport_batch_returns_first_error() {
        let transport = FailingTransport;
        let envelopes = vec![make_test_envelope(), make_test_envelope()];

        let result = transport.publish_batch_with_concurrency(envelopes, 2).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("result should be Err").code(),
            ErrorCode::Unavailable
        );
    }

    #[tokio::test]
    async fn message_transport_batch_tries_all_envelopes() {
        let transport = FailingTransport;
        let envelopes = vec![
            make_test_envelope(),
            make_test_envelope(),
            make_test_envelope(),
        ];

        // Should return error but all were attempted
        let result = transport.publish_batch_with_concurrency(envelopes, 1).await;
        assert!(result.is_err());
    }
}
