//! Bounded, ephemeral in-memory Pub/Sub transport.

use crate::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Delivery, Envelope, ErrorCode,
    HealthCheckable, MessageTransport, QualityOfService, Stoppable, Waitable, telemetry,
};
use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

/// An in-memory AtMostOnce broadcast transport for local development and tests.
///
/// Every [`Self::subscribe`] call owns an independent Tokio broadcast cursor, so a publication
/// reaches all active local subscribers rather than being work-shared like [`crate::MemoryTransport`].
/// The ring has a fixed capacity: a slow subscriber receives a transient lag error instead of
/// causing an unbounded allocation or making publication wait indefinitely.
pub struct MemoryPubSubTransport {
    sender: broadcast::Sender<Envelope>,
    receiver: Mutex<broadcast::Receiver<Envelope>>,
    acceptance: AcceptanceGate,
}

impl MemoryPubSubTransport {
    /// Creates an ephemeral broadcast transport with a positive retained-message capacity.
    pub fn new(capacity: usize) -> CatgaResult<Self> {
        if capacity == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "memory Pub/Sub capacity must be greater than zero",
            ));
        }
        let (sender, receiver) = broadcast::channel(capacity);
        Ok(Self {
            sender,
            receiver: Mutex::new(receiver),
            acceptance: AcceptanceGate::default(),
        })
    }

    /// Registers and returns another independent local subscriber.
    ///
    /// The returned value shares the sender and acceptance gate but has its own receiver cursor.
    /// It observes publications that occur after this method returns.
    pub fn subscribe(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: Mutex::new(self.sender.subscribe()),
            acceptance: self.acceptance.clone(),
        }
    }
}

impl Clone for MemoryPubSubTransport {
    /// Clones this transport as an additional Pub/Sub subscriber.
    fn clone(&self) -> Self {
        self.subscribe()
    }
}

#[async_trait]
impl MessageTransport for MemoryPubSubTransport {
    /// Broadcasts one AtMostOnce envelope to every active local subscriber.
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("memory", "pubsub", async {
            self.acceptance.ensure_accepting()?;
            if envelope.metadata().quality_of_service() != QualityOfService::AtMostOnce {
                return Err(CatgaError::new(
                    ErrorCode::Unsupported,
                    "memory Pub/Sub supports AtMostOnce only; use MemoryTransport for queued delivery",
                ));
            }
            // A broadcast with no receivers is a valid ephemeral publication, just as a broker can
            // accept a message when no client is currently subscribed.
            let _ = self.sender.send(envelope);
            Ok(())
        })
        .await
    }

    /// Waits for the next publication for this subscriber cursor.
    async fn receive(&self) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("memory", "pubsub", async {
            let mut receiver = self.receiver.lock().await;
            match receiver.recv().await {
                Ok(envelope) => Ok(Delivery::new(envelope)),
                Err(broadcast::error::RecvError::Closed) => Err(CatgaError::new(
                    ErrorCode::Internal,
                    "memory Pub/Sub sender is closed",
                )),
                Err(broadcast::error::RecvError::Lagged(dropped)) => Err(CatgaError::new(
                    ErrorCode::Transient,
                    format!("memory Pub/Sub subscriber lagged {dropped} messages"),
                )),
            }
        })
        .await
    }
}

impl Stoppable for MemoryPubSubTransport {
    /// Rejects later broadcasts while existing subscriber cursors may drain.
    fn stop_accepting(&self) {
        self.acceptance.stop_accepting();
    }

    /// Returns whether new broadcasts are currently accepted.
    fn is_accepting(&self) -> bool {
        self.acceptance.is_accepting()
    }
}

#[async_trait]
impl AsyncInitializable for MemoryPubSubTransport {
    /// In-memory channel setup already completed in [`Self::new`].
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl HealthCheckable for MemoryPubSubTransport {
    /// An allocated in-memory channel is always ready.
    fn is_healthy(&self) -> bool {
        true
    }

    /// Returns a concise readiness description.
    fn health_status(&self) -> Option<&str> {
        Some("memory Pub/Sub transport is ready")
    }
}

#[async_trait]
impl Waitable for MemoryPubSubTransport {
    /// Broadcast deliveries have no acknowledgement token, so completion is immediate.
    async fn wait_for_completion(&self, _: CancellationToken) -> CatgaResult<()> {
        Ok(())
    }

    /// Returns zero because broadcasts do not retain acknowledgement-backed work.
    fn pending_operations(&self) -> usize {
        0
    }
}
