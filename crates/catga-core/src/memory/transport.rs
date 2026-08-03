use std::sync::Arc;

use async_trait::async_trait;
use crate::{
    AcceptanceGate, Acknowledger, AsyncInitializable, CatgaError, CatgaResult, Delivery,
    Destination, DestinationTransport, Envelope, ErrorCode, HealthCheckable, MessageTransport,
    OperationGuard, OperationTracker, Stoppable, Waitable, telemetry,
};
use dashmap::{DashMap, mapref::entry::Entry};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

/// A bounded FIFO transport for local development and deterministic tests.
#[derive(Clone, Debug)]
pub struct MemoryTransport {
    sender: mpsc::Sender<Envelope>,
    receiver: Arc<Mutex<mpsc::Receiver<Envelope>>>,
    destination_capacity: usize,
    destinations: Arc<DashMap<Destination, Arc<MemoryDestination>>>,
    operations: OperationTracker,
    acceptance: AcceptanceGate,
}

/// One explicitly declared bounded destination queue.
///
/// Its receiver mutex is local to this destination and protects only FIFO receive ownership; it
/// neither serializes unrelated destinations nor guards network I/O.
#[derive(Debug)]
struct MemoryDestination {
    sender: mpsc::Sender<Envelope>,
    receiver: Mutex<mpsc::Receiver<Envelope>>,
}

/// Acknowledges one local delivery and releases its shutdown-drain slot exactly once.
struct MemoryDeliveryAcknowledger {
    operation: OperationGuard,
}

impl MemoryDeliveryAcknowledger {
    /// Marks the delivery as no longer in flight, regardless of its processing outcome.
    fn complete(&self) {
        self.operation.complete();
    }
}

#[async_trait]
impl Acknowledger for MemoryDeliveryAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.complete();
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.complete();
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "memory transport does not support negative acknowledgement",
        ))
    }
}

impl MemoryTransport {
    /// Creates a transport with a fixed number of in-flight envelopes.
    pub fn new(capacity: usize) -> CatgaResult<Self> {
        if capacity == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "memory transport capacity must be greater than zero",
            ));
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok(Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            destination_capacity: capacity,
            destinations: Arc::new(DashMap::new()),
            operations: OperationTracker::default(),
            acceptance: AcceptanceGate::default(),
        })
    }

    /// Explicitly provisions one bounded, named local destination.
    ///
    /// A repeated declaration returns [`ErrorCode::Conflict`].  The queue capacity equals the
    /// transport capacity configured by [`Self::new`], so local tests retain the same backpressure
    /// bound for every destination.
    pub fn declare_destination(&self, destination: Destination) -> CatgaResult<()> {
        let (sender, receiver) = mpsc::channel(self.destination_capacity);
        let queue = Arc::new(MemoryDestination {
            sender,
            receiver: Mutex::new(receiver),
        });
        match self.destinations.entry(destination) {
            Entry::Vacant(entry) => {
                entry.insert(queue);
                Ok(())
            }
            Entry::Occupied(_) => Err(CatgaError::new(
                ErrorCode::Conflict,
                "memory transport destination is already declared",
            )),
        }
    }

    fn destination(&self, destination: &Destination) -> CatgaResult<Arc<MemoryDestination>> {
        self.destinations
            .get(destination)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::NotFound,
                    "memory transport destination is not declared",
                )
            })
    }
}

#[async_trait]
impl MessageTransport for MemoryTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("memory", "queue", async {
            self.acceptance.ensure_accepting()?;
            self.sender.send(envelope).await.map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "memory transport receiver is closed")
            })
        })
        .await
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("memory", "queue", async {
            let envelope = self.receiver.lock().await.recv().await.ok_or_else(|| {
                CatgaError::new(ErrorCode::Internal, "memory transport sender is closed")
            })?;
            let operation = self.operations.begin_operation();
            Ok(Delivery::with_acknowledger(
                envelope,
                Box::new(MemoryDeliveryAcknowledger { operation }),
            ))
        })
        .await
    }

    async fn ack(&self, delivery: Delivery) -> CatgaResult<()> {
        delivery.acknowledge().await
    }
}

#[async_trait]
impl DestinationTransport for MemoryTransport {
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("memory", "destination_queue", async {
            self.acceptance.ensure_accepting()?;
            let sender = self.destination(destination)?.sender.clone();
            sender.send(envelope).await.map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "memory transport destination receiver is closed",
                )
            })
        })
        .await
    }

    async fn receive_from(&self, destination: &Destination) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("memory", "destination_queue", async {
            let queue = self.destination(destination)?;
            let envelope = queue.receiver.lock().await.recv().await.ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "memory transport destination sender is closed",
                )
            })?;
            let operation = self.operations.begin_operation();
            Ok(Delivery::with_acknowledger(
                envelope,
                Box::new(MemoryDeliveryAcknowledger { operation }),
            ))
        })
        .await
    }

    fn declare_destination(&self, destination: &Destination) -> CatgaResult<()> {
        MemoryTransport::declare_destination(self, destination.clone())
    }
}

impl Stoppable for MemoryTransport {
    fn stop_accepting(&self) {
        self.acceptance.stop_accepting();
    }

    fn is_accepting(&self) -> bool {
        self.acceptance.is_accepting()
    }
}

#[async_trait]
impl AsyncInitializable for MemoryTransport {
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl HealthCheckable for MemoryTransport {
    fn is_healthy(&self) -> bool {
        true
    }

    fn health_status(&self) -> Option<&str> {
        Some("in-memory transport is ready")
    }
}

#[async_trait]
impl Waitable for MemoryTransport {
    async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        self.operations.wait_for_completion(cancellation).await
    }

    fn pending_operations(&self) -> usize {
        self.operations.pending_operations()
    }
}
