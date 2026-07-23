use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, Delivery, Envelope, ErrorCode, MessageTransport};
use tokio::sync::{Mutex, mpsc};

/// A bounded FIFO transport for local development and deterministic tests.
#[derive(Clone)]
pub struct MemoryTransport {
    sender: mpsc::Sender<Envelope>,
    receiver: Arc<Mutex<mpsc::Receiver<Envelope>>>,
}

impl MemoryTransport {
    /// Creates a transport with a fixed number of in-flight envelopes.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "memory transport capacity must be greater than zero"
        );
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }
}

#[async_trait]
impl MessageTransport for MemoryTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        self.sender.send(envelope).await.map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "memory transport receiver is closed")
        })
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        self.receiver
            .lock()
            .await
            .recv()
            .await
            .map(Delivery::new)
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Internal, "memory transport sender is closed")
            })
    }

    async fn ack(&self, _: Delivery) -> CatgaResult<()> {
        Ok(())
    }
}
