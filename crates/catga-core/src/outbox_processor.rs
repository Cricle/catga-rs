use std::sync::Arc;

use crate::{CatgaError, CatgaResult, ErrorCode, MessageTransport, OutboxStore};

/// Counts outcomes from one outbox scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxRun {
    published: usize,
    failed: usize,
}

impl OutboxRun {
    /// Returns how many messages were published and acknowledged.
    pub const fn published(self) -> usize {
        self.published
    }

    /// Returns how many messages were released for a later retry.
    pub const fn failed(self) -> usize {
        self.failed
    }
}

/// Claims, publishes, and acknowledges bounded batches from an outbox store.
pub struct OutboxProcessor<S, T> {
    store: Arc<S>,
    transport: Arc<T>,
    owner: Box<str>,
    batch_size: usize,
}

impl<S, T> OutboxProcessor<S, T>
where
    S: OutboxStore,
    T: MessageTransport,
{
    /// Creates a processor owned by `owner` that claims at most `batch_size` messages per scan.
    pub fn new(
        store: Arc<S>,
        transport: Arc<T>,
        owner: impl Into<Box<str>>,
        batch_size: usize,
    ) -> CatgaResult<Self> {
        if batch_size == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "outbox batch size must be greater than zero",
            ));
        }
        Ok(Self {
            store,
            transport,
            owner: owner.into(),
            batch_size,
        })
    }

    /// Processes one bounded batch, releasing failed deliveries for a later retry.
    pub async fn flush_once(&self) -> CatgaResult<OutboxRun> {
        let messages = self.store.claim(&self.owner, self.batch_size).await?;
        let mut run = OutboxRun::default();
        for message in messages {
            let id = message.id();
            let published = self.transport.publish(message.envelope().clone()).await;
            if published.is_ok() && self.store.ack(&self.owner, id).await.is_ok() {
                run.published += 1;
            } else {
                self.store.release(&self.owner, id).await?;
                run.failed += 1;
            }
        }
        Ok(run)
    }
}
