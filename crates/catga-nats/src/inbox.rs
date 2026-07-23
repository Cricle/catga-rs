//! JetStream KV-backed inbox records built on the shared claim state machine.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaResult, IdempotencyStore, InboxStore, ProcessingState};

use crate::NatsIdempotency;

/// JetStream KV-backed inbox with atomic per-message processing transitions.
pub struct NatsInbox {
    records: NatsIdempotency,
}

impl NatsInbox {
    /// Connects and provisions a one-history KV bucket for inbox message IDs.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        NatsIdempotency::connect(server, bucket)
            .await
            .map(|records| Self { records })
    }
}

#[async_trait]
impl InboxStore for NatsInbox {
    async fn try_claim(&self, message_id: u64) -> CatgaResult<bool> {
        self.records.try_claim(&message_id.to_string()).await
    }

    async fn complete(&self, message_id: u64, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        self.records.complete(&message_id.to_string(), result).await
    }

    async fn fail(&self, message_id: u64) -> CatgaResult<()> {
        self.records.fail(&message_id.to_string()).await
    }

    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>> {
        self.records.state(&message_id.to_string()).await
    }

    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        self.records.result(&message_id.to_string()).await
    }
}
