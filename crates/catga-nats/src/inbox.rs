//! JetStream KV-backed inbox records built on the shared claim state machine.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use catga_core::{
    CatgaResult, DEFAULT_INBOX_CLAIM_LEASE, IdempotencyStore, InboxStore, ProcessingState,
    inbox_claim_expires_at, telemetry,
};

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
        self.try_claim_for(message_id, DEFAULT_INBOX_CLAIM_LEASE)
            .await
    }

    async fn try_claim_for(&self, message_id: u64, lease: Duration) -> CatgaResult<bool> {
        telemetry::record_persistence("nats", "inbox", "try_claim", async {
            self.records
                .try_claim_until(&message_id.to_string(), inbox_claim_expires_at(lease)?)
                .await
        })
        .await
    }

    async fn complete(&self, message_id: u64, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "inbox", "complete", async {
            self.records.complete(&message_id.to_string(), result).await
        })
        .await
    }

    async fn fail(&self, message_id: u64) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "inbox", "fail", async {
            self.records.fail(&message_id.to_string()).await
        })
        .await
    }

    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>> {
        telemetry::record_persistence("nats", "inbox", "state", async {
            self.records.state(&message_id.to_string()).await
        })
        .await
    }

    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        telemetry::record_persistence("nats", "inbox", "result", async {
            self.records.result(&message_id.to_string()).await
        })
        .await
    }

    async fn cleanup_completed(&self, retention: Duration, limit: usize) -> CatgaResult<usize> {
        telemetry::record_persistence("nats", "inbox", "cleanup", async {
            self.records.cleanup_completed_for(retention, limit).await
        })
        .await
    }
}
