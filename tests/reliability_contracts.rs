//! Reliability storage contract tests.

use std::sync::Mutex;

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata, OutboxMessage, OutboxState,
    OutboxStore, validate_outbox_claim_limit,
};

#[derive(Default)]
struct RecordingOutbox {
    messages: Mutex<Vec<OutboxMessage>>,
}

#[async_trait]
impl OutboxStore for RecordingOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }

    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        validate_outbox_claim_limit(limit)?;
        let mut messages = self.messages.lock().unwrap();
        let mut claimed = Vec::new();
        for message in messages
            .iter_mut()
            .filter(|message| message.state() == OutboxState::Pending)
        {
            if claimed.len() == limit {
                break;
            }
            message.claim_until_with_token(owner, format!("recording-{}", message.id()), u64::MAX);
            claimed.push(message.clone());
        }
        Ok(claimed)
    }

    async fn ack(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        if let Some(message) = self.messages.lock().unwrap().iter_mut().find(|message| {
            message.id() == id
                && message.owner() == Some(owner)
                && message.claim_token() == Some(claim_token)
        }) {
            message.mark_published(0);
        }
        Ok(())
    }

    async fn release(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        let mut messages = self.messages.lock().unwrap();
        if let Some(message) = messages.iter_mut().find(|message| {
            message.id() == id
                && message.owner() == Some(owner)
                && message.claim_token() == Some(claim_token)
        }) {
            message.release();
        }
        Ok(())
    }

    async fn record_failure(
        &self,
        owner: &str,
        id: u64,
        claim_token: &str,
        reason: &str,
    ) -> CatgaResult<()> {
        let mut messages = self.messages.lock().map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "recording outbox mutex was poisoned")
        })?;
        if let Some(message) = messages.iter_mut().find(|message| {
            message.id() == id
                && message.owner() == Some(owner)
                && message.claim_token() == Some(claim_token)
        }) {
            message.record_failure(reason);
        }
        Ok(())
    }

    async fn cancel(&self, id: u64) -> CatgaResult<bool> {
        let mut messages = self.messages.lock().map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "recording outbox mutex was poisoned")
        })?;
        let Some(index) = messages
            .iter()
            .position(|message| message.id() == id && message.state() == OutboxState::Pending)
        else {
            return Ok(false);
        };
        messages.remove(index);
        Ok(true)
    }
}

fn message(id: u64) -> OutboxMessage {
    OutboxMessage::new(Envelope::new(
        id,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(id, None),
    ))
}

#[tokio::test]
async fn outbox_contract_supports_enqueue_claim_and_token_ack() {
    let store = RecordingOutbox::default();
    store.enqueue(message(1)).await.unwrap();

    let claimed = store.claim("worker-a", 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].owner(), Some("worker-a"));

    store
        .ack("worker-a", 1, claimed[0].claim_token().unwrap())
        .await
        .unwrap();
    assert!(store.claim("worker-b", 1).await.unwrap().is_empty());
}
