//! Reliability storage contract tests.

use std::sync::Mutex;

use async_trait::async_trait;
use catga_core::{CatgaResult, Envelope, MessageMetadata, OutboxMessage, OutboxState, OutboxStore};

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
        let mut messages = self.messages.lock().unwrap();
        let mut claimed = Vec::new();
        for message in messages
            .iter_mut()
            .filter(|message| message.state() == OutboxState::Pending)
        {
            if claimed.len() == limit {
                break;
            }
            message.claim(owner);
            claimed.push(message.clone());
        }
        Ok(claimed)
    }

    async fn ack(&self, owner: &str, id: u64) -> CatgaResult<()> {
        self.messages.lock().unwrap().retain(|message| {
            message.id() != id || message.owner().is_none_or(|current| current != owner)
        });
        Ok(())
    }

    async fn release(&self, owner: &str, id: u64) -> CatgaResult<()> {
        let mut messages = self.messages.lock().unwrap();
        if let Some(message) = messages
            .iter_mut()
            .find(|message| message.id() == id && message.owner() == Some(owner))
        {
            message.release();
        }
        Ok(())
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
async fn outbox_contract_supports_enqueue_claim_and_owner_ack() {
    let store = RecordingOutbox::default();
    store.enqueue(message(1)).await.unwrap();

    let claimed = store.claim("worker-a", 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].owner(), Some("worker-a"));

    store.ack("worker-a", 1).await.unwrap();
    assert!(store.claim("worker-b", 1).await.unwrap().is_empty());
}
