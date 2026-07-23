use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, OutboxMessage, OutboxState, OutboxStore};
use dashmap::DashMap;

/// A shard-locked, process-local outbox for development and deterministic tests.
#[derive(Default)]
pub struct MemoryOutbox {
    messages: DashMap<u64, OutboxMessage>,
}

#[async_trait]
impl OutboxStore for MemoryOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        let id = message.id();
        if self.messages.insert(id, message).is_some() {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "an outbox message with this identifier already exists",
            ));
        }
        Ok(())
    }

    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        let mut claimed = Vec::with_capacity(limit);
        for mut entry in self.messages.iter_mut() {
            if claimed.len() == limit {
                break;
            }
            if entry.state() == OutboxState::Pending {
                entry.claim(owner);
                claimed.push(entry.clone());
            }
        }
        Ok(claimed)
    }

    async fn ack(&self, owner: &str, id: u64) -> CatgaResult<()> {
        let owned_by_caller = self
            .messages
            .get(&id)
            .is_some_and(|message| message.owner() == Some(owner));
        if owned_by_caller {
            self.messages.remove(&id);
        }
        Ok(())
    }

    async fn release(&self, owner: &str, id: u64) -> CatgaResult<()> {
        if let Some(mut message) = self.messages.get_mut(&id)
            && message.owner() == Some(owner)
        {
            message.release();
        }
        Ok(())
    }
}
