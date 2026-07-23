use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    Behavior, CachedResultCodec, CatgaError, CatgaResult, ErrorCode, InboxStore, Next, Request,
};

/// Supplies the transport message identifier used to suppress duplicate consumer processing.
pub trait InboxKey {
    /// Returns the stable identifier of the delivered transport message.
    fn inbox_message_id(&self) -> u64;
}

/// Reuses completed consumer results and prevents duplicate inbound handler execution.
pub struct InboxBehavior<C> {
    store: Arc<dyn InboxStore>,
    codec: C,
}

impl<C> InboxBehavior<C> {
    /// Creates an inbox behavior backed by `store` and the response `codec`.
    pub fn new(store: Arc<dyn InboxStore>, codec: C) -> Self {
        Self { store, codec }
    }
}

#[async_trait]
impl<M, C> Behavior<M> for InboxBehavior<C>
where
    M: Request + InboxKey,
    C: CachedResultCodec<M::Response>,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let message_id = message.inbox_message_id();
        if !self.store.try_claim(message_id).await? {
            return self
                .store
                .result(message_id)
                .await?
                .map(|cached| self.codec.decode(&cached))
                .transpose()?
                .ok_or_else(|| {
                    CatgaError::new(ErrorCode::Conflict, "inbox message is already claimed")
                });
        }

        match next.run(message).await {
            Ok(response) => {
                let cached = self.codec.encode(&response)?;
                self.store.complete(message_id, Some(cached)).await?;
                Ok(response)
            }
            Err(error) => {
                self.store.fail(message_id).await?;
                Err(error)
            }
        }
    }
}
