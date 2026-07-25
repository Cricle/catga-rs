use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    Behavior, CachedResultCodec, CatgaError, CatgaResult, ErrorCode, IdempotencyStore, Next,
    Request,
};

/// Supplies the stable key used to suppress duplicate request execution.
pub trait IdempotencyKey {
    /// Returns the key that uniquely identifies this logical request.
    fn idempotency_key(&self) -> &str;
}

/// Reuses completed responses and prevents concurrent duplicate handler execution.
pub struct IdempotencyBehavior<C> {
    store: Arc<dyn IdempotencyStore>,
    codec: C,
}

impl<C> IdempotencyBehavior<C> {
    /// Creates a behavior backed by `store` and the response `codec`.
    pub fn new(store: Arc<dyn IdempotencyStore>, codec: C) -> Self {
        Self { store, codec }
    }
}

#[async_trait]
impl<M, C> Behavior<M> for IdempotencyBehavior<C>
where
    M: Request + IdempotencyKey,
    C: CachedResultCodec<M::Response>,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let key: Box<str> = message.idempotency_key().into();
        if !self.store.try_claim(&key).await? {
            return self
                .store
                .result(&key)
                .await?
                .map(|cached| self.codec.decode(&cached))
                .transpose()?
                .ok_or_else(|| {
                    CatgaError::new(ErrorCode::Conflict, "idempotency key is already claimed")
                });
        }

        match next.run(message).await {
            Ok(response) => {
                let cached = self.codec.encode(&response)?;
                self.store.complete(&key, Some(cached)).await?;
                Ok(response)
            }
            Err(error) => {
                self.store.fail(&key).await?;
                Err(error)
            }
        }
    }
}
