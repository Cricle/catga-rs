use std::{panic::AssertUnwindSafe, sync::Arc};

use async_trait::async_trait;
use futures::FutureExt;

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

    async fn fail_claim(&self, key: &str, original_error: &CatgaError) {
        if let Err(cleanup_error) = self.store.fail(key).await {
            tracing::warn!(
                target: crate::TRACING_TARGET,
                error = %cleanup_error.message(),
                original_error = %original_error.message(),
                "idempotency claim cleanup failed while preserving the original pipeline error"
            );
        }
    }

    async fn next_result<T>(
        &self,
        operation: impl std::future::Future<Output = CatgaResult<T>>,
    ) -> CatgaResult<T> {
        match AssertUnwindSafe(operation).catch_unwind().await {
            Ok(result) => result,
            Err(_) => Err(CatgaError::new(
                ErrorCode::Internal,
                "idempotency pipeline processing panicked",
            )),
        }
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

        match self.next_result(next.run(message)).await {
            Ok(response) => {
                let cached = match self.codec.encode(&response) {
                    Ok(cached) => cached,
                    Err(error) => {
                        self.fail_claim(&key, &error).await;
                        return Err(error);
                    }
                };
                self.store.complete(&key, Some(cached)).await?;
                Ok(response)
            }
            Err(error) => {
                self.fail_claim(&key, &error).await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_trait_basic() {
        struct TestKey(String);
        impl IdempotencyKey for TestKey {
            fn idempotency_key(&self) -> &str {
                &self.0
            }
        }
        let test_key = TestKey(String::from("key"));
        assert_eq!(test_key.idempotency_key(), "key");
    }
}
