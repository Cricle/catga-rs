use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{
    Behavior, CachedResultCodec, CatgaError, CatgaResult, DEFAULT_INBOX_CLAIM_LEASE, ErrorCode,
    InboxStore, Next, Request, validate_inbox_claim_lease,
};

/// Supplies the transport message identifier used to suppress duplicate consumer processing.
pub trait InboxKey {
    /// Returns the stable identifier of the delivered transport message.
    ///
    /// A value of zero denotes that the delivery has no stable identifier. The
    /// inbox behavior bypasses deduplication for such messages, because using
    /// one shared sentinel would incorrectly cache unrelated requests.
    fn inbox_message_id(&self) -> u64;
}

/// Reuses completed consumer results and prevents duplicate inbound handler execution.
pub struct InboxBehavior<C> {
    store: Arc<dyn InboxStore>,
    codec: C,
    claim_lease: Duration,
}

impl<C> InboxBehavior<C> {
    /// Creates an inbox behavior backed by `store` and the response `codec`.
    pub fn new(store: Arc<dyn InboxStore>, codec: C) -> Self {
        Self {
            store,
            codec,
            claim_lease: DEFAULT_INBOX_CLAIM_LEASE,
        }
    }

    /// Replaces the exclusive-processing lease used for each inbox claim.
    ///
    /// The lease is validated before construction and is passed unchanged to
    /// [`InboxStore::try_claim_for`]. A caller should choose a value longer
    /// than its handler's normal worst-case execution time while retaining a
    /// bounded recovery window after a process crash.
    pub fn with_claim_lease(mut self, claim_lease: Duration) -> CatgaResult<Self> {
        validate_inbox_claim_lease(claim_lease)?;
        self.claim_lease = claim_lease;
        Ok(self)
    }

    /// Returns the exclusive-processing lease used for inbox claims.
    pub const fn claim_lease(&self) -> Duration {
        self.claim_lease
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
        if message_id == 0 {
            return next.run(message).await;
        }
        if !self
            .store
            .try_claim_for(message_id, self.claim_lease)
            .await?
        {
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
