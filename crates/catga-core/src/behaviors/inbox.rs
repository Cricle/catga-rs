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
            let result = next.run(message).await;
            crate::telemetry::record_inbox_outcome(if result.is_ok() {
                "bypassed"
            } else {
                "failure"
            });
            return result;
        }
        let claimed = match self.store.try_claim_for(message_id, self.claim_lease).await {
            Ok(claimed) => claimed,
            Err(error) => {
                crate::telemetry::record_inbox_outcome("failure");
                return Err(error);
            }
        };
        if !claimed {
            let cached = match self.store.result(message_id).await {
                Ok(cached) => cached,
                Err(error) => {
                    crate::telemetry::record_inbox_outcome("failure");
                    return Err(error);
                }
            };
            let Some(cached) = cached else {
                crate::telemetry::record_inbox_outcome("conflict");
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "inbox message is already claimed",
                ));
            };
            let result = self.codec.decode(&cached);
            crate::telemetry::record_inbox_outcome(if result.is_ok() { "hit" } else { "failure" });
            return result;
        }

        match next.run(message).await {
            Ok(response) => {
                let cached = match self.codec.encode(&response) {
                    Ok(cached) => cached,
                    Err(error) => {
                        crate::telemetry::record_inbox_outcome("failure");
                        return Err(error);
                    }
                };
                if let Err(error) = self.store.complete(message_id, Some(cached)).await {
                    crate::telemetry::record_inbox_outcome("failure");
                    return Err(error);
                }
                crate::telemetry::record_inbox_outcome("processed");
                Ok(response)
            }
            Err(error) => {
                if let Err(store_error) = self.store.fail(message_id).await {
                    crate::telemetry::record_inbox_outcome("failure");
                    return Err(store_error);
                }
                crate::telemetry::record_inbox_outcome("failure");
                Err(error)
            }
        }
    }
}
