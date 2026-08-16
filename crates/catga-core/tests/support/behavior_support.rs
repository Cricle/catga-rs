//! Shared request fixture for pipeline-behavior contract tests.
//!
//! Every item here is exercised by each test binary that includes the file,
//! keeping the shared fixture free of dead-code warnings.

use std::sync::Arc;

use catga_core::{
    BatchKeyProvider, BatchOptions, BatchOptionsProvider, Correlated, DeadLetterEnvelope,
    DistributedLockKey, Envelope, IdempotencyKey, InboxKey, Message, MessageMetadata,
    OutboxEnvelope, Registry, Request,
};

/// Cloneable request carrying a stable identity, shard key, and correlation hint.
#[derive(Clone)]
pub struct Req {
    pub id: u64,
    pub key: &'static str,
    pub correlation: Option<u64>,
}

impl Default for Req {
    fn default() -> Self {
        Self {
            id: 0,
            key: "default",
            correlation: None,
        }
    }
}

impl Message for Req {}
impl Request for Req {
    type Response = u64;
}
impl IdempotencyKey for Req {
    fn idempotency_key(&self) -> &str {
        self.key
    }
}
impl InboxKey for Req {
    fn inbox_message_id(&self) -> u64 {
        self.id
    }
}
impl DistributedLockKey for Req {
    fn distributed_lock_key(&self) -> Box<str> {
        self.key.into()
    }
}
impl Correlated for Req {
    fn metadata(&self) -> MessageMetadata {
        MessageMetadata::new(self.id, self.correlation)
    }
}

/// Builds a small envelope used by durability fixtures.
pub fn env(id: u64) -> Envelope {
    Envelope::new(
        id,
        "BehaviorReq",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

impl DeadLetterEnvelope for Req {
    fn dead_letter_envelope(&self) -> Envelope {
        env(self.id)
    }
}
impl OutboxEnvelope for Req {
    fn outbox_envelope(&self) -> Envelope {
        env(self.id)
    }
}
impl BatchKeyProvider for Req {
    fn batch_key(&self) -> Option<Box<str>> {
        if self.key == "default" {
            None
        } else {
            Some(self.key.into())
        }
    }
}
impl BatchOptionsProvider for Req {
    fn batch_options() -> BatchOptions {
        BatchOptions::default()
    }
}

/// Builds a mediator whose `Req` handler is `handler`.
pub fn req_mediator<H>(handler: H) -> Arc<catga_core::Mediator>
where
    H: catga_core::Handler<Req> + 'static,
{
    let mut registry = Registry::new();
    registry
        .register_request::<Req, _>(handler)
        .expect("request registration succeeds");
    Arc::new(catga_core::Mediator::new(registry))
}
