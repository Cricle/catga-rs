use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    Behavior, CatgaResult, DeadLetter, DeadLetterStore, Envelope, ErrorCode, Next, Request,
};

/// Converts a typed request into the durable envelope retained on terminal failure.
pub trait DeadLetterEnvelope {
    /// Builds the serialized envelope used for dead-letter retention.
    fn dead_letter_envelope(&self) -> Envelope;
}

/// Retains terminal request failures while allowing transient failures to be retried normally.
pub struct DeadLetterBehavior {
    store: Arc<dyn DeadLetterStore>,
    attempts: u32,
}

impl DeadLetterBehavior {
    /// Creates a behavior that records terminal failures with the supplied attempt count.
    pub fn new<S>(store: Arc<S>, attempts: u32) -> Self
    where
        S: DeadLetterStore + 'static,
    {
        Self { store, attempts }
    }
}

#[async_trait]
impl<M> Behavior<M> for DeadLetterBehavior
where
    M: Request + Clone + DeadLetterEnvelope,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let original = message.clone();
        match next.run(message).await {
            Err(error) if error.code() != ErrorCode::Transient => {
                self.store
                    .enqueue(DeadLetter::new(
                        original.dead_letter_envelope(),
                        error.message(),
                        self.attempts,
                    ))
                    .await?;
                Err(error)
            }
            result => result,
        }
    }
}
