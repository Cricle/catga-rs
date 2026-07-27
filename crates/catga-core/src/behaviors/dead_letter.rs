use std::{panic::AssertUnwindSafe, sync::Arc};

use async_trait::async_trait;
use futures::FutureExt;

use crate::{
    Behavior, CatgaResult, Command, CommandBehavior, CommandNext, DeadLetter, DeadLetterStore,
    Envelope, ErrorCode, Next, Request,
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

    async fn record_failure(&self, envelope: Envelope, error: &crate::CatgaError) {
        let letter =
            DeadLetter::from_failure(envelope, error, self.attempts, "behavior.dead_letter");
        match letter {
            Ok(letter) => {
                if let Err(write_error) = self.store.enqueue(letter).await {
                    tracing::warn!(
                        target: crate::TRACING_TARGET,
                        error = %write_error.message(),
                        "dead-letter persistence failed while preserving the original pipeline error"
                    );
                }
            }
            Err(build_error) => tracing::warn!(
                target: crate::TRACING_TARGET,
                error = %build_error.message(),
                "dead-letter construction failed while preserving the original pipeline error"
            ),
        }
    }

    async fn next_result<T>(
        &self,
        operation: impl std::future::Future<Output = CatgaResult<T>>,
    ) -> CatgaResult<T> {
        match AssertUnwindSafe(operation).catch_unwind().await {
            Ok(result) => result,
            Err(_) => Err(crate::CatgaError::new(
                ErrorCode::Internal,
                "dead-letter pipeline processing panicked",
            )),
        }
    }
}

#[async_trait]
impl<M> Behavior<M> for DeadLetterBehavior
where
    M: Request + DeadLetterEnvelope,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let envelope = message.dead_letter_envelope();
        match self.next_result(next.run(message)).await {
            Err(error) if error.code() != ErrorCode::Transient => {
                self.record_failure(envelope, &error).await;
                Err(error)
            }
            result => result,
        }
    }
}

#[async_trait]
impl<C> CommandBehavior<C> for DeadLetterBehavior
where
    C: Command + DeadLetterEnvelope,
{
    async fn handle(&self, command: C, next: CommandNext<C>) -> CatgaResult<()> {
        let envelope = command.dead_letter_envelope();
        match self.next_result(next.run(command)).await {
            Err(error) if error.code() != ErrorCode::Transient => {
                self.record_failure(envelope, &error).await;
                Err(error)
            }
            result => result,
        }
    }
}
