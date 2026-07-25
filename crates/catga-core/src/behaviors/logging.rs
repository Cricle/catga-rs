use std::time::Instant;

use async_trait::async_trait;

use crate::{Behavior, CatgaResult, Next, Request, TRACING_TARGET, current_correlation_id};

/// Emits structured request lifecycle logs without changing request semantics.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoggingBehavior;

#[async_trait]
impl<M> Behavior<M> for LoggingBehavior
where
    M: Request,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let request_type = std::any::type_name::<M>();
        let correlation_id = current_correlation_id();
        tracing::info!(
            target: TRACING_TARGET,
            catga_kind = "request",
            request_type,
            ?correlation_id,
            "catga request started"
        );
        let started = Instant::now();
        let result = next.run(message).await;
        let duration_ms = started.elapsed().as_secs_f64() * 1_000.0;
        match &result {
            Ok(_) => tracing::info!(
                target: TRACING_TARGET,
                catga_kind = "request",
                request_type,
                ?correlation_id,
                duration_ms,
                "catga request succeeded"
            ),
            Err(error) => tracing::warn!(
                target: TRACING_TARGET,
                catga_kind = "request",
                request_type,
                ?correlation_id,
                duration_ms,
                error = error.message(),
                "catga request failed"
            ),
        }
        result
    }
}
