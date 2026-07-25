use std::time::Instant;

use async_trait::async_trait;
use tracing::Instrument;

use crate::{Behavior, CatgaResult, Next, Request, observability};

/// Adds a structured child span around one typed request pipeline.
#[derive(Clone, Copy, Debug, Default)]
pub struct TracingBehavior;

#[async_trait]
impl<M> Behavior<M> for TracingBehavior
where
    M: Request,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let request_type = std::any::type_name::<M>();
        let span = observability::pipeline_span(request_type);
        let started = Instant::now();
        let result = next.run(message).instrument(span.clone()).await;
        observability::record_pipeline(&span, request_type, started.elapsed(), &result);
        result
    }
}
