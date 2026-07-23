//! Reliability behavior tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Handler, Mediator, Pipeline, Registry, Request,
    RetryBehavior,
};

#[derive(Clone, Debug)]
struct Work;

impl catga_core::Message for Work {}

impl Request for Work {
    type Response = &'static str;
}

struct FailsThenSucceeds(Arc<AtomicUsize>);

#[async_trait]
impl Handler<Work> for FailsThenSucceeds {
    async fn handle(&self, _: Work) -> CatgaResult<&'static str> {
        if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(CatgaError::new(ErrorCode::Transient, "try again"));
        }
        Ok("ok")
    }
}

struct TerminalFailure(Arc<AtomicUsize>);

#[async_trait]
impl Handler<Work> for TerminalFailure {
    async fn handle(&self, _: Work) -> CatgaResult<&'static str> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(CatgaError::new(ErrorCode::Validation, "bad request"))
    }
}

fn pipeline() -> Pipeline<Work> {
    Pipeline::new().with(RetryBehavior::new(2, Duration::ZERO))
}

#[tokio::test]
async fn retry_behavior_replays_transient_errors_but_not_terminal_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Work, _>(FailsThenSucceeds(Arc::clone(&attempts)))
        .unwrap();
    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send_with(Work, &pipeline()).await.unwrap(), "ok");
    assert_eq!(attempts.load(Ordering::Relaxed), 2);

    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Work, _>(TerminalFailure(Arc::clone(&attempts)))
        .unwrap();
    let mediator = Mediator::new(registry);
    assert_eq!(
        mediator
            .send_with(Work, &pipeline())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}
