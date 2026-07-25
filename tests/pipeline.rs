//! Typed mediator pipeline tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catga_core::{
    Behavior, CatgaError, CatgaResult, ErrorCode, Handler, Mediator, Next, Pipeline, Registry,
    Request, RetryBehavior, RetryJitter,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[derive(Debug)]
struct Double(u64);

impl catga_core::Message for Double {}

impl Request for Double {
    type Response = u64;
}

#[derive(Debug, Clone)]
struct RetryableRequest;

impl catga_core::Message for RetryableRequest {}

impl Request for RetryableRequest {
    type Response = ();
}

struct DoubleHandler(Arc<Mutex<Vec<&'static str>>>);

struct RetryableHandler(AtomicUsize);

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, message: Double) -> CatgaResult<u64> {
        self.0.lock().unwrap().push("handler");
        Ok(message.0 * 2)
    }
}

#[async_trait]
impl Handler<RetryableRequest> for RetryableHandler {
    async fn handle(&self, _: RetryableRequest) -> CatgaResult<()> {
        if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(CatgaError::new(ErrorCode::Transient, "retry once"));
        }
        Ok(())
    }
}

struct TraceBehavior {
    entered: &'static str,
    exited: &'static str,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Behavior<Double> for TraceBehavior {
    async fn handle(&self, message: Double, next: Next<Double>) -> CatgaResult<u64> {
        self.trace.lock().unwrap().push(self.entered);
        let response = next.run(message).await?;
        self.trace.lock().unwrap().push(self.exited);
        Ok(response)
    }
}

#[tokio::test]
async fn pipeline_behaviors_wrap_a_registered_handler_in_registration_order() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register_request::<Double, _>(DoubleHandler(trace.clone()))
        .unwrap();
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new()
        .with(TraceBehavior {
            entered: "a+",
            exited: "a-",
            trace: trace.clone(),
        })
        .with(TraceBehavior {
            entered: "b+",
            exited: "b-",
            trace: trace.clone(),
        });

    assert_eq!(mediator.send_with(Double(4), &pipeline).await.unwrap(), 8);
    assert_eq!(*trace.lock().unwrap(), ["a+", "b+", "handler", "b-", "a-"]);
}

#[tokio::test]
async fn retry_behavior_uses_fixed_jitter_without_waiting_for_the_base_delay() {
    let mut registry = Registry::new();
    registry
        .register_request::<RetryableRequest, _>(RetryableHandler(AtomicUsize::new(0)))
        .expect("test registry accepts one handler");
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(RetryBehavior::with_jitter(
        1,
        Duration::from_secs(1),
        RetryJitter::fixed(Duration::ZERO),
    ));

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        mediator.send_with(RetryableRequest, &pipeline),
    )
    .await
    .expect("fixed zero jitter avoids the one-second base delay");
    assert_eq!(result, Ok(()));
}
