//! Typed mediator pipeline tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catga_core::{
    Behavior, CatgaError, CatgaResult, Command, CommandBehavior, CommandHandler, CommandNext,
    CommandPipeline, ErrorCode, Handler, MAX_PIPELINE_DEPTH, Mediator, Next, Pipeline, Registry,
    Request, RetryBehavior, RetryJitter, current_cancellation,
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

#[derive(Debug)]
struct ShipOrder;

impl catga_core::Message for ShipOrder {}
impl Command for ShipOrder {}

struct DoubleHandler(Arc<Mutex<Vec<&'static str>>>);

struct RetryableHandler(AtomicUsize);

struct ShipOrderHandler(Arc<Mutex<Vec<&'static str>>>);

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

#[async_trait]
impl CommandHandler<ShipOrder> for ShipOrderHandler {
    async fn handle(&self, _: ShipOrder) -> CatgaResult<()> {
        self.0.lock().unwrap().push("handler");
        Ok(())
    }
}

struct TraceBehavior {
    entered: &'static str,
    exited: &'static str,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

struct PassThroughBehavior;

struct CommandTraceBehavior {
    entered: &'static str,
    exited: &'static str,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

struct CommandPassThroughBehavior;

struct CommandCancellationScopeBehavior(Arc<AtomicUsize>);

#[async_trait]
impl Behavior<Double> for PassThroughBehavior {
    async fn handle(&self, message: Double, next: Next<Double>) -> CatgaResult<u64> {
        next.run(message).await
    }
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

#[async_trait]
impl CommandBehavior<ShipOrder> for CommandTraceBehavior {
    async fn handle(&self, command: ShipOrder, next: CommandNext<ShipOrder>) -> CatgaResult<()> {
        self.trace.lock().unwrap().push(self.entered);
        next.run(command).await?;
        self.trace.lock().unwrap().push(self.exited);
        Ok(())
    }
}

#[async_trait]
impl CommandBehavior<ShipOrder> for CommandPassThroughBehavior {
    async fn handle(&self, command: ShipOrder, next: CommandNext<ShipOrder>) -> CatgaResult<()> {
        next.run(command).await
    }
}

#[async_trait]
impl CommandBehavior<ShipOrder> for CommandCancellationScopeBehavior {
    async fn handle(&self, command: ShipOrder, next: CommandNext<ShipOrder>) -> CatgaResult<()> {
        self.0.store(
            usize::from(current_cancellation().is_some()),
            Ordering::Release,
        );
        next.run(command).await
    }
}

#[test]
fn default_retry_behavior_uses_full_jitter() {
    assert!(matches!(
        RetryBehavior::new(1, Duration::ZERO).jitter_policy(),
        RetryJitter::Full { .. }
    ));
    assert_eq!(
        RetryBehavior::with_jitter(1, Duration::ZERO, RetryJitter::none()).jitter_policy(),
        RetryJitter::none()
    );
    assert_eq!(
        RetryBehavior::with_jitter(1, Duration::ZERO, RetryJitter::fixed(Duration::ZERO))
            .jitter_policy(),
        RetryJitter::fixed(Duration::ZERO)
    );
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
async fn command_pipeline_behaviors_wrap_a_registered_handler_in_registration_order() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register_command::<ShipOrder, _>(ShipOrderHandler(Arc::clone(&trace)))
        .expect("command handler registers");
    let mediator = Mediator::new(registry);
    let pipeline = CommandPipeline::new()
        .with(CommandTraceBehavior {
            entered: "a+",
            exited: "a-",
            trace: Arc::clone(&trace),
        })
        .with(CommandTraceBehavior {
            entered: "b+",
            exited: "b-",
            trace: Arc::clone(&trace),
        });

    mediator
        .send_command_with(ShipOrder, &pipeline)
        .await
        .expect("command pipeline dispatch succeeds");
    assert_eq!(*trace.lock().unwrap(), ["a+", "b+", "handler", "b-", "a-"]);
}

#[tokio::test]
async fn command_pipeline_exposes_the_cancellation_scope() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let observed_scope = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_command::<ShipOrder, _>(ShipOrderHandler(trace))
        .expect("command handler registers");
    let mediator = Mediator::new(registry);
    let pipeline = CommandPipeline::new().with(CommandCancellationScopeBehavior(Arc::clone(
        &observed_scope,
    )));

    mediator
        .send_command_with_cancellation_and_pipeline(
            ShipOrder,
            &pipeline,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("command dispatch succeeds");

    assert_eq!(observed_scope.load(Ordering::Acquire), 1);
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

#[tokio::test]
async fn pipeline_rejects_depths_above_the_supported_bound_before_the_handler_runs() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register_request::<Double, _>(DoubleHandler(Arc::clone(&trace)))
        .expect("test registry accepts one handler");
    let mediator = Mediator::new(registry);
    let mut pipeline = Pipeline::new();
    for _ in 0..=MAX_PIPELINE_DEPTH {
        pipeline = pipeline.with(PassThroughBehavior);
    }

    let error = mediator
        .send_with(Double(4), &pipeline)
        .await
        .expect_err("an oversized pipeline must be rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(trace.lock().expect("trace lock").is_empty());
}

#[tokio::test]
async fn command_pipeline_rejects_depths_above_the_supported_bound_before_the_handler_runs() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register_command::<ShipOrder, _>(ShipOrderHandler(Arc::clone(&trace)))
        .expect("command handler registers");
    let mediator = Mediator::new(registry);
    let mut pipeline = CommandPipeline::new();
    for _ in 0..=MAX_PIPELINE_DEPTH {
        pipeline = pipeline.with(CommandPassThroughBehavior);
    }

    let error = mediator
        .send_command_with(ShipOrder, &pipeline)
        .await
        .expect_err("an oversized command pipeline must be rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(trace.lock().expect("trace lock").is_empty());
}
