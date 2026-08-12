//! Strict pipeline contracts: behavior execution order, short-circuiting, error and panic
//! mapping, value flow, chain reuse, and construction-time depth bounds.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    Behavior, CatgaError, CatgaResult, Command, CommandBehavior, CommandNext, CommandPipeline,
    ErrorCode, MAX_PIPELINE_DEPTH, Mediator, Message, MessageTypeId, Next, Pipeline, Registry,
    Request, command_handler, request_handler,
};

struct OrderTypeId;
impl MessageTypeId for OrderTypeId {
    const NAME: &'static str = "Order";
}

struct Order(u64);
impl Message for Order {}
impl Request for Order {
    type Response = u64;
    type TypeId = OrderTypeId;
}

struct StepTypeId;
impl MessageTypeId for StepTypeId {
    const NAME: &'static str = "Step";
}

struct Step(usize);
impl Message for Step {}
impl Command for Step {
    type TypeId = StepTypeId;
}

type Trace = Arc<Mutex<Vec<String>>>;

fn trace_entry(log: &Trace, entry: String) {
    log.lock()
        .expect("pipeline trace mutex poisoned")
        .push(entry);
}

/// Records `before`/`after` markers around the rest of the chain, whatever the outcome.
struct TraceBehavior {
    log: Trace,
    name: &'static str,
}

impl TraceBehavior {
    fn new(log: &Trace, name: &'static str) -> Self {
        Self {
            log: Arc::clone(log),
            name,
        }
    }
}

#[async_trait]
impl Behavior<Order> for TraceBehavior {
    async fn handle(&self, message: Order, next: Next<Order>) -> CatgaResult<u64> {
        trace_entry(&self.log, format!("before:{}", self.name));
        let result = next.run(message).await;
        trace_entry(&self.log, format!("after:{}", self.name));
        result
    }
}

#[async_trait]
impl CommandBehavior<Step> for TraceBehavior {
    async fn handle(&self, command: Step, next: CommandNext<Step>) -> CatgaResult<()> {
        trace_entry(&self.log, format!("before:{}", self.name));
        let result = next.run(command).await;
        trace_entry(&self.log, format!("after:{}", self.name));
        result
    }
}

/// Returns a prepared response without ever invoking the rest of the chain.
struct ShortCircuit {
    log: Trace,
    name: &'static str,
    response: u64,
}

#[async_trait]
impl Behavior<Order> for ShortCircuit {
    async fn handle(&self, _: Order, _: Next<Order>) -> CatgaResult<u64> {
        trace_entry(&self.log, format!("short:{}", self.name));
        Ok(self.response)
    }
}

#[async_trait]
impl CommandBehavior<Step> for ShortCircuit {
    async fn handle(&self, _: Step, _: CommandNext<Step>) -> CatgaResult<()> {
        trace_entry(&self.log, format!("short:{}", self.name));
        Ok(())
    }
}

/// Fails with a prepared error without invoking the rest of the chain.
struct Failing {
    log: Trace,
    name: &'static str,
    code: ErrorCode,
    message: &'static str,
}

#[async_trait]
impl Behavior<Order> for Failing {
    async fn handle(&self, _: Order, _: Next<Order>) -> CatgaResult<u64> {
        trace_entry(&self.log, format!("fail:{}", self.name));
        Err(CatgaError::new(self.code, self.message))
    }
}

#[async_trait]
impl CommandBehavior<Step> for Failing {
    async fn handle(&self, _: Step, _: CommandNext<Step>) -> CatgaResult<()> {
        trace_entry(&self.log, format!("fail:{}", self.name));
        Err(CatgaError::new(self.code, self.message))
    }
}

struct Panic;

#[async_trait]
impl Behavior<Order> for Panic {
    async fn handle(&self, _: Order, _: Next<Order>) -> CatgaResult<u64> {
        panic!("request behavior exploded");
    }
}

#[async_trait]
impl CommandBehavior<Step> for Panic {
    async fn handle(&self, _: Step, _: CommandNext<Step>) -> CatgaResult<()> {
        panic!("command behavior exploded");
    }
}

struct Pass;

#[async_trait]
impl Behavior<Order> for Pass {
    async fn handle(&self, message: Order, next: Next<Order>) -> CatgaResult<u64> {
        next.run(message).await
    }
}

#[async_trait]
impl CommandBehavior<Step> for Pass {
    async fn handle(&self, command: Step, next: CommandNext<Step>) -> CatgaResult<()> {
        next.run(command).await
    }
}

/// Rewrites the in-flight request before handing it to the next stage.
struct AddToMessage(u64);

#[async_trait]
impl Behavior<Order> for AddToMessage {
    async fn handle(&self, message: Order, next: Next<Order>) -> CatgaResult<u64> {
        next.run(Order(message.0 + self.0)).await
    }
}

/// Rewrites the response produced by the rest of the chain.
struct AddToResponse(u64);

#[async_trait]
impl Behavior<Order> for AddToResponse {
    async fn handle(&self, message: Order, next: Next<Order>) -> CatgaResult<u64> {
        Ok(next.run(message).await? + self.0)
    }
}

/// Invokes the remaining chain twice and combines both responses.
struct RunTwice;

#[async_trait]
impl Behavior<Order> for RunTwice {
    async fn handle(&self, message: Order, next: Next<Order>) -> CatgaResult<u64> {
        let first = next.run(Order(message.0)).await?;
        let second = next.run(Order(message.0 + 10)).await?;
        Ok(first + second)
    }
}

/// Registers a request handler that records `handler` and doubles the order value.
fn doubling_mediator(handler_calls: &Arc<AtomicUsize>, log: &Trace) -> CatgaResult<Mediator> {
    let mut registry = Registry::new();
    registry.register_request::<Order, _>(request_handler({
        let handler_calls = Arc::clone(handler_calls);
        let log = Arc::clone(log);
        move |order: Order| {
            let handler_calls = Arc::clone(&handler_calls);
            let log = Arc::clone(&log);
            async move {
                handler_calls.fetch_add(1, Ordering::SeqCst);
                trace_entry(&log, "handler".to_string());
                Ok(order.0 * 2)
            }
        }
    }))?;
    Ok(Mediator::new(registry))
}

/// Registers a command handler that adds the step value to the shared counter.
fn counting_mediator(steps: &Arc<AtomicUsize>) -> CatgaResult<Mediator> {
    let mut registry = Registry::new();
    registry.register_command::<Step, _>(command_handler({
        let steps = Arc::clone(steps);
        move |step: Step| {
            let steps = Arc::clone(&steps);
            async move {
                steps.fetch_add(step.0, Ordering::SeqCst);
                Ok(())
            }
        }
    }))?;
    Ok(Mediator::new(registry))
}

#[tokio::test]
async fn behaviors_wrap_the_handler_in_exact_registration_order() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let pipeline = Pipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(TraceBehavior::new(&log, "b"))
        .with(TraceBehavior::new(&log, "c"));
    assert_eq!(pipeline.len(), 3);
    assert!(!pipeline.is_empty());

    assert_eq!(mediator.send_with(Order(1), &pipeline).await?, 2);

    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        [
            "before:a", "before:b", "before:c", "handler", "after:c", "after:b", "after:a"
        ]
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_pipeline_dispatches_identically_on_every_reuse() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let pipeline = Pipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(TraceBehavior::new(&log, "b"));

    assert_eq!(mediator.send_with(Order(1), &pipeline).await?, 2);
    assert_eq!(mediator.send_with(Order(2), &pipeline).await?, 4);

    let expected = ["before:a", "before:b", "handler", "after:b", "after:a"];
    let observed = log.lock().expect("pipeline trace mutex poisoned");
    assert_eq!(observed.len(), expected.len() * 2);
    assert_eq!(&observed[..expected.len()], expected.as_slice());
    assert_eq!(
        &observed[expected.len()..],
        expected.as_slice(),
        "the materialized behavior chain is shared, not rebuilt, between dispatches"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn a_short_circuiting_behavior_prevents_inner_behaviors_and_the_handler() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let pipeline = Pipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(ShortCircuit {
            log: Arc::clone(&log),
            name: "b",
            response: 99,
        })
        .with(TraceBehavior::new(&log, "c"));

    let response = mediator.send_with(Order(1), &pipeline).await?;

    assert_eq!(
        response, 99,
        "the short-circuit response flows back through the outer behavior"
    );
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        ["before:a", "short:b", "after:a"],
        "behavior c and the handler never run"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn behavior_errors_propagate_unchanged_and_skip_inner_stages() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let pipeline = Pipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(Failing {
            log: Arc::clone(&log),
            name: "b",
            code: ErrorCode::PipelineFailed,
            message: "policy rejected",
        })
        .with(TraceBehavior::new(&log, "c"));

    let error = mediator
        .send_with(Order(1), &pipeline)
        .await
        .expect_err("the behavior failure must reach the caller");

    assert_eq!(error.code(), ErrorCode::PipelineFailed);
    assert_eq!(error.message(), "policy rejected");
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        ["before:a", "fail:b", "after:a"],
        "outer behaviors still resume; inner stages and the handler never run"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn a_panicking_behavior_becomes_a_structured_internal_error() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let pipeline = Pipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(Panic);

    let error = mediator
        .send_with(Order(1), &pipeline)
        .await
        .expect_err("a behavior panic must be converted, not propagated");

    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);

    let steps = Arc::new(AtomicUsize::new(0));
    let mediator = counting_mediator(&steps)?;
    let command_pipeline = CommandPipeline::new().with(Panic);

    let error = mediator
        .send_command_with(Step(1), &command_pipeline)
        .await
        .expect_err("a command behavior panic must be converted, not propagated");

    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(steps.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn messages_and_responses_flow_through_the_chain_by_value() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let pipeline = Pipeline::new().with(AddToMessage(1)).with(AddToResponse(3));

    // (2 + 1) doubled by the handler, then + 3 on the way out.
    assert_eq!(mediator.send_with(Order(2), &pipeline).await?, 9);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_behavior_may_run_the_remaining_chain_more_than_once() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let pipeline = Pipeline::new()
        .with(RunTwice)
        .with(TraceBehavior::new(&log, "inner"));

    // RunTwice dispatches Order(1) and Order(11); the handler doubles each.
    assert_eq!(mediator.send_with(Order(1), &pipeline).await?, 24);
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        [
            "before:inner",
            "handler",
            "after:inner",
            "before:inner",
            "handler",
            "after:inner"
        ],
        "each next.run drives the full remaining chain again"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn construction_time_depth_bounds_reject_the_first_behavior_beyond_the_limit()
-> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;

    let mut pipeline = Pipeline::new();
    for _ in 0..MAX_PIPELINE_DEPTH {
        pipeline = pipeline
            .try_with(Pass)
            .expect("behaviors up to the limit are accepted");
    }
    assert_eq!(pipeline.len(), MAX_PIPELINE_DEPTH);
    // A pipeline at the exact limit still dispatches.
    assert_eq!(mediator.send_with(Order(1), &pipeline).await?, 2);
    let error = pipeline
        .try_with(Pass)
        .err()
        .expect("one behavior beyond the limit must be rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    let mut shared_pipeline = Pipeline::new();
    for _ in 0..MAX_PIPELINE_DEPTH {
        shared_pipeline = shared_pipeline
            .try_with_shared(Arc::new(Pass))
            .expect("shared behaviors up to the limit are accepted");
    }
    let error = shared_pipeline
        .try_with_shared(Arc::new(Pass))
        .err()
        .expect("one shared behavior beyond the limit must be rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    let mut command_pipeline = CommandPipeline::new();
    for _ in 0..MAX_PIPELINE_DEPTH {
        command_pipeline = command_pipeline
            .try_with(Pass)
            .expect("command behaviors up to the limit are accepted");
    }
    let error = command_pipeline
        .try_with(Pass)
        .err()
        .expect("one command behavior beyond the limit must be rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[tokio::test]
async fn empty_pipelines_dispatch_directly_to_the_handler() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let steps = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_command::<Step, _>(command_handler({
        let steps = Arc::clone(&steps);
        move |step: Step| {
            let steps = Arc::clone(&steps);
            async move {
                steps.fetch_add(step.0, Ordering::SeqCst);
                Ok(())
            }
        }
    }))?;
    let command_mediator = Mediator::new(registry);

    let pipeline = Pipeline::<Order>::new();
    assert!(pipeline.is_empty());
    assert_eq!(pipeline.len(), 0);
    assert_eq!(mediator.send_with(Order(5), &pipeline).await?, 10);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        log.lock().expect("pipeline trace mutex poisoned").len(),
        1,
        "only the handler marker is recorded"
    );

    let command_pipeline = CommandPipeline::<Step>::new();
    assert!(command_pipeline.is_empty());
    command_mediator
        .send_command_with(Step(2), &command_pipeline)
        .await?;
    assert_eq!(steps.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn a_pipeline_terminal_reports_not_found_when_no_handler_is_registered() -> CatgaResult<()> {
    let log = Trace::default();
    let mediator = Mediator::new(Registry::new());
    let pipeline = Pipeline::new().with(TraceBehavior::new(&log, "outer"));

    let error = mediator
        .send_with(Order(1), &pipeline)
        .await
        .expect_err("the missing handler must fail at the pipeline terminal");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        ["before:outer", "after:outer"],
        "unlike plain send, pipelined dispatch runs behaviors before the routing miss surfaces"
    );

    let command_pipeline = CommandPipeline::new().with(TraceBehavior::new(&log, "command"));
    let error = mediator
        .send_command_with(Step(1), &command_pipeline)
        .await
        .expect_err("the missing command handler must fail at the pipeline terminal");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        [
            "before:outer",
            "after:outer",
            "before:command",
            "after:command"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn shared_and_owned_behaviors_compose_in_registration_order() -> CatgaResult<()> {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = doubling_mediator(&handler_calls, &log)?;
    let shared: Arc<dyn Behavior<Order>> = Arc::new(TraceBehavior::new(&log, "shared"));
    let pipeline = Pipeline::new()
        .with(TraceBehavior::new(&log, "first"))
        .with_shared(Arc::clone(&shared))
        .with(TraceBehavior::new(&log, "last"));

    assert_eq!(mediator.send_with(Order(3), &pipeline).await?, 6);
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        [
            "before:first",
            "before:shared",
            "before:last",
            "handler",
            "after:last",
            "after:shared",
            "after:first"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn command_behaviors_wrap_the_handler_and_can_short_circuit() -> CatgaResult<()> {
    let steps = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = counting_mediator(&steps)?;
    let pipeline = CommandPipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(TraceBehavior::new(&log, "b"));
    mediator.send_command_with(Step(2), &pipeline).await?;
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        ["before:a", "before:b", "after:b", "after:a"]
    );
    assert_eq!(steps.load(Ordering::SeqCst), 2);

    log.lock().expect("pipeline trace mutex poisoned").clear();
    let pipeline = CommandPipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(ShortCircuit {
            log: Arc::clone(&log),
            name: "b",
            response: 0,
        })
        .with(TraceBehavior::new(&log, "c"));
    mediator.send_command_with(Step(5), &pipeline).await?;
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        ["before:a", "short:b", "after:a"],
        "the short-circuited command never reaches behavior c or the handler"
    );
    assert_eq!(steps.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn command_behavior_errors_propagate_unchanged_and_skip_the_handler() -> CatgaResult<()> {
    let steps = Arc::new(AtomicUsize::new(0));
    let log = Trace::default();
    let mediator = counting_mediator(&steps)?;
    let pipeline = CommandPipeline::new()
        .with(TraceBehavior::new(&log, "a"))
        .with(Failing {
            log: Arc::clone(&log),
            name: "b",
            code: ErrorCode::Validation,
            message: "step rejected",
        });

    let error = mediator
        .send_command_with(Step(7), &pipeline)
        .await
        .expect_err("the command behavior failure must reach the caller");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "step rejected");
    assert_eq!(
        *log.lock().expect("pipeline trace mutex poisoned"),
        ["before:a", "fail:b", "after:a"]
    );
    assert_eq!(
        steps.load(Ordering::SeqCst),
        0,
        "the command handler never runs"
    );
    Ok(())
}
