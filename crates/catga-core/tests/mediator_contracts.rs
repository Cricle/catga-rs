//! Strict mediator contracts: routing, single vs. multi-handler semantics, fan-out order,
//! batch failure isolation, and concurrent dispatch bookkeeping.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Command, ErrorCode, Event, EventHandler, Mediator, MediatorHandle,
    Message, MessageTypeId, Registry, Request, command_handler, event_handler, request_handler,
};

struct EchoTypeId;
impl MessageTypeId for EchoTypeId {
    const NAME: &'static str = "Echo";
}

struct Echo(u64);
impl Message for Echo {}
impl Request for Echo {
    type Response = u64;
    type TypeId = EchoTypeId;
}

struct BumpTypeId;
impl MessageTypeId for BumpTypeId {
    const NAME: &'static str = "Bump";
}

struct Bump(usize);
impl Message for Bump {}
impl Command for Bump {
    type TypeId = BumpTypeId;
}

struct TickTypeId;
impl MessageTypeId for TickTypeId {
    const NAME: &'static str = "Tick";
}

#[derive(Clone)]
struct Tick;
impl Message for Tick {}
impl Event for Tick {
    type TypeId = TickTypeId;
}

struct UnhandledRequestTypeId;
impl MessageTypeId for UnhandledRequestTypeId {
    const NAME: &'static str = "UnhandledRequest";
}

struct UnhandledRequest;
impl Message for UnhandledRequest {}
impl Request for UnhandledRequest {
    type Response = ();
    type TypeId = UnhandledRequestTypeId;
}

struct UnhandledCommandTypeId;
impl MessageTypeId for UnhandledCommandTypeId {
    const NAME: &'static str = "UnhandledCommand";
}

struct UnhandledCommand;
impl Message for UnhandledCommand {}
impl Command for UnhandledCommand {
    type TypeId = UnhandledCommandTypeId;
}

/// Records its name into the shared fan-out log, then optionally fails with a prepared error.
struct Recorder {
    log: Arc<Mutex<Vec<&'static str>>>,
    name: &'static str,
    failure: Option<(ErrorCode, &'static str)>,
}

#[async_trait]
impl EventHandler<Tick> for Recorder {
    async fn handle(&self, _: Tick) -> CatgaResult<()> {
        self.log
            .lock()
            .expect("fan-out log mutex poisoned")
            .push(self.name);
        match self.failure {
            Some((code, message)) => Err(CatgaError::new(code, message)),
            None => Ok(()),
        }
    }
}

fn doubling_mediator() -> CatgaResult<Mediator> {
    let mut registry = Registry::new();
    registry
        .register_request::<Echo, _>(request_handler(|echo: Echo| async move { Ok(echo.0 * 2) }))?;
    Ok(Mediator::new(registry))
}

#[tokio::test]
async fn a_request_routes_to_its_single_registered_handler() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry
        .register_request::<Echo, _>(request_handler(|echo: Echo| async move { Ok(echo.0 * 2) }))?;
    assert!(registry.get_handler::<Echo>());
    assert!(!registry.get_handler::<UnhandledRequest>());

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Echo(21)).await?, 42);
    Ok(())
}

#[tokio::test]
async fn missing_request_and_command_handlers_fail_with_not_found_without_panicking()
-> CatgaResult<()> {
    let mediator = doubling_mediator()?;

    let request_error = mediator
        .send(UnhandledRequest)
        .await
        .expect_err("a request without a handler must fail");
    assert_eq!(request_error.code(), ErrorCode::NotFound);
    assert!(request_error.message().contains("not registered"));

    let command_error = mediator
        .send_command(UnhandledCommand)
        .await
        .expect_err("a command without a handler must fail");
    assert_eq!(command_error.code(), ErrorCode::NotFound);
    assert!(command_error.message().contains("not registered"));

    // A routing miss does not poison the mediator; registered handlers keep working.
    assert_eq!(mediator.send(Echo(7)).await?, 14);
    Ok(())
}

#[tokio::test]
async fn publishing_without_registered_handlers_is_a_no_op() -> CatgaResult<()> {
    let mediator = Mediator::new(Registry::new());
    mediator.publish(Tick).await?;
    Ok(())
}

#[tokio::test]
async fn handler_errors_reach_the_caller_with_code_and_message_intact() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Echo, _>(request_handler(|_: Echo| async {
        Err(CatgaError::new(ErrorCode::Forbidden, "denied by policy"))
    }))?;
    registry.register_command::<Bump, _>(command_handler(|_: Bump| async {
        Err(CatgaError::new(ErrorCode::Transient, "try again later"))
    }))?;
    let mediator = Mediator::new(registry);

    let request_error = mediator
        .send(Echo(1))
        .await
        .expect_err("the request handler failure must surface");
    assert_eq!(request_error.code(), ErrorCode::Forbidden);
    assert_eq!(request_error.message(), "denied by policy");

    let command_error = mediator
        .send_command(Bump(1))
        .await
        .expect_err("the command handler failure must surface");
    assert_eq!(command_error.code(), ErrorCode::Transient);
    assert_eq!(command_error.message(), "try again later");
    Ok(())
}

#[tokio::test]
async fn duplicate_registration_conflicts_and_preserves_the_first_handler() -> CatgaResult<()> {
    let bumps = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Echo, _>(request_handler(|echo: Echo| async move { Ok(echo.0 * 2) }))?;
    registry.register_command::<Bump, _>(command_handler({
        let bumps = Arc::clone(&bumps);
        move |bump: Bump| {
            let bumps = Arc::clone(&bumps);
            async move {
                bumps.fetch_add(bump.0, Ordering::SeqCst);
                Ok(())
            }
        }
    }))?;

    let request_conflict = registry
        .register_request::<Echo, _>(request_handler(
            |echo: Echo| async move { Ok(echo.0 * 100) },
        ))
        .expect_err("a second request handler must conflict");
    assert_eq!(request_conflict.code(), ErrorCode::Conflict);

    let command_conflict = registry
        .register_command::<Bump, _>(command_handler(|_: Bump| async { Ok(()) }))
        .expect_err("a second command handler must conflict");
    assert_eq!(command_conflict.code(), ErrorCode::Conflict);

    let mediator = Mediator::new(registry);
    assert_eq!(
        mediator.send(Echo(2)).await?,
        4,
        "the rejected duplicate must not replace the first request handler"
    );
    mediator.send_command(Bump(1)).await?;
    assert_eq!(
        bumps.load(Ordering::SeqCst),
        1,
        "the rejected duplicate must not replace the first command handler"
    );
    Ok(())
}

#[tokio::test]
async fn event_fanout_invokes_every_handler_in_registration_order() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    for name in ["first", "second", "third"] {
        registry.register_event::<Tick, _>(Recorder {
            log: Arc::clone(&log),
            name,
            failure: None,
        });
    }
    let mediator = Mediator::new(registry);

    mediator.publish(Tick).await?;

    assert_eq!(
        *log.lock().expect("fan-out log mutex poisoned"),
        ["first", "second", "third"]
    );
    Ok(())
}

#[tokio::test]
async fn a_failing_event_handler_neither_stops_nor_reorders_the_fan_out() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "first",
        failure: None,
    });
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "middle",
        failure: Some((ErrorCode::Conflict, "middle rejected")),
    });
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "last",
        failure: None,
    });
    let mediator = Mediator::new(registry);

    let error = mediator
        .publish(Tick)
        .await
        .expect_err("the fan-out failure must be reported after delivery completes");

    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(error.message(), "middle rejected");
    assert_eq!(
        *log.lock().expect("fan-out log mutex poisoned"),
        ["first", "middle", "last"],
        "every handler still runs, in registration order, despite the failure"
    );
    Ok(())
}

#[tokio::test]
async fn sequential_fanout_returns_the_earliest_failure_in_registration_order() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "first",
        failure: Some((ErrorCode::HandlerFailed, "first failure")),
    });
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "second",
        failure: None,
    });
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "third",
        failure: Some((ErrorCode::Timeout, "third failure")),
    });
    let mediator = Mediator::new(registry);

    let error = mediator
        .publish(Tick)
        .await
        .expect_err("the first observed failure must be returned");

    assert_eq!(error.code(), ErrorCode::HandlerFailed);
    assert_eq!(error.message(), "first failure");
    assert_eq!(
        *log.lock().expect("fan-out log mutex poisoned"),
        ["first", "second", "third"]
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_fanout_runs_every_handler_despite_a_failure() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "first",
        failure: None,
    });
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "middle",
        failure: Some((ErrorCode::Unavailable, "shard offline")),
    });
    registry.register_event::<Tick, _>(Recorder {
        log: Arc::clone(&log),
        name: "last",
        failure: None,
    });
    let mediator = Mediator::new(registry);

    let error = mediator
        .publish_with_concurrency(Tick, 3)
        .await
        .expect_err("the single fan-out failure must be returned");

    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.message(), "shard offline");
    let mut observed = log.lock().expect("fan-out log mutex poisoned").clone();
    observed.sort_unstable();
    assert_eq!(
        observed,
        ["first", "last", "middle"],
        "concurrent fan-out still delivers to every handler"
    );
    Ok(())
}

#[tokio::test]
async fn a_concurrency_limit_of_one_serializes_fanout_in_registration_order() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    for name in ["first", "second", "third"] {
        registry.register_event::<Tick, _>(Recorder {
            log: Arc::clone(&log),
            name,
            failure: None,
        });
    }
    let mediator = Mediator::new(registry);

    mediator.publish_with_concurrency(Tick, 1).await?;

    assert_eq!(
        *log.lock().expect("fan-out log mutex poisoned"),
        ["first", "second", "third"]
    );
    Ok(())
}

#[tokio::test]
async fn send_batch_preserves_order_and_isolates_per_item_failures() -> CatgaResult<()> {
    let invocations = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_request::<Echo, _>(request_handler({
        let invocations = Arc::clone(&invocations);
        move |echo: Echo| {
            let invocations = Arc::clone(&invocations);
            async move {
                invocations.fetch_add(1, Ordering::SeqCst);
                if echo.0.is_multiple_of(2) {
                    Ok(echo.0 * 2)
                } else {
                    Err(CatgaError::new(
                        ErrorCode::HandlerFailed,
                        "odd values are rejected",
                    ))
                }
            }
        }
    }))?;
    let mediator = Mediator::new(registry);

    let responses = mediator
        .send_batch([Echo(1), Echo(2), Echo(3), Echo(4)], 2)
        .await?;
    let simplified: Vec<Result<u64, ErrorCode>> = responses
        .iter()
        .map(|response| response.as_ref().copied().map_err(CatgaError::code))
        .collect();

    assert_eq!(
        simplified,
        [
            Err(ErrorCode::HandlerFailed),
            Ok(4),
            Err(ErrorCode::HandlerFailed),
            Ok(8)
        ],
        "each item keeps its own outcome at its input position"
    );
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        4,
        "one failing item must not abort the rest of the batch"
    );
    Ok(())
}

#[tokio::test]
async fn send_batch_bounds_in_flight_dispatch_to_the_concurrency_limit() -> CatgaResult<()> {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_request::<Echo, _>(request_handler({
        let in_flight = Arc::clone(&in_flight);
        let max_in_flight = Arc::clone(&max_in_flight);
        move |echo: Echo| {
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            async move {
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(current, Ordering::SeqCst);
                tokio::task::yield_now().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(echo.0)
            }
        }
    }))?;
    let mediator = Mediator::new(registry);

    let responses = mediator.send_batch((0..10).map(Echo), 2).await?;

    assert!(responses.iter().all(|response| response.is_ok()));
    assert_eq!(
        max_in_flight.load(Ordering::SeqCst),
        2,
        "buffered dispatch reaches but never exceeds the configured limit"
    );
    assert_eq!(
        in_flight.load(Ordering::SeqCst),
        0,
        "no dispatch leaks past batch completion"
    );
    Ok(())
}

#[tokio::test]
async fn one_mediator_serves_many_concurrent_tasks_with_exact_bookkeeping() -> CatgaResult<()> {
    const TASKS: usize = 8;
    const ROUNDS_PER_TASK: usize = 25;

    let bumps = Arc::new(AtomicUsize::new(0));
    let ticks = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Echo, _>(request_handler(|echo: Echo| async move { Ok(echo.0 + 1) }))?;
    registry.register_command::<Bump, _>(command_handler({
        let bumps = Arc::clone(&bumps);
        move |bump: Bump| {
            let bumps = Arc::clone(&bumps);
            async move {
                bumps.fetch_add(bump.0, Ordering::SeqCst);
                Ok(())
            }
        }
    }))?;
    for _ in 0..2 {
        registry.register_event::<Tick, _>(event_handler({
            let ticks = Arc::clone(&ticks);
            move |_: Tick| {
                let ticks = Arc::clone(&ticks);
                async move {
                    ticks.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        }));
    }
    let mediator = Arc::new(Mediator::new(registry));

    let mut workers = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let mediator = Arc::clone(&mediator);
        workers.push(tokio::spawn(async move {
            for round in 0..ROUNDS_PER_TASK {
                mediator
                    .send_command(Bump(1))
                    .await
                    .expect("command dispatch succeeds");
                let expected = u64::try_from(round).expect("round index fits u64") + 1;
                assert_eq!(
                    mediator
                        .send(Echo(u64::try_from(round).expect("round index fits u64")))
                        .await
                        .expect("request dispatch succeeds"),
                    expected
                );
                mediator
                    .publish(Tick)
                    .await
                    .expect("event fan-out succeeds");
                tokio::task::yield_now().await;
            }
        }));
    }
    for worker in workers {
        worker.await.expect("worker task does not panic");
    }

    assert_eq!(bumps.load(Ordering::SeqCst), TASKS * ROUNDS_PER_TASK);
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        TASKS * ROUNDS_PER_TASK * 2,
        "both fan-out handlers observe every concurrent publish exactly once"
    );
    Ok(())
}

#[tokio::test]
async fn bound_handle_clones_dispatch_from_concurrent_tasks() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry
        .register_request::<Echo, _>(request_handler(|echo: Echo| async move { Ok(echo.0 * 3) }))?;
    let handle = MediatorHandle::new();
    handle.bind(Arc::new(Mediator::new(registry)))?;

    let mut workers = Vec::new();
    for index in 0..4u64 {
        let handle = handle.clone();
        workers.push(tokio::spawn(async move {
            assert!(handle.is_bound());
            assert_eq!(
                handle
                    .send(Echo(index))
                    .await
                    .expect("handle dispatch succeeds"),
                index * 3
            );
        }));
    }
    for worker in workers {
        worker.await.expect("worker task does not panic");
    }
    Ok(())
}
