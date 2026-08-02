//! Strict public mediator contracts.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    Behavior, CatgaError, CatgaResult, Command, CommandBehavior, CommandHandler, CommandNext,
    CommandPipeline, ErrorCode, Event, EventHandler, Handler, MAX_MEDIATOR_BATCH_SIZE,
    MAX_PIPELINE_DEPTH, Mediator, MediatorHandle, Message, Next, Pipeline, Registry, Request,
    current_cancellation,
};
use futures::StreamExt;
use tokio::{sync::Notify, time::timeout};
use tokio_util::sync::CancellationToken;

mod __catga_types {
    pub struct PingTypeId;
    impl catga_core::MessageTypeId for PingTypeId {
        const NAME: &'static str = "Ping";
    }
    pub struct BlockingRequestTypeId;
    impl catga_core::MessageTypeId for BlockingRequestTypeId {
        const NAME: &'static str = "BlockingRequest";
    }
    pub struct AddTypeId;
    impl catga_core::MessageTypeId for AddTypeId {
        const NAME: &'static str = "Add";
    }
    pub struct NoticeTypeId;
    impl catga_core::MessageTypeId for NoticeTypeId {
        const NAME: &'static str = "Notice";
    }
}

#[derive(Clone)]
struct Ping(u8);

impl Message for Ping {}

impl Request for Ping {
    type Response = u8;
    type TypeId = __catga_types::PingTypeId;
}

#[derive(Clone)]
struct BlockingRequest;

impl Message for BlockingRequest {}

impl Request for BlockingRequest {
    type Response = ();
    type TypeId = __catga_types::BlockingRequestTypeId;
}

#[derive(Clone)]
struct Add(u8);

impl Message for Add {}
impl Command for Add {
    type TypeId = __catga_types::AddTypeId;
}

#[derive(Clone)]
struct Notice(u8);

impl Message for Notice {}
impl Event for Notice {
    type TypeId = __catga_types::NoticeTypeId;
}

struct Double;

#[async_trait]
impl Handler<Ping> for Double {
    async fn handle(&self, message: Ping) -> CatgaResult<u8> {
        Ok(message.0 * 2)
    }
}

struct AddTo(Arc<AtomicUsize>);

#[async_trait]
impl CommandHandler<Add> for AddTo {
    async fn handle(&self, command: Add) -> CatgaResult<()> {
        self.0.fetch_add(usize::from(command.0), Ordering::SeqCst);
        Ok(())
    }
}

struct RecordNotice {
    values: Arc<Mutex<Vec<u8>>>,
    fail_on: Option<u8>,
}

#[async_trait]
impl EventHandler<Notice> for RecordNotice {
    async fn handle(&self, event: Notice) -> CatgaResult<()> {
        self.values
            .lock()
            .expect("event record mutex poisoned")
            .push(event.0);
        match self.fail_on {
            Some(value) if value == event.0 => {
                Err(CatgaError::new(ErrorCode::HandlerFailed, "event rejected"))
            }
            _ => Ok(()),
        }
    }
}

fn mediator_with_handlers(
    commands: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<u8>>>,
) -> CatgaResult<Mediator> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(Double)?;
    registry.register_command::<Add, _>(AddTo(commands))?;
    registry.register_event::<Notice, _>(RecordNotice {
        values: events,
        fail_on: None,
    });
    Ok(Mediator::new(registry))
}

#[derive(Clone)]
struct RequestTrace {
    trace: Arc<Mutex<Vec<String>>>,
    name: &'static str,
}

#[async_trait]
impl Behavior<Ping> for RequestTrace {
    async fn handle(&self, message: Ping, next: Next<Ping>) -> CatgaResult<u8> {
        self.trace
            .lock()
            .expect("request trace mutex poisoned")
            .push(format!("before:{}", self.name));
        let response = next.run(message).await?;
        self.trace
            .lock()
            .expect("request trace mutex poisoned")
            .push(format!("after:{}", self.name));
        Ok(response)
    }
}

#[derive(Clone)]
struct CommandTrace {
    trace: Arc<Mutex<Vec<String>>>,
    name: &'static str,
}

#[async_trait]
impl CommandBehavior<Add> for CommandTrace {
    async fn handle(&self, command: Add, next: CommandNext<Add>) -> CatgaResult<()> {
        self.trace
            .lock()
            .expect("command trace mutex poisoned")
            .push(format!("before:{}", self.name));
        next.run(command).await?;
        self.trace
            .lock()
            .expect("command trace mutex poisoned")
            .push(format!("after:{}", self.name));
        Ok(())
    }
}

struct PassRequest;

#[async_trait]
impl Behavior<Ping> for PassRequest {
    async fn handle(&self, message: Ping, next: Next<Ping>) -> CatgaResult<u8> {
        next.run(message).await
    }
}

struct PassCommand;

#[async_trait]
impl CommandBehavior<Add> for PassCommand {
    async fn handle(&self, command: Add, next: CommandNext<Add>) -> CatgaResult<()> {
        next.run(command).await
    }
}

struct PanicRequest;

#[async_trait]
impl Handler<Ping> for PanicRequest {
    async fn handle(&self, _: Ping) -> CatgaResult<u8> {
        panic!("request panic");
    }
}

struct PanicCommand;

#[async_trait]
impl CommandHandler<Add> for PanicCommand {
    async fn handle(&self, _: Add) -> CatgaResult<()> {
        panic!("command panic");
    }
}

struct PanicEvent;

#[async_trait]
impl EventHandler<Notice> for PanicEvent {
    async fn handle(&self, _: Notice) -> CatgaResult<()> {
        panic!("event panic");
    }
}

struct CancellationAwareRequestHandler {
    started: Arc<Notify>,
    observed_cancellation_scope: Arc<AtomicUsize>,
}

#[async_trait]
impl Handler<BlockingRequest> for CancellationAwareRequestHandler {
    async fn handle(&self, _: BlockingRequest) -> CatgaResult<()> {
        if current_cancellation().is_some() {
            self.observed_cancellation_scope
                .fetch_add(1, Ordering::AcqRel);
        }
        self.started.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[tokio::test]
async fn unbound_handle_rejects_every_dispatch_entrypoint() {
    let handle = MediatorHandle::new();
    let cancellation = CancellationToken::new();

    assert!(!handle.is_bound());
    assert!(
        matches!(handle.send(Ping(1)).await, Err(error) if error.code() == ErrorCode::Unavailable)
    );
    assert!(
        matches!(handle.send_with_cancellation(Ping(1), cancellation.clone()).await, Err(error) if error.code() == ErrorCode::Unavailable)
    );
    assert!(
        matches!(handle.send_command(Add(1)).await, Err(error) if error.code() == ErrorCode::Unavailable)
    );
    assert!(
        matches!(handle.send_command_with_cancellation(Add(1), cancellation.clone()).await, Err(error) if error.code() == ErrorCode::Unavailable)
    );
    assert!(
        matches!(handle.publish(Notice(1)).await, Err(error) if error.code() == ErrorCode::Unavailable)
    );
    assert!(
        matches!(handle.publish_with_cancellation(Notice(1), cancellation).await, Err(error) if error.code() == ErrorCode::Unavailable)
    );
}

#[tokio::test]
async fn bound_handle_and_mediator_route_requests_commands_and_events() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mediator = Arc::new(mediator_with_handlers(
        Arc::clone(&commands),
        Arc::clone(&events),
    )?);
    let handle = MediatorHandle::new();

    handle.bind(Arc::clone(&mediator))?;
    assert!(handle.is_bound());
    assert!(matches!(handle.bind(mediator), Err(error) if error.code() == ErrorCode::Conflict));
    assert_eq!(handle.send(Ping(21)).await?, 42);
    handle.send_command(Add(3)).await?;
    handle.publish(Notice(7)).await?;

    assert_eq!(commands.load(Ordering::SeqCst), 3);
    assert_eq!(*events.lock().expect("event record mutex poisoned"), [7]);
    Ok(())
}

#[tokio::test]
async fn request_and_command_pipelines_wrap_the_registered_handlers() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mediator = mediator_with_handlers(Arc::clone(&commands), events)?;
    let request_trace = Arc::new(Mutex::new(Vec::new()));
    let command_trace = Arc::new(Mutex::new(Vec::new()));
    let request_pipeline = Pipeline::new()
        .with(RequestTrace {
            trace: Arc::clone(&request_trace),
            name: "one",
        })
        .with(RequestTrace {
            trace: Arc::clone(&request_trace),
            name: "two",
        });
    let command_pipeline = CommandPipeline::new()
        .with(CommandTrace {
            trace: Arc::clone(&command_trace),
            name: "one",
        })
        .with(CommandTrace {
            trace: Arc::clone(&command_trace),
            name: "two",
        });

    assert_eq!(mediator.send_with(Ping(4), &request_pipeline).await?, 8);
    mediator
        .send_command_with(Add(5), &command_pipeline)
        .await?;

    assert_eq!(
        *request_trace.lock().expect("request trace mutex poisoned"),
        ["before:one", "before:two", "after:two", "after:one"]
    );
    assert_eq!(
        *command_trace.lock().expect("command trace mutex poisoned"),
        ["before:one", "before:two", "after:two", "after:one"]
    );
    assert_eq!(commands.load(Ordering::SeqCst), 5);
    Ok(())
}

#[tokio::test]
async fn cancellation_prevents_dispatch_in_plain_and_pipeline_paths() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mediator = mediator_with_handlers(Arc::clone(&commands), Arc::clone(&events))?;
    let request_pipeline = Pipeline::new().with(PassRequest);
    let command_pipeline = CommandPipeline::new().with(PassCommand);
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    assert!(
        matches!(mediator.send_with_cancellation(Ping(1), cancellation.clone()).await, Err(error) if error.code() == ErrorCode::Cancelled)
    );
    assert!(
        matches!(mediator.send_with_cancellation_and_pipeline(Ping(1), &request_pipeline, cancellation.clone()).await, Err(error) if error.code() == ErrorCode::Cancelled)
    );
    assert!(
        matches!(mediator.send_command_with_cancellation(Add(1), cancellation.clone()).await, Err(error) if error.code() == ErrorCode::Cancelled)
    );
    assert!(
        matches!(mediator.send_command_with_cancellation_and_pipeline(Add(1), &command_pipeline, cancellation.clone()).await, Err(error) if error.code() == ErrorCode::Cancelled)
    );
    assert!(
        matches!(mediator.publish_with_cancellation(Notice(1), cancellation).await, Err(error) if error.code() == ErrorCode::Cancelled)
    );
    assert_eq!(commands.load(Ordering::SeqCst), 0);
    assert!(
        events
            .lock()
            .expect("event record mutex poisoned")
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn in_flight_request_cancellation_drops_the_handler_and_keeps_the_mediator_usable()
-> CatgaResult<()> {
    let started = Arc::new(Notify::new());
    let observed_cancellation_scope = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_request::<BlockingRequest, _>(CancellationAwareRequestHandler {
        started: Arc::clone(&started),
        observed_cancellation_scope: Arc::clone(&observed_cancellation_scope),
    })?;
    registry.register_request::<Ping, _>(Double)?;
    let mediator = Arc::new(Mediator::new(registry));
    let cancellation = CancellationToken::new();
    let waiting_for_handler = started.notified();
    let dispatch_mediator = Arc::clone(&mediator);
    let dispatch_cancellation = cancellation.clone();
    let dispatch = tokio::spawn(async move {
        dispatch_mediator
            .send_with_cancellation(BlockingRequest, dispatch_cancellation)
            .await
    });

    timeout(std::time::Duration::from_secs(1), waiting_for_handler)
        .await
        .expect("handler starts before cancellation");
    cancellation.cancel();
    assert!(matches!(
        timeout(std::time::Duration::from_secs(1), dispatch)
            .await
            .expect("cancelled dispatch completes")
            .expect("dispatch task does not panic"),
        Err(error) if error.code() == ErrorCode::Cancelled
    ));
    assert_eq!(observed_cancellation_scope.load(Ordering::Acquire), 1);
    assert_eq!(mediator.send(Ping(6)).await?, 12);
    Ok(())
}

#[tokio::test]
async fn oversized_request_and_command_pipelines_are_rejected_before_dispatch() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mediator = mediator_with_handlers(Arc::clone(&commands), events)?;
    let mut request_pipeline = Pipeline::new();
    let mut command_pipeline = CommandPipeline::new();
    for _ in 0..=MAX_PIPELINE_DEPTH {
        request_pipeline = request_pipeline.with(PassRequest);
        command_pipeline = command_pipeline.with(PassCommand);
    }

    assert!(
        matches!(mediator.send_with(Ping(1), &request_pipeline).await, Err(error) if error.code() == ErrorCode::Validation)
    );
    assert!(
        matches!(mediator.send_command_with(Add(1), &command_pipeline).await, Err(error) if error.code() == ErrorCode::Validation)
    );
    assert_eq!(commands.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn send_batch_validates_limits_and_preserves_response_order() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mediator = mediator_with_handlers(commands, events)?;

    assert!(
        matches!(mediator.send_batch([Ping(1)], 0).await, Err(error) if error.code() == ErrorCode::Validation)
    );
    let responses = mediator.send_batch([Ping(3), Ping(1), Ping(2)], 2).await?;
    assert_eq!(responses, [Ok(6), Ok(2), Ok(4)]);
    assert!(
        matches!(mediator.send_batch((0..=MAX_MEDIATOR_BATCH_SIZE as u16).map(|value| Ping(value as u8)), 1).await, Err(error) if error.code() == ErrorCode::Validation)
    );
    Ok(())
}

#[tokio::test]
async fn send_stream_lazily_routes_each_request() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mediator = mediator_with_handlers(commands, events)?;
    let responses = mediator
        .send_stream(futures::stream::iter([Ping(2), Ping(4), Ping(6)]))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(responses, [Ok(4), Ok(8), Ok(12)]);
    Ok(())
}

#[tokio::test]
async fn event_batches_and_concurrent_fanout_validate_limits_and_complete_delivery()
-> CatgaResult<()> {
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry.register_event::<Notice, _>(RecordNotice {
        values: Arc::clone(&records),
        fail_on: Some(2),
    });
    registry.register_event::<Notice, _>(RecordNotice {
        values: Arc::clone(&records),
        fail_on: None,
    });
    let mediator = Mediator::new(registry);

    assert!(
        matches!(mediator.publish_batch([Notice(1)], 0).await, Err(error) if error.code() == ErrorCode::Validation)
    );
    assert!(
        matches!(mediator.publish_with_concurrency(Notice(1), 0).await, Err(error) if error.code() == ErrorCode::Validation)
    );
    assert!(
        matches!(mediator.publish_batch([Notice(1), Notice(2), Notice(3)], 2).await, Err(error) if error.code() == ErrorCode::HandlerFailed)
    );
    assert_eq!(
        records.lock().expect("event record mutex poisoned").len(),
        6
    );
    records.lock().expect("event record mutex poisoned").clear();

    mediator.publish_with_concurrency(Notice(4), 2).await?;
    let mut delivered = records.lock().expect("event record mutex poisoned").clone();
    delivered.sort_unstable();
    assert_eq!(delivered, [4, 4]);
    Ok(())
}

#[tokio::test]
async fn handler_panics_become_structured_internal_errors() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(PanicRequest)?;
    registry.register_command::<Add, _>(PanicCommand)?;
    registry.register_event::<Notice, _>(PanicEvent);
    let mediator = Mediator::new(registry);

    assert!(
        matches!(mediator.send(Ping(1)).await, Err(error) if error.code() == ErrorCode::Internal)
    );
    assert!(
        matches!(mediator.send_command(Add(1)).await, Err(error) if error.code() == ErrorCode::Internal)
    );
    assert!(
        matches!(mediator.publish(Notice(1)).await, Err(error) if error.code() == ErrorCode::Internal)
    );
    Ok(())
}
