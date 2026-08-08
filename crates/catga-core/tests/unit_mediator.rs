//! Unit tests for mediator and message dispatch.

use async_trait::async_trait;
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use catga_core::{
    CatgaError, CatgaResult, Command, CommandBehavior, CommandHandler, CommandNext,
    CommandPipeline, DefaultMessageTypeId, ErrorCode, Event, EventHandler, Handler, MAX_MEDIATOR_BATCH_SIZE,
    MAX_PIPELINE_DEPTH, Mediator, MediatorHandle, Message, Registry, Request,
};

#[test]
fn max_mediator_batch_size_value() {
    assert_eq!(MAX_MEDIATOR_BATCH_SIZE, 1024);
}

#[derive(Clone)]
struct TestRequest;

impl Message for TestRequest {}

impl Request for TestRequest {
    type Response = String;
    type TypeId = DefaultMessageTypeId;
}

#[derive(Clone)]
struct TestCommand;

impl Message for TestCommand {}

impl Command for TestCommand {
    type TypeId = DefaultMessageTypeId;
}

#[derive(Clone)]
struct TestEvent;

impl Message for TestEvent {}

impl Event for TestEvent {
    type TypeId = DefaultMessageTypeId;
}

struct EchoHandler {
    prefix: String,
}

#[async_trait]
impl Handler<TestRequest> for EchoHandler {
    async fn handle(&self, _: TestRequest) -> CatgaResult<String> {
        Ok(format!("{}echo", self.prefix))
    }
}

struct CommandCounter {
    count: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl CommandHandler<TestCommand> for CommandCounter {
    async fn handle(&self, _: TestCommand) -> CatgaResult<()> {
        self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

struct EventRecorder {
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EventHandler<TestEvent> for EventRecorder {
    async fn handle(&self, _event: TestEvent) -> CatgaResult<()> {
        self.events
            .lock()
            .expect("lock should succeed")
            .push("recorded".to_string());
        Ok(())
    }
}

#[test]
fn mediator_handle_default_not_bound() {
    let handle = MediatorHandle::new();
    assert!(!handle.is_bound());
}

#[test]
fn mediator_handle_new_creates_unbound_handle() {
    let handle = MediatorHandle::new();
    assert!(!handle.is_bound());
}

#[tokio::test]
async fn mediator_handle_bind_success() {
    let mut registry = Registry::new();
    registry
        .register_request::<TestRequest, _>(EchoHandler {
            prefix: String::new(),
        })
        .expect("registration should succeed");
    let mediator = Mediator::new(registry);

    let handle = MediatorHandle::new();
    let result = handle.bind(Arc::new(mediator));

    assert!(result.is_ok());
    assert!(handle.is_bound());
}

#[tokio::test]
async fn mediator_handle_bind_twice_fails() {
    let mut registry = Registry::new();
    registry
        .register_request::<TestRequest, _>(EchoHandler {
            prefix: String::new(),
        })
        .expect("registration should succeed");
    let mediator = Mediator::new(registry);

    let handle = MediatorHandle::new();
    handle
        .bind(Arc::new(mediator))
        .expect("bind should succeed");

    let another_mediator = Mediator::new(Registry::new());
    let result = handle.bind(Arc::new(another_mediator));

    assert!(result.is_err());
    assert_eq!(
        result.expect_err("conflict expected").code(),
        ErrorCode::Conflict
    );
}

#[tokio::test]
async fn send_batch_rejects_zero_concurrency() {
    let mut registry = Registry::new();
    registry
        .register_request::<TestRequest, _>(EchoHandler {
            prefix: String::new(),
        })
        .expect("registration should succeed");
    let mediator = Mediator::new(registry);

    let result = mediator.send_batch([TestRequest], 0).await;
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn send_batch_rejects_oversized_batch() {
    let mut registry = Registry::new();
    registry
        .register_request::<TestRequest, _>(EchoHandler {
            prefix: String::new(),
        })
        .expect("registration should succeed");
    let mediator = Mediator::new(registry);

    let oversized: Vec<TestRequest> = (0..=MAX_MEDIATOR_BATCH_SIZE)
        .map(|_| TestRequest)
        .collect();

    let result = mediator.send_batch(oversized, 1).await;
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn send_batch_respects_concurrency_limit() {
    let mut registry = Registry::new();
    registry
        .register_request::<TestRequest, _>(EchoHandler {
            prefix: String::new(),
        })
        .expect("registration should succeed");
    let mediator = Mediator::new(registry);

    let requests = [TestRequest, TestRequest, TestRequest];
    let result = mediator.send_batch(requests, 1).await;

    assert!(result.is_ok());
    let responses = result.expect("ok expected");
    assert_eq!(responses.len(), 3);
}

#[tokio::test]
async fn publish_batch_rejects_zero_concurrency() {
    let mut registry = Registry::new();
    registry.register_event::<TestEvent, _>(EventRecorder {
        events: Arc::new(Mutex::new(Vec::new())),
    });
    let mediator = Mediator::new(registry);

    let result = mediator.publish_batch([TestEvent], 0).await;
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn publish_batch_with_events() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);

    let mut registry = Registry::new();
    registry.register_event::<TestEvent, _>(EventRecorder {
        events: events_clone,
    });
    let mediator = Mediator::new(registry);

    let result = mediator.publish_batch([TestEvent, TestEvent], 1).await;
    assert!(result.is_ok());

    let recorded = events.lock().expect("lock should succeed");
    assert_eq!(recorded.len(), 2);
}

#[tokio::test]
async fn publish_with_concurrency_rejects_zero() {
    let mut registry = Registry::new();
    registry.register_event::<TestEvent, _>(EventRecorder {
        events: Arc::new(Mutex::new(Vec::new())),
    });
    let mediator = Mediator::new(registry);

    let result = mediator.publish_with_concurrency(TestEvent, 0).await;
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn publish_with_concurrency_with_no_handlers() {
    let registry = Registry::new();
    let mediator = Mediator::new(registry);

    let result = mediator.publish_with_concurrency(TestEvent, 1).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_stream_routes_requests() {
    let mut registry = Registry::new();
    registry
        .register_request::<TestRequest, _>(EchoHandler {
            prefix: String::new(),
        })
        .expect("registration should succeed");
    let mediator = Mediator::new(registry);

    let stream = futures::stream::iter([TestRequest, TestRequest]);
    let results: Vec<_> = mediator.send_stream(stream).collect().await;

    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.is_ok()));
}

#[tokio::test]
async fn command_pipeline_depth_limit() {
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut registry = Registry::new();
    registry
        .register_command::<TestCommand, _>(CommandCounter {
            count: counter.clone(),
        })
        .expect("registration should succeed");
    let mediator = Mediator::new(registry);

    let mut pipeline = CommandPipeline::new();
    struct PassCommand;
    #[async_trait]
    impl CommandBehavior<TestCommand> for PassCommand {
        async fn handle(
            &self,
            command: TestCommand,
            next: CommandNext<TestCommand>,
        ) -> CatgaResult<()> {
            next.run(command).await
        }
    }
    for _ in 0..=MAX_PIPELINE_DEPTH {
        pipeline = pipeline.with(PassCommand);
    }

    let result = mediator.send_command_with(TestCommand, &pipeline).await;
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn publish_returns_first_handler_error() {
    struct FailingHandler;
    #[async_trait]
    impl EventHandler<TestEvent> for FailingHandler {
        async fn handle(&self, _: TestEvent) -> CatgaResult<()> {
            Err(CatgaError::new(ErrorCode::HandlerFailed, "handler failed"))
        }
    }

    let mut registry = Registry::new();
    registry.register_event::<TestEvent, _>(FailingHandler);
    let mediator = Mediator::new(registry);

    let result = mediator.publish(TestEvent).await;
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("handler failed expected").code(),
        ErrorCode::HandlerFailed
    );
}

#[tokio::test]
async fn publish_with_concurrency_runs_handlers_concurrently() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    struct SlowHandler {
        delay: Duration,
        started: Arc<AtomicBool>,
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EventHandler<TestEvent> for SlowHandler {
        async fn handle(&self, _: TestEvent) -> CatgaResult<()> {
            self.started.store(true, Ordering::SeqCst);
            self.counter.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(())
        }
    }

    let started1 = Arc::new(AtomicBool::new(false));
    let started2 = Arc::new(AtomicBool::new(false));

    let handler1 = SlowHandler {
        delay: Duration::from_millis(50),
        started: started1,
        counter: Arc::new(AtomicUsize::new(0)),
    };
    let handler2 = SlowHandler {
        delay: Duration::from_millis(50),
        started: started2,
        counter: Arc::new(AtomicUsize::new(0)),
    };

    let mut registry = Registry::new();
    registry.register_event::<TestEvent, _>(handler1);
    registry.register_event::<TestEvent, _>(handler2);
    let mediator = Mediator::new(registry);

    let start = Instant::now();
    mediator
        .publish_with_concurrency(TestEvent, 2)
        .await
        .expect("publish should succeed");
    let elapsed = start.elapsed();

    assert!(elapsed < Duration::from_millis(80));
}
