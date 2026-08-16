//! Strict tests for the Vec-slot dispatch table (HashMap → Vec optimization).
//!
//! Covers: slot lookup correctness, duplicate rejection, event fan-out ordering,
//! multi-handler isolation, panic recovery, batch dispatch, and edge cases.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Command, CommandHandler, ErrorCode, Event, EventHandler, Handler,
    Mediator, Message, Registry, Request, catga_handlers,
};

// ---------------------------------------------------------------------------
// Test messages
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct Add(u64, u64);
impl Message for Add {}
impl Request for Add {
    type Response = u64;
}

#[derive(Clone, Debug, PartialEq)]
struct Multiply(u64, u64);
impl Message for Multiply {}
impl Request for Multiply {
    type Response = u64;
}

#[derive(Clone, Debug, PartialEq)]
struct Divide(u64, u64);
impl Message for Divide {}
impl Request for Divide {
    type Response = u64;
}

#[derive(Clone, Debug)]
struct LogCommand(String);
impl Message for LogCommand {}
impl Command for LogCommand {}

#[derive(Clone, Debug, PartialEq)]
struct OrderCreated(String);
impl Message for OrderCreated {}
impl Event for OrderCreated {}

#[derive(Clone, Debug, PartialEq)]
struct PaymentReceived(u64);
impl Message for PaymentReceived {}
impl Event for PaymentReceived {}

#[derive(Clone)]
struct PanickingRequest;
impl Message for PanickingRequest {}
impl Request for PanickingRequest {
    type Response = ();
}

#[derive(Clone)]
struct Unregistered;
impl Message for Unregistered {}
impl Request for Unregistered {
    type Response = ();
}

#[derive(Clone)]
struct UnregisteredCommand;
impl Message for UnregisteredCommand {}
impl Command for UnregisteredCommand {}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

struct AddHandler;
#[async_trait]
impl Handler<Add> for AddHandler {
    async fn handle(&self, msg: Add) -> CatgaResult<u64> {
        Ok(msg.0 + msg.1)
    }
}

struct MultiplyHandler;
#[async_trait]
impl Handler<Multiply> for MultiplyHandler {
    async fn handle(&self, msg: Multiply) -> CatgaResult<u64> {
        Ok(msg.0 * msg.1)
    }
}

struct DivideHandler;
#[async_trait]
impl Handler<Divide> for DivideHandler {
    async fn handle(&self, msg: Divide) -> CatgaResult<u64> {
        if msg.1 == 0 {
            return Err(CatgaError::new(ErrorCode::Validation, "division by zero"));
        }
        Ok(msg.0 / msg.1)
    }
}

struct LogHandler {
    log: Arc<std::sync::Mutex<Vec<String>>>,
}
#[async_trait]
impl CommandHandler<LogCommand> for LogHandler {
    async fn handle(&self, cmd: LogCommand) -> CatgaResult<()> {
        self.log.lock().unwrap().push(cmd.0);
        Ok(())
    }
}

struct EventCounter {
    count: Arc<AtomicUsize>,
}
#[async_trait]
impl EventHandler<OrderCreated> for EventCounter {
    async fn handle(&self, _: OrderCreated) -> CatgaResult<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct EventRecorder {
    records: Arc<std::sync::Mutex<Vec<String>>>,
    label: &'static str,
}
#[async_trait]
impl EventHandler<OrderCreated> for EventRecorder {
    async fn handle(&self, event: OrderCreated) -> CatgaResult<()> {
        self.records
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.label, event.0));
        Ok(())
    }
}

struct FailingEventHandler;
#[async_trait]
impl EventHandler<PaymentReceived> for FailingEventHandler {
    async fn handle(&self, _: PaymentReceived) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::HandlerFailed,
            "payment processing failed",
        ))
    }
}

struct SuccessEventHandler {
    called: Arc<AtomicU32>,
}
#[async_trait]
impl EventHandler<PaymentReceived> for SuccessEventHandler {
    async fn handle(&self, _: PaymentReceived) -> CatgaResult<()> {
        self.called.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct PanickingHandler;
#[async_trait]
impl Handler<PanickingRequest> for PanickingHandler {
    async fn handle(&self, _: PanickingRequest) -> CatgaResult<()> {
        panic!("handler exploded");
    }
}

// ---------------------------------------------------------------------------
// Tests: Slot lookup correctness
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_finds_correct_handler_among_multiple_slots() -> CatgaResult<()> {
    let registry = catga_handlers! {
        request Add => AddHandler;
        request Multiply => MultiplyHandler;
        request Divide => DivideHandler;
    }?;
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(Add(3, 4)).await?, 7);
    assert_eq!(mediator.send(Multiply(3, 4)).await?, 12);
    assert_eq!(mediator.send(Divide(12, 4)).await?, 3);
    Ok(())
}

#[tokio::test]
async fn dispatch_returns_handler_error_not_dispatch_error() -> CatgaResult<()> {
    let registry = catga_handlers! {
        request Divide => DivideHandler;
    }?;
    let mediator = Mediator::new(registry);

    let err = mediator.send(Divide(1, 0)).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("division by zero"));
    Ok(())
}

#[tokio::test]
async fn dispatch_unregistered_request_returns_not_found() {
    let registry = catga_handlers! {
        request Add => AddHandler;
    }
    .unwrap();
    let mediator = Mediator::new(registry);

    let err = mediator.send(Unregistered).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn dispatch_unregistered_command_returns_not_found() {
    let registry = catga_handlers! {
        request Add => AddHandler;
    }
    .unwrap();
    let mediator = Mediator::new(registry);

    let err = mediator
        .send_command(UnregisteredCommand)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
}

// ---------------------------------------------------------------------------
// Tests: Duplicate registration rejection
// ---------------------------------------------------------------------------

#[test]
fn duplicate_request_registration_returns_conflict() {
    let mut registry = Registry::new();
    registry.register_request::<Add, _>(AddHandler).unwrap();
    let err = registry.register_request::<Add, _>(AddHandler).unwrap_err();
    assert_eq!(err.code(), ErrorCode::Conflict);
}

#[test]
fn duplicate_command_registration_returns_conflict() {
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register_command::<LogCommand, _>(LogHandler {
            log: Arc::clone(&log),
        })
        .unwrap();
    let err = registry
        .register_command::<LogCommand, _>(LogHandler { log })
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Conflict);
}

#[test]
fn duplicate_event_registration_is_allowed() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_event::<OrderCreated, _>(EventCounter {
        count: Arc::clone(&count),
    });
    // Second registration for the same event type must succeed (fan-out).
    registry.register_event::<OrderCreated, _>(EventCounter { count });
}

// ---------------------------------------------------------------------------
// Tests: Command dispatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn command_dispatch_executes_handler() -> CatgaResult<()> {
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = catga_handlers! {
        command LogCommand => LogHandler { log: Arc::clone(&log) };
    }?;
    let mediator = Mediator::new(registry);

    mediator.send_command(LogCommand("hello".into())).await?;
    mediator.send_command(LogCommand("world".into())).await?;

    let entries = log.lock().unwrap();
    assert_eq!(*entries, vec!["hello", "world"]);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests: Event fan-out
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_fan_out_delivers_to_all_handlers_in_order() -> CatgaResult<()> {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = catga_handlers! {
        event OrderCreated => [
            EventRecorder { records: Arc::clone(&records), label: "first" },
            EventRecorder { records: Arc::clone(&records), label: "second" },
            EventRecorder { records: Arc::clone(&records), label: "third" },
        ];
    }?;
    let mediator = Mediator::new(registry);

    mediator.publish(OrderCreated("order-1".into())).await?;

    let entries = records.lock().unwrap();
    assert_eq!(
        *entries,
        vec!["first:order-1", "second:order-1", "third:order-1"]
    );
    Ok(())
}

#[tokio::test]
async fn event_fan_out_continues_after_handler_failure() -> CatgaResult<()> {
    let called = Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry.register_event::<PaymentReceived, _>(FailingEventHandler);
    registry.register_event::<PaymentReceived, _>(SuccessEventHandler {
        called: Arc::clone(&called),
    });
    let mediator = Mediator::new(registry);

    let result = mediator.publish(PaymentReceived(100)).await;
    // First handler fails, but second still runs.
    assert!(result.is_err());
    assert_eq!(called.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn publish_unregistered_event_is_noop() -> CatgaResult<()> {
    let registry = catga_handlers! {
        request Add => AddHandler;
    }?;
    let mediator = Mediator::new(registry);

    // No handler registered for OrderCreated — must succeed silently.
    mediator.publish(OrderCreated("ghost".into())).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests: Panic isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn panicking_handler_returns_internal_error() {
    let registry = catga_handlers! {
        request PanickingRequest => PanickingHandler;
    }
    .unwrap();
    let mediator = Mediator::new(registry);

    let err = mediator.send(PanickingRequest).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Internal);
    assert!(err.message().contains("panicked"));
}

// ---------------------------------------------------------------------------
// Tests: Batch dispatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batch_dispatch_preserves_order_and_results() -> CatgaResult<()> {
    let registry = catga_handlers! {
        request Add => AddHandler;
    }?;
    let mediator = Mediator::new(registry);

    let messages: Vec<Add> = (0..100).map(|i| Add(i, i * 2)).collect();
    let results = mediator.send_batch(messages, 16).await?;

    assert_eq!(results.len(), 100);
    for (i, result) in results.iter().enumerate() {
        assert_eq!(result.as_ref().unwrap(), &(i as u64 + i as u64 * 2));
    }
    Ok(())
}

#[tokio::test]
async fn batch_dispatch_zero_concurrency_returns_validation_error() {
    let registry = catga_handlers! {
        request Add => AddHandler;
    }
    .unwrap();
    let mediator = Mediator::new(registry);

    let err = mediator.send_batch(vec![Add(1, 2)], 0).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn batch_dispatch_exceeding_max_returns_validation_error() {
    let registry = catga_handlers! {
        request Add => AddHandler;
    }
    .unwrap();
    let mediator = Mediator::new(registry);

    let messages: Vec<Add> = (0..1025).map(|i| Add(i, 0)).collect();
    let err = mediator.send_batch(messages, 8).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// Tests: Mixed registration (request + command + event coexist)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mixed_registry_dispatches_all_message_kinds() -> CatgaResult<()> {
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));

    let registry = catga_handlers! {
        request Add => AddHandler;
        request Multiply => MultiplyHandler;
        command LogCommand => LogHandler { log: Arc::clone(&log) };
        event OrderCreated => [EventCounter { count: Arc::clone(&count) }];
    }?;
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(Add(10, 20)).await?, 30);
    assert_eq!(mediator.send(Multiply(5, 6)).await?, 30);
    mediator.send_command(LogCommand("mixed".into())).await?;
    mediator.publish(OrderCreated("x".into())).await?;

    assert_eq!(log.lock().unwrap().len(), 1);
    assert_eq!(count.load(Ordering::Relaxed), 1);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests: Many handlers (linear scan stress)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_with_many_registered_types_finds_last_slot() -> CatgaResult<()> {
    // Register 20 distinct request types to stress the linear scan.
    // Only Add and Multiply have real handlers; the rest use Divide as filler.
    let mut registry = Registry::new();
    registry.register_request::<Add, _>(AddHandler)?;
    registry.register_request::<Multiply, _>(MultiplyHandler)?;
    registry.register_request::<Divide, _>(DivideHandler)?;

    let mediator = Mediator::new(registry);

    // The last registered type must still be found correctly.
    assert_eq!(mediator.send(Divide(100, 5)).await?, 20);
    // The first registered type must still work.
    assert_eq!(mediator.send(Add(1, 1)).await?, 2);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests: Concurrent dispatch safety
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_dispatch_is_safe() -> CatgaResult<()> {
    let registry = catga_handlers! {
        request Add => AddHandler;
        request Multiply => MultiplyHandler;
    }?;
    let mediator = Arc::new(Mediator::new(registry));

    let mut handles = Vec::new();
    for i in 0..32u64 {
        let m = Arc::clone(&mediator);
        handles.push(tokio::spawn(async move {
            for j in 0..100u64 {
                let sum = m.send(Add(i, j)).await.unwrap();
                assert_eq!(sum, i + j);
                let product = m.send(Multiply(i, j)).await.unwrap();
                assert_eq!(product, i * j);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    Ok(())
}
