//! Contract coverage for the built-in testing helpers: handler spies,
//! event-handler spies, message capture, and assertion helpers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use catga_core::ErrorCode;
use catga_core::{
    CatgaError, CatgaResult, Event, EventHandler, EventHandlerSpy, Handler, HandlerSpy, Message,
    MessageCapture, Request, assert_contains, assert_error_code, assert_failure, assert_success,
    assert_value,
};

#[derive(Clone, Debug, PartialEq)]
struct Ping {
    sequence: u64,
}

impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
}

#[derive(Clone, Debug, PartialEq)]
struct Pong {
    sequence: u64,
}

impl Message for Pong {}
impl Event for Pong {}

struct DoublingHandler;

#[async_trait]
impl Handler<Ping> for DoublingHandler {
    async fn handle(&self, message: Ping) -> CatgaResult<u64> {
        Ok(message.sequence * 2)
    }
}

struct CountingEventHandler {
    seen: Arc<AtomicU64>,
}

#[async_trait]
impl EventHandler<Pong> for CountingEventHandler {
    async fn handle(&self, event: Pong) -> CatgaResult<()> {
        self.seen.fetch_add(event.sequence, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn handler_spy_records_calls_around_a_real_handler() {
    let spy = HandlerSpy::new(DoublingHandler);
    assert_eq!(spy.call_count(), 0);
    assert!(spy.calls().is_empty());
    assert!(spy.last_call().is_none());

    assert_eq!(assert_success(spy.handle(Ping { sequence: 2 }).await), 4);
    assert_eq!(assert_success(spy.handle(Ping { sequence: 3 }).await), 6);

    assert_eq!(spy.call_count(), 2);
    assert_eq!(
        spy.calls(),
        vec![Ping { sequence: 2 }, Ping { sequence: 3 }]
    );
    assert_eq!(spy.last_call(), Some(Ping { sequence: 3 }));
}

#[tokio::test]
async fn handler_spy_actions_record_before_running_even_on_failure() {
    let spy: HandlerSpy<Ping, _> = HandlerSpy::with_action(|request: Ping| async move {
        if request.sequence == 0 {
            Err(CatgaError::new(ErrorCode::Validation, "zero ping rejected"))
        } else {
            Ok(request.sequence + 10)
        }
    });

    assert_eq!(assert_success(spy.handle(Ping { sequence: 1 }).await), 11);
    assert_error_code(
        spy.handle(Ping { sequence: 0 }).await,
        ErrorCode::Validation,
    );

    // Both calls were recorded, including the failed one.
    assert_eq!(spy.call_count(), 2);
    assert_eq!(spy.last_call(), Some(Ping { sequence: 0 }));
}

#[tokio::test]
async fn handler_spy_without_handler_reports_not_found() {
    let spy: HandlerSpy<Ping, _> = HandlerSpy::without_handler();
    let error = assert_failure(spy.handle(Ping { sequence: 1 }).await);
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert!(!error.message().is_empty());
    // The unhandled request is still recorded for assertions.
    assert_eq!(spy.call_count(), 1);
    assert_eq!(spy.calls(), vec![Ping { sequence: 1 }]);
}

#[tokio::test]
async fn event_handler_spy_modes_record_and_delegate() {
    // A bare spy records without side effects.
    let bare: EventHandlerSpy<Pong> = EventHandlerSpy::new();
    let default_spy: EventHandlerSpy<Pong> = EventHandlerSpy::default();
    assert_success(bare.handle(Pong { sequence: 1 }).await);
    assert_success(default_spy.handle(Pong { sequence: 2 }).await);
    assert_eq!(bare.calls(), vec![Pong { sequence: 1 }]);
    assert_eq!(default_spy.call_count(), 1);

    // A wrapping spy delegates and preserves the handler outcome.
    let seen = Arc::new(AtomicU64::new(0));
    let wrapping = EventHandlerSpy::with_handler(CountingEventHandler { seen: seen.clone() });
    assert_success(wrapping.handle(Pong { sequence: 4 }).await);
    assert_success(wrapping.handle(Pong { sequence: 6 }).await);
    assert_eq!(seen.load(Ordering::SeqCst), 10);
    assert_eq!(
        wrapping.calls(),
        vec![Pong { sequence: 4 }, Pong { sequence: 6 }]
    );
    assert_eq!(wrapping.last_call(), Some(Pong { sequence: 6 }));

    // An action spy records before running, even for failing actions.
    let action = EventHandlerSpy::with_action(|event: Pong| async move {
        if event.sequence == 13 {
            Err(CatgaError::new(ErrorCode::HandlerFailed, "unlucky event"))
        } else {
            Ok(())
        }
    });
    assert_success(action.handle(Pong { sequence: 1 }).await);
    assert_error_code(
        action.handle(Pong { sequence: 13 }).await,
        ErrorCode::HandlerFailed,
    );
    assert_eq!(action.call_count(), 2);
    assert_eq!(action.last_call(), Some(Pong { sequence: 13 }));
}

#[test]
fn message_capture_records_published_and_consumed_values_in_order() {
    let capture: MessageCapture<u64> = MessageCapture::default();
    assert!(capture.published().is_empty());
    assert!(capture.consumed().is_empty());

    capture.record_published(1);
    capture.record_published(2);
    capture.record_consumed(1);
    capture.record_published(3);
    capture.record_consumed(2);

    assert_eq!(capture.published(), vec![1, 2, 3]);
    assert_eq!(capture.consumed(), vec![1, 2]);

    capture.clear();
    assert!(capture.published().is_empty());
    assert!(capture.consumed().is_empty());
}

#[test]
fn assertion_helpers_cover_success_and_failure_shapes() {
    assert_eq!(assert_success(Ok::<_, CatgaError>(7)), 7);
    assert_eq!(assert_value(Ok::<_, CatgaError>(9), 9), 9);

    let error = assert_failure(Err::<u64, _>(CatgaError::new(ErrorCode::Internal, "boom")));
    assert_eq!(error.code(), ErrorCode::Internal);

    let checked = assert_error_code(
        Err::<u64, _>(CatgaError::new(ErrorCode::Unavailable, "down")),
        ErrorCode::Unavailable,
    );
    assert_eq!(checked.code(), ErrorCode::Unavailable);

    let matches = assert_contains(vec![1, 2, 3, 4], |value| *value % 2 == 0);
    assert_eq!(matches, vec![2, 4]);
}

#[test]
fn assertion_helpers_panic_on_unexpected_shapes() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let failed = CatgaError::new(ErrorCode::Internal, "boom");
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            assert_success(Err::<u64, _>(failed.clone()))
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| assert_failure(Ok::<u64, CatgaError>(
            1
        ))))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| assert_value(
            Ok::<u64, CatgaError>(1),
            2
        )))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            assert_value(Err::<u64, _>(failed.clone()), 2)
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| assert_contains(
            vec![1_u64],
            |value| *value == 2
        )))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            assert_error_code(Ok::<u64, CatgaError>(1), ErrorCode::Internal)
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            assert_error_code(Err::<u64, _>(failed), ErrorCode::Validation)
        }))
        .is_err()
    );
}
