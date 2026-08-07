//! Shared test-only helper utilities.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Event, EventHandler, Handler, Request};
use catga_core::testing::{
    EventHandlerSpy, HandlerSpy, MessageCapture, assert_contains, assert_error_code,
    assert_failure, assert_success, assert_value,
};

#[derive(Clone, Debug, PartialEq)]
struct Add(u32);
impl catga_core::Message for Add {}
impl Request for Add {
    type Response = u32;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Doubler;
#[async_trait]
impl Handler<Add> for Doubler {
    async fn handle(&self, request: Add) -> CatgaResult<u32> {
        Ok(request.0 * 2)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Recorded(u32);
impl catga_core::Message for Recorded {}
impl Event for Recorded {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct EventCounter(Arc<std::sync::atomic::AtomicU32>);

#[async_trait]
impl EventHandler<Recorded> for EventCounter {
    async fn handle(&self, event: Recorded) -> CatgaResult<()> {
        self.0
            .fetch_add(event.0, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn handler_spy_records_calls_without_changing_the_handler_result() {
    let spy = HandlerSpy::new(Doubler);
    assert_eq!(spy.handle(Add(4)).await.expect("handler succeeds"), 8);
    assert_eq!(spy.handle(Add(2)).await.expect("handler succeeds"), 4);
    assert_eq!(spy.call_count(), 2);
    assert_eq!(spy.calls(), vec![Add(4), Add(2)]);
    assert_eq!(spy.last_call(), Some(Add(2)));
}

#[tokio::test]
async fn handler_spy_executes_an_async_action_and_records_the_request() {
    let spy = HandlerSpy::<Add, _>::with_action(|request| async move { Ok(request.0 + 1) });

    assert_eq!(spy.handle(Add(8)).await.expect("action succeeds"), 9);
    assert_eq!(spy.calls(), vec![Add(8)]);
}

#[tokio::test]
async fn handler_spy_without_an_inner_handler_records_and_returns_not_found() {
    let spy = HandlerSpy::<Add, _>::without_handler();

    let error = spy
        .handle(Add(4))
        .await
        .expect_err("an unconfigured spy must fail explicitly");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(spy.calls(), vec![Add(4)]);
}

#[tokio::test]
async fn event_handler_spy_records_events_without_requiring_an_inner_handler() {
    let spy = EventHandlerSpy::new();

    spy.handle(Recorded(4)).await.expect("event spy succeeds");
    spy.handle(Recorded(2)).await.expect("event spy succeeds");

    assert_eq!(spy.call_count(), 2);
    assert_eq!(spy.calls(), vec![Recorded(4), Recorded(2)]);
    assert_eq!(spy.last_call(), Some(Recorded(2)));
}

#[tokio::test]
async fn event_handler_spy_delegates_after_recording() {
    let total = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let spy = EventHandlerSpy::with_handler(EventCounter(Arc::clone(&total)));

    spy.handle(Recorded(7))
        .await
        .expect("wrapped event handler succeeds");

    assert_eq!(spy.calls(), vec![Recorded(7)]);
    assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), 7);
}

#[tokio::test]
async fn event_handler_spy_executes_an_async_action_and_records_the_event() {
    let total = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let spy = EventHandlerSpy::<Recorded>::with_action({
        let total = Arc::clone(&total);
        move |event| {
            let total = Arc::clone(&total);
            async move {
                total.fetch_add(event.0, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        }
    });

    spy.handle(Recorded(5)).await.expect("action succeeds");
    assert_eq!(spy.calls(), vec![Recorded(5)]);
    assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), 5);
}

#[test]
fn message_capture_keeps_published_and_consumed_values_separate() {
    let capture = Arc::new(MessageCapture::default());
    capture.record_published("created");
    capture.record_consumed("processed");
    assert_eq!(capture.published(), vec!["created"]);
    assert_eq!(capture.consumed(), vec!["processed"]);
    capture.clear();
    assert!(capture.published().is_empty());
    assert!(capture.consumed().is_empty());
}

#[test]
fn message_capture_returns_each_stream_in_recording_order() {
    let capture = MessageCapture::default();
    for value in 0_u8..32 {
        capture.record_published(value);
        capture.record_consumed(value);
    }

    assert_eq!(capture.published(), (0_u8..32).collect::<Vec<_>>());
    assert_eq!(capture.consumed(), (0_u8..32).collect::<Vec<_>>());
}

#[test]
fn result_assertions_return_the_original_result_for_composition() {
    assert_eq!(assert_success(Ok(3_u32)), 3);
    let error = CatgaError::new(ErrorCode::Validation, "invalid");
    assert_eq!(
        assert_error_code::<()>(Err(error), ErrorCode::Validation).code(),
        ErrorCode::Validation
    );
}

#[test]
fn assertion_helpers_return_verified_values_and_errors() {
    assert_eq!(assert_value(Ok(3_u32), 3), 3);
    let error = assert_failure::<()>(Err(CatgaError::new(ErrorCode::Timeout, "late")));
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(
        assert_contains([1_u32, 2, 3], |value| *value % 2 == 1),
        vec![1, 3]
    );
}
