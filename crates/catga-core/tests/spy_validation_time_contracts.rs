//! Contract coverage for test-support spies and captures, the validation
//! behavior pipeline stage, and the Unix-millisecond time helpers.

use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DefaultMessageTypeId, ErrorCode, Event, EventHandler, Handler,
    Mediator, Message, MessageCapture, Pipeline, Registry, Request, assert_error_code,
    assert_failure, assert_success,
    time::{
        UnixMillisError, checked_unix_millis, now_unix_millis, now_unix_millis_or,
        signed_unix_millis,
    },
    validation::{ValidationBehavior, Validator},
};

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct Ping(u8);

impl Message for Ping {}

impl Request for Ping {
    type Response = u8;
    type TypeId = DefaultMessageTypeId;
}

struct Double;

#[async_trait]
impl Handler<Ping> for Double {
    async fn handle(&self, message: Ping) -> CatgaResult<u8> {
        Ok(message.0 * 2)
    }
}

struct NoticeTypeId;
impl catga_core::MessageTypeId for NoticeTypeId {
    const NAME: &'static str = "Notice";
}

#[derive(Clone, Debug, PartialEq)]
struct Notice(u8);

impl Message for Notice {}

impl Event for Notice {
    type TypeId = NoticeTypeId;
}

struct RejectOddNotice;

#[async_trait]
impl EventHandler<Notice> for RejectOddNotice {
    async fn handle(&self, event: Notice) -> CatgaResult<()> {
        if event.0 % 2 == 1 {
            Err(CatgaError::new(
                ErrorCode::Validation,
                "odd notice rejected",
            ))
        } else {
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// MessageCapture
// ---------------------------------------------------------------------------

#[test]
fn message_capture_records_in_order_and_clears() {
    let capture = MessageCapture::<String>::default();
    capture.record_published("first".to_owned());
    capture.record_published("second".to_owned());
    capture.record_consumed("eaten".to_owned());

    assert_eq!(capture.published(), ["first", "second"]);
    assert_eq!(capture.consumed(), ["eaten"]);

    capture.clear();
    assert!(capture.published().is_empty());
    assert!(capture.consumed().is_empty());
}

// ---------------------------------------------------------------------------
// HandlerSpy and friends
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handler_spy_records_calls_and_delegates() {
    let spy = catga_core::testing::HandlerSpy::new(Double);
    assert_eq!(spy.call_count(), 0);
    assert!(spy.last_call().is_none());

    assert_eq!(assert_success(spy.handle(Ping(3)).await), 6);
    assert_eq!(assert_success(spy.handle(Ping(4)).await), 8);

    assert_eq!(spy.call_count(), 2);
    assert_eq!(spy.calls(), [Ping(3), Ping(4)]);
    assert_eq!(spy.last_call(), Some(Ping(4)));
}

#[tokio::test]
async fn spy_action_handler_records_even_failed_actions() {
    let spy = catga_core::testing::HandlerSpy::with_action(|request: Ping| async move {
        if request.0 == 0 {
            Err(CatgaError::new(ErrorCode::Validation, "zero ping"))
        } else {
            Ok(request.0 + 1)
        }
    });

    assert_eq!(assert_success(spy.handle(Ping(1)).await), 2);
    assert_error_code(spy.handle(Ping(0)).await, ErrorCode::Validation);
    // The failing request is still retained for assertions.
    assert_eq!(spy.call_count(), 2);
    assert_eq!(spy.last_call(), Some(Ping(0)));
}

#[tokio::test]
async fn missing_spy_handler_reports_not_found_but_records() {
    let spy = catga_core::testing::HandlerSpy::<Ping, _>::without_handler();
    let error = assert_failure(spy.handle(Ping(7)).await);
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(spy.calls(), [Ping(7)]);
}

#[tokio::test]
async fn event_handler_spies_record_and_delegate() {
    let plain = catga_core::testing::EventHandlerSpy::<Notice>::new();
    assert_eq!(
        catga_core::testing::EventHandlerSpy::<Notice>::default().call_count(),
        0
    );
    assert_success(plain.handle(Notice(2)).await);
    assert_eq!(plain.calls(), [Notice(2)]);

    let delegating = catga_core::testing::EventHandlerSpy::with_handler(RejectOddNotice);
    assert_success(delegating.handle(Notice(2)).await);
    assert_error_code(delegating.handle(Notice(3)).await, ErrorCode::Validation);
    assert_eq!(delegating.call_count(), 2);
    assert_eq!(delegating.calls(), [Notice(2), Notice(3)]);
    assert_eq!(delegating.last_call(), Some(Notice(3)));

    let acting = catga_core::testing::EventHandlerSpy::with_action(|event: Notice| async move {
        if event.0 > 0 {
            Ok(())
        } else {
            Err(CatgaError::new(ErrorCode::Internal, "empty notice"))
        }
    });
    assert_error_code(acting.handle(Notice(0)).await, ErrorCode::Internal);
    assert_eq!(acting.call_count(), 1);
    assert_eq!(acting.last_call(), Some(Notice(0)));
}

// ---------------------------------------------------------------------------
// ValidationBehavior
// ---------------------------------------------------------------------------

struct PositivePing;

#[async_trait]
impl Validator<Ping> for PositivePing {
    async fn validate(&self, request: &Ping, errors: &mut Vec<Box<str>>) -> CatgaResult<()> {
        if request.0 == 0 {
            errors.push("ping must be positive".into());
        }
        Ok(())
    }
}

struct ShortPing;

#[async_trait]
impl Validator<Ping> for ShortPing {
    async fn validate(&self, request: &Ping, errors: &mut Vec<Box<str>>) -> CatgaResult<()> {
        if request.0 > 100 {
            errors.push("ping must not exceed 100".into());
        }
        Ok(())
    }
}

struct CrashingValidator;

#[async_trait]
impl Validator<Ping> for CrashingValidator {
    async fn validate(&self, _request: &Ping, _errors: &mut Vec<Box<str>>) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Internal, "validator cannot run"))
    }
}

fn ping_mediator() -> Mediator {
    let mut registry = Registry::new();
    assert_success(registry.register_request::<Ping, _>(Double));
    Mediator::new(registry)
}

#[tokio::test]
async fn validation_behavior_gates_the_pipeline() {
    let mediator = ping_mediator();

    // Empty and default behaviors keep the handler fast path.
    let empty = Pipeline::new().with(ValidationBehavior::<Ping>::empty());
    assert_eq!(assert_success(mediator.send_with(Ping(2), &empty).await), 4);
    let defaulted = Pipeline::new().with(ValidationBehavior::<Ping>::default());
    assert_eq!(
        assert_success(mediator.send_with(Ping(3), &defaulted).await),
        6
    );

    // Passing requests reach the handler with every validator consulted.
    let passing = Pipeline::new().with(ValidationBehavior::new([
        Arc::new(PositivePing) as Arc<dyn Validator<Ping>>,
        Arc::new(ShortPing) as Arc<dyn Validator<Ping>>,
    ]));
    assert_eq!(
        assert_success(mediator.send_with(Ping(5), &passing).await),
        10
    );

    // Collected validation errors become one structured Validation error.
    let error = assert_failure(mediator.send_with(Ping(0), &passing).await);
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(error.message().contains("ping must be positive"));

    // A validator that cannot run surfaces its operational error unchanged.
    let crashing = Pipeline::new().with(ValidationBehavior::single(CrashingValidator));
    assert_error_code(
        mediator.send_with(Ping(1), &crashing).await,
        ErrorCode::Internal,
    );
}

// ---------------------------------------------------------------------------
// Unix-millisecond time helpers
// ---------------------------------------------------------------------------

#[test]
fn unix_millis_helpers_convert_and_guard_ranges() {
    assert!(now_unix_millis() > 0);
    // A real clock sits after the epoch, so the default is not needed; the
    // call still exercises the fallback-accepting entry point.
    assert!(now_unix_millis_or(7) >= 7);

    assert_eq!(checked_unix_millis(UNIX_EPOCH), Ok(0));
    assert_eq!(
        checked_unix_millis(UNIX_EPOCH + Duration::from_millis(7)),
        Ok(7)
    );
    assert_eq!(
        checked_unix_millis(UNIX_EPOCH - Duration::from_secs(1)),
        Err(UnixMillisError::BeforeEpoch)
    );

    assert_eq!(
        UnixMillisError::BeforeEpoch.to_string(),
        "time precedes the Unix epoch"
    );
    assert_eq!(
        UnixMillisError::ExceedsRange.to_string(),
        "millisecond count exceeds the u64 range"
    );
    let as_error: &dyn std::error::Error = &UnixMillisError::BeforeEpoch;
    assert!(!as_error.to_string().is_empty());

    assert_eq!(signed_unix_millis(UNIX_EPOCH), Some(0));
    assert_eq!(
        signed_unix_millis(UNIX_EPOCH + Duration::from_millis(9)),
        Some(9)
    );
    assert_eq!(
        signed_unix_millis(UNIX_EPOCH - Duration::from_millis(9)),
        Some(-9)
    );
}
