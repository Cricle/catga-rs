use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_core::{
    DelayedEvent, DelayedMessage, DelayedRequest, ErrorCode, Event, Message, Request,
};

struct Deferred {
    scheduled_at: Option<SystemTime>,
    delay: Option<Duration>,
}

impl Message for Deferred {}

impl DelayedMessage for Deferred {
    fn scheduled_at(&self) -> Option<SystemTime> {
        self.scheduled_at
    }

    fn delay(&self) -> Option<Duration> {
        self.delay
    }
}

struct DeferredRequest;

impl Message for DeferredRequest {}

impl Request for DeferredRequest {
    type Response = ();
}

impl DelayedMessage for DeferredRequest {}

#[derive(Clone)]
struct DeferredEvent;

impl Message for DeferredEvent {}
impl Event for DeferredEvent {}
impl DelayedMessage for DeferredEvent {}

#[test]
fn delayed_message_prefers_its_absolute_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(10);
    let scheduled_at = now + Duration::from_secs(20);
    let message = Deferred {
        scheduled_at: Some(scheduled_at),
        delay: Some(Duration::from_secs(60)),
    };

    assert_eq!(
        message.deliver_at(now).expect("valid deadline"),
        scheduled_at
    );
}

#[test]
fn delayed_message_calculates_relative_deadline_from_the_supplied_clock() {
    let now = UNIX_EPOCH + Duration::from_secs(10);
    let message = Deferred {
        scheduled_at: None,
        delay: Some(Duration::from_secs(20)),
    };

    assert_eq!(
        message.deliver_at(now).expect("valid deadline"),
        now + Duration::from_secs(20)
    );
}

#[test]
fn delayed_message_without_a_schedule_is_due_at_the_supplied_clock() {
    let now = UNIX_EPOCH + Duration::from_secs(10);
    let message = Deferred {
        scheduled_at: None,
        delay: None,
    };

    assert_eq!(message.deliver_at(now).expect("immediate delivery"), now);
}

#[test]
fn delayed_message_rejects_deadlines_before_the_portable_epoch() {
    let message = Deferred {
        scheduled_at: Some(UNIX_EPOCH - Duration::from_secs(1)),
        delay: None,
    };

    let error = message
        .deliver_at(UNIX_EPOCH)
        .expect_err("outbox metadata cannot preserve a pre-epoch deadline");

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn delayed_request_and_event_markers_compose_without_boilerplate() {
    fn accepts_delayed_request(_: &impl DelayedRequest) {}
    fn accepts_delayed_event(_: &impl DelayedEvent) {}

    accepts_delayed_request(&DeferredRequest);
    accepts_delayed_event(&DeferredEvent);
}
