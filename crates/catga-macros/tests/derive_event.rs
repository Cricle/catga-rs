//! Tests for the catga_event derive macro.

use catga_core::{Event, Message};
use catga_macros::catga_event;

#[derive(Clone, catga_event)]
struct UserCreated {
    user_id: String,
}

#[derive(Clone, catga_event)]
struct OrderShipped {
    order_id: u64,
    tracking: String,
}

#[test]
fn implements_message() {
    let evt = UserCreated {
        user_id: "123".into(),
    };
    assert!(evt.message_type().ends_with("UserCreated"));
}

#[test]
fn implements_event() {
    fn assert_event<T: Event>() {}
    assert_event::<UserCreated>();
    assert_event::<OrderShipped>();
}

#[test]
fn event_is_clone() {
    let evt = UserCreated {
        user_id: "123".into(),
    };
    let _cloned = evt.clone();
}
