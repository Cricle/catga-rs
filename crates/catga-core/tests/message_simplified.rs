//! Tests for simplified message traits with TypeId pattern.

#![allow(dead_code)]

use catga_core::{Command, Event, DelayedMessage, Message, MessageTypeId, Request, MessagePriority};

mod __catga_types {
    pub struct GetUserTypeId;
    impl catga_core::MessageTypeId for GetUserTypeId {
        const NAME: &'static str = "GetUser";
    }

    pub struct CreditTypeId;
    impl catga_core::MessageTypeId for CreditTypeId {
        const NAME: &'static str = "Credit";
    }

    pub struct BalanceChangedTypeId;
    impl catga_core::MessageTypeId for BalanceChangedTypeId {
        const NAME: &'static str = "BalanceChanged";
    }
}

#[derive(Clone, Debug)]
struct GetUser(String);

impl Message for GetUser {
    fn schema_version(&self) -> u32 {
        1
    }
    fn priority(&self) -> MessagePriority {
        MessagePriority::Normal
    }
}

impl Request for GetUser {
    type Response = String;
    type TypeId = __catga_types::GetUserTypeId;
}

impl DelayedMessage for GetUser {}

#[derive(Clone, Debug)]
struct Credit {
    amount: u64,
}

impl Message for Credit {
    fn schema_version(&self) -> u32 {
        1
    }
    fn priority(&self) -> MessagePriority {
        MessagePriority::High
    }
}

impl Command for Credit {
    type TypeId = __catga_types::CreditTypeId;
}

#[derive(Clone, Debug)]
struct BalanceChanged {
    old_balance: u64,
    new_balance: u64,
}

impl Message for BalanceChanged {
    fn schema_version(&self) -> u32 {
        1
    }
    fn priority(&self) -> MessagePriority {
        MessagePriority::Normal
    }
}

impl Event for BalanceChanged {
    type TypeId = __catga_types::BalanceChangedTypeId;
}

impl DelayedMessage for BalanceChanged {}

// --- MessageTypeId tests ---

#[test]
fn message_type_id_name() {
    let name = <GetUser as Request>::TypeId::NAME;
    assert_eq!(name, "GetUser");
}

#[test]
fn command_type_id_name() {
    let name = <Credit as Command>::TypeId::NAME;
    assert_eq!(name, "Credit");
}

#[test]
fn event_type_id_name() {
    let name = <BalanceChanged as Event>::TypeId::NAME;
    assert_eq!(name, "BalanceChanged");
}

// --- Request tests ---

#[test]
fn request_response_type() {
    let _: <GetUser as Request>::Response = "user@example.com".to_string();
}

#[test]
fn request_implies_message() {
    fn assert_message<M: Message>() {}
    assert_message::<GetUser>();
}

// --- Command tests ---

#[test]
fn command_implies_message() {
    fn assert_message<M: Message>() {}
    assert_message::<Credit>();
}

// --- Event tests ---

#[test]
fn event_implies_message() {
    fn assert_message<M: Message>() {}
    assert_message::<BalanceChanged>();
}

#[test]
fn event_implies_clone() {
    fn assert_clone<T: Clone>() {}
    assert_clone::<BalanceChanged>();
}

// --- Message defaults ---

#[test]
fn message_default_schema_version() {
    let msg = GetUser("alice".to_string());
    assert_eq!(msg.schema_version(), 1);
}

#[test]
fn message_default_priority() {
    let msg = GetUser("alice".to_string());
    assert_eq!(msg.priority(), MessagePriority::Normal);
}

#[test]
fn command_default_priority() {
    let cmd = Credit { amount: 100 };
    // Command itself doesn't override priority, so it's Message's default
    // (but we explicitly set High above)
    assert_eq!(cmd.priority(), MessagePriority::High);
}

// --- DelayedMessage backwards-compat ---

#[test]
fn delayed_request_has_default_scheduled_at() {
    let msg = GetUser("alice".to_string());
    assert!(msg.scheduled_at().is_none());
}

#[test]
fn delayed_event_has_default_scheduled_at() {
    let evt = BalanceChanged {
        old_balance: 0,
        new_balance: 100,
    };
    assert!(evt.scheduled_at().is_none());
}

// --- DelayedRequest / DelayedEvent blanket impls ---

#[test]
fn delayed_request_blanket_impl() {
    use catga_core::DelayedRequest;
    fn assert_delayed<M: DelayedRequest>() {}
    assert_delayed::<GetUser>();
}

#[test]
fn delayed_event_blanket_impl() {
    use catga_core::DelayedEvent;
    fn assert_delayed<M: DelayedEvent>() {}
    assert_delayed::<BalanceChanged>();
}

// --- MessagePriority ordering ---

#[test]
fn message_priority_variants() {
    use catga_core::MessagePriority;
    assert_eq!(MessagePriority::Low as u8, 0);
    assert_eq!(MessagePriority::Normal as u8, 1);
    assert_eq!(MessagePriority::High as u8, 2);
    assert_eq!(MessagePriority::Critical as u8, 3);
}

#[test]
fn message_priority_default_is_normal() {
    let priority = MessagePriority::default();
    assert_eq!(priority, MessagePriority::Normal);
}

// --- MessagePriority serde roundtrip ---

#[test]
fn message_priority_serde_roundtrip() {
    use catga_core::MessagePriority;
    for priority in [
        MessagePriority::Low,
        MessagePriority::Normal,
        MessagePriority::High,
        MessagePriority::Critical,
    ] {
        let bytes = serde_json::to_vec(&priority).expect("serde_json::to_vec should succeed");
        let restored: MessagePriority =
            serde_json::from_slice(&bytes).expect("serde_json::from_slice should succeed");
        assert_eq!(priority, restored);
    }
}

// --- MessagePriority Clone + Eq + PartialEq ---

#[test]
fn message_priority_clone_eq() {
    use catga_core::MessagePriority;
    let a = MessagePriority::High;
    let b = MessagePriority::High;
    let c = MessagePriority::Critical;
    assert_eq!(a, b);
    assert_eq!(a.clone(), a);
    assert_ne!(a, c);
}
