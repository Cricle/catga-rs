//! Unit tests for MailboxPriority.

use catga_core::{Envelope, MessageMetadata, MessagePriority};
use robustmq::Priority;

use catga_robustmq::MailboxPriority;

#[test]
fn test_mailbox_priority_as_sdk_critical() {
    assert_eq!(MailboxPriority::Critical.as_sdk(), Priority::High);
}

#[test]
fn test_mailbox_priority_as_sdk_high() {
    assert_eq!(MailboxPriority::High.as_sdk(), Priority::High);
}

#[test]
fn test_mailbox_priority_as_sdk_normal() {
    assert_eq!(MailboxPriority::Normal.as_sdk(), Priority::Normal);
}

#[test]
fn test_mailbox_priority_as_sdk_low() {
    assert_eq!(MailboxPriority::Low.as_sdk(), Priority::Low);
}

#[test]
fn test_from_message_priority_critical() {
    assert_eq!(
        MailboxPriority::from(MessagePriority::Critical),
        MailboxPriority::Critical
    );
}

#[test]
fn test_from_message_priority_high() {
    assert_eq!(
        MailboxPriority::from(MessagePriority::High),
        MailboxPriority::High
    );
}

#[test]
fn test_from_message_priority_normal() {
    assert_eq!(
        MailboxPriority::from(MessagePriority::Normal),
        MailboxPriority::Normal
    );
}

#[test]
fn test_from_message_priority_low() {
    assert_eq!(
        MailboxPriority::from(MessagePriority::Low),
        MailboxPriority::Low
    );
}

#[test]
fn test_from_envelope_critical() {
    let metadata = MessageMetadata::new(1, None).with_priority(MessagePriority::Critical);
    let envelope = Envelope::new(1, "test", vec![], metadata);
    assert_eq!(
        MailboxPriority::from_envelope(&envelope),
        MailboxPriority::Critical
    );
}

#[test]
fn test_from_envelope_high() {
    let metadata = MessageMetadata::new(1, None).with_priority(MessagePriority::High);
    let envelope = Envelope::new(1, "test", vec![], metadata);
    assert_eq!(
        MailboxPriority::from_envelope(&envelope),
        MailboxPriority::High
    );
}

#[test]
fn test_from_envelope_normal() {
    let metadata = MessageMetadata::new(1, None).with_priority(MessagePriority::Normal);
    let envelope = Envelope::new(1, "test", vec![], metadata);
    assert_eq!(
        MailboxPriority::from_envelope(&envelope),
        MailboxPriority::Normal
    );
}

#[test]
fn test_from_envelope_low() {
    let metadata = MessageMetadata::new(1, None).with_priority(MessagePriority::Low);
    let envelope = Envelope::new(1, "test", vec![], metadata);
    assert_eq!(
        MailboxPriority::from_envelope(&envelope),
        MailboxPriority::Low
    );
}

#[test]
fn test_mailbox_priority_clone() {
    let original = MailboxPriority::High;
    let cloned = original;
    assert_eq!(original, cloned);
}

#[test]
fn test_mailbox_priority_debug() {
    let priority = MailboxPriority::Critical;
    let debug_str = format!("{:?}", priority);
    assert_eq!(debug_str, "Critical");
}

#[test]
fn test_mailbox_priority_eq() {
    assert_eq!(MailboxPriority::Critical, MailboxPriority::Critical);
    assert_eq!(MailboxPriority::High, MailboxPriority::High);
    assert_eq!(MailboxPriority::Normal, MailboxPriority::Normal);
    assert_eq!(MailboxPriority::Low, MailboxPriority::Low);
    assert_ne!(MailboxPriority::Critical, MailboxPriority::Normal);
}

#[test]
fn test_mailbox_priority_ord_by_priority_mapping() {
    assert_eq!(MailboxPriority::Critical.as_sdk(), Priority::High);
    assert_eq!(MailboxPriority::High.as_sdk(), Priority::High);
    assert_ne!(MailboxPriority::Critical.as_sdk(), Priority::Normal);
    assert_ne!(MailboxPriority::High.as_sdk(), Priority::Low);
}

#[test]
fn test_from_message_priority_all_variants() {
    let test_cases = [
        (MessagePriority::Critical, MailboxPriority::Critical),
        (MessagePriority::High, MailboxPriority::High),
        (MessagePriority::Normal, MailboxPriority::Normal),
        (MessagePriority::Low, MailboxPriority::Low),
    ];
    for (input, expected) in test_cases {
        assert_eq!(MailboxPriority::from(input), expected);
    }
}

#[test]
fn test_from_envelope_preserves_priority() {
    use catga_core::Envelope;
    for priority in [
        MessagePriority::Critical,
        MessagePriority::High,
        MessagePriority::Normal,
        MessagePriority::Low,
    ] {
        let metadata = MessageMetadata::new(1, None).with_priority(priority);
        let envelope = Envelope::new(1, "test", vec![], metadata);
        let mailbox_priority = MailboxPriority::from_envelope(&envelope);
        let expected = MailboxPriority::from(priority);
        assert_eq!(mailbox_priority, expected);
    }
}

#[test]
fn test_mailbox_priority_copy() {
    let original = MailboxPriority::High;
    let copied = original;
    assert_eq!(original, copied);
}

#[test]
fn test_mailbox_priority_default_conversion() {
    for mp in [
        MailboxPriority::Critical,
        MailboxPriority::High,
        MailboxPriority::Normal,
        MailboxPriority::Low,
    ] {
        let sdk = mp.as_sdk();
        match sdk {
            Priority::High | Priority::Normal | Priority::Low => {}
            _ => panic!("Invalid SDK priority"),
        }
    }
}
