//! Pure priority conversion contracts for the RobustMQ boundary.

use catga_core::{Envelope, MessageMetadata, MessagePriority};
use catga_robustmq::MailboxPriority;
use robustmq::Priority;

#[test]
fn every_catga_priority_has_a_stable_mailbox_mapping() {
    for (input, adapter, sdk) in [
        (
            MessagePriority::Critical,
            MailboxPriority::Critical,
            Priority::High,
        ),
        (MessagePriority::High, MailboxPriority::High, Priority::High),
        (
            MessagePriority::Normal,
            MailboxPriority::Normal,
            Priority::Normal,
        ),
        (MessagePriority::Low, MailboxPriority::Low, Priority::Low),
    ] {
        assert_eq!(MailboxPriority::from(input), adapter);
        assert_eq!(adapter.as_sdk(), sdk);
    }
}

#[test]
fn envelope_priority_is_the_only_input_to_mailbox_priority() {
    for (priority, expected) in [
        (MessagePriority::Critical, MailboxPriority::Critical),
        (MessagePriority::High, MailboxPriority::High),
        (MessagePriority::Normal, MailboxPriority::Normal),
        (MessagePriority::Low, MailboxPriority::Low),
    ] {
        let envelope = Envelope::versioned(
            44,
            "catga.robustmq.priority",
            vec![7, 8, 9],
            MessageMetadata::new(42, Some(24)).with_priority(priority),
            3,
        );

        assert_eq!(MailboxPriority::from_envelope(&envelope), expected);
    }
}
