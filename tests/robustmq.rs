//! RobustMQ mailbox adapter tests.

use catga_robustmq::MailboxPriority;
use robustmq::Priority;

#[test]
fn mailbox_priority_maps_without_protocol_leakage() {
    assert_eq!(MailboxPriority::Critical.as_sdk(), Priority::High);
    assert_eq!(MailboxPriority::Normal.as_sdk(), Priority::Normal);
    assert_eq!(MailboxPriority::Low.as_sdk(), Priority::Low);
}
