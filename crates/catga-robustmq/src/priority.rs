use catga_core::{Envelope, MessagePriority};
use robustmq::Priority;

/// Protocol-neutral priority for mq9 mailbox delivery.
///
/// ```
/// use catga_robustmq::MailboxPriority;
/// use robustmq::Priority;
///
/// assert_eq!(MailboxPriority::Critical.as_sdk(), Priority::High);
/// assert_eq!(MailboxPriority::Low.as_sdk(), Priority::Low);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MailboxPriority {
    /// Time-sensitive delivery.
    Critical,
    /// Important application delivery.
    High,
    /// Standard application delivery.
    Normal,
    /// Deferrable delivery.
    Low,
}

impl MailboxPriority {
    /// Maps a Catga envelope's requested priority to the mailbox priority.
    ///
    /// RobustMQ exposes three broker levels, so Catga's `High` and `Critical`
    /// priorities both map to its highest level.
    pub fn from_envelope(envelope: &Envelope) -> Self {
        Self::from(envelope.metadata().priority())
    }

    /// Converts the Catga priority to the RobustMQ SDK value.
    pub const fn as_sdk(self) -> Priority {
        match self {
            Self::Critical | Self::High => Priority::High,
            Self::Normal => Priority::Normal,
            Self::Low => Priority::Low,
        }
    }
}

impl From<MessagePriority> for MailboxPriority {
    fn from(priority: MessagePriority) -> Self {
        match priority {
            MessagePriority::Critical => Self::Critical,
            MessagePriority::High => Self::High,
            MessagePriority::Normal => Self::Normal,
            MessagePriority::Low => Self::Low,
        }
    }
}

#[cfg(test)]
mod tests {
    use catga_core::{Envelope, MessageMetadata, MessagePriority};
    use robustmq::Priority;

    use super::MailboxPriority;

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
        // Both Critical and High map to SDK Priority::High
        assert_eq!(MailboxPriority::Critical.as_sdk(), Priority::High);
        assert_eq!(MailboxPriority::High.as_sdk(), Priority::High);
        // These should be different from Normal and Low
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
        let copied = original; // Copy, not clone
        assert_eq!(original, copied);
    }

    #[test]
    fn test_mailbox_priority_default_conversion() {
        // Test that the conversion is consistent
        for mp in [
            MailboxPriority::Critical,
            MailboxPriority::High,
            MailboxPriority::Normal,
            MailboxPriority::Low,
        ] {
            let sdk = mp.as_sdk();
            // Verify all variants produce a valid SDK priority
            #[allow(unreachable_patterns)]
            match sdk {
                Priority::High | Priority::Normal | Priority::Low => {}
                _ => panic!("Invalid SDK priority"),
            }
        }
    }
}
