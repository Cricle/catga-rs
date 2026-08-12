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
    ///
    /// ```
    /// use catga_core::{Envelope, MessageMetadata, MessagePriority};
    /// use catga_robustmq::MailboxPriority;
    ///
    /// let metadata = MessageMetadata::new(1, None).with_priority(MessagePriority::Critical);
    /// let envelope = Envelope::new(1, "Order", Vec::new(), metadata);
    /// assert_eq!(MailboxPriority::from_envelope(&envelope), MailboxPriority::Critical);
    /// ```
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
