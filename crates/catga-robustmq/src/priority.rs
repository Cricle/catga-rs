use robustmq::Priority;

/// Protocol-neutral priority for mq9 mailbox delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MailboxPriority {
    /// Time-sensitive delivery.
    Critical,
    /// Standard application delivery.
    Normal,
    /// Deferrable delivery.
    Low,
}

impl MailboxPriority {
    /// Converts the Catga priority to the RobustMQ SDK value.
    pub const fn as_sdk(self) -> Priority {
        match self {
            Self::Critical => Priority::High,
            Self::Normal => Priority::Normal,
            Self::Low => Priority::Low,
        }
    }
}
