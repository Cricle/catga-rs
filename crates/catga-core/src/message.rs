/// A value that can be handled or transported by Catga.
pub trait Message: Send + Sync + 'static {
    /// Returns the stable Rust type name used by the default registry.
    fn message_type(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// A message that produces a typed response.
pub trait Request: Message {
    /// The value returned by the matching request handler.
    type Response: Send + 'static;
}

/// A message that has no response value.
pub trait Command: Message {}

/// A message delivered to zero or more subscribers.
pub trait Event: Message + Clone {}

/// Identifiers propagated with a message through a distributed operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageMetadata {
    message_id: u64,
    correlation_id: Option<u64>,
}

impl MessageMetadata {
    /// Creates metadata for a message and its optional causal root.
    pub const fn new(message_id: u64, correlation_id: Option<u64>) -> Self {
        Self {
            message_id,
            correlation_id,
        }
    }

    /// Returns the unique message identifier.
    pub const fn message_id(self) -> u64 {
        self.message_id
    }

    /// Returns the optional distributed correlation identifier.
    pub const fn correlation_id(self) -> Option<u64> {
        self.correlation_id
    }
}
