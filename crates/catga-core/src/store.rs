use async_trait::async_trait;

use crate::{CatgaResult, MessageMetadata};

/// A serialized message ready for durable delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    id: u64,
    message_type: Box<str>,
    payload: Vec<u8>,
    metadata: MessageMetadata,
    schema_version: u32,
}

impl Envelope {
    /// Creates an envelope from its identity, type, serialized payload, and metadata.
    pub fn new(
        id: u64,
        message_type: impl Into<Box<str>>,
        payload: Vec<u8>,
        metadata: MessageMetadata,
    ) -> Self {
        Self::versioned(id, message_type, payload, metadata, 1)
    }

    /// Creates an envelope with an explicit event schema version.
    pub fn versioned(
        id: u64,
        message_type: impl Into<Box<str>>,
        payload: Vec<u8>,
        metadata: MessageMetadata,
        schema_version: u32,
    ) -> Self {
        Self {
            id,
            message_type: message_type.into(),
            payload,
            metadata,
            schema_version,
        }
    }

    /// Returns the durable message identifier.
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Returns the serialized message type name.
    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    /// Returns the serialized payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the transport metadata.
    pub const fn metadata(&self) -> MessageMetadata {
        self.metadata
    }

    /// Returns the schema version used to serialize this event payload.
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

/// The lifecycle state of a durable outbox message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxState {
    /// The message has not yet been claimed for delivery.
    Pending,
    /// One worker owns the delivery attempt.
    Claimed,
}

/// A message persisted until a transport acknowledges delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxMessage {
    envelope: Envelope,
    state: OutboxState,
    owner: Option<Box<str>>,
}

impl OutboxMessage {
    /// Creates a pending outbox message.
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            state: OutboxState::Pending,
            owner: None,
        }
    }

    /// Returns the durable message identifier.
    pub const fn id(&self) -> u64 {
        self.envelope.id()
    }

    /// Returns the current delivery state.
    pub const fn state(&self) -> OutboxState {
        self.state
    }

    /// Returns the worker that currently owns delivery.
    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }

    /// Records the worker that exclusively owns the next delivery attempt.
    pub fn claim(&mut self, owner: impl Into<Box<str>>) {
        self.state = OutboxState::Claimed;
        self.owner = Some(owner.into());
    }

    /// Returns this message to the pending state after a failed delivery attempt.
    pub fn release(&mut self) {
        self.state = OutboxState::Pending;
        self.owner = None;
    }

    /// Returns the serialized envelope that must be published.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }
}

/// Persists outbound messages until their transport delivery is acknowledged.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Adds a message in the pending state.
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()>;

    /// Atomically claims up to `limit` pending messages for a worker.
    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>>;

    /// Removes a message only when its current worker acknowledges it.
    async fn ack(&self, owner: &str, id: u64) -> CatgaResult<()>;

    /// Returns a worker-owned message to pending after a failed delivery attempt.
    async fn release(&self, owner: &str, id: u64) -> CatgaResult<()>;
}
