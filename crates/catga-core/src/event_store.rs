use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;

use crate::{CatgaResult, Envelope};

/// One immutable event persisted in a versioned stream.
#[derive(Clone, Debug)]
pub struct StoredEvent {
    version: i64,
    envelope: Arc<Envelope>,
    timestamp: SystemTime,
}

impl StoredEvent {
    /// Creates a stored event assigned to one stream version.
    pub fn new(version: i64, envelope: Arc<Envelope>, timestamp: SystemTime) -> Self {
        Self {
            version,
            envelope,
            timestamp,
        }
    }

    /// Returns the zero-based stream version assigned at append time.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns the serialized event envelope without copying its payload.
    pub const fn envelope(&self) -> &Arc<Envelope> {
        &self.envelope
    }

    /// Returns when this event entered the stream.
    pub const fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
}

/// A versioned immutable read snapshot of one event stream.
#[derive(Clone, Debug)]
pub struct EventStream {
    stream_id: Box<str>,
    version: i64,
    events: Vec<StoredEvent>,
}

impl EventStream {
    /// Creates a stream snapshot from stored events.
    pub fn new(stream_id: impl Into<Box<str>>, version: i64, events: Vec<StoredEvent>) -> Self {
        Self {
            stream_id: stream_id.into(),
            version,
            events,
        }
    }

    /// Returns the stream identifier.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Returns the latest version known to this snapshot, or `-1` when absent.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns the requested immutable event range.
    pub fn events(&self) -> &[StoredEvent] {
        &self.events
    }
}

/// Appends and reads serialized events using optimistic stream versions.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Appends events and returns the resulting stream version.
    async fn append(
        &self,
        stream_id: &str,
        events: Vec<Envelope>,
        expected_version: Option<i64>,
    ) -> CatgaResult<i64>;

    /// Reads up to `max_count` events beginning at `from_version`.
    async fn read(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventStream>;

    /// Returns the current stream version, or `-1` when the stream does not exist.
    async fn version(&self, stream_id: &str) -> CatgaResult<i64>;

    /// Returns every currently known stream identifier.
    async fn stream_ids(&self) -> CatgaResult<Vec<String>>;
}
