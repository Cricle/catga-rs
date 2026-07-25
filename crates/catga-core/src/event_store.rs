use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;

use crate::{CatgaError, CatgaResult, Envelope, ErrorCode};

/// Largest number of records an event-store page may retain at once.
///
/// Callers that need to process more records must follow the page cursor. This keeps replay,
/// projection, and subscription memory proportional to this bound rather than stream history.
pub const MAX_EVENT_STORE_PAGE_SIZE: usize = 1_024;

/// Validates a requested event-store page size.
///
/// Backends call this before allocating a result page so every implementation shares the same
/// memory bound.
pub fn validate_event_store_page_size(max_count: usize) -> CatgaResult<()> {
    if max_count == 0 || max_count > MAX_EVENT_STORE_PAGE_SIZE {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "event-store page size must be between 1 and MAX_EVENT_STORE_PAGE_SIZE",
        ));
    }
    Ok(())
}

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

/// Lightweight metadata used to inspect stream history without cloning event payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionInfo {
    version: i64,
    timestamp: SystemTime,
    event_type: Box<str>,
}

impl VersionInfo {
    /// Creates one version-history record.
    pub fn new(version: i64, timestamp: SystemTime, event_type: impl Into<Box<str>>) -> Self {
        Self {
            version,
            timestamp,
            event_type: event_type.into(),
        }
    }

    /// Returns the stream version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns when this version entered the stream.
    pub const fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns the serialized event type name.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }
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

/// One bounded page of events plus the version from which to resume reading.
#[derive(Clone, Debug)]
pub struct EventPage {
    stream: EventStream,
    next_version: Option<u64>,
}

impl EventPage {
    /// Creates an event page.
    pub fn new(stream: EventStream, next_version: Option<u64>) -> Self {
        Self {
            stream,
            next_version,
        }
    }

    /// Returns the immutable event stream snapshot for this page.
    pub const fn stream(&self) -> &EventStream {
        &self.stream
    }

    /// Returns the version from which to request the following page, when one is available.
    pub const fn next_version(&self) -> Option<u64> {
        self.next_version
    }
}

/// One bounded page of lightweight version metadata.
#[derive(Clone, Debug)]
pub struct VersionHistoryPage {
    entries: Vec<VersionInfo>,
    next_version: Option<u64>,
}

impl VersionHistoryPage {
    /// Creates a version-history page.
    pub fn new(entries: Vec<VersionInfo>, next_version: Option<u64>) -> Self {
        Self {
            entries,
            next_version,
        }
    }

    /// Returns the lightweight metadata retained by this page.
    pub fn entries(&self) -> &[VersionInfo] {
        &self.entries
    }

    /// Returns the version from which to request the following page, when one is available.
    pub const fn next_version(&self) -> Option<u64> {
        self.next_version
    }
}

/// One bounded, lexically ordered page of event-stream identifiers.
#[derive(Clone, Debug)]
pub struct StreamIdsPage {
    ids: Vec<String>,
    next_stream_id: Option<String>,
}

impl StreamIdsPage {
    /// Creates a stream-identifier page.
    pub fn new(ids: Vec<String>, next_stream_id: Option<String>) -> Self {
        Self {
            ids,
            next_stream_id,
        }
    }

    /// Returns the lexically ordered identifiers retained by this page.
    pub fn ids(&self) -> &[String] {
        &self.ids
    }

    /// Returns the exclusive lexical cursor for the following page, when one is available.
    pub fn next_stream_id(&self) -> Option<&str> {
        self.next_stream_id.as_deref()
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

    /// Reads one validated, bounded event page beginning at `from_version`.
    async fn read_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventPage>;

    /// Returns the current stream version, or `-1` when the stream does not exist.
    async fn version(&self, stream_id: &str) -> CatgaResult<i64>;

    /// Reads one validated page of events through the inclusive version bound.
    async fn read_to_version_page(
        &self,
        stream_id: &str,
        from_version: u64,
        to_version: i64,
        max_count: usize,
    ) -> CatgaResult<EventPage>;

    /// Reads one validated page of events at or before `upper_bound`.
    ///
    /// The cursor advances through physical stream versions even when this page has no matching
    /// events, so callers must follow [`EventPage::next_version`] until it is absent.
    async fn read_to_time_page(
        &self,
        stream_id: &str,
        from_version: u64,
        upper_bound: SystemTime,
        max_count: usize,
    ) -> CatgaResult<EventPage>;

    /// Reads one validated page of version metadata beginning at `from_version`.
    async fn version_history_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<VersionHistoryPage>;

    /// Reads one validated lexical page of stream identifiers after `after`.
    async fn stream_ids_page(
        &self,
        after: Option<&str>,
        max_count: usize,
    ) -> CatgaResult<StreamIdsPage>;
}
