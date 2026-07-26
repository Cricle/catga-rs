use std::{collections::BinaryHeap, sync::Arc, time::SystemTime};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, EventPage, EventStore, EventStream, StoredEvent,
    StreamIdsPage, VersionHistoryPage, VersionInfo, telemetry, validate_event_store_page_size,
};
use dashmap::DashMap;

/// A lock-free snapshot event store for development and deterministic tests.
#[derive(Default)]
pub struct MemoryEventStore {
    streams: DashMap<Box<str>, Arc<MemoryEventStream>>,
}

struct MemoryEventStream {
    events: ArcSwap<Vec<StoredEvent>>,
}

impl Default for MemoryEventStream {
    fn default() -> Self {
        Self {
            events: ArcSwap::from_pointee(Vec::new()),
        }
    }
}

impl MemoryEventStore {
    fn page_stream(&self, stream_id: &str, from_version: u64, max_count: usize) -> EventStream {
        let Some(stream) = self.streams.get(stream_id) else {
            return EventStream::new(stream_id, -1, Vec::new());
        };
        let snapshot = stream.events.load_full();
        let start = usize::try_from(from_version).unwrap_or(usize::MAX);
        let events = snapshot
            .get(start..)
            .unwrap_or_default()
            .iter()
            .take(max_count)
            .cloned()
            .collect();
        EventStream::new(stream_id, snapshot.len() as i64 - 1, events)
    }

    fn record<T>(
        operation: &'static str,
        action: impl FnOnce() -> CatgaResult<T>,
    ) -> CatgaResult<T> {
        let mut telemetry = telemetry::persistence_operation("memory", "event_store", operation);
        let result = action();
        telemetry.complete(&result);
        result
    }
}

#[async_trait]
impl EventStore for MemoryEventStore {
    async fn append(
        &self,
        stream_id: &str,
        events: Vec<Envelope>,
        expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        Self::record("append", || {
            if events.is_empty() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "event batch must not be empty",
                ));
            }
            let stream = self.streams.entry(stream_id.into()).or_default().clone();
            stream.append(events, expected_version)
        })
    }

    async fn read_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        validate_event_store_page_size(max_count)?;
        Self::record("read_page", || {
            let stream = self.page_stream(stream_id, from_version, max_count);
            let next_version = stream.events().last().and_then(|event| {
                (event.version() < stream.version())
                    .then(|| u64::try_from(event.version().saturating_add(1)).ok())
                    .flatten()
            });
            Ok(EventPage::new(stream, next_version))
        })
    }

    async fn version(&self, stream_id: &str) -> CatgaResult<i64> {
        Self::record("version", || {
            Ok(self
                .streams
                .get(stream_id)
                .map_or(-1, |stream| stream.events.load().len() as i64 - 1))
        })
    }

    async fn read_to_version_page(
        &self,
        stream_id: &str,
        from_version: u64,
        to_version: i64,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        validate_event_store_page_size(max_count)?;
        Self::record("read_to_version_page", || {
            if to_version < 0 {
                return Ok(EventPage::new(
                    EventStream::new(stream_id, -1, Vec::new()),
                    None,
                ));
            }
            let stream = self.page_stream(stream_id, from_version, max_count);
            let events: Vec<_> = stream
                .events()
                .iter()
                .take_while(|event| event.version() <= to_version)
                .cloned()
                .collect();
            let page_version = events.last().map_or(-1, StoredEvent::version);
            let next_version = events.last().and_then(|event| {
                (event.version() < to_version && event.version() < stream.version())
                    .then(|| u64::try_from(event.version().saturating_add(1)).ok())
                    .flatten()
            });
            Ok(EventPage::new(
                EventStream::new(stream_id, page_version, events),
                next_version,
            ))
        })
    }

    async fn read_to_time_page(
        &self,
        stream_id: &str,
        from_version: u64,
        upper_bound: SystemTime,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        validate_event_store_page_size(max_count)?;
        Self::record("read_to_time_page", || {
            let stream = self.page_stream(stream_id, from_version, max_count);
            let last_scanned = stream.events().last().map(StoredEvent::version);
            let events: Vec<_> = stream
                .events()
                .iter()
                .filter(|event| event.timestamp() <= upper_bound)
                .cloned()
                .collect();
            let version = events.last().map_or(-1, StoredEvent::version);
            let next_version = last_scanned.and_then(|version| {
                (version < stream.version())
                    .then(|| u64::try_from(version.saturating_add(1)).ok())
                    .flatten()
            });
            Ok(EventPage::new(
                EventStream::new(stream_id, version, events),
                next_version,
            ))
        })
    }

    async fn version_history_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        validate_event_store_page_size(max_count)?;
        Self::record("version_history_page", || {
            let stream = self.page_stream(stream_id, from_version, max_count);
            let entries = stream
                .events()
                .iter()
                .map(|event| {
                    VersionInfo::new(
                        event.version(),
                        event.timestamp(),
                        event.envelope().message_type(),
                    )
                })
                .collect();
            let next_version = stream.events().last().and_then(|event| {
                (event.version() < stream.version())
                    .then(|| u64::try_from(event.version().saturating_add(1)).ok())
                    .flatten()
            });
            Ok(VersionHistoryPage::new(entries, next_version))
        })
    }

    async fn stream_ids_page(
        &self,
        after: Option<&str>,
        max_count: usize,
    ) -> CatgaResult<StreamIdsPage> {
        validate_event_store_page_size(max_count)?;
        Self::record("stream_ids_page", || {
            let mut ids = BinaryHeap::with_capacity(max_count);
            let mut has_more = false;
            for entry in &self.streams {
                let id = entry.key().to_string();
                if after.is_some_and(|cursor| id.as_str() <= cursor) {
                    continue;
                }
                if ids.len() < max_count {
                    ids.push(id);
                } else {
                    has_more = true;
                    let largest = ids.peek().map(String::as_str);
                    if largest.is_some_and(|largest| id.as_str() < largest) {
                        let _ = ids.pop();
                        ids.push(id);
                    }
                }
            }
            let mut ids = ids.into_vec();
            ids.sort_unstable();
            let next_stream_id = has_more.then(|| ids.last().cloned()).flatten();
            Ok(StreamIdsPage::new(ids, next_stream_id))
        })
    }
}

impl MemoryEventStream {
    fn append(&self, events: Vec<Envelope>, expected_version: Option<i64>) -> CatgaResult<i64> {
        loop {
            let current = self.events.load_full();
            let version = current.len() as i64 - 1;
            if expected_version.is_some_and(|expected| expected != version) {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "event stream version conflict",
                ));
            }
            let timestamp = SystemTime::now();
            let mut next = Vec::with_capacity(current.len() + events.len());
            next.extend(current.iter().cloned());
            next.extend(
                events
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(offset, envelope)| {
                        StoredEvent::new(version + offset as i64 + 1, Arc::new(envelope), timestamp)
                    }),
            );
            let previous = self.events.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&*previous, &current) {
                return Ok(version + events.len() as i64);
            }
        }
    }
}
