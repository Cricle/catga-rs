use std::{
    collections::BinaryHeap,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use async_trait::async_trait;
use crate::{
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
    /// Append-optimized event storage.
    ///
    /// Writers take the write lock and push in place (amortized O(1) per event) instead of
    /// copying the whole history through a compare-and-swap loop. Readers take the read lock
    /// only long enough to clone the requested page of `Arc`-shared events.
    events: RwLock<Vec<StoredEvent>>,
}

impl Default for MemoryEventStream {
    fn default() -> Self {
        Self {
            events: RwLock::new(Vec::new()),
        }
    }
}

impl MemoryEventStore {
    fn page_stream(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventStream> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Ok(EventStream::new(stream_id, -1, Vec::new()));
        };
        let snapshot = stream
            .events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let start = usize::try_from(from_version).unwrap_or(usize::MAX);
        let events = snapshot
            .get(start..)
            .unwrap_or_default()
            .iter()
            .take(max_count)
            .cloned()
            .collect();
        Ok(EventStream::new(
            stream_id,
            event_stream_version(snapshot.len())?,
            events,
        ))
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
                return self.streams.get(stream_id).map_or(Ok(-1), |stream| {
                    event_stream_version(
                        stream
                            .events
                            .read()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .len(),
                    )
                });
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
            let stream = self.page_stream(stream_id, from_version, max_count)?;
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
            self.streams.get(stream_id).map_or(Ok(-1), |stream| {
                event_stream_version(
                    stream
                        .events
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .len(),
                )
            })
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
            let stream = self.page_stream(stream_id, from_version, max_count)?;
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
            let stream = self.page_stream(stream_id, from_version, max_count)?;
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
            let stream = self.page_stream(stream_id, from_version, max_count)?;
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
        let mut stored = self
            .events
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut version = event_stream_version(stored.len())?;
        if expected_version.is_some_and(|expected| expected != version) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "event stream version conflict",
            ));
        }
        let appended_version = checked_appended_event_version(version, events.len())?;
        let timestamp = SystemTime::now();
        stored.try_reserve_exact(events.len()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "event stream allocation exceeds memory range",
            )
        })?;
        for envelope in &events {
            version = version.checked_add(1).ok_or_else(|| {
                CatgaError::new(ErrorCode::Internal, "event stream version is exhausted")
            })?;
            stored.push(StoredEvent::new(
                version,
                Arc::new(envelope.clone()),
                timestamp,
            ));
        }
        Ok(appended_version)
    }
}

fn event_stream_version(event_count: usize) -> CatgaResult<i64> {
    i64::try_from(event_count)
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "event stream version is exhausted"))?
        .checked_sub(1)
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "event stream version is exhausted"))
}

fn checked_appended_event_version(current: i64, event_count: usize) -> CatgaResult<i64> {
    current
        .checked_add(i64::try_from(event_count).map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "event stream version is exhausted")
        })?)
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "event stream version is exhausted"))
}
