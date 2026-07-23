use std::{sync::Arc, time::SystemTime};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, EventStore, EventStream, StoredEvent, VersionInfo,
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

#[async_trait]
impl EventStore for MemoryEventStore {
    async fn append(
        &self,
        stream_id: &str,
        events: Vec<Envelope>,
        expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        if events.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "event batch must not be empty",
            ));
        }
        let stream = self.streams.entry(stream_id.into()).or_default().clone();
        stream.append(events, expected_version)
    }

    async fn read(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventStream> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Ok(EventStream::new(stream_id, -1, Vec::new()));
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
        Ok(EventStream::new(
            stream_id,
            snapshot.len() as i64 - 1,
            events,
        ))
    }

    async fn version(&self, stream_id: &str) -> CatgaResult<i64> {
        Ok(self
            .streams
            .get(stream_id)
            .map_or(-1, |stream| stream.events.load().len() as i64 - 1))
    }

    async fn read_to_version(&self, stream_id: &str, to_version: i64) -> CatgaResult<EventStream> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Ok(EventStream::new(stream_id, -1, Vec::new()));
        };
        let snapshot = stream.events.load_full();
        let count = usize::try_from(to_version.saturating_add(1))
            .unwrap_or(0)
            .min(snapshot.len());
        Ok(EventStream::new(
            stream_id,
            count as i64 - 1,
            snapshot[..count].to_vec(),
        ))
    }

    async fn read_to_time(
        &self,
        stream_id: &str,
        upper_bound: SystemTime,
    ) -> CatgaResult<EventStream> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Ok(EventStream::new(stream_id, -1, Vec::new()));
        };
        let snapshot = stream.events.load_full();
        let events: Vec<_> = snapshot
            .iter()
            .filter(|event| event.timestamp() <= upper_bound)
            .cloned()
            .collect();
        let version = events.last().map_or(-1, StoredEvent::version);
        Ok(EventStream::new(stream_id, version, events))
    }

    async fn version_history(&self, stream_id: &str) -> CatgaResult<Vec<VersionInfo>> {
        Ok(self.streams.get(stream_id).map_or_else(Vec::new, |stream| {
            stream
                .events
                .load()
                .iter()
                .map(|event| {
                    VersionInfo::new(
                        event.version(),
                        event.timestamp(),
                        event.envelope().message_type(),
                    )
                })
                .collect()
        }))
    }

    async fn stream_ids(&self) -> CatgaResult<Vec<String>> {
        let mut ids: Vec<_> = self
            .streams
            .iter()
            .map(|entry| entry.key().to_string())
            .collect();
        ids.sort_unstable();
        Ok(ids)
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
