//! An event-store read view that upgrades historical payload schemas.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    CatgaResult, Envelope, EventStore, EventStream, EventVersionRegistry, StoredEvent, VersionInfo,
};

/// Applies registered event schema upgrades to read results without rewriting history.
pub struct UpgradingEventStore<'a, S: ?Sized> {
    inner: &'a S,
    versions: &'a EventVersionRegistry,
}

impl<'a, S: ?Sized> UpgradingEventStore<'a, S> {
    /// Creates a read view over an event store and immutable event-version registry snapshots.
    pub const fn new(inner: &'a S, versions: &'a EventVersionRegistry) -> Self {
        Self { inner, versions }
    }
}

#[async_trait]
impl<S: ?Sized> EventStore for UpgradingEventStore<'_, S>
where
    S: EventStore,
{
    async fn append(
        &self,
        stream_id: &str,
        events: Vec<Envelope>,
        expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        self.inner.append(stream_id, events, expected_version).await
    }

    async fn read(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventStream> {
        self.upgrade(self.inner.read(stream_id, from_version, max_count).await?)
    }

    async fn version(&self, stream_id: &str) -> CatgaResult<i64> {
        self.inner.version(stream_id).await
    }

    async fn read_to_version(&self, stream_id: &str, to_version: i64) -> CatgaResult<EventStream> {
        self.upgrade(self.inner.read_to_version(stream_id, to_version).await?)
    }

    async fn read_to_time(
        &self,
        stream_id: &str,
        upper_bound: std::time::SystemTime,
    ) -> CatgaResult<EventStream> {
        self.upgrade(self.inner.read_to_time(stream_id, upper_bound).await?)
    }

    async fn version_history(&self, stream_id: &str) -> CatgaResult<Vec<VersionInfo>> {
        self.inner.version_history(stream_id).await
    }

    async fn stream_ids(&self) -> CatgaResult<Vec<String>> {
        self.inner.stream_ids().await
    }
}

impl<S: ?Sized> UpgradingEventStore<'_, S> {
    fn upgrade(&self, stream: EventStream) -> CatgaResult<EventStream> {
        if !stream
            .events()
            .iter()
            .any(|event| self.versions.has_upgraders(event.envelope().message_type()))
        {
            return Ok(stream);
        }
        let events = stream
            .events()
            .iter()
            .map(|event| self.upgrade_event(event))
            .collect::<CatgaResult<Vec<_>>>()?;
        Ok(EventStream::new(
            stream.stream_id(),
            stream.version(),
            events,
        ))
    }

    fn upgrade_event(&self, event: &StoredEvent) -> CatgaResult<StoredEvent> {
        if !self.versions.has_upgraders(event.envelope().message_type()) {
            return Ok(event.clone());
        }
        let envelope = self
            .versions
            .upgrade_to_latest((**event.envelope()).clone())?;
        Ok(StoredEvent::new(
            event.version(),
            Arc::new(envelope),
            event.timestamp(),
        ))
    }
}
