//! Core time-travel state reconstruction contract tests.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{
    Aggregate, CatgaError, CatgaResult, Envelope, ErrorCode, EventPage, EventStore, EventStream,
    MessageMetadata, StoredEvent, StreamIdsPage, TimeTravelService, VersionHistoryPage,
    VersionInfo,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Counter {
    id: Box<str>,
    version: i64,
    total: u64,
}

impl Aggregate for Counter {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            version: -1,
            total: 0,
        }
    }

    fn stream_id(id: &str) -> Box<str> {
        format!("counter:{id}").into()
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn apply(&mut self, envelope: &Envelope) -> CatgaResult<()> {
        let amount = envelope.payload().first().copied().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "counter event requires one payload byte",
            )
        })?;
        self.total += u64::from(amount);
        self.version += 1;
        Ok(())
    }

    fn pending_events(&self) -> &[Envelope] {
        &[]
    }

    fn clear_pending_events(&mut self) {}
}

struct StaticEventStore {
    events: Vec<StoredEvent>,
}

impl StaticEventStore {
    fn new(events: impl IntoIterator<Item = (u8, SystemTime)>) -> Self {
        Self {
            events: events
                .into_iter()
                .enumerate()
                .map(|(version, (amount, timestamp))| {
                    let version = i64::try_from(version).unwrap_or(i64::MAX);
                    StoredEvent::new(
                        version,
                        Arc::new(Envelope::new(
                            u64::try_from(version).unwrap_or(u64::MAX),
                            "counter.incremented",
                            vec![amount],
                            MessageMetadata::new(u64::try_from(version).unwrap_or(u64::MAX), None),
                        )),
                        timestamp,
                    )
                })
                .collect(),
        }
    }

    fn page(
        &self,
        stream_id: &str,
        from_version: u64,
        include: impl Fn(&StoredEvent) -> bool,
    ) -> EventPage {
        let events = self
            .events
            .iter()
            .filter(|event| {
                u64::try_from(event.version()).is_ok_and(|version| version >= from_version)
            })
            .filter(|event| include(event))
            .cloned()
            .collect();
        let version = self.events.last().map_or(-1, StoredEvent::version);
        EventPage::new(EventStream::new(stream_id, version, events), None)
    }
}

#[async_trait]
impl EventStore for StaticEventStore {
    async fn append(
        &self,
        _stream_id: &str,
        _events: Vec<Envelope>,
        _expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "static test store is read-only",
        ))
    }

    async fn read_page(
        &self,
        stream_id: &str,
        from_version: u64,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        Ok(self.page(stream_id, from_version, |_| true))
    }

    async fn version(&self, _stream_id: &str) -> CatgaResult<i64> {
        Ok(self.events.last().map_or(-1, StoredEvent::version))
    }

    async fn read_to_version_page(
        &self,
        stream_id: &str,
        from_version: u64,
        to_version: i64,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        Ok(self.page(stream_id, from_version, |event| {
            event.version() <= to_version
        }))
    }

    async fn read_to_time_page(
        &self,
        stream_id: &str,
        from_version: u64,
        upper_bound: SystemTime,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        Ok(self.page(stream_id, from_version, |event| {
            event.timestamp() <= upper_bound
        }))
    }

    async fn version_history_page(
        &self,
        _stream_id: &str,
        from_version: u64,
        _max_count: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        Ok(VersionHistoryPage::new(
            self.events
                .iter()
                .filter(|event| {
                    u64::try_from(event.version()).is_ok_and(|version| version >= from_version)
                })
                .map(|event| {
                    VersionInfo::new(
                        event.version(),
                        event.timestamp(),
                        event.envelope().message_type(),
                    )
                })
                .collect(),
            None,
        ))
    }

    async fn stream_ids_page(
        &self,
        _after: Option<&str>,
        _max_count: usize,
    ) -> CatgaResult<StreamIdsPage> {
        Ok(StreamIdsPage::new(vec!["counter:one".to_owned()], None))
    }
}

fn store() -> StaticEventStore {
    let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    StaticEventStore::new([
        (1, start),
        (2, start + Duration::from_secs(1)),
        (3, start + Duration::from_secs(2)),
    ])
}

#[tokio::test]
async fn time_travel_rebuilds_versions_times_and_history_pages() -> CatgaResult<()> {
    let store = store();
    let service = TimeTravelService::<Counter, _>::new(&store);
    let state = service
        .state_at_version("one", 1)
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "expected historical state"))?;
    assert_eq!(state.id(), "one");
    assert_eq!(state.version(), 1);
    assert_eq!(state.total, 3);
    assert!(service.state_at_version("one", -1).await?.is_none());

    let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(1_001);
    let at_time = service
        .state_at_time("one", cutoff)
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "expected time-bounded state"))?;
    assert_eq!(at_time.total, 3);

    let history = service.version_history_page("one", 1, 10).await?;
    assert_eq!(history.entries().len(), 2);
    assert_eq!(history.entries()[0].version(), 1);
    Ok(())
}

#[tokio::test]
async fn time_travel_compares_inclusive_states_and_rejects_reversed_ranges() -> CatgaResult<()> {
    let store = store();
    let service = TimeTravelService::<Counter, _>::new(&store);
    let comparison = service.compare_versions("one", 0, 2).await?;
    assert_eq!(comparison.from_version(), 0);
    assert_eq!(comparison.to_version(), 2);
    assert_eq!(comparison.from_state().map(|state| state.total), Some(1));
    assert_eq!(comparison.to_state().map(|state| state.total), Some(6));
    assert_eq!(comparison.events_between().len(), 2);
    assert_eq!(
        comparison.events_between()[0].event_type(),
        "counter.incremented"
    );
    assert!(matches!(
        service.compare_versions("one", 2, 1).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}
