//! Tests for projection module

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;

use catga_core::{
    CatgaResult, Envelope, ErrorCode, EventPage, EventStore, EventStream, MessageMetadata,
    Projection, ProjectionCheckpoint, ProjectionCheckpointStore, StoredEvent, StreamIdsPage,
    VersionHistoryPage, CatgaError,
};

#[derive(Default)]
struct TestEvents {
    stream_ids: Vec<String>,
    pages: Mutex<VecDeque<(u64, EventPage)>>,
    requested_versions: Mutex<Vec<u64>>,
}

impl TestEvents {
    fn with_stream_page(stream_id: &str, from_version: u64, page: EventPage) -> Self {
        Self {
            stream_ids: vec![stream_id.to_owned()],
            pages: Mutex::new(VecDeque::from([(from_version, page)])),
            requested_versions: Mutex::new(Vec::new()),
        }
    }

    fn requested_versions(&self) -> Vec<u64> {
        self.requested_versions
            .lock()
            .expect("test requested-version lock")
            .clone()
    }
}

#[async_trait]
impl EventStore for TestEvents {
    async fn append(
        &self,
        _stream_id: &str,
        _events: Vec<Envelope>,
        _expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        unreachable!("projection tests do not append events")
    }

    async fn read_page(
        &self,
        stream_id: &str,
        from_version: u64,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        self.requested_versions
            .lock()
            .expect("test requested-version lock")
            .push(from_version);
        let (expected_version, page) = self
            .pages
            .lock()
            .expect("test event-page lock")
            .pop_front()
            .expect("test event page is configured");
        assert_eq!(stream_id, page.stream().stream_id());
        assert_eq!(from_version, expected_version);
        Ok(page)
    }

    async fn version(&self, _stream_id: &str) -> CatgaResult<i64> {
        unreachable!("projection tests read configured pages")
    }

    async fn read_to_version_page(
        &self,
        _stream_id: &str,
        _from_version: u64,
        _to_version: i64,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        unreachable!("projection tests do not use version-bounded reads")
    }

    async fn read_to_time_page(
        &self,
        _stream_id: &str,
        _from_version: u64,
        _upper_bound: SystemTime,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        unreachable!("projection tests do not use time-bounded reads")
    }

    async fn version_history_page(
        &self,
        _stream_id: &str,
        _from_version: u64,
        _max_count: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        unreachable!("projection tests do not inspect version history")
    }

    async fn stream_ids_page(
        &self,
        _after: Option<&str>,
        _max_count: usize,
    ) -> CatgaResult<StreamIdsPage> {
        Ok(StreamIdsPage::new(self.stream_ids.clone(), None))
    }
}

#[derive(Default)]
struct TestCheckpoints {
    checkpoint: Option<ProjectionCheckpoint>,
    saved: Mutex<Vec<ProjectionCheckpoint>>,
}

#[async_trait]
impl ProjectionCheckpointStore for TestCheckpoints {
    async fn save(&self, checkpoint: ProjectionCheckpoint) -> CatgaResult<()> {
        self.saved
            .lock()
            .expect("test checkpoint lock")
            .push(checkpoint);
        Ok(())
    }

    async fn load(
        &self,
        _projection_name: &str,
        _stream_id: &str,
    ) -> CatgaResult<Option<ProjectionCheckpoint>> {
        Ok(self.checkpoint.clone())
    }

    async fn delete(&self, _projection_name: &str, _stream_id: &str) -> CatgaResult<()> {
        unreachable!("projection tests do not delete one checkpoint")
    }

    async fn delete_all(&self, _projection_name: &str) -> CatgaResult<()> {
        unreachable!("projection tests do not rebuild")
    }
}

#[derive(Default)]
struct TestProjection {
    applied: Mutex<Vec<i64>>,
}

#[async_trait]
impl Projection for TestProjection {
    fn name(&self) -> &str {
        "test-projection"
    }

    async fn apply(&self, event: &StoredEvent) -> CatgaResult<()> {
        self.applied
            .lock()
            .expect("test projection lock")
            .push(event.version());
        Ok(())
    }

    async fn reset(&self) -> CatgaResult<()> {
        self.applied.lock().expect("test projection lock").clear();
        Ok(())
    }
}

fn stored_event(version: i64) -> StoredEvent {
    StoredEvent::new(
        version,
        Arc::new(Envelope::new(
            1,
            "test.event",
            Vec::new(),
            MessageMetadata::new(1, None),
        )),
        SystemTime::UNIX_EPOCH,
    )
}

fn next_version_after(version: i64) -> CatgaResult<u64> {
    let next = version.checked_add(1).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Validation,
            "projection checkpoint version cannot advance beyond i64::MAX",
        )
    })?;
    u64::try_from(next).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "projection checkpoint version cannot be negative",
        )
    })
}

#[test]
fn projection_version_overflow_is_a_validation_error() {
    let error = next_version_after(i64::MAX).expect_err("max version must not wrap");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn empty_stream_catalog_returns_without_reading_events() {
    let events = TestEvents::default();
    let checkpoints = TestCheckpoints::default();
    let projection = TestProjection::default();

    let run = catga_core::projection::CatchUpProjectionRunner::new(&events, &checkpoints, &projection)
        .run()
        .await
        .expect("empty catalog is a successful projection run");

    assert_eq!(run.applied(), 0);
    assert_eq!(run.streams(), 0);
    assert!(events.requested_versions().is_empty());
}

#[tokio::test]
async fn checkpoint_recovery_reads_the_next_event_version_and_persists_progress() {
    let events = TestEvents::with_stream_page(
        "orders-42",
        8,
        EventPage::new(
            EventStream::new("orders-42", 8, vec![stored_event(8)]),
            None,
        ),
    );
    let checkpoints = TestCheckpoints {
        checkpoint: Some(ProjectionCheckpoint::new("test-projection", "orders-42", 7)),
        saved: Mutex::new(Vec::new()),
    };
    let projection = TestProjection::default();

    let run = catga_core::projection::CatchUpProjectionRunner::new(&events, &checkpoints, &projection)
        .run()
        .await
        .expect("checkpointed event is replayed");

    assert_eq!(run.applied(), 1);
    assert_eq!(events.requested_versions(), vec![8]);
    assert_eq!(
        *projection.applied.lock().expect("test projection lock"),
        vec![8]
    );
    assert_eq!(
        checkpoints.saved.lock().expect("test checkpoint lock")[0].version(),
        8
    );
}

#[tokio::test]
async fn maximum_checkpoint_is_rejected_before_reading_or_applying_events() {
    let events = TestEvents {
        stream_ids: vec!["orders-42".to_owned()],
        ..Default::default()
    };
    let checkpoints = TestCheckpoints {
        checkpoint: Some(ProjectionCheckpoint::new(
            "test-projection",
            "orders-42",
            i64::MAX,
        )),
        saved: Mutex::new(Vec::new()),
    };
    let projection = TestProjection::default();

    let error = catga_core::projection::CatchUpProjectionRunner::new(&events, &checkpoints, &projection)
        .run()
        .await
        .expect_err("maximum checkpoint cannot advance");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(events.requested_versions().is_empty());
    assert!(
        projection
            .applied
            .lock()
            .expect("test projection lock")
            .is_empty()
    );
}
