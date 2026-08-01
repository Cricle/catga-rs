//! Event-stream projection contracts and catch-up replay.

use std::{num::NonZeroUsize, time::SystemTime};

use async_trait::async_trait;

use crate::{
    CatgaError, CatgaResult, ErrorCode, EventStore, MAX_EVENT_STORE_PAGE_SIZE, StoredEvent,
};

const DEFAULT_BATCH_SIZE: usize = 256;

/// The durable progress of one projection over one event stream.
///
/// ```
/// use catga_core::ProjectionCheckpoint;
///
/// let checkpoint = ProjectionCheckpoint::new("order-totals", "stream-42", 10);
/// assert_eq!(checkpoint.projection_name(), "order-totals");
/// assert_eq!(checkpoint.stream_id(), "stream-42");
/// assert_eq!(checkpoint.version(), 10);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionCheckpoint {
    projection_name: Box<str>,
    stream_id: Box<str>,
    version: i64,
    updated_at: SystemTime,
}

impl ProjectionCheckpoint {
    /// Creates a checkpoint after the given stream version has been applied.
    pub fn new(
        projection_name: impl Into<Box<str>>,
        stream_id: impl Into<Box<str>>,
        version: i64,
    ) -> Self {
        Self {
            projection_name: projection_name.into(),
            stream_id: stream_id.into(),
            version,
            updated_at: SystemTime::now(),
        }
    }

    /// Restores a checkpoint with its original durable update timestamp.
    pub fn from_persisted(
        projection_name: impl Into<Box<str>>,
        stream_id: impl Into<Box<str>>,
        version: i64,
        updated_at: SystemTime,
    ) -> Self {
        Self {
            projection_name: projection_name.into(),
            stream_id: stream_id.into(),
            version,
            updated_at,
        }
    }

    /// Returns the projection name that owns this checkpoint.
    pub fn projection_name(&self) -> &str {
        &self.projection_name
    }

    /// Returns the event stream tracked by this checkpoint.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Returns the last applied zero-based event version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns when this checkpoint was persisted.
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }
}

/// Stores durable projection progress independently for each event stream.
#[async_trait]
pub trait ProjectionCheckpointStore: Send + Sync {
    /// Persists the last successfully applied event for one projection and stream.
    async fn save(&self, checkpoint: ProjectionCheckpoint) -> CatgaResult<()>;

    /// Loads the last successfully applied event for one projection and stream.
    async fn load(
        &self,
        projection_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<ProjectionCheckpoint>>;

    /// Removes the checkpoint for one projection and stream.
    async fn delete(&self, projection_name: &str, stream_id: &str) -> CatgaResult<()>;

    /// Removes every checkpoint owned by one projection.
    async fn delete_all(&self, projection_name: &str) -> CatgaResult<()>;
}

/// Updates a read model from immutable stored events.
#[async_trait]
pub trait Projection: Send + Sync {
    /// Returns a stable projection name shared by all runner instances.
    fn name(&self) -> &str;

    /// Applies one event to the read model.
    async fn apply(&self, event: &StoredEvent) -> CatgaResult<()>;

    /// Clears the read model before a full replay.
    async fn reset(&self) -> CatgaResult<()>;
}

/// Applies newly received stored events to one projection without checkpoint I/O.
///
/// This is the low-latency counterpart to [`CatchUpProjectionRunner`]. The
/// caller selects the durable subscription and ordering policy; this wrapper
/// only forwards an immutable event reference to the projection.
pub struct LiveProjection<'a, P: ?Sized> {
    projection: &'a P,
}

impl<'a, P: ?Sized> LiveProjection<'a, P>
where
    P: Projection,
{
    /// Creates a live handler for one projection.
    pub const fn new(projection: &'a P) -> Self {
        Self { projection }
    }

    /// Applies one newly delivered immutable stored event.
    pub async fn handle(&self, event: &StoredEvent) -> CatgaResult<()> {
        self.projection.apply(event).await
    }
}

/// Summary returned after a projection run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProjectionRun {
    applied: usize,
    streams: usize,
}

impl ProjectionRun {
    /// Returns the number of events applied in this invocation.
    pub const fn applied(&self) -> usize {
        self.applied
    }

    /// Returns the number of streams inspected in this invocation.
    pub const fn streams(&self) -> usize {
        self.streams
    }
}

/// Replays events into a projection and persists a per-stream checkpoint after every event.
pub struct CatchUpProjectionRunner<'a, E: ?Sized, C: ?Sized, P: ?Sized> {
    events: &'a E,
    checkpoints: &'a C,
    projection: &'a P,
    batch_size: NonZeroUsize,
}

impl<'a, E: ?Sized, C: ?Sized, P: ?Sized> CatchUpProjectionRunner<'a, E, C, P>
where
    E: EventStore,
    C: ProjectionCheckpointStore,
    P: Projection,
{
    /// Creates a runner with a bounded default page size.
    pub fn new(events: &'a E, checkpoints: &'a C, projection: &'a P) -> Self {
        Self::with_batch_size(
            events,
            checkpoints,
            projection,
            NonZeroUsize::new(DEFAULT_BATCH_SIZE).unwrap_or(NonZeroUsize::MIN),
        )
    }

    /// Creates a runner with an explicit bounded event-store page size.
    ///
    /// Values above [`MAX_EVENT_STORE_PAGE_SIZE`] are capped to the store-wide limit.
    pub fn with_batch_size(
        events: &'a E,
        checkpoints: &'a C,
        projection: &'a P,
        batch_size: NonZeroUsize,
    ) -> Self {
        Self {
            events,
            checkpoints,
            projection,
            batch_size: NonZeroUsize::new(batch_size.get().min(MAX_EVENT_STORE_PAGE_SIZE))
                .unwrap_or(NonZeroUsize::MIN),
        }
    }

    /// Applies events that were not yet included in each stream checkpoint.
    pub async fn run(&self) -> CatgaResult<ProjectionRun> {
        let mut run = ProjectionRun {
            applied: 0,
            streams: 0,
        };
        let mut after = None;
        loop {
            let page = self
                .events
                .stream_ids_page(after.as_deref(), MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for stream_id in page.ids() {
                run.streams += 1;
                self.run_stream(stream_id, &mut run).await?;
            }
            let Some(next) = page.next_stream_id() else {
                break;
            };
            after = Some(next.to_owned());
        }
        Ok(run)
    }

    /// Clears the read model and checkpoints, then replays every persisted event.
    pub async fn rebuild(&self) -> CatgaResult<ProjectionRun> {
        self.projection.reset().await?;
        self.checkpoints.delete_all(self.projection.name()).await?;
        self.run().await
    }

    async fn run_stream(&self, stream_id: &str, run: &mut ProjectionRun) -> CatgaResult<()> {
        let checkpoint = self
            .checkpoints
            .load(self.projection.name(), stream_id)
            .await?;
        let mut next_version = checkpoint
            .map(|checkpoint| next_version_after(checkpoint.version()))
            .transpose()?
            .unwrap_or(0);
        loop {
            let page = self
                .events
                .read_page(stream_id, next_version, self.batch_size.get())
                .await?;
            if page.stream().events().is_empty() {
                return Ok(());
            }
            for event in page.stream().events() {
                self.projection.apply(event).await?;
                self.checkpoints
                    .save(ProjectionCheckpoint::new(
                        self.projection.name(),
                        stream_id,
                        event.version(),
                    ))
                    .await?;
                next_version = next_version_after(event.version())?;
                run.applied += 1;
            }
            if page.next_version().is_none() {
                return Ok(());
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::SystemTime,
    };

    use async_trait::async_trait;

    use super::{
        CatchUpProjectionRunner, Projection, ProjectionCheckpoint, ProjectionCheckpointStore,
        next_version_after,
    };
    use crate::{
        CatgaResult, Envelope, ErrorCode, EventPage, EventStore, EventStream, MessageMetadata,
        StoredEvent, StreamIdsPage, VersionHistoryPage,
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

        let run = CatchUpProjectionRunner::new(&events, &checkpoints, &projection)
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

        let run = CatchUpProjectionRunner::new(&events, &checkpoints, &projection)
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

        let error = CatchUpProjectionRunner::new(&events, &checkpoints, &projection)
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
}
