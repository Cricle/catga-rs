//! Connected NATS event-store projection runner.

use std::num::NonZeroUsize;

use catga_core::{CatchUpProjectionRunner, CatgaResult, Projection, ProjectionRun};

use crate::{NatsEventStore, NatsProjectionCheckpoints};

/// Names the NATS resources used by one catch-up projection.
///
/// No default is provided because stream subjects and checkpoint buckets are deployment-owned
/// resources. Use a stable checkpoint bucket when a projection must resume after restart; use a
/// separate bucket for an isolated rebuild environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsProjectionConfig {
    /// JetStream stream used by the event store.
    pub event_stream: Box<str>,
    /// Literal NATS subject prefix used by the event store.
    pub event_subject_prefix: Box<str>,
    /// JetStream KV bucket containing projection checkpoints.
    pub checkpoint_bucket: Box<str>,
}

/// Runs one read-model projection over a NATS event store with durable NATS checkpoints.
///
/// This runner is intentionally an EventStore replay tool, not a message-transport consumer.
/// Call [`Self::run`] at startup or on a schedule to catch up from durable checkpoints. Run live
/// transport delivery separately with `CompetingConsumer` when the application needs sub-second
/// propagation; that consumer and the event-store replay have different acknowledgement and
/// ordering contracts.
pub struct NatsProjectionRunner<P> {
    events: NatsEventStore,
    checkpoints: NatsProjectionCheckpoints,
    projection: P,
    batch_size: Option<NonZeroUsize>,
}

impl<P> NatsProjectionRunner<P>
where
    P: Projection,
{
    /// Connects the event store and checkpoint bucket required by one projection.
    pub async fn connect(
        server: &str,
        config: NatsProjectionConfig,
        projection: P,
    ) -> CatgaResult<Self> {
        let events =
            NatsEventStore::connect(server, config.event_stream, config.event_subject_prefix)
                .await?;
        let checkpoints =
            NatsProjectionCheckpoints::connect(server, config.checkpoint_bucket).await?;
        Ok(Self {
            events,
            checkpoints,
            projection,
            batch_size: None,
        })
    }

    /// Uses an explicit maximum event-store page size for each projection stream.
    pub const fn with_batch_size(mut self, batch_size: NonZeroUsize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Returns the owned projection so callers can inspect its read-model state.
    pub const fn projection(&self) -> &P {
        &self.projection
    }

    /// Applies persisted events that are newer than their durable checkpoints.
    pub async fn run(&self) -> CatgaResult<ProjectionRun> {
        self.runner().run().await
    }

    /// Clears the read model and checkpoints, then replays every persisted event.
    pub async fn rebuild(&self) -> CatgaResult<ProjectionRun> {
        self.runner().rebuild().await
    }

    fn runner(&self) -> CatchUpProjectionRunner<'_, NatsEventStore, NatsProjectionCheckpoints, P> {
        match self.batch_size {
            Some(batch_size) => CatchUpProjectionRunner::with_batch_size(
                &self.events,
                &self.checkpoints,
                &self.projection,
                batch_size,
            ),
            None => CatchUpProjectionRunner::new(&self.events, &self.checkpoints, &self.projection),
        }
    }
}
