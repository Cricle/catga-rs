//! Persistent event-stream subscriptions with per-stream checkpoints.

use std::{num::NonZeroUsize, time::SystemTime};

use async_trait::async_trait;

use crate::{CatgaError, CatgaResult, ErrorCode, EventStore, StoredEvent};

const DEFAULT_BATCH_SIZE: usize = 256;

/// Immutable definition of a durable event-stream subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistentSubscription {
    name: Box<str>,
    stream_pattern: Box<str>,
    event_types: Box<[Box<str>]>,
}

impl PersistentSubscription {
    /// Creates a subscription matching one exact stream, a prefix ending in `*`, or all streams.
    pub fn new(name: impl Into<Box<str>>, stream_pattern: impl Into<Box<str>>) -> Self {
        Self {
            name: name.into(),
            stream_pattern: stream_pattern.into(),
            event_types: Box::new([]),
        }
    }

    /// Restricts the subscription to the supplied serialized event types.
    pub fn with_event_types<T>(mut self, event_types: T) -> Self
    where
        T: IntoIterator,
        T::Item: Into<Box<str>>,
    {
        self.event_types = event_types
            .into_iter()
            .map(Into::into)
            .collect::<Vec<Box<str>>>()
            .into_boxed_slice();
        self
    }

    /// Returns the stable subscription name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the subscribed stream pattern.
    pub fn stream_pattern(&self) -> &str {
        &self.stream_pattern
    }

    /// Returns the optional serialized event-type filter.
    pub fn event_types(&self) -> &[Box<str>] {
        &self.event_types
    }

    /// Returns whether a stream belongs to this subscription.
    pub fn matches_stream(&self, stream_id: &str) -> bool {
        if self.stream_pattern.as_ref() == "*" {
            return true;
        }
        self.stream_pattern
            .strip_suffix('*')
            .map_or(self.stream_pattern.as_ref() == stream_id, |prefix| {
                stream_id.starts_with(prefix)
            })
    }

    /// Returns whether an event type belongs to this subscription.
    pub fn matches_event_type(&self, event_type: &str) -> bool {
        self.event_types.is_empty()
            || self
                .event_types
                .iter()
                .any(|allowed| allowed.as_ref() == event_type)
    }
}

/// Durable progress of one subscription through one event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionCheckpoint {
    subscription_name: Box<str>,
    stream_id: Box<str>,
    version: i64,
    updated_at: SystemTime,
}

impl SubscriptionCheckpoint {
    /// Creates a checkpoint after inspecting the supplied stream version.
    pub fn new(
        subscription_name: impl Into<Box<str>>,
        stream_id: impl Into<Box<str>>,
        version: i64,
    ) -> Self {
        Self {
            subscription_name: subscription_name.into(),
            stream_id: stream_id.into(),
            version,
            updated_at: SystemTime::now(),
        }
    }

    /// Returns the subscription that owns this checkpoint.
    pub fn subscription_name(&self) -> &str {
        &self.subscription_name
    }

    /// Returns the event stream represented by this checkpoint.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Returns the last inspected zero-based stream version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns when this checkpoint was persisted.
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }
}

/// Stores persistent subscriptions, per-stream progress, and short-lived consumer leases.
#[async_trait]
pub trait SubscriptionStore: Send + Sync {
    /// Creates or replaces a subscription definition.
    async fn save(&self, subscription: PersistentSubscription) -> CatgaResult<()>;

    /// Loads one subscription definition.
    async fn load(&self, name: &str) -> CatgaResult<Option<PersistentSubscription>>;

    /// Deletes a subscription definition and its associated state.
    async fn delete(&self, name: &str) -> CatgaResult<()>;

    /// Lists every durable subscription definition.
    async fn list(&self) -> CatgaResult<Vec<PersistentSubscription>>;

    /// Saves the last inspected event version for one subscription and stream.
    async fn save_checkpoint(&self, checkpoint: SubscriptionCheckpoint) -> CatgaResult<()>;

    /// Loads the last inspected event version for one subscription and stream.
    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<SubscriptionCheckpoint>>;

    /// Attempts to acquire one subscription's exclusive competing-consumer lease.
    async fn try_acquire(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<bool>;

    /// Releases a lease only when it still belongs to the named consumer.
    async fn release(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<()>;
}

/// Handles one subscribed stored event.
#[async_trait]
pub trait SubscriptionHandler: Send + Sync {
    /// Applies one event selected by the subscription definition.
    async fn handle(&self, event: &StoredEvent) -> CatgaResult<()>;
}

/// Summary returned after one subscription pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SubscriptionRun {
    handled: usize,
    streams: usize,
}

impl SubscriptionRun {
    /// Returns the number of events delivered to the subscription handler.
    pub const fn handled(&self) -> usize {
        self.handled
    }

    /// Returns the number of matching streams inspected.
    pub const fn streams(&self) -> usize {
        self.streams
    }
}

/// Replays persisted events into one durable subscription definition.
pub struct SubscriptionRunner<'a, E: ?Sized, S: ?Sized, H: ?Sized> {
    events: &'a E,
    subscriptions: &'a S,
    handler: &'a H,
    batch_size: NonZeroUsize,
}

impl<'a, E: ?Sized, S: ?Sized, H: ?Sized> SubscriptionRunner<'a, E, S, H>
where
    E: EventStore,
    S: SubscriptionStore,
    H: SubscriptionHandler,
{
    /// Creates a runner with a bounded default read page size.
    pub fn new(events: &'a E, subscriptions: &'a S, handler: &'a H) -> Self {
        Self::with_batch_size(
            events,
            subscriptions,
            handler,
            NonZeroUsize::new(DEFAULT_BATCH_SIZE)
                .expect("the default subscription batch is non-zero"),
        )
    }

    /// Creates a runner with an explicit bounded read page size.
    pub const fn with_batch_size(
        events: &'a E,
        subscriptions: &'a S,
        handler: &'a H,
        batch_size: NonZeroUsize,
    ) -> Self {
        Self {
            events,
            subscriptions,
            handler,
            batch_size,
        }
    }

    /// Processes every newly persisted event selected by one subscription.
    pub async fn run_once(&self, subscription_name: &str) -> CatgaResult<SubscriptionRun> {
        let subscription = self
            .subscriptions
            .load(subscription_name)
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "subscription does not exist"))?;
        let mut stream_ids = self.events.stream_ids().await?;
        stream_ids.sort_unstable();
        let mut run = SubscriptionRun::default();
        for stream_id in stream_ids
            .into_iter()
            .filter(|stream_id| subscription.matches_stream(stream_id))
        {
            run.streams += 1;
            run.handled += self.run_stream(&subscription, &stream_id).await?;
        }
        Ok(run)
    }

    async fn run_stream(
        &self,
        subscription: &PersistentSubscription,
        stream_id: &str,
    ) -> CatgaResult<usize> {
        let checkpoint = self
            .subscriptions
            .load_checkpoint(subscription.name(), stream_id)
            .await?;
        let mut next_version = checkpoint.map_or(0, |checkpoint| checkpoint.version() + 1);
        let mut handled = 0;
        loop {
            let stream = self
                .events
                .read(
                    stream_id,
                    u64::try_from(next_version).unwrap_or(0),
                    self.batch_size.get(),
                )
                .await?;
            if stream.events().is_empty() {
                return Ok(handled);
            }
            for event in stream.events() {
                if subscription.matches_event_type(event.envelope().message_type()) {
                    self.handler.handle(event).await?;
                    handled += 1;
                }
                self.subscriptions
                    .save_checkpoint(SubscriptionCheckpoint::new(
                        subscription.name(),
                        stream_id,
                        event.version(),
                    ))
                    .await?;
                next_version = event.version() + 1;
            }
            if stream.events().len() < self.batch_size.get() {
                return Ok(handled);
            }
        }
    }
}

/// Runs a subscription only while the caller owns its exclusive consumer lease.
pub struct CompetingSubscriptionRunner<'a, E: ?Sized, S: ?Sized, H: ?Sized> {
    runner: SubscriptionRunner<'a, E, S, H>,
    subscription_name: &'a str,
    consumer_id: &'a str,
}

impl<'a, E: ?Sized, S: ?Sized, H: ?Sized> CompetingSubscriptionRunner<'a, E, S, H>
where
    E: EventStore,
    S: SubscriptionStore,
    H: SubscriptionHandler,
{
    /// Creates a lease-protected runner for one subscription and consumer identity.
    pub fn new(
        events: &'a E,
        subscriptions: &'a S,
        handler: &'a H,
        subscription_name: &'a str,
        consumer_id: &'a str,
    ) -> Self {
        Self {
            runner: SubscriptionRunner::new(events, subscriptions, handler),
            subscription_name,
            consumer_id,
        }
    }

    /// Returns `None` when another consumer owns the lease, otherwise processes one pass.
    pub async fn try_run_once(&self) -> CatgaResult<Option<SubscriptionRun>> {
        if !self
            .runner
            .subscriptions
            .try_acquire(self.subscription_name, self.consumer_id)
            .await?
        {
            return Ok(None);
        }
        let result = self.runner.run_once(self.subscription_name).await;
        let release = self
            .runner
            .subscriptions
            .release(self.subscription_name, self.consumer_id)
            .await;
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(run), Ok(())) => Ok(Some(run)),
        }
    }
}
