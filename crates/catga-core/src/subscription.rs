//! Persistent event-stream subscriptions with per-stream checkpoints.

use std::{
    num::NonZeroUsize,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    CatgaError, CatgaResult, ErrorCode, EventStore, MAX_EVENT_STORE_PAGE_SIZE, StoredEvent,
};

const DEFAULT_BATCH_SIZE: usize = 256;

/// Timing configuration for a caller-owned continuous subscription loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionLoopOptions {
    poll_interval: Duration,
}

impl SubscriptionLoopOptions {
    /// Creates loop options with a nonzero interval between completed subscription passes.
    pub fn new(poll_interval: Duration) -> CatgaResult<Self> {
        if poll_interval.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "subscription poll interval must be greater than zero",
            ));
        }
        Ok(Self { poll_interval })
    }

    /// Returns the delay after a completed subscription pass.
    pub const fn poll_interval(self) -> Duration {
        self.poll_interval
    }
}

impl Default for SubscriptionLoopOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
        }
    }
}

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

    fn combine(&mut self, other: Self) {
        self.handled = self.handled.saturating_add(other.handled);
        self.streams = self.streams.saturating_add(other.streams);
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
            NonZeroUsize::new(DEFAULT_BATCH_SIZE).unwrap_or(NonZeroUsize::MIN),
        )
    }

    /// Creates a runner with an explicit bounded read page size.
    ///
    /// Values above [`MAX_EVENT_STORE_PAGE_SIZE`] are capped to the store-wide limit.
    pub fn with_batch_size(
        events: &'a E,
        subscriptions: &'a S,
        handler: &'a H,
        batch_size: NonZeroUsize,
    ) -> Self {
        Self {
            events,
            subscriptions,
            handler,
            batch_size: NonZeroUsize::new(batch_size.get().min(MAX_EVENT_STORE_PAGE_SIZE))
                .unwrap_or(NonZeroUsize::MIN),
        }
    }

    /// Processes every newly persisted event selected by one subscription.
    ///
    /// A checkpoint at [`i64::MAX`] is terminal because the signed event-version domain has no
    /// later value. The runner treats that stream as complete instead of overflowing while
    /// calculating a follow-up read position.
    pub async fn run_once(&self, subscription_name: &str) -> CatgaResult<SubscriptionRun> {
        let subscription = self
            .subscriptions
            .load(subscription_name)
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "subscription does not exist"))?;
        let mut run = SubscriptionRun::default();
        let mut after = None;
        loop {
            let page = self
                .events
                .stream_ids_page(after.as_deref(), MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for stream_id in page
                .ids()
                .iter()
                .filter(|stream_id| subscription.matches_stream(stream_id))
            {
                run.streams += 1;
                run.handled += self.run_stream(&subscription, stream_id).await?;
            }
            let Some(next) = page.next_stream_id() else {
                break;
            };
            after = Some(next.to_owned());
        }
        Ok(run)
    }

    /// Repeatedly processes a subscription until `shutdown` is cancelled.
    ///
    /// The first pass starts immediately. Cancellation is observed before each later pass and
    /// while waiting for [`SubscriptionLoopOptions::poll_interval`]; an already-started pass
    /// completes so its successfully handled events can persist their checkpoints. This method
    /// creates no task, so callers retain supervision and shutdown ownership of its future.
    pub async fn run_until_cancelled(
        &self,
        subscription_name: &str,
        options: SubscriptionLoopOptions,
        shutdown: CancellationToken,
    ) -> CatgaResult<SubscriptionRun> {
        let mut total = SubscriptionRun::default();
        while !shutdown.is_cancelled() {
            total.combine(self.run_once(subscription_name).await?);
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(options.poll_interval()) => {}
            }
        }
        Ok(total)
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
        let mut handled = 0;
        let Some(mut next_version) =
            checkpoint.map_or(Some(0), |checkpoint| advance_version(checkpoint.version()))
        else {
            return Ok(handled);
        };
        loop {
            let page = self
                .events
                .read_page(
                    stream_id,
                    u64::try_from(next_version).unwrap_or(0),
                    self.batch_size.get(),
                )
                .await?;
            if page.stream().events().is_empty() {
                return Ok(handled);
            }
            for event in page.stream().events() {
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
                let Some(next) = advance_version(event.version()) else {
                    return Ok(handled);
                };
                next_version = next;
            }
            if page.next_version().is_none() {
                return Ok(handled);
            }
        }
    }

    async fn run_next(&self, subscription_name: &str) -> CatgaResult<bool> {
        let subscription = self
            .subscriptions
            .load(subscription_name)
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "subscription does not exist"))?;
        let mut after = None;
        loop {
            let page = self
                .events
                .stream_ids_page(after.as_deref(), MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for stream_id in page
                .ids()
                .iter()
                .filter(|stream_id| subscription.matches_stream(stream_id))
            {
                if self.run_next_in_stream(&subscription, stream_id).await? {
                    return Ok(true);
                }
            }
            let Some(next) = page.next_stream_id() else {
                break;
            };
            after = Some(next.to_owned());
        }
        Ok(false)
    }

    async fn run_next_in_stream(
        &self,
        subscription: &PersistentSubscription,
        stream_id: &str,
    ) -> CatgaResult<bool> {
        let checkpoint = self
            .subscriptions
            .load_checkpoint(subscription.name(), stream_id)
            .await?;
        let Some(mut next_version) =
            checkpoint.map_or(Some(0), |checkpoint| advance_version(checkpoint.version()))
        else {
            return Ok(false);
        };
        loop {
            let page = self
                .events
                .read_page(
                    stream_id,
                    u64::try_from(next_version).unwrap_or(0),
                    self.batch_size.get(),
                )
                .await?;
            if page.stream().events().is_empty() {
                return Ok(false);
            }
            for event in page.stream().events() {
                let selected = subscription.matches_event_type(event.envelope().message_type());
                if selected {
                    self.handler.handle(event).await?;
                }
                self.subscriptions
                    .save_checkpoint(SubscriptionCheckpoint::new(
                        subscription.name(),
                        stream_id,
                        event.version(),
                    ))
                    .await?;
                if selected {
                    return Ok(true);
                }
                let Some(next) = advance_version(event.version()) else {
                    return Ok(false);
                };
                next_version = next;
            }
            if page.next_version().is_none() {
                return Ok(false);
            }
        }
    }
}

fn advance_version(version: i64) -> Option<i64> {
    version.checked_add(1)
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

    /// Attempts to handle at most one selected event while holding this consumer's lease.
    ///
    /// `Ok(None)` means another consumer currently owns the lease. `Ok(Some(true))` means one
    /// matching event was handled and checkpointed. `Ok(Some(false))` means this consumer
    /// acquired and released the lease without finding any matching pending event. Filtered
    /// events still advance their per-stream checkpoints, so repeated calls do not rescan them.
    pub async fn try_process_next(&self) -> CatgaResult<Option<bool>> {
        if !self
            .runner
            .subscriptions
            .try_acquire(self.subscription_name, self.consumer_id)
            .await?
        {
            return Ok(None);
        }
        let result = self.runner.run_next(self.subscription_name).await;
        let release = self
            .runner
            .subscriptions
            .release(self.subscription_name, self.consumer_id)
            .await;
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(processed), Ok(())) => Ok(Some(processed)),
        }
    }
}
