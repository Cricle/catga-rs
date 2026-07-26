//! Aggregate state reconstruction at historical event versions and timestamps.

use std::{marker::PhantomData, time::SystemTime};

use crate::aggregate::next_event_version;
use crate::{
    Aggregate, CatgaError, CatgaResult, EnhancedSnapshotStore, ErrorCode, EventStore,
    MAX_EVENT_STORE_PAGE_SIZE, VersionHistoryPage, VersionInfo,
};

/// Maximum event metadata records retained by one [`StateComparison`].
///
/// [`TimeTravelService::compare_versions`] reconstructs both requested states with bounded event
/// pages, but its caller-owned comparison result also contains event metadata. Limiting that
/// output prevents a wide version range from materializing an unbounded history. Call
/// [`TimeTravelService::version_history_page`] to process a larger history incrementally.
pub const MAX_STATE_COMPARISON_EVENTS: usize = MAX_EVENT_STORE_PAGE_SIZE;

/// A pair of reconstructed aggregate states and the events between their versions.
#[derive(Clone, Debug)]
pub struct StateComparison<A> {
    from_state: Option<A>,
    to_state: Option<A>,
    from_version: i64,
    to_version: i64,
    events_between: Vec<VersionInfo>,
}

impl<A> StateComparison<A> {
    /// Returns the state at the inclusive starting version.
    pub fn from_state(&self) -> Option<&A> {
        self.from_state.as_ref()
    }
    /// Returns the state at the inclusive ending version.
    pub fn to_state(&self) -> Option<&A> {
        self.to_state.as_ref()
    }
    /// Returns the requested starting version.
    pub const fn from_version(&self) -> i64 {
        self.from_version
    }
    /// Returns the requested ending version.
    pub const fn to_version(&self) -> i64 {
        self.to_version
    }
    /// Returns lightweight metadata for events in `(from_version, to_version]`.
    ///
    /// This is caller-owned comparison output. Large comparisons should instead iterate
    /// [`TimeTravelService::version_history_page`] to keep caller memory bounded.
    pub fn events_between(&self) -> &[VersionInfo] {
        &self.events_between
    }
}

/// Rebuilds event-sourced aggregates at historical stream boundaries.
pub struct TimeTravelService<'a, A, E: ?Sized> {
    events: &'a E,
    aggregate: PhantomData<A>,
}

impl<'a, A, E: ?Sized> TimeTravelService<'a, A, E>
where
    A: Aggregate,
    E: EventStore,
{
    /// Creates a time-travel service over one event store.
    pub const fn new(events: &'a E) -> Self {
        Self {
            events,
            aggregate: PhantomData,
        }
    }

    /// Returns aggregate state at the inclusive event-stream version using bounded pages.
    pub async fn state_at_version(&self, id: &str, version: i64) -> CatgaResult<Option<A>> {
        if version < 0 {
            return Ok(None);
        }
        let stream_id = A::stream_id(id);
        self.rebuild_version(id, &stream_id, version).await
    }

    /// Returns aggregate state after every event stored no later than the timestamp.
    pub async fn state_at_time(&self, id: &str, upper_bound: SystemTime) -> CatgaResult<Option<A>> {
        self.rebuild_time(id, &A::stream_id(id), upper_bound).await
    }

    /// Reads one bounded page of lightweight version metadata for one aggregate stream.
    pub async fn version_history_page(
        &self,
        id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        self.events
            .version_history_page(&A::stream_id(id), from_version, max_count)
            .await
    }

    /// Reconstructs and compares state at two inclusive versions.
    pub async fn compare_versions(
        &self,
        id: &str,
        from_version: i64,
        to_version: i64,
    ) -> CatgaResult<StateComparison<A>> {
        validate_comparison(from_version, to_version)?;
        let stream_id = A::stream_id(id);
        let from_state = self.state_at_version(id, from_version).await?;
        let to_state = self.state_at_version(id, to_version).await?;
        let events_between = self
            .comparison_history(&stream_id, from_version, to_version)
            .await?;
        Ok(StateComparison {
            from_state,
            to_state,
            from_version,
            to_version,
            events_between,
        })
    }

    async fn rebuild_version(
        &self,
        id: &str,
        stream_id: &str,
        target: i64,
    ) -> CatgaResult<Option<A>> {
        let mut aggregate = A::new(id);
        let mut found = false;
        let mut cursor = 0;
        loop {
            let page = self
                .events
                .read_to_version_page(stream_id, cursor, target, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for event in page.stream().events() {
                found = true;
                apply_event(&mut aggregate, event.envelope(), event.version())?;
            }
            let Some(next) = page.next_version() else {
                break;
            };
            cursor = next;
        }
        Ok(found.then_some(aggregate))
    }

    async fn rebuild_time(
        &self,
        id: &str,
        stream_id: &str,
        upper_bound: SystemTime,
    ) -> CatgaResult<Option<A>> {
        let mut aggregate = A::new(id);
        let mut found = false;
        let mut cursor = 0;
        loop {
            let page = self
                .events
                .read_to_time_page(stream_id, cursor, upper_bound, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for event in page.stream().events() {
                found = true;
                apply_event(&mut aggregate, event.envelope(), event.version())?;
            }
            let Some(next) = page.next_version() else {
                break;
            };
            cursor = next;
        }
        Ok(found.then_some(aggregate))
    }

    async fn comparison_history(
        &self,
        stream_id: &str,
        from: i64,
        to: i64,
    ) -> CatgaResult<Vec<VersionInfo>> {
        if to < 0 {
            return Ok(Vec::new());
        }
        let mut cursor = 0;
        let mut history = Vec::with_capacity(MAX_STATE_COMPARISON_EVENTS);
        loop {
            let page = self
                .events
                .read_to_version_page(stream_id, cursor, to, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for event in page
                .stream()
                .events()
                .iter()
                .filter(|event| event.version() > from)
            {
                if history.len() == MAX_STATE_COMPARISON_EVENTS {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "state comparison history exceeds the bounded result limit",
                    ));
                }
                history.push(VersionInfo::new(
                    event.version(),
                    event.timestamp(),
                    event.envelope().message_type(),
                ));
            }
            let Some(next) = page.next_version() else {
                break;
            };
            cursor = next;
        }
        Ok(history)
    }
}

/// Rebuilds historical aggregate state from the latest compatible snapshot and later events.
pub struct SnapshotTimeTravelService<'a, A, E: ?Sized, S: ?Sized> {
    events: &'a E,
    snapshots: &'a S,
    aggregate: PhantomData<A>,
}

impl<'a, A, E: ?Sized, S: ?Sized> SnapshotTimeTravelService<'a, A, E, S>
where
    A: Aggregate,
    E: EventStore,
    S: EnhancedSnapshotStore,
{
    /// Creates a time-travel service that uses `snapshots` whenever possible.
    pub const fn new(events: &'a E, snapshots: &'a S) -> Self {
        Self {
            events,
            snapshots,
            aggregate: PhantomData,
        }
    }

    /// Returns aggregate state at an inclusive stream version using a snapshot at or before it.
    pub async fn state_at_version(&self, id: &str, version: i64) -> CatgaResult<Option<A>> {
        if version < 0 {
            return Ok(None);
        }
        let stream_id = A::stream_id(id);
        let snapshot = self
            .snapshots
            .load_at_version::<A>(&stream_id, version)
            .await?;
        let (mut aggregate, next_cursor, mut found) = snapshot_state(id, snapshot)?;
        let Some(mut cursor) = next_cursor else {
            return Ok(found.then_some(aggregate));
        };
        loop {
            let page = self
                .events
                .read_to_version_page(&stream_id, cursor, version, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for event in page.stream().events() {
                found = true;
                apply_event(&mut aggregate, event.envelope(), event.version())?;
            }
            let Some(next) = page.next_version() else {
                break;
            };
            cursor = next;
        }
        Ok(found.then_some(aggregate))
    }

    /// Returns aggregate state after all events persisted no later than `upper_bound`.
    pub async fn state_at_time(&self, id: &str, upper_bound: SystemTime) -> CatgaResult<Option<A>> {
        let stream_id = A::stream_id(id);
        let mut cursor = 0;
        let mut target = None;
        loop {
            let page = self
                .events
                .read_to_time_page(&stream_id, cursor, upper_bound, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            if let Some(event) = page.stream().events().last() {
                target = Some(event.version());
            }
            let Some(next) = page.next_version() else {
                break;
            };
            cursor = next;
        }
        let Some(target) = target else {
            return Ok(None);
        };
        let snapshot = self
            .snapshots
            .load_at_version::<A>(&stream_id, target)
            .await?;
        let (mut aggregate, next_cursor, _) = snapshot_state(id, snapshot)?;
        let Some(mut cursor) = next_cursor else {
            return Ok(Some(aggregate));
        };
        let from_version = i64::try_from(cursor).unwrap_or(i64::MAX);
        loop {
            let page = self
                .events
                .read_to_time_page(&stream_id, cursor, upper_bound, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for event in page
                .stream()
                .events()
                .iter()
                .filter(|event| event.version() >= from_version)
            {
                apply_event(&mut aggregate, event.envelope(), event.version())?;
            }
            let Some(next) = page.next_version() else {
                break;
            };
            cursor = next;
        }
        Ok(Some(aggregate))
    }

    /// Reads one bounded page of lightweight version metadata for one aggregate stream.
    pub async fn version_history_page(
        &self,
        id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        self.events
            .version_history_page(&A::stream_id(id), from_version, max_count)
            .await
    }

    /// Reconstructs and compares state at two inclusive versions using snapshots for both states.
    pub async fn compare_versions(
        &self,
        id: &str,
        from_version: i64,
        to_version: i64,
    ) -> CatgaResult<StateComparison<A>> {
        validate_comparison(from_version, to_version)?;
        let from_state = self.state_at_version(id, from_version).await?;
        let to_state = self.state_at_version(id, to_version).await?;
        let helper = TimeTravelService::<A, E>::new(self.events);
        let events_between = helper
            .comparison_history(&A::stream_id(id), from_version, to_version)
            .await?;
        Ok(StateComparison {
            from_state,
            to_state,
            from_version,
            to_version,
            events_between,
        })
    }
}

fn validate_comparison(from_version: i64, to_version: i64) -> CatgaResult<()> {
    if from_version > to_version {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "comparison starting version must not exceed the ending version",
        ));
    }
    Ok(())
}

fn apply_event<A: Aggregate>(
    aggregate: &mut A,
    envelope: &crate::Envelope,
    version: i64,
) -> CatgaResult<()> {
    aggregate.apply(envelope)?;
    if aggregate.version() != version {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "aggregate apply did not advance to the stored event version",
        ));
    }
    Ok(())
}

fn snapshot_state<A: Aggregate>(
    id: &str,
    snapshot: Option<crate::Snapshot<A>>,
) -> CatgaResult<(A, Option<u64>, bool)> {
    match snapshot {
        Some(snapshot) => {
            let aggregate = (*snapshot.shared_state()).clone();
            if aggregate.version() != snapshot.version() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "aggregate snapshot version does not match its state",
                ));
            }
            Ok((aggregate, next_event_version(snapshot.version()), true))
        }
        None => Ok((A::new(id), Some(0), false)),
    }
}
