//! Aggregate state reconstruction at historical event versions and timestamps.

use std::marker::PhantomData;

use crate::{
    Aggregate, CatgaError, CatgaResult, EnhancedSnapshotStore, ErrorCode, EventStore, EventStream,
    VersionInfo,
};

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

    /// Returns aggregate state at the inclusive event-stream version.
    pub async fn state_at_version(&self, id: &str, version: i64) -> CatgaResult<Option<A>> {
        if version < 0 {
            return Ok(None);
        }
        let stream_id = A::stream_id(id);
        let stream = self.events.read_to_version(&stream_id, version).await?;
        Self::rebuild(id, &stream)
    }

    /// Returns aggregate state after every event stored no later than the timestamp.
    pub async fn state_at_time(
        &self,
        id: &str,
        upper_bound: std::time::SystemTime,
    ) -> CatgaResult<Option<A>> {
        let stream_id = A::stream_id(id);
        let stream = self.events.read_to_time(&stream_id, upper_bound).await?;
        Self::rebuild(id, &stream)
    }

    /// Returns lightweight version metadata for one aggregate stream.
    pub async fn version_history(&self, id: &str) -> CatgaResult<Vec<VersionInfo>> {
        self.events.version_history(&A::stream_id(id)).await
    }

    /// Reconstructs and compares state at two inclusive versions.
    pub async fn compare_versions(
        &self,
        id: &str,
        from_version: i64,
        to_version: i64,
    ) -> CatgaResult<StateComparison<A>> {
        if from_version > to_version {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "comparison starting version must not exceed the ending version",
            ));
        }
        let stream_id = A::stream_id(id);
        let from_stream = if from_version < 0 {
            EventStream::new(stream_id.as_ref(), -1, Vec::new())
        } else {
            self.events
                .read_to_version(&stream_id, from_version)
                .await?
        };
        let to_stream = if to_version < 0 {
            EventStream::new(stream_id.as_ref(), -1, Vec::new())
        } else {
            self.events.read_to_version(&stream_id, to_version).await?
        };
        let events_between = to_stream
            .events()
            .iter()
            .filter(|event| event.version() > from_version && event.version() <= to_version)
            .map(|event| {
                VersionInfo::new(
                    event.version(),
                    event.timestamp(),
                    event.envelope().message_type(),
                )
            })
            .collect();
        Ok(StateComparison {
            from_state: Self::rebuild(id, &from_stream)?,
            to_state: Self::rebuild(id, &to_stream)?,
            from_version,
            to_version,
            events_between,
        })
    }

    fn rebuild(id: &str, stream: &EventStream) -> CatgaResult<Option<A>> {
        if stream.events().is_empty() {
            return Ok(None);
        }
        let mut aggregate = A::new(id);
        for event in stream.events() {
            aggregate.apply(event.envelope())?;
            if aggregate.version() != event.version() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "aggregate apply did not advance to the stored event version",
                ));
            }
        }
        Ok(Some(aggregate))
    }
}

/// Rebuilds historical aggregate state from the latest compatible snapshot and later events.
///
/// Unlike [`TimeTravelService`], this service requires an
/// [`EnhancedSnapshotStore`] at construction, making its snapshot dependency
/// explicit in the type system. It shares an immutable snapshot through
/// [`std::sync::Arc`] and clones the aggregate once before applying only the
/// events that follow that snapshot. A malformed snapshot whose embedded
/// aggregate version differs from its stored version is rejected.
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
        let stream = self.events.read_to_version(&stream_id, version).await?;
        self.rebuild(id, version, &stream).await
    }

    /// Returns aggregate state after all events persisted no later than `upper_bound`.
    pub async fn state_at_time(
        &self,
        id: &str,
        upper_bound: std::time::SystemTime,
    ) -> CatgaResult<Option<A>> {
        let stream_id = A::stream_id(id);
        let stream = self.events.read_to_time(&stream_id, upper_bound).await?;
        if stream.events().is_empty() {
            return Ok(None);
        }
        self.rebuild(id, stream.version(), &stream).await
    }

    /// Returns lightweight version metadata for one aggregate stream.
    pub async fn version_history(&self, id: &str) -> CatgaResult<Vec<VersionInfo>> {
        self.events.version_history(&A::stream_id(id)).await
    }

    /// Reconstructs and compares state at two inclusive versions using snapshots for both states.
    pub async fn compare_versions(
        &self,
        id: &str,
        from_version: i64,
        to_version: i64,
    ) -> CatgaResult<StateComparison<A>> {
        if from_version > to_version {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "comparison starting version must not exceed the ending version",
            ));
        }
        let from_state = self.state_at_version(id, from_version).await?;
        let to_state = self.state_at_version(id, to_version).await?;
        let stream = if to_version < 0 {
            EventStream::new(A::stream_id(id), -1, Vec::new())
        } else {
            self.events
                .read_to_version(&A::stream_id(id), to_version)
                .await?
        };
        let events_between = stream
            .events()
            .iter()
            .filter(|event| event.version() > from_version && event.version() <= to_version)
            .map(|event| {
                VersionInfo::new(
                    event.version(),
                    event.timestamp(),
                    event.envelope().message_type(),
                )
            })
            .collect();
        Ok(StateComparison {
            from_state,
            to_state,
            from_version,
            to_version,
            events_between,
        })
    }

    async fn rebuild(
        &self,
        id: &str,
        target_version: i64,
        stream: &EventStream,
    ) -> CatgaResult<Option<A>> {
        let stream_id = A::stream_id(id);
        let snapshot = self
            .snapshots
            .load_at_version::<A>(&stream_id, target_version)
            .await?;
        let (mut aggregate, from_version) = match snapshot {
            Some(snapshot) => {
                let aggregate = (*snapshot.shared_state()).clone();
                if aggregate.version() != snapshot.version() {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "aggregate snapshot version does not match its state",
                    ));
                }
                (aggregate, snapshot.version().saturating_add(1))
            }
            None if stream.events().is_empty() => return Ok(None),
            None => (A::new(id), 0),
        };
        for event in stream
            .events()
            .iter()
            .filter(|event| event.version() >= from_version && event.version() <= target_version)
        {
            aggregate.apply(event.envelope())?;
            if aggregate.version() != event.version() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "aggregate apply did not advance to the stored event version",
                ));
            }
        }
        Ok(Some(aggregate))
    }
}
