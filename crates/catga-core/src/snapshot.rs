//! Typed aggregate-state snapshots for event-sourced streams.

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;

use crate::CatgaResult;

/// An immutable, versioned state snapshot for one event stream.
#[derive(Clone, Debug)]
pub struct Snapshot<S> {
    stream_id: Box<str>,
    state: Arc<S>,
    version: i64,
    timestamp: SystemTime,
}

impl<S> Snapshot<S> {
    /// Creates a snapshot by moving its state into shared immutable ownership.
    pub fn new(stream_id: impl Into<Box<str>>, state: S, version: i64) -> Self {
        Self::from_shared(stream_id, Arc::new(state), version, SystemTime::now())
    }

    /// Creates a snapshot from already shared state and an explicit timestamp.
    pub fn from_shared(
        stream_id: impl Into<Box<str>>,
        state: Arc<S>,
        version: i64,
        timestamp: SystemTime,
    ) -> Self {
        Self {
            stream_id: stream_id.into(),
            state,
            version,
            timestamp,
        }
    }

    /// Returns the event stream represented by this snapshot.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Returns the immutable aggregate state without copying it.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Returns shared ownership of the immutable aggregate state.
    pub fn shared_state(&self) -> Arc<S> {
        Arc::clone(&self.state)
    }

    /// Returns the zero-based event stream version represented by this state.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns when the snapshot was saved.
    pub const fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
}

/// Persists and retrieves the latest typed snapshot for each event stream.
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Saves a snapshot unless a newer snapshot already exists for its stream.
    async fn save<S>(&self, snapshot: Snapshot<S>) -> CatgaResult<()>
    where
        S: Send + Sync + 'static;

    /// Loads the latest snapshot, retaining immutable shared state on success.
    async fn load<S>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<S>>>
    where
        S: Send + Sync + 'static;

    /// Removes the latest snapshot for one stream.
    async fn delete(&self, stream_id: &str) -> CatgaResult<()>;
}
