use std::{any::Any, sync::Arc, time::SystemTime};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Snapshot, SnapshotStore};
use dashmap::DashMap;

/// A lock-free, single-latest-snapshot store for development and deterministic tests.
#[derive(Default)]
pub struct MemorySnapshots {
    streams: DashMap<Box<str>, Arc<MemorySnapshotSlot>>,
}

struct MemorySnapshotSlot {
    snapshot: ArcSwap<SnapshotEntry>,
}

struct SnapshotEntry {
    state: Option<Arc<dyn Any + Send + Sync>>,
    version: i64,
    timestamp: SystemTime,
}

impl Default for MemorySnapshotSlot {
    fn default() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(SnapshotEntry {
                state: None,
                version: -1,
                timestamp: SystemTime::UNIX_EPOCH,
            }),
        }
    }
}

#[async_trait]
impl SnapshotStore for MemorySnapshots {
    async fn save<S>(&self, snapshot: Snapshot<S>) -> CatgaResult<()>
    where
        S: Send + Sync + 'static,
    {
        let slot = self
            .streams
            .entry(snapshot.stream_id().into())
            .or_default()
            .clone();
        slot.save(snapshot)
    }

    async fn load<S>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<S>>>
    where
        S: Send + Sync + 'static,
    {
        let Some(slot) = self.streams.get(stream_id) else {
            return Ok(None);
        };
        slot.load(stream_id)
    }

    async fn delete(&self, stream_id: &str) -> CatgaResult<()> {
        self.streams.remove(stream_id);
        Ok(())
    }
}

impl MemorySnapshotSlot {
    fn save<S>(&self, snapshot: Snapshot<S>) -> CatgaResult<()>
    where
        S: Send + Sync + 'static,
    {
        let version = snapshot.version();
        let timestamp = snapshot.timestamp();
        let state: Arc<dyn Any + Send + Sync> = snapshot.shared_state();
        let next = Arc::new(SnapshotEntry {
            state: Some(state),
            version,
            timestamp,
        });
        loop {
            let current = self.snapshot.load_full();
            if current.version > version {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "a newer snapshot already exists for this stream",
                ));
            }
            let previous = self.snapshot.compare_and_swap(&current, Arc::clone(&next));
            if Arc::ptr_eq(&*previous, &current) {
                return Ok(());
            }
        }
    }

    fn load<S>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<S>>>
    where
        S: Send + Sync + 'static,
    {
        let current = self.snapshot.load_full();
        let Some(state) = &current.state else {
            return Ok(None);
        };
        let state = Arc::clone(state).downcast::<S>().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "the requested snapshot state type does not match the stored state",
            )
        })?;
        Ok(Some(Snapshot::from_shared(
            stream_id,
            state,
            current.version,
            current.timestamp,
        )))
    }
}
