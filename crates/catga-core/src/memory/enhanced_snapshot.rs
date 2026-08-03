//! Lock-free, copy-on-write historical snapshots for in-memory streams.

use std::{any::Any, collections::BTreeMap, sync::Arc, time::SystemTime};

use crate::{
    CatgaError, CatgaResult, EnhancedSnapshotStore, ErrorCode, Snapshot, SnapshotInfo,
    SnapshotStore,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use dashmap::DashMap;

/// A multi-version in-memory snapshot store with lock-free reads and CAS writes.
#[derive(Default)]
pub struct MemoryEnhancedSnapshots {
    streams: DashMap<Box<str>, Arc<MemoryEnhancedSnapshotSlot>>,
}

struct MemoryEnhancedSnapshotSlot {
    history: ArcSwap<SnapshotHistory>,
}

#[derive(Clone, Default)]
struct SnapshotHistory {
    entries: BTreeMap<i64, SnapshotEntry>,
}

#[derive(Clone)]
struct SnapshotEntry {
    state: Arc<dyn Any + Send + Sync>,
    timestamp: SystemTime,
}

impl Default for MemoryEnhancedSnapshotSlot {
    fn default() -> Self {
        Self {
            history: ArcSwap::from_pointee(SnapshotHistory::default()),
        }
    }
}

#[async_trait]
impl SnapshotStore for MemoryEnhancedSnapshots {
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
        let Some(slot) = self.streams.get(stream_id).map(|slot| Arc::clone(&slot)) else {
            return Ok(None);
        };
        slot.load_latest(stream_id)
    }

    async fn delete(&self, stream_id: &str) -> CatgaResult<()> {
        self.streams.remove(stream_id);
        Ok(())
    }
}

#[async_trait]
impl EnhancedSnapshotStore for MemoryEnhancedSnapshots {
    async fn load_at_version<S>(
        &self,
        stream_id: &str,
        version: i64,
    ) -> CatgaResult<Option<Snapshot<S>>>
    where
        S: Send + Sync + 'static,
    {
        let Some(slot) = self.streams.get(stream_id).map(|slot| Arc::clone(&slot)) else {
            return Ok(None);
        };
        slot.load_at_version(stream_id, version)
    }

    async fn history(&self, stream_id: &str) -> CatgaResult<Vec<SnapshotInfo>> {
        let Some(slot) = self.streams.get(stream_id).map(|slot| Arc::clone(&slot)) else {
            return Ok(Vec::new());
        };
        Ok(slot
            .history
            .load()
            .entries
            .iter()
            .map(|(&version, entry)| SnapshotInfo::new(version, entry.timestamp))
            .collect())
    }

    async fn delete_before_version(&self, stream_id: &str, version: i64) -> CatgaResult<()> {
        let Some(slot) = self.streams.get(stream_id).map(|slot| Arc::clone(&slot)) else {
            return Ok(());
        };
        slot.update(|current| {
            if current
                .entries
                .first_key_value()
                .is_none_or(|(&oldest, _)| oldest >= version)
            {
                return None;
            }
            let mut next = current.clone();
            next.entries
                .retain(|saved_version, _| *saved_version >= version);
            Some(next)
        });
        Ok(())
    }

    async fn cleanup(&self, stream_id: &str, keep_count: usize) -> CatgaResult<()> {
        let Some(slot) = self.streams.get(stream_id).map(|slot| Arc::clone(&slot)) else {
            return Ok(());
        };
        slot.update(|current| {
            if current.entries.len() <= keep_count {
                return None;
            }
            let mut next = current.clone();
            while next.entries.len() > keep_count {
                next.entries.pop_first();
            }
            Some(next)
        });
        Ok(())
    }
}

impl MemoryEnhancedSnapshotSlot {
    fn save<S>(&self, snapshot: Snapshot<S>) -> CatgaResult<()>
    where
        S: Send + Sync + 'static,
    {
        let version = snapshot.version();
        let entry = SnapshotEntry {
            state: snapshot.shared_state(),
            timestamp: snapshot.timestamp(),
        };
        self.update_result(|current| {
            if current
                .entries
                .last_key_value()
                .is_some_and(|(&latest, _)| latest > version)
            {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "a newer snapshot already exists for this stream",
                ));
            }
            let mut next = current.clone();
            next.entries.insert(version, entry.clone());
            Ok(next)
        })
    }

    fn load_latest<S>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<S>>>
    where
        S: Send + Sync + 'static,
    {
        let current = self.history.load();
        let Some((&version, entry)) = current.entries.last_key_value() else {
            return Ok(None);
        };
        Self::typed_snapshot(stream_id, version, entry)
    }

    fn load_at_version<S>(&self, stream_id: &str, version: i64) -> CatgaResult<Option<Snapshot<S>>>
    where
        S: Send + Sync + 'static,
    {
        let current = self.history.load();
        let Some((&saved_version, entry)) = current.entries.range(..=version).next_back() else {
            return Ok(None);
        };
        Self::typed_snapshot(stream_id, saved_version, entry)
    }

    fn typed_snapshot<S>(
        stream_id: &str,
        version: i64,
        entry: &SnapshotEntry,
    ) -> CatgaResult<Option<Snapshot<S>>>
    where
        S: Send + Sync + 'static,
    {
        let state = Arc::clone(&entry.state).downcast::<S>().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "the requested snapshot state type does not match the stored state",
            )
        })?;
        Ok(Some(Snapshot::from_shared(
            stream_id,
            state,
            version,
            entry.timestamp,
        )))
    }

    fn update(&self, transform: impl Fn(&SnapshotHistory) -> Option<SnapshotHistory>) {
        loop {
            let current = self.history.load_full();
            let Some(next) = transform(&current) else {
                return;
            };
            let next = Arc::new(next);
            let previous = self.history.compare_and_swap(&current, next);
            if Arc::ptr_eq(&*previous, &current) {
                return;
            }
        }
    }

    fn update_result(
        &self,
        transform: impl Fn(&SnapshotHistory) -> CatgaResult<SnapshotHistory>,
    ) -> CatgaResult<()> {
        loop {
            let current = self.history.load_full();
            let next = Arc::new(transform(&current)?);
            let previous = self.history.compare_and_swap(&current, next);
            if Arc::ptr_eq(&*previous, &current) {
                return Ok(());
            }
        }
    }
}
