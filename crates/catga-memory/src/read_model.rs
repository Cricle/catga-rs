//! Lock-free in-memory read-model change tracking and shared state storage.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use catga_core::{CatgaResult, ChangeRecord, ChangeTracker, ReadModelStore};
use dashmap::DashMap;

/// A sharded in-memory tracker with per-change atomic completion state.
#[derive(Default)]
pub struct MemoryChangeTracker {
    changes: DashMap<Box<str>, Arc<TrackedChange>>,
}

struct TrackedChange {
    record: ChangeRecord,
    synced: AtomicBool,
}

#[async_trait]
impl ChangeTracker for MemoryChangeTracker {
    fn track(&self, change: ChangeRecord) {
        self.changes.insert(
            change.id().into(),
            Arc::new(TrackedChange {
                record: change,
                synced: AtomicBool::new(false),
            }),
        );
    }
    async fn pending(&self) -> CatgaResult<Vec<ChangeRecord>> {
        Ok(self
            .changes
            .iter()
            .filter(|change| !change.synced.load(Ordering::Acquire))
            .map(|change| change.record.clone())
            .collect())
    }
    async fn mark_synced(&self, change_ids: &[Box<str>]) -> CatgaResult<()> {
        for id in change_ids {
            if let Some(change) = self.changes.get(id.as_ref()) {
                change.synced.store(true, Ordering::Release);
            }
        }
        Ok(())
    }
}

/// A sharded read-model table retaining values in shared immutable ownership.
pub struct MemoryReadModels<M> {
    models: DashMap<Box<str>, Arc<M>>,
}

impl<M> Default for MemoryReadModels<M> {
    fn default() -> Self {
        Self {
            models: DashMap::new(),
        }
    }
}

#[async_trait]
impl<M> ReadModelStore<M> for MemoryReadModels<M>
where
    M: Send + Sync + 'static,
{
    async fn get(&self, id: &str) -> CatgaResult<Option<Arc<M>>> {
        Ok(self.models.get(id).map(|model| Arc::clone(&model)))
    }
    async fn save(&self, id: &str, model: Arc<M>) -> CatgaResult<()> {
        self.models.insert(id.into(), model);
        Ok(())
    }
    async fn delete(&self, id: &str) -> CatgaResult<()> {
        self.models.remove(id);
        Ok(())
    }
}
