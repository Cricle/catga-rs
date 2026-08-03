//! Lock-free in-memory read-model change tracking and shared state storage.

use std::sync::Arc;

use async_trait::async_trait;
use crate::{
    CatgaResult, ChangeRecord, ChangeTracker, ReadModelStore, validate_read_model_page_size,
};
use dashmap::DashMap;

/// A sharded in-memory tracker that releases each change after it is acknowledged.
#[derive(Default)]
pub struct MemoryChangeTracker {
    changes: DashMap<Box<str>, ChangeRecord>,
}

#[async_trait]
impl ChangeTracker for MemoryChangeTracker {
    fn track(&self, change: ChangeRecord) {
        self.changes.insert(change.id().into(), change);
    }
    async fn pending_page(&self, max_count: usize) -> CatgaResult<Vec<ChangeRecord>> {
        validate_read_model_page_size(max_count)?;
        Ok(self
            .changes
            .iter()
            .take(max_count)
            .map(|change| change.value().clone())
            .collect())
    }
    async fn mark_synced(&self, change_ids: &[Box<str>]) -> CatgaResult<()> {
        for id in change_ids {
            self.changes.remove(id.as_ref());
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
