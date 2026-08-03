//! Lock-free, versioned in-memory progress for recoverable DSL flow steps.

use crate::CatgaResult;
use crate::flow::dsl_progress::{DslStepProgress, DslStepProgressStore};
use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry};

/// A sharded in-memory store of explicitly encoded DSL step progress.
#[derive(Default)]
pub struct MemoryDslStepProgress {
    progress: DashMap<(Box<str>, u32), DslStepProgress>,
}

#[async_trait]
impl DslStepProgressStore for MemoryDslStepProgress {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        Ok(match self.progress.entry(key(&progress)) {
            Entry::Vacant(entry) => {
                entry.insert(progress);
                true
            }
            Entry::Occupied(_) => false,
        })
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        if !DslStepProgress::is_next_version(expected_version, next.version()) {
            return Ok(false);
        }
        match self.progress.entry(key(&next)) {
            Entry::Occupied(mut entry) if entry.get().version() == expected_version => {
                entry.insert(next);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        Ok(self
            .progress
            .get(&(flow_id.into(), step_index))
            .map(|entry| entry.clone()))
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        Ok(self
            .progress
            .remove(&(flow_id.into(), step_index))
            .is_some())
    }
}

fn key(progress: &DslStepProgress) -> (Box<str>, u32) {
    (progress.flow_id().into(), progress.step_index())
}
