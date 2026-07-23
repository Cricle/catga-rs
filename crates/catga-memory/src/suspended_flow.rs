//! Lock-free in-memory persistence for suspended flow continuations.

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use catga_core::CatgaResult;
use catga_flow::{FlowContinuation, SuspendedFlowStore};
use dashmap::{DashMap, mapref::entry::Entry};

/// An in-memory suspended-flow store with per-flow pointer CAS updates.
#[derive(Default)]
pub struct MemorySuspendedFlows {
    continuations: DashMap<Box<str>, Arc<ContinuationSlot>>,
}

struct ContinuationSlot {
    continuation: ArcSwap<FlowContinuation>,
}

impl ContinuationSlot {
    fn new(continuation: FlowContinuation) -> Self {
        Self {
            continuation: ArcSwap::from_pointee(continuation),
        }
    }

    fn replace(
        &self,
        expected: &Arc<FlowContinuation>,
        next: FlowContinuation,
    ) -> Option<FlowContinuation> {
        let next = Arc::new(next);
        let previous = self
            .continuation
            .compare_and_swap(expected, Arc::clone(&next));
        Arc::ptr_eq(&*previous, expected).then(|| (*next).clone())
    }
}

#[async_trait]
impl SuspendedFlowStore for MemorySuspendedFlows {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        Ok(match self.continuations.entry(continuation.state().id().into()) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(ContinuationSlot::new(continuation)));
                true
            }
            Entry::Occupied(_) => false,
        })
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self
            .continuations
            .get(flow_id)
            .map(|slot| (*slot.continuation.load_full()).clone()))
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        if next.state().version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        let Some(slot) = self
            .continuations
            .get(next.state().id())
            .map(|entry| Arc::clone(&entry))
        else {
            return Ok(false);
        };
        loop {
            let current = slot.continuation.load_full();
            if current.state().version() != expected_version {
                return Ok(false);
            }
            if slot.replace(&current, next.clone()).is_some() {
                return Ok(true);
            }
        }
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        let payload: Arc<[u8]> = payload.into();
        let Some(slot) = self
            .continuations
            .get(flow_id)
            .map(|entry| Arc::clone(&entry))
        else {
            return Ok(false);
        };
        loop {
            let current = slot.continuation.load_full();
            if current.state().version() != version {
                return Ok(false);
            }
            let Some(wait) = current.wait() else {
                return Ok(false);
            };
            let next_wait = wait.record_success(child_id, Arc::clone(&payload));
            if next_wait.completed_count() == wait.completed_count() {
                return Ok(true);
            }
            let next = (*current).clone().with_wait(next_wait);
            if slot.replace(&current, next).is_some() {
                return Ok(true);
            }
        }
    }
}
