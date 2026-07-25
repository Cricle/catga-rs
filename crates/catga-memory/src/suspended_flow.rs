//! Lock-free in-memory persistence for suspended flow continuations.

use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowQuery, FlowSummary, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore,
};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::suspended_flow_timeout::{DueIndex, receipt_token};

/// An in-memory suspended-flow store with per-flow pointer CAS updates.
#[derive(Default)]
pub struct MemorySuspendedFlows {
    continuations: DashMap<Box<str>, Arc<ContinuationSlot>>,
    due: Mutex<DueIndex>,
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
        let mut due = self.due.lock().map_err(lock_error)?;
        Ok(
            match self.continuations.entry(continuation.state().id().into()) {
                Entry::Vacant(entry) => {
                    due.replace(&continuation);
                    entry.insert(Arc::new(ContinuationSlot::new(continuation)));
                    true
                }
                Entry::Occupied(_) => false,
            },
        )
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self
            .continuations
            .get(flow_id)
            .map(|slot| (*slot.continuation.load_full()).clone()))
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        let mut summaries = Vec::with_capacity(query.max_results());
        for entry in self.continuations.iter().take(query.max_scan()) {
            let continuation = entry.value().continuation.load();
            if query.matches(&continuation) {
                summaries.push(FlowSummary::from_continuation(&continuation));
                if summaries.len() == query.max_results() {
                    break;
                }
            }
        }
        Ok(summaries)
    }

    async fn delete(&self, flow_id: &str, expected_version: i64) -> CatgaResult<bool> {
        let mut due = self.due.lock().map_err(lock_error)?;
        let Some(slot) = self
            .continuations
            .get(flow_id)
            .map(|entry| Arc::clone(&entry))
        else {
            return Ok(false);
        };
        loop {
            let current = slot.continuation.load_full();
            if current.state().version() != expected_version {
                return Ok(false);
            }
            if self
                .continuations
                .remove_if(flow_id, |_, candidate| {
                    Arc::ptr_eq(&slot, candidate)
                        && Arc::ptr_eq(&current, &candidate.continuation.load_full())
                })
                .is_some()
            {
                due.remove(flow_id);
                return Ok(true);
            }
            if self.continuations.get(flow_id).is_none() {
                return Ok(false);
            }
        }
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        if next.state().version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        let mut due = self.due.lock().map_err(lock_error)?;
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
                due.replace(&next);
                return Ok(true);
            }
        }
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        if next.state().id() != expected.state().id()
            || next.state().version() != expected.state().version().saturating_add(1)
        {
            return Ok(false);
        }
        let mut due = self.due.lock().map_err(lock_error)?;
        let Some(slot) = self
            .continuations
            .get(expected.state().id())
            .map(|entry| Arc::clone(&entry))
        else {
            return Ok(false);
        };
        loop {
            let current = slot.continuation.load_full();
            if current.as_ref() != expected {
                return Ok(false);
            }
            if slot.replace(&current, next.clone()).is_some() {
                due.replace(&next);
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
        let mut due = self.due.lock().map_err(lock_error)?;
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
            if slot.replace(&current, next.clone()).is_some() {
                due.replace(&next);
                return Ok(true);
            }
        }
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool> {
        let mut due = self.due.lock().map_err(lock_error)?;
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
            let next_wait = wait.record_failure(child_id, error.clone());
            if next_wait.completed_count() == wait.completed_count() {
                return Ok(true);
            }
            let next = (*current).clone().with_wait(next_wait);
            if slot.replace(&current, next.clone()).is_some() {
                due.replace(&next);
                return Ok(true);
            }
        }
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let mut due = self.due.lock().map_err(lock_error)?;
        let Some(slot) = self
            .continuations
            .get(flow_id)
            .map(|entry| Arc::clone(&entry))
        else {
            return Ok(false);
        };
        loop {
            let current = slot.continuation.load_full();
            if current.state().owner() != Some(owner) || current.state().version() != version {
                return Ok(false);
            }
            let next = (*current)
                .clone()
                .with_state(current.state().clone().heartbeated_at(SystemTime::now()));
            if slot.replace(&current, next.clone()).is_some() {
                due.replace(&next);
                return Ok(true);
            }
        }
    }
}

#[async_trait]
impl TimedOutFlowStore for MemorySuspendedFlows {
    async fn poll_timed_out(
        &self,
        poll: &TimedOutFlowPoll,
    ) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
        Ok(self.due.lock().map_err(lock_error)?.poll(poll))
    }

    async fn ack_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        let token = receipt_token(receipt).ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "memory timeout receipt is invalid")
        })?;
        self.due.lock().map_err(lock_error)?.ack(token);
        Ok(())
    }

    async fn release_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        let token = receipt_token(receipt).ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "memory timeout receipt is invalid")
        })?;
        let continuation = self
            .continuations
            .get(receipt.flow_id())
            .map(|slot| slot.continuation.load_full());
        self.due
            .lock()
            .map_err(lock_error)?
            .release(token, continuation.as_deref());
        Ok(())
    }
}

fn lock_error<T>(error: std::sync::PoisonError<T>) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}
