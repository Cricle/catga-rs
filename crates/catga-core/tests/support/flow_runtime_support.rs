//! Shared fixtures for the durable [`catga_core::flow::FlowRuntime`] gap tests:
//! an in-memory [`SuspendedFlowStore`] with fault-injection switches, schedulers
//! with deterministic failure modes, and a recording child launcher.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;
use catga_core::flow::{
    FlowChildLauncher, FlowContinuation, FlowQuery, FlowScheduler, FlowSummary,
    MemoryFlowScheduler, SuspendedFlowStore,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// In-memory suspended-flow store with targeted fault injection.
#[derive(Default)]
pub struct GapFlowStore {
    records: Mutex<HashMap<Box<str>, FlowContinuation>>,
    /// The next `update` loses its version race and returns `false`.
    pub fail_next_update: AtomicBool,
    /// The next `claim` loses its compare-and-swap and returns `false`.
    pub fail_next_claim: AtomicBool,
    /// Every `heartbeat` reports lost ownership while set.
    pub fail_heartbeats: AtomicBool,
    /// `record_wait_success` applies the result but loses the acknowledgement.
    pub drop_wait_ack: AtomicBool,
    /// `record_wait_failure` refuses to record and returns `false`.
    pub refuse_wait_record: AtomicBool,
}

impl GapFlowStore {
    pub fn continuation(&self, flow_id: &str) -> FlowContinuation {
        self.records
            .lock()
            .expect("store lock")
            .get(flow_id)
            .cloned()
            .expect("the flow continuation exists")
    }
}

#[async_trait]
impl SuspendedFlowStore for GapFlowStore {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let id: Box<str> = continuation.state().id().into();
        if records.contains_key(&id) {
            return Ok(false);
        }
        records.insert(id, continuation);
        Ok(true)
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self
            .records
            .lock()
            .expect("store lock")
            .get(flow_id)
            .cloned())
    }

    async fn get_by_wait_correlation(
        &self,
        correlation_id: &str,
    ) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self
            .records
            .lock()
            .expect("store lock")
            .values()
            .find(|continuation| {
                continuation
                    .wait()
                    .is_some_and(|wait| wait.correlation_id() == correlation_id)
            })
            .cloned())
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        let records = self.records.lock().expect("store lock");
        let mut summaries = Vec::new();
        for continuation in records.values().take(query.max_scan()) {
            if query.matches(continuation) {
                summaries.push(FlowSummary::from_continuation(continuation));
                if summaries.len() == query.max_results() {
                    break;
                }
            }
        }
        Ok(summaries)
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        if self.fail_next_update.swap(false, Ordering::SeqCst) {
            return Ok(false);
        }
        let mut records = self.records.lock().expect("store lock");
        let id: Box<str> = next.state().id().into();
        let Some(current) = records.get(&id) else {
            return Ok(false);
        };
        if current.state().version() != expected_version {
            return Ok(false);
        }
        records.insert(id, next);
        Ok(true)
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        if self.fail_next_claim.swap(false, Ordering::SeqCst) {
            return Ok(false);
        }
        let mut records = self.records.lock().expect("store lock");
        let id: Box<str> = expected.state().id().into();
        let Some(current) = records.get(&id) else {
            return Ok(false);
        };
        if current != expected {
            return Ok(false);
        }
        records.insert(id, next);
        Ok(true)
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let Some(current) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if current.state().version() != version {
            return Ok(false);
        }
        let Some(wait) = current.wait() else {
            return Ok(false);
        };
        let next_wait = wait.record_success(child_id, payload);
        if next_wait.results().len() == wait.results().len() {
            return Ok(false);
        }
        records.insert(flow_id.into(), current.with_wait(next_wait));
        Ok(!self.drop_wait_ack.load(Ordering::SeqCst))
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool> {
        if self.refuse_wait_record.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let mut records = self.records.lock().expect("store lock");
        let Some(current) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if current.state().version() != version {
            return Ok(false);
        }
        let Some(wait) = current.wait() else {
            return Ok(false);
        };
        let next_wait = wait.record_failure(child_id, error);
        if next_wait.results().len() == wait.results().len() {
            return Ok(false);
        }
        records.insert(flow_id.into(), current.with_wait(next_wait));
        Ok(true)
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        if self.fail_heartbeats.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let mut records = self.records.lock().expect("store lock");
        let Some(current) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if current.state().version() != version || current.state().owner() != Some(owner) {
            return Ok(false);
        }
        let heartbeated = current.state().clone().heartbeated_at(SystemTime::now());
        records.insert(flow_id.into(), current.with_state(heartbeated));
        Ok(true)
    }
}

/// A scheduler whose registrations can never be cancelled.
#[derive(Default)]
pub struct CancelFailingScheduler {
    inner: MemoryFlowScheduler,
}

#[async_trait]
impl FlowScheduler for CancelFailingScheduler {
    async fn schedule_resume(
        &self,
        flow_id: &str,
        state_id: &str,
        due_at: SystemTime,
    ) -> CatgaResult<Box<str>> {
        self.inner.schedule_resume(flow_id, state_id, due_at).await
    }

    async fn cancel_resume(&self, _schedule_id: &str) -> CatgaResult<bool> {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "schedule store unavailable",
        ))
    }
}

/// A scheduler that fails its first `failures` registrations, then succeeds.
pub struct FlakyScheduler {
    remaining_failures: AtomicUsize,
    issued: AtomicUsize,
}

impl FlakyScheduler {
    pub fn failing_times(failures: usize) -> Self {
        Self {
            remaining_failures: AtomicUsize::new(failures),
            issued: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl FlowScheduler for FlakyScheduler {
    async fn schedule_resume(
        &self,
        _flow_id: &str,
        _state_id: &str,
        _due_at: SystemTime,
    ) -> CatgaResult<Box<str>> {
        let mut remaining = self.remaining_failures.load(Ordering::SeqCst);
        loop {
            if remaining == 0 {
                let issued = self.issued.fetch_add(1, Ordering::SeqCst);
                return Ok(format!("sched-{issued}").into_boxed_str());
            }
            match self.remaining_failures.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Err(CatgaError::new(
                        ErrorCode::Unavailable,
                        "schedule identities exhausted",
                    ));
                }
                Err(actual) => remaining = actual,
            }
        }
    }

    async fn cancel_resume(&self, _schedule_id: &str) -> CatgaResult<bool> {
        Ok(true)
    }
}

/// Records every child launch; optionally rejects the first or every launch.
#[derive(Default)]
pub struct RecordingLauncher {
    pub launches: Mutex<Vec<(String, String, String)>>,
    pub fail_first: AtomicBool,
    pub fail_all: AtomicBool,
}

impl RecordingLauncher {
    pub fn launch_count(&self) -> usize {
        self.launches.lock().expect("launcher lock").len()
    }
}

#[async_trait]
impl FlowChildLauncher for RecordingLauncher {
    async fn launch(
        &self,
        parent_flow_id: &str,
        child_id: &str,
        correlation_id: &str,
    ) -> CatgaResult<()> {
        if self.fail_all.load(Ordering::SeqCst) || self.fail_first.swap(false, Ordering::SeqCst) {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "child launcher rejected the child",
            ));
        }
        self.launches.lock().expect("launcher lock").push((
            parent_flow_id.to_owned(),
            child_id.to_owned(),
            correlation_id.to_owned(),
        ));
        Ok(())
    }
}
