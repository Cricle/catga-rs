use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::{
    FlowRuntime, FlowRuntimeResult, FlowScheduler, SuspendedFlowStore, metrics::FlowMetrics,
};

use super::FlowRegistry;

/// Runs durable flows against the definition snapshot currently registered for their flow type.
///
/// Each public operation acquires one immutable definition before invoking the underlying runtime.
/// Reloads therefore never block active steps, while a later resume observes the replacement.
pub struct RegistryFlowRuntime<S: ?Sized, H: ?Sized> {
    store: Arc<S>,
    scheduler: Arc<H>,
    registry: Arc<FlowRegistry>,
    owner: Box<str>,
    stale_after: Duration,
    metrics: FlowMetrics,
}

impl<S, H> RegistryFlowRuntime<S, H>
where
    S: SuspendedFlowStore + ?Sized,
    H: FlowScheduler + ?Sized,
{
    /// Creates a runtime that resolves durable flow types through `registry`.
    pub fn new(
        store: Arc<S>,
        scheduler: Arc<H>,
        registry: Arc<FlowRegistry>,
        owner: impl Into<Box<str>>,
    ) -> Self {
        Self {
            store,
            scheduler,
            registry,
            owner: owner.into(),
            stale_after: Duration::from_secs(30),
            metrics: FlowMetrics::default(),
        }
    }

    /// Sets how long an unheartbeated running continuation remains exclusively owned.
    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    /// Starts a new flow using the definition currently registered as `flow_type`.
    pub async fn start(
        &self,
        flow_id: impl Into<Box<str>>,
        flow_type: &str,
        data: impl Into<Arc<[u8]>>,
    ) -> CatgaResult<FlowRuntimeResult> {
        self.runtime_for_type(flow_type)?.start(flow_id, data).await
    }

    /// Resumes a flow using its persisted flow type's current definition.
    pub async fn resume(&self, flow_id: &str) -> CatgaResult<FlowRuntimeResult> {
        self.runtime_for_id(flow_id).await?.resume(flow_id).await
    }

    /// Resumes a flow and evaluates waits and delays at `now`.
    pub async fn resume_at(
        &self,
        flow_id: &str,
        now: SystemTime,
    ) -> CatgaResult<FlowRuntimeResult> {
        self.runtime_for_id(flow_id)
            .await?
            .resume_at(flow_id, now)
            .await
    }

    /// Cancels a flow using its persisted flow type's current definition.
    pub async fn cancel(&self, flow_id: &str) -> CatgaResult<FlowRuntimeResult> {
        self.runtime_for_id(flow_id).await?.cancel(flow_id).await
    }

    /// Records a successful child result and resumes if its wait becomes ready.
    pub async fn record_wait_success(
        &self,
        flow_id: &str,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<FlowRuntimeResult> {
        self.runtime_for_id(flow_id)
            .await?
            .record_wait_success(flow_id, child_id, payload)
            .await
    }

    /// Records a failed child result and resumes if its wait becomes resolved.
    pub async fn record_wait_failure(
        &self,
        flow_id: &str,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<FlowRuntimeResult> {
        self.runtime_for_id(flow_id)
            .await?
            .record_wait_failure(flow_id, child_id, error)
            .await
    }

    /// Refreshes the caller's durable execution lease.
    pub async fn heartbeat(&self, flow_id: &str, version: i64) -> CatgaResult<bool> {
        self.runtime_for_id(flow_id)
            .await?
            .heartbeat(flow_id, version)
            .await
    }

    async fn runtime_for_id(&self, flow_id: &str) -> CatgaResult<FlowRuntime<S, H>> {
        let continuation = self
            .store
            .get(flow_id)
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "flow does not exist"))?;
        self.runtime_for_type(continuation.state().flow_type())
    }

    fn runtime_for_type(&self, flow_type: &str) -> CatgaResult<FlowRuntime<S, H>> {
        let definition = self.registry.get(flow_type).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "no flow definition is registered for this flow type",
            )
        })?;
        Ok(FlowRuntime::with_shared_definition(
            Arc::clone(&self.store),
            Arc::clone(&self.scheduler),
            definition.shared_definition(),
            self.owner.clone(),
            self.stale_after,
            self.metrics.clone(),
        ))
    }
}
