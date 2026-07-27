use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};

use std::time::SystemTime;

use crate::{FlowContinuation, FlowStatus};

/// The maximum number of continuation summaries a query may return.
pub const MAX_FLOW_QUERY_RESULTS: usize = 1_000;
/// The maximum number of continuation records a query may inspect.
pub const MAX_FLOW_QUERY_SCAN: usize = 10_000;

/// A compact discovery record that does not expose a flow's input or wait-result payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowSummary {
    id: Box<str>,
    flow_type: Box<str>,
    status: FlowStatus,
    version: i64,
    created_at: SystemTime,
    updated_at: SystemTime,
}

impl FlowSummary {
    /// Creates a summary from already validated persistence columns.
    pub fn new(
        id: impl Into<Box<str>>,
        flow_type: impl Into<Box<str>>,
        status: FlowStatus,
        version: i64,
        created_at: SystemTime,
    ) -> Self {
        Self {
            id: id.into(),
            flow_type: flow_type.into(),
            status,
            version,
            created_at,
            updated_at: created_at,
        }
    }

    /// Creates a summary from a durable continuation.
    pub fn from_continuation(continuation: &FlowContinuation) -> Self {
        Self::new(
            continuation.state().id(),
            continuation.state().flow_type(),
            continuation.state().status(),
            continuation.state().version(),
            continuation.created_at(),
        )
        .with_updated_at(continuation.updated_at())
    }

    /// Returns the stable flow identity.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the flow definition type.
    pub fn flow_type(&self) -> &str {
        &self.flow_type
    }

    /// Returns the current lifecycle status.
    pub const fn status(&self) -> FlowStatus {
        self.status
    }

    /// Returns the current optimistic-concurrency version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns when the durable continuation was created.
    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns when the durable continuation was last successfully changed.
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Sets the timestamp of the last successful durable change.
    #[must_use]
    pub const fn with_updated_at(mut self, updated_at: SystemTime) -> Self {
        self.updated_at = updated_at;
        self
    }
}

/// Bounded filters for discovery of durable flow continuations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowQuery {
    max_results: usize,
    max_scan: usize,
    status: Option<FlowStatus>,
    flow_type: Option<Box<str>>,
    created_at: Option<(SystemTime, SystemTime)>,
}

impl FlowQuery {
    /// Creates a query with explicit result and scan limits.
    pub fn new(max_results: usize, max_scan: usize) -> CatgaResult<Self> {
        if max_results == 0
            || max_scan == 0
            || max_results > max_scan
            || max_results > MAX_FLOW_QUERY_RESULTS
            || max_scan > MAX_FLOW_QUERY_SCAN
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow query limits must be positive, ordered, and within supported bounds",
            ));
        }
        Ok(Self {
            max_results,
            max_scan,
            status: None,
            flow_type: None,
            created_at: None,
        })
    }

    /// Filters continuations by their current lifecycle status.
    pub const fn with_status(mut self, status: FlowStatus) -> Self {
        self.status = Some(status);
        self
    }

    /// Filters continuations by their registered flow type.
    pub fn with_flow_type(mut self, flow_type: impl Into<Box<str>>) -> Self {
        self.flow_type = Some(flow_type.into());
        self
    }

    /// Filters continuations by a half-open creation-time range `[start, end)`.
    pub fn created_between(mut self, start: SystemTime, end: SystemTime) -> CatgaResult<Self> {
        if start >= end {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow query creation range must have a start before its end",
            ));
        }
        self.created_at = Some((start, end));
        Ok(self)
    }

    /// Returns whether a continuation matches every configured filter.
    pub fn matches(&self, continuation: &FlowContinuation) -> bool {
        let state = continuation.state();
        self.status.is_none_or(|status| state.status() == status)
            && self
                .flow_type
                .as_deref()
                .is_none_or(|flow_type| state.flow_type() == flow_type)
            && self.created_at.is_none_or(|(start, end)| {
                let created_at = continuation.created_at();
                created_at >= start && created_at < end
            })
    }

    /// Returns whether an already compacted summary matches every configured filter.
    pub fn matches_summary(&self, summary: &FlowSummary) -> bool {
        self.status.is_none_or(|status| summary.status() == status)
            && self
                .flow_type
                .as_deref()
                .is_none_or(|flow_type| summary.flow_type() == flow_type)
            && self.created_at.is_none_or(|(start, end)| {
                let created_at = summary.created_at();
                created_at >= start && created_at < end
            })
    }

    /// Returns the maximum number of summaries to return.
    pub const fn max_results(&self) -> usize {
        self.max_results
    }

    /// Returns the maximum number of continuations to inspect.
    pub const fn max_scan(&self) -> usize {
        self.max_scan
    }
}

/// Persists suspended flow continuations with optimistic concurrency.
#[async_trait]
pub trait SuspendedFlowStore: Send + Sync {
    /// Creates a continuation when no flow has the same identity.
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool>;

    /// Loads one continuation by flow identity.
    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>>;

    /// Loads the suspended continuation owning an active wait correlation identity.
    ///
    /// Implementations must use an indexed lookup rather than a whole-store scan. The returned
    /// continuation is only a snapshot: callers still use the normal version-fenced wait-result
    /// operations before accepting a child completion.
    async fn get_by_wait_correlation(
        &self,
        _correlation_id: &str,
    ) -> CatgaResult<Option<FlowContinuation>> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "wait-correlation lookup is not supported by this store",
        ))
    }

    /// Returns summaries matching `query` after inspecting at most its configured scan bound.
    async fn query(&self, _query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "suspended-flow discovery is not supported by this store",
        ))
    }

    /// Deletes a continuation only when its current state version equals `expected_version`.
    async fn delete(&self, _flow_id: &str, _expected_version: i64) -> CatgaResult<bool> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "suspended-flow deletion is not supported by this store",
        ))
    }

    /// Replaces a continuation after a business-state version transition.
    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool>;

    /// Atomically claims a resumable continuation only when it still exactly matches `expected`.
    ///
    /// This guards a stale-owner takeover against a heartbeat or child-result update that keeps
    /// the business-state version unchanged.
    async fn claim(&self, expected: &FlowContinuation, next: FlowContinuation)
    -> CatgaResult<bool>;

    /// Records one successful child payload without changing the business-state version.
    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool>;

    /// Records one failed child result without changing the business-state version.
    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool>;

    /// Refreshes the current owner's liveness without changing the business-state version.
    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool>;
}
