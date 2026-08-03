//! Low-cardinality metrics for durable and local Flow execution.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Instant, SystemTime},
};

/// Counter incremented after a durable flow continuation is created.
pub(crate) const FLOWS_STARTED: &str = "catga.flow.started";

/// Counter incremented after a durable flow reaches its successful terminal state.
pub(crate) const FLOWS_COMPLETED: &str = "catga.flow.completed";

/// Counter incremented after a durable flow reaches its failed terminal state.
pub(crate) const FLOWS_FAILED: &str = "catga.flow.failed";

/// Counter incremented before one registered durable flow step executes.
pub(crate) const FLOW_STEPS_EXECUTED: &str = "catga.flow.step.executed";

/// Counter incremented after one registered durable flow step returns an outcome.
pub(crate) const FLOW_STEPS_SUCCEEDED: &str = "catga.flow.step.succeeded";

/// Counter incremented after one registered durable flow step returns an error.
pub(crate) const FLOW_STEPS_FAILED: &str = "catga.flow.step.failed";

/// Histogram recording the elapsed milliseconds of one active durable Flow drive.
pub(crate) const FLOW_DURATION: &str = "catga.flow.duration";

/// Histogram recording wall-clock time from durable flow creation to its terminal outcome.
///
/// Unlike [`FLOW_DURATION`], this includes time spent durably suspended between drives. The sole
/// `outcome` label is `success`, `failure`, or `cancelled`; flow identities and definition names
/// deliberately remain trace-only fields to keep metric cardinality bounded.
pub(crate) const FLOW_LATENCY: &str = "catga.flow.latency";

/// Histogram recording the elapsed milliseconds of one registered Flow step.
pub(crate) const FLOW_STEP_DURATION: &str = "catga.flow.step.duration";

/// Gauge tracking active in-process durable Flow drives for one runtime.
pub(crate) const FLOWS_ACTIVE: &str = "catga.flow.active";

/// Shares bounded runtime metrics state without retaining Flow identities.
#[derive(Clone, Default)]
pub(crate) struct FlowMetrics {
    state: Arc<FlowMetricsState>,
}

#[derive(Default)]
struct FlowMetricsState {
    active: AtomicUsize,
}

impl FlowMetrics {
    /// Records that a runtime successfully created a durable continuation.
    pub(crate) fn record_started(&self) {
        metrics::counter!(FLOWS_STARTED).increment(1);
    }

    /// Records that this runtime successfully persisted a completed terminal state.
    pub(crate) fn record_completed(&self, created_at: SystemTime) {
        metrics::counter!(FLOWS_COMPLETED).increment(1);
        self.record_latency("success", created_at);
    }

    /// Records that this runtime successfully persisted a failed terminal state.
    pub(crate) fn record_failed(&self, created_at: SystemTime) {
        metrics::counter!(FLOWS_FAILED).increment(1);
        self.record_latency("failure", created_at);
    }

    /// Records that this runtime successfully persisted a cancelled terminal state.
    pub(crate) fn record_cancelled(&self, created_at: SystemTime) {
        self.record_latency("cancelled", created_at);
    }

    fn record_latency(&self, outcome: &'static str, created_at: SystemTime) {
        // A backwards wall-clock adjustment must not make a histogram sample negative. This
        // metric is observational only, so the safest bounded value is zero milliseconds.
        let duration_ms = SystemTime::now()
            .duration_since(created_at)
            .unwrap_or_default()
            .as_secs_f64()
            * 1_000.0;
        metrics::histogram!(FLOW_LATENCY, "outcome" => outcome).record(duration_ms);
    }

    /// Starts instrumentation for one claimed Flow drive.
    pub(crate) fn begin_execution(&self, flow_id: &str, flow_type: &str) -> FlowExecution {
        let active = self.state.active.fetch_add(1, Ordering::AcqRel) + 1;
        metrics::gauge!(FLOWS_ACTIVE).set(active as f64);
        FlowExecution {
            state: Arc::clone(&self.state),
            span: tracing::info_span!(
                target: crate::TRACING_TARGET,
                "catga.flow.execute",
                catga_kind = "flow",
                flow_id,
                flow_type,
                outcome = tracing::field::Empty,
                duration_ms = tracing::field::Empty,
            ),
            started: Instant::now(),
            completed: false,
        }
    }
}

/// Tracks one caller-owned execution of a claimed durable Flow.
pub(crate) struct FlowExecution {
    state: Arc<FlowMetricsState>,
    span: tracing::Span,
    started: Instant,
    completed: bool,
}

impl FlowExecution {
    /// Starts instrumentation for one registered step under this Flow execution.
    pub(crate) fn begin_step(&self, step_name: &str) -> FlowStepOperation {
        metrics::counter!(FLOW_STEPS_EXECUTED).increment(1);
        FlowStepOperation {
            span: tracing::info_span!(
                target: crate::TRACING_TARGET,
                parent: &self.span,
                "catga.flow.step",
                catga_kind = "flow_step",
                step_name,
                outcome = tracing::field::Empty,
                duration_ms = tracing::field::Empty,
            ),
            started: Instant::now(),
            completed: false,
        }
    }

    /// Records the final execution outcome once and restores the active gauge.
    pub(crate) fn complete(&mut self, outcome: &'static str) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.span.record("outcome", outcome);
        let duration_ms = self.started.elapsed().as_secs_f64() * 1_000.0;
        self.span.record("duration_ms", duration_ms);
        metrics::histogram!(FLOW_DURATION, "outcome" => outcome).record(duration_ms);
        let active = self
            .state
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(1)
            })
            .map_or(0, |previous| previous - 1);
        metrics::gauge!(FLOWS_ACTIVE).set(active as f64);
    }
}

impl Drop for FlowExecution {
    fn drop(&mut self) {
        self.complete("aborted");
    }
}

/// Tracks one caller-owned registered Flow step.
pub(crate) struct FlowStepOperation {
    span: tracing::Span,
    started: Instant,
    completed: bool,
}

impl FlowStepOperation {
    /// Records a successful or failed step result once.
    pub(crate) fn complete(&mut self, outcome: &'static str) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.span.record("outcome", outcome);
        let duration_ms = self.started.elapsed().as_secs_f64() * 1_000.0;
        self.span.record("duration_ms", duration_ms);
        metrics::histogram!(FLOW_STEP_DURATION, "outcome" => outcome).record(duration_ms);
        match outcome {
            "success" => metrics::counter!(FLOW_STEPS_SUCCEEDED).increment(1),
            "failure" => metrics::counter!(FLOW_STEPS_FAILED).increment(1),
            "aborted" => {}
            _ => {}
        }
    }

    /// Returns the span that must surround the step handler future.
    pub(crate) fn span(&self) -> tracing::Span {
        self.span.clone()
    }
}

impl Drop for FlowStepOperation {
    fn drop(&mut self) {
        self.complete("aborted");
    }
}

/// Tracks one `for_each` operation and its in-flight item actions.
pub(crate) struct ForEachMetrics {
    mode: &'static str,
    active: Arc<AtomicUsize>,
    started: Instant,
}

impl ForEachMetrics {
    pub(crate) fn new(mode: &'static str) -> Arc<Self> {
        Arc::new(Self {
            mode,
            active: Arc::new(AtomicUsize::new(0)),
            started: Instant::now(),
        })
    }

    pub(crate) fn begin_item(self: &Arc<Self>) -> ForEachItemMetrics {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        metrics::gauge!("catga.flow.foreach.in_flight", "mode" => self.mode).set(active as f64);
        ForEachItemMetrics {
            metrics: Arc::clone(self),
            started: Instant::now(),
            recorded: false,
        }
    }

    fn record_item(&self, success: bool, elapsed_seconds: f64) {
        let current = self.active.fetch_sub(1, Ordering::AcqRel) - 1;
        metrics::gauge!("catga.flow.foreach.in_flight", "mode" => self.mode).set(current as f64);
        if success {
            metrics::counter!("catga.flow.foreach.items.processed", "mode" => self.mode)
                .increment(1);
        } else {
            metrics::counter!("catga.flow.foreach.items.failed", "mode" => self.mode).increment(1);
        }
        metrics::histogram!("catga.flow.foreach.item.duration", "mode" => self.mode)
            .record(elapsed_seconds);
    }
}

impl Drop for ForEachMetrics {
    fn drop(&mut self) {
        metrics::histogram!("catga.flow.foreach.duration", "mode" => self.mode)
            .record(self.started.elapsed().as_secs_f64());
    }
}

/// Records one item outcome and restores the active gauge even if its future is cancelled.
pub(crate) struct ForEachItemMetrics {
    metrics: Arc<ForEachMetrics>,
    started: Instant,
    recorded: bool,
}

impl ForEachItemMetrics {
    pub(crate) fn complete(mut self, success: bool) {
        self.recorded = true;
        self.metrics
            .record_item(success, self.started.elapsed().as_secs_f64());
    }
}

impl Drop for ForEachItemMetrics {
    fn drop(&mut self) {
        if !self.recorded {
            self.metrics
                .record_item(false, self.started.elapsed().as_secs_f64());
        }
    }
}
