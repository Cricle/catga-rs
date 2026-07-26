//! Stable, bounded-cardinality telemetry helpers for Catga adapters.
//!
//! This module builds on the application's configured [`metrics`] recorder and
//! [`tracing`] subscriber. It neither installs global telemetry nor starts a
//! background task. Applications remain free to export these signals through
//! their preferred OpenTelemetry, Prometheus, or logging integration.

use std::{future::Future, time::Instant};

use crate::{CatgaResult, ErrorCode, observability::TRACING_TARGET};
use tracing::Instrument;

/// Counter incremented once for every instrumented persistence operation.
///
/// Records use the bounded `backend`, `component`, `operation`, and `outcome`
/// labels. Adapter implementations must not put stream identifiers, message
/// identifiers, user keys, or other unbounded values in these labels.
pub const PERSISTENCE_OPERATIONS: &str = "catga.persistence.operations";

/// Histogram that records the elapsed milliseconds of a persistence operation.
///
/// Its labels are identical to [`PERSISTENCE_OPERATIONS`].
pub const PERSISTENCE_DURATION: &str = "catga.persistence.duration";

/// Counter incremented when a persistence operation returns a typed conflict.
///
/// Records use the same bounded `backend`, `component`, and `operation` labels as
/// [`PERSISTENCE_OPERATIONS`].
pub const PERSISTENCE_CONFLICTS: &str = "catga.persistence.conflicts";

/// Counter incremented when a claim or lease operation finds an existing owner.
///
/// Records use the same bounded `backend`, `component`, and `operation` labels as
/// [`PERSISTENCE_OPERATIONS`].
pub const PERSISTENCE_CONTENTION: &str = "catga.persistence.contention";

/// Counter incremented for every outbox envelope published and acknowledged.
pub const OUTBOX_PUBLISHED: &str = "catga.outbox.published";

/// Counter incremented for every outbox envelope released for a later retry.
pub const OUTBOX_FAILED: &str = "catga.outbox.failed";

/// Counter incremented when a retry behavior schedules another handler attempt.
pub const RESILIENCE_RETRIES: &str = "catga.resilience.retries";

/// Gauge containing the number of retry attempts currently waiting for backoff.
///
/// This gauge has no labels, so retry inputs and message identifiers cannot increase cardinality.
pub const RESILIENCE_RETRY_PENDING: &str = "catga.resilience.retry.pending";

/// Counter for bounded Inbox behavior outcomes.
///
/// Its sole `outcome` label is one of `processed`, `hit`, `conflict`, `failure`, or `bypassed`.
pub const INBOX_OUTCOMES: &str = "catga.inbox.outcomes";

/// Histogram that records distributed-lock acquisition latency in milliseconds.
///
/// Its sole `outcome` label is one of `success`, `contention`, or `failure`.
/// Resource keys and owner identifiers are deliberately omitted.
pub const DISTRIBUTED_LOCK_ACQUIRE_DURATION: &str = "catga.distributed_lock.acquire.duration";

/// Counter for bounded distributed-lock acquisition outcomes.
///
/// Its sole `outcome` label is one of `success`, `contention`, or `failure`.
pub const DISTRIBUTED_LOCK_ACQUIRE_OUTCOMES: &str = "catga.distributed_lock.acquire.outcomes";

/// Gauge containing the number of distributed locks currently held by this process.
///
/// This gauge has no labels, so resource keys and owners cannot increase cardinality.
pub const DISTRIBUTED_LOCK_HELD: &str = "catga.distributed_lock.held";

/// Counter for bounded distributed-lock release outcomes.
///
/// Its sole `outcome` label is one of `success`, `failure`, or `ownership_lost`.
pub const DISTRIBUTED_LOCK_RELEASE_OUTCOMES: &str = "catga.distributed_lock.release.outcomes";

/// Counter incremented when a circuit breaker transitions into its open state.
pub const RESILIENCE_CIRCUIT_OPENED: &str = "catga.resilience.circuit.opened";

/// Counter incremented after a transport accepts one published envelope.
pub const MESSAGES_PUBLISHED: &str = "catga.messages.published";

/// Counter incremented after one transport publish attempt returns an error.
pub const MESSAGES_FAILED: &str = "catga.messages.failed";

/// Counter incremented when an in-flight publish future is cancelled.
pub const MESSAGES_ABORTED: &str = "catga.messages.aborted";

/// Histogram that records the elapsed milliseconds of one publish attempt.
pub const MESSAGE_PUBLISH_DURATION: &str = "catga.messages.publish.duration";

/// Counter incremented after a transport returns one received delivery.
///
/// Records have only the bounded static `backend` and `mode` labels. Adapters
/// must not attach destinations, stream names, message identifiers, or payload
/// data, because those values may have unbounded cardinality.
pub const MESSAGES_RECEIVED: &str = "catga.messages.received";

/// Counter incremented after one transport receive attempt returns an error.
///
/// Records have only the bounded static `backend` and `mode` labels.
pub const MESSAGES_RECEIVE_FAILED: &str = "catga.messages.receive.failed";

/// Counter incremented when an in-flight receive future is cancelled.
///
/// Records have only the bounded static `backend` and `mode` labels. This
/// signal is emitted by [`MessageReceiveOperation::drop`] only when no prior
/// success or failure outcome was recorded.
pub const MESSAGES_RECEIVE_ABORTED: &str = "catga.messages.receive.aborted";

/// Histogram that records the elapsed milliseconds of one receive attempt.
///
/// Records use the bounded static `backend`, `mode`, and `outcome` labels.
/// `outcome` is exactly one of `success`, `failure`, or `aborted`.
pub const MESSAGE_RECEIVE_DURATION: &str = "catga.messages.receive.duration";

/// Measures one persistence operation without changing its result or control flow.
///
/// Create a guard with [`persistence_operation`], run the existing adapter
/// operation, and call [`Self::complete`] with its original result. The guard
/// records a success or failure at most once. If the future holding it is
/// cancelled before completion, its destructor records the bounded `aborted`
/// outcome instead. The guard performs no allocation, locking, or I/O itself.
#[derive(Debug)]
pub struct Operation {
    started: Instant,
    span: tracing::Span,
    backend: &'static str,
    component: &'static str,
    operation: &'static str,
    completed: bool,
}

/// Starts a bounded telemetry operation for a persistence adapter.
///
/// `backend`, `component`, and `operation` must be static, low-cardinality
/// names such as `"memory"`, `"event_store"`, and `"append"`. The returned
/// guard creates a child tracing span below the current span and emits the
/// generic counter and duration histogram when completed or dropped.
pub fn persistence_operation(
    backend: &'static str,
    component: &'static str,
    operation: &'static str,
) -> Operation {
    let span = tracing::info_span!(
        target: TRACING_TARGET,
        "catga.persistence",
        catga_kind = "persistence",
        backend,
        component,
        operation,
        outcome = tracing::field::Empty,
        error = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );
    Operation {
        started: Instant::now(),
        span,
        backend,
        component,
        operation,
        completed: false,
    }
}

/// Awaits one persistence operation while recording its original outcome.
///
/// This is the asynchronous counterpart to manually creating an
/// [`Operation`]. It holds the guard in the caller's future state machine, so
/// it adds no allocation or task. Cancelling the returned future drops the
/// incomplete guard and records the `aborted` outcome; completing it returns
/// the exact [`CatgaResult`] produced by `future`.
pub async fn record_persistence<T>(
    backend: &'static str,
    component: &'static str,
    operation: &'static str,
    future: impl Future<Output = CatgaResult<T>>,
) -> CatgaResult<T> {
    let mut guard = persistence_operation(backend, component, operation);
    let result = future.await;
    guard.complete(&result);
    result
}

/// Awaits a boolean claim operation and records a non-error contention outcome.
///
/// `backend`, `component`, and `operation` must be static, low-cardinality names. A successful
/// `false` result retains the generic `success` operation outcome because the backend call itself
/// succeeded, while also incrementing [`PERSISTENCE_CONTENTION`].
pub async fn record_persistence_claim(
    backend: &'static str,
    component: &'static str,
    operation: &'static str,
    future: impl Future<Output = CatgaResult<bool>>,
) -> CatgaResult<bool> {
    let mut guard = persistence_operation(backend, component, operation);
    let result = future.await;
    guard.complete_claim(&result);
    result
}

/// Awaits one transport publish while recording its original result.
///
/// `backend` and `mode` must be static low-cardinality labels, such as
/// `"redis"` and `"stream"`. Destination and message identifiers are omitted
/// from metrics to keep recorder memory bounded. The publish future runs under
/// a producer tracing span. Cancelling the returned future records
/// [`MESSAGES_ABORTED`] and returns no synthetic error because the future was
/// never allowed to produce one.
pub async fn record_message_publish<T>(
    backend: &'static str,
    mode: &'static str,
    future: impl Future<Output = CatgaResult<T>>,
) -> CatgaResult<T> {
    let mut guard = MessagePublishOperation::new(backend, mode);
    let result = future.instrument(guard.span.clone()).await;
    guard.complete(&result);
    result
}

/// Awaits one transport receive while recording its original result.
///
/// `backend` and `mode` must be static low-cardinality labels, such as
/// `"redis"` and `"stream"`. Receive operations run below a consumer tracing
/// span. Cancelling the returned future drops its guard, which records the
/// [`MESSAGES_RECEIVE_ABORTED`] outcome without creating a replacement error
/// or changing the acknowledgement state of a delivery.
pub async fn record_message_receive<T>(
    backend: &'static str,
    mode: &'static str,
    future: impl Future<Output = CatgaResult<T>>,
) -> CatgaResult<T> {
    let mut guard = MessageReceiveOperation::new(backend, mode);
    let result = future.instrument(guard.span.clone()).await;
    guard.complete(&result);
    result
}

/// Tracks one outbound transport attempt with bounded metrics and a producer span.
///
/// Most adapters should call [`record_message_publish`] instead of constructing
/// this guard directly. The explicit type is public for adapters that need to
/// start a publish operation before assembling their future.
#[derive(Debug)]
pub struct MessagePublishOperation {
    started: Instant,
    span: tracing::Span,
    backend: &'static str,
    mode: &'static str,
    completed: bool,
}

/// Tracks one inbound transport attempt with bounded metrics and a consumer span.
///
/// Most adapters should call [`record_message_receive`] instead of constructing
/// this guard directly. It records at most one outcome and does not own a
/// delivery or an acknowledgement token, so telemetry cannot affect delivery
/// acknowledgement semantics.
#[derive(Debug)]
pub struct MessageReceiveOperation {
    started: Instant,
    span: tracing::Span,
    backend: &'static str,
    mode: &'static str,
    completed: bool,
}

/// RAII guard for one retry attempt waiting in bounded backoff.
pub(crate) struct RetryPending;

/// RAII guard for one distributed lock held by this process.
pub(crate) struct DistributedLockHeld;

/// Records one distributed-lock acquisition with a fixed outcome label.
///
/// Callers must use one of the values documented by
/// [`DISTRIBUTED_LOCK_ACQUIRE_OUTCOMES`].
pub(crate) fn record_distributed_lock_acquire(outcome: &'static str, started: Instant) {
    let duration_ms = started.elapsed().as_secs_f64() * 1_000.0;
    metrics::counter!(DISTRIBUTED_LOCK_ACQUIRE_OUTCOMES, "outcome" => outcome).increment(1);
    metrics::histogram!(DISTRIBUTED_LOCK_ACQUIRE_DURATION, "outcome" => outcome)
        .record(duration_ms);
}

/// Records one distributed-lock release with a fixed outcome label.
///
/// Callers must use one of the values documented by
/// [`DISTRIBUTED_LOCK_RELEASE_OUTCOMES`].
pub(crate) fn record_distributed_lock_release(outcome: &'static str) {
    metrics::counter!(DISTRIBUTED_LOCK_RELEASE_OUTCOMES, "outcome" => outcome).increment(1);
}

/// Adds one currently held distributed lock until the returned guard is dropped.
pub(crate) fn distributed_lock_held() -> DistributedLockHeld {
    metrics::gauge!(DISTRIBUTED_LOCK_HELD).increment(1.0);
    DistributedLockHeld
}

/// Records a fixed, low-cardinality Inbox behavior outcome.
///
/// Callers must use one of the values documented by [`INBOX_OUTCOMES`].
pub(crate) fn record_inbox_outcome(outcome: &'static str) {
    metrics::counter!(INBOX_OUTCOMES, "outcome" => outcome).increment(1);
}

/// Adds one retry to the pending-backoff gauge until the returned guard is dropped.
pub(crate) fn retry_pending() -> RetryPending {
    metrics::gauge!(RESILIENCE_RETRY_PENDING).increment(1.0);
    RetryPending
}

impl Drop for RetryPending {
    fn drop(&mut self) {
        metrics::gauge!(RESILIENCE_RETRY_PENDING).decrement(1.0);
    }
}

impl Drop for DistributedLockHeld {
    fn drop(&mut self) {
        metrics::gauge!(DISTRIBUTED_LOCK_HELD).decrement(1.0);
    }
}

impl MessagePublishOperation {
    /// Starts one outbound publish attempt for static `backend` and `mode` labels.
    pub fn new(backend: &'static str, mode: &'static str) -> Self {
        Self {
            started: Instant::now(),
            span: tracing::info_span!(
                target: TRACING_TARGET,
                "catga.message.publish",
                catga_kind = "messaging",
                backend,
                mode,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
                duration_ms = tracing::field::Empty,
            ),
            backend,
            mode,
            completed: false,
        }
    }

    /// Records a successful or failed result once without consuming it.
    pub fn complete<T>(&mut self, result: &CatgaResult<T>) {
        match result {
            Ok(_) => self.record("success", None),
            Err(error) => self.record("failure", Some(error.message())),
        }
    }

    fn record(&mut self, outcome: &'static str, error: Option<&str>) {
        if self.completed {
            return;
        }
        self.completed = true;

        let duration_ms = self.started.elapsed().as_secs_f64() * 1_000.0;
        self.span.record("outcome", outcome);
        self.span.record("duration_ms", duration_ms);
        if let Some(error) = error {
            self.span.record("error", error);
        }
        metrics::histogram!(
            MESSAGE_PUBLISH_DURATION,
            "backend" => self.backend,
            "mode" => self.mode,
            "outcome" => outcome,
        )
        .record(duration_ms);
        match outcome {
            "success" => metrics::counter!(MESSAGES_PUBLISHED, "backend" => self.backend, "mode" => self.mode)
                .increment(1),
            "failure" => metrics::counter!(MESSAGES_FAILED, "backend" => self.backend, "mode" => self.mode)
                .increment(1),
            "aborted" => metrics::counter!(MESSAGES_ABORTED, "backend" => self.backend, "mode" => self.mode)
                .increment(1),
            _ => {}
        }
    }
}

impl Drop for MessagePublishOperation {
    fn drop(&mut self) {
        self.record("aborted", None);
    }
}

impl MessageReceiveOperation {
    /// Starts one inbound receive attempt for static `backend` and `mode` labels.
    pub fn new(backend: &'static str, mode: &'static str) -> Self {
        Self {
            started: Instant::now(),
            span: tracing::info_span!(
                target: TRACING_TARGET,
                "catga.message.receive",
                catga_kind = "messaging",
                backend,
                mode,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
                duration_ms = tracing::field::Empty,
            ),
            backend,
            mode,
            completed: false,
        }
    }

    /// Records a successful or failed result once without consuming it.
    pub fn complete<T>(&mut self, result: &CatgaResult<T>) {
        match result {
            Ok(_) => self.record("success", None),
            Err(error) => self.record("failure", Some(error.message())),
        }
    }

    fn record(&mut self, outcome: &'static str, error: Option<&str>) {
        if self.completed {
            return;
        }
        self.completed = true;

        let duration_ms = self.started.elapsed().as_secs_f64() * 1_000.0;
        self.span.record("outcome", outcome);
        self.span.record("duration_ms", duration_ms);
        if let Some(error) = error {
            self.span.record("error", error);
        }
        metrics::histogram!(
            MESSAGE_RECEIVE_DURATION,
            "backend" => self.backend,
            "mode" => self.mode,
            "outcome" => outcome,
        )
        .record(duration_ms);
        match outcome {
            "success" => metrics::counter!(MESSAGES_RECEIVED, "backend" => self.backend, "mode" => self.mode)
                .increment(1),
            "failure" => metrics::counter!(MESSAGES_RECEIVE_FAILED, "backend" => self.backend, "mode" => self.mode)
                .increment(1),
            "aborted" => metrics::counter!(MESSAGES_RECEIVE_ABORTED, "backend" => self.backend, "mode" => self.mode)
                .increment(1),
            _ => {}
        }
    }
}

impl Drop for MessageReceiveOperation {
    fn drop(&mut self) {
        self.record("aborted", None);
    }
}

impl Operation {
    /// Records the original result as a successful or failed operation.
    ///
    /// Calling this method repeatedly is harmless: only the first call records
    /// a metric sample. The supplied result is borrowed, so the caller returns
    /// the exact same value and error without telemetry modifying it.
    pub fn complete<T>(&mut self, result: &CatgaResult<T>) {
        match result {
            Ok(_) => self.record("success", None, false),
            Err(error) => self.record(
                "failure",
                Some(error.message()),
                error.code() == ErrorCode::Conflict,
            ),
        }
    }

    /// Records a boolean claim result and counts an owned/contended `false` result separately.
    ///
    /// Repeated calls are harmless; only the first result produces metrics.
    pub fn complete_claim(&mut self, result: &CatgaResult<bool>) {
        if self.completed {
            return;
        }
        if matches!(result, Ok(false)) {
            metrics::counter!(
                PERSISTENCE_CONTENTION,
                "backend" => self.backend,
                "component" => self.component,
                "operation" => self.operation,
            )
            .increment(1);
        }
        self.complete(result);
    }

    fn record(&mut self, outcome: &'static str, error: Option<&str>, conflict: bool) {
        if self.completed {
            return;
        }
        self.completed = true;

        let duration_ms = self.started.elapsed().as_secs_f64() * 1_000.0;
        self.span.record("outcome", outcome);
        self.span.record("duration_ms", duration_ms);
        if let Some(error) = error {
            self.span.record("error", error);
        }
        metrics::counter!(
            PERSISTENCE_OPERATIONS,
            "backend" => self.backend,
            "component" => self.component,
            "operation" => self.operation,
            "outcome" => outcome,
        )
        .increment(1);
        metrics::histogram!(
            PERSISTENCE_DURATION,
            "backend" => self.backend,
            "component" => self.component,
            "operation" => self.operation,
            "outcome" => outcome,
        )
        .record(duration_ms);
        if conflict {
            metrics::counter!(
                PERSISTENCE_CONFLICTS,
                "backend" => self.backend,
                "component" => self.component,
                "operation" => self.operation,
            )
            .increment(1);
        }
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        self.record("aborted", None, false);
    }
}
