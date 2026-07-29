//! Structured tracing and metrics hooks backed by the Rust ecosystem.

use std::time::Duration;

use crate::{CatgaResult, Message, Request, current_correlation_id};

/// The tracing target used by every Catga framework event and span.
///
/// ```
/// use catga_core::TRACING_TARGET;
///
/// assert_eq!(TRACING_TARGET, "catga");
/// ```
pub const TRACING_TARGET: &str = "catga";

pub(crate) fn request_span(request_type: &'static str) -> tracing::Span {
    let span = tracing::info_span!(
        target: TRACING_TARGET,
        "catga.request",
        catga_kind = "request",
        request_type,
        correlation_id = tracing::field::Empty,
        success = tracing::field::Empty,
        error = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );
    if let Some(correlation_id) = current_correlation_id() {
        span.record("correlation_id", correlation_id);
    }
    span
}

pub(crate) fn command_span(command_type: &'static str) -> tracing::Span {
    let span = tracing::info_span!(
        target: TRACING_TARGET,
        "catga.command",
        catga_kind = "command",
        command_type,
        correlation_id = tracing::field::Empty,
        success = tracing::field::Empty,
        error = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );
    if let Some(correlation_id) = current_correlation_id() {
        span.record("correlation_id", correlation_id);
    }
    span
}

pub(crate) fn event_span(event_type: &'static str, handler_count: usize) -> tracing::Span {
    let span = tracing::info_span!(
        target: TRACING_TARGET,
        "catga.event",
        catga_kind = "event",
        event_type,
        handler_count,
        correlation_id = tracing::field::Empty,
        success = tracing::field::Empty,
        error = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );
    if let Some(correlation_id) = current_correlation_id() {
        span.record("correlation_id", correlation_id);
    }
    span
}

pub(crate) fn pipeline_span(request_type: &'static str) -> tracing::Span {
    let span = tracing::info_span!(
        target: TRACING_TARGET,
        "catga.pipeline",
        catga_kind = "pipeline",
        request_type,
        correlation_id = tracing::field::Empty,
        success = tracing::field::Empty,
        error = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );
    if let Some(correlation_id) = current_correlation_id() {
        span.record("correlation_id", correlation_id);
    }
    span
}

pub(crate) fn record_request<T>(
    span: &tracing::Span,
    request_type: &'static str,
    elapsed: Duration,
    result: &CatgaResult<T>,
) {
    record_result(
        "catga.requests.executed",
        "catga.request.duration",
        span,
        request_type,
        elapsed,
        result,
    );
}

pub(crate) fn record_command(
    span: &tracing::Span,
    command_type: &'static str,
    elapsed: Duration,
    result: &CatgaResult<()>,
) {
    record_result(
        "catga.commands.executed",
        "catga.command.duration",
        span,
        command_type,
        elapsed,
        result,
    );
}

pub(crate) fn record_event(
    span: &tracing::Span,
    event_type: &'static str,
    elapsed: Duration,
    result: &CatgaResult<()>,
) {
    record_result(
        "catga.events.published",
        "catga.event.duration",
        span,
        event_type,
        elapsed,
        result,
    );
}

pub(crate) fn record_pipeline<T>(
    span: &tracing::Span,
    kind: &'static str,
    behavior_count: usize,
    elapsed: Duration,
    result: &CatgaResult<T>,
) {
    let duration_ms = elapsed.as_secs_f64() * 1_000.0;
    let outcome = outcome(result);
    metrics::histogram!("catga.pipeline.behavior_count", "kind" => kind, "outcome" => outcome)
        .record(behavior_count as f64);
    metrics::histogram!("catga.pipeline.duration", "kind" => kind, "outcome" => outcome)
        .record(duration_ms);
    metrics::counter!("catga.pipeline.executed", "kind" => kind, "outcome" => outcome).increment(1);
    span.record("duration_ms", duration_ms);
}

/// Adds a message's opted-in trace tags as structured events below `span`.
///
/// `tracing` requires event field names to be known at compile time, while Catga message tags are
/// application-defined. Keeping the key and value in one structured debug event preserves both in
/// every tracing/OpenTelemetry bridge without reflection or allocating tag collections. The
/// `enabled!` guard avoids walking annotated fields when debug tracing is disabled.
pub(crate) fn record_message_tags<M: Message>(span: &tracing::Span, message: &M) {
    if !tracing::enabled!(target: TRACING_TARGET, tracing::Level::DEBUG) {
        return;
    }
    message.visit_trace_tags(&mut |name, value| {
        tracing::debug!(
            target: TRACING_TARGET,
            parent: span,
            catga_trace_tag = name,
            catga_trace_value = %value,
            "catga message trace tag"
        );
    });
}

/// Adds a successful request response's opted-in trace tags as structured events below `span`.
///
/// The tracing-enabled check is deliberately before inspecting `result`: applications that do
/// not enable Catga debug tracing do not visit response values, allocate tag collections, or
/// expose response data. Response values never flow into `metrics` labels.
pub(crate) fn record_response_tags<M: Request>(
    span: &tracing::Span,
    result: &CatgaResult<M::Response>,
) {
    if !tracing::enabled!(target: TRACING_TARGET, tracing::Level::DEBUG) {
        return;
    }
    let Ok(response) = result else {
        return;
    };
    M::visit_response_trace_tags(response, &mut |name, value| {
        tracing::debug!(
            target: TRACING_TARGET,
            parent: span,
            catga_trace_source = "response",
            catga_trace_tag = name,
            catga_trace_value = %value,
            "catga response trace tag"
        );
    });
}

fn record_result<T>(
    counter: &'static str,
    histogram: &'static str,
    span: &tracing::Span,
    message_type: &'static str,
    elapsed: Duration,
    result: &CatgaResult<T>,
) {
    let duration_ms = elapsed.as_secs_f64() * 1_000.0;
    let outcome = outcome(result);
    metrics::histogram!(histogram, "outcome" => outcome).record(duration_ms);
    match result {
        Ok(_) => {
            span.record("success", true);
            span.record("duration_ms", duration_ms);
            metrics::counter!(counter, "outcome" => outcome).increment(1);
            tracing::debug!(target: TRACING_TARGET, message_type, duration_ms, "catga operation completed");
        }
        Err(error) => {
            span.record("success", false);
            span.record("error", error.message());
            span.record("duration_ms", duration_ms);
            metrics::counter!(counter, "outcome" => outcome).increment(1);
            tracing::warn!(target: TRACING_TARGET, message_type, duration_ms, error = error.message(), "catga operation failed");
        }
    }
}

fn outcome<T>(result: &CatgaResult<T>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(error) if error.code() == crate::ErrorCode::Cancelled => "aborted",
        Err(_) => "failure",
    }
}
