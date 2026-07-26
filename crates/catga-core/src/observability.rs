//! Structured tracing and metrics hooks backed by the Rust ecosystem.

use std::time::Duration;

use crate::{CatgaResult, Message, current_correlation_id};

/// The tracing target used by every Catga framework event and span.
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
        "catga.commands.executed",
        "catga.command.duration",
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
    request_type: &'static str,
    elapsed: Duration,
    result: &CatgaResult<T>,
) {
    record_result(
        "catga.pipeline.executed",
        "catga.pipeline.duration",
        span,
        request_type,
        elapsed,
        result,
    );
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

fn record_result<T>(
    counter: &'static str,
    histogram: &'static str,
    span: &tracing::Span,
    message_type: &'static str,
    elapsed: Duration,
    result: &CatgaResult<T>,
) {
    let duration_ms = elapsed.as_secs_f64() * 1_000.0;
    metrics::histogram!(histogram, "message_type" => message_type).record(duration_ms);
    match result {
        Ok(_) => {
            span.record("success", true);
            span.record("duration_ms", duration_ms);
            metrics::counter!(counter, "message_type" => message_type, "success" => "true")
                .increment(1);
            tracing::debug!(target: TRACING_TARGET, message_type, duration_ms, "catga operation completed");
        }
        Err(error) => {
            span.record("success", false);
            span.record("error", error.message());
            span.record("duration_ms", duration_ms);
            metrics::counter!(counter, "message_type" => message_type, "success" => "false")
                .increment(1);
            tracing::warn!(target: TRACING_TARGET, message_type, duration_ms, error = error.message(), "catga operation failed");
        }
    }
}
