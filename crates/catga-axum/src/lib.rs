#![forbid(unsafe_code)]
//! Axum adapters for Catga's framework-independent result types.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use catga_core::{CatgaError, ErrorCode};
use serde::Serialize;

/// Header used to propagate request correlation identifiers.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

/// Reads a numeric correlation identifier or allocates a monotonic process-local fallback.
pub fn correlation_id(headers: &axum::http::HeaderMap) -> u64 {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed))
}

/// An Axum response wrapper for a [`CatgaError`].
pub struct CatgaHttpError(CatgaError);

impl From<CatgaError> for CatgaHttpError {
    fn from(error: CatgaError) -> Self {
        Self(error)
    }
}

impl IntoResponse for CatgaHttpError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            code: error_code_name(self.0.code()),
            message: self.0.message(),
        };
        (status_code(self.0.code()), Json(body)).into_response()
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'static str,
    message: &'a str,
}

fn status_code(code: ErrorCode) -> StatusCode {
    match code {
        ErrorCode::Validation => StatusCode::UNPROCESSABLE_ENTITY,
        ErrorCode::NotFound => StatusCode::NOT_FOUND,
        ErrorCode::Conflict => StatusCode::CONFLICT,
        ErrorCode::Cancelled | ErrorCode::Timeout => StatusCode::REQUEST_TIMEOUT,
        ErrorCode::Unsupported => StatusCode::NOT_IMPLEMENTED,
        ErrorCode::Transient => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_code_name(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Validation => "validation",
        ErrorCode::NotFound => "not_found",
        ErrorCode::Conflict => "conflict",
        ErrorCode::Cancelled => "cancelled",
        ErrorCode::Timeout => "timeout",
        ErrorCode::Unsupported => "unsupported",
        ErrorCode::Transient => "transient",
        ErrorCode::Internal => "internal",
    }
}
