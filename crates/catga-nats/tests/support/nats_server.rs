//! NATS server location for service-backed tests.
//!
//! Tests locate NATS through `CATGA_NATS_URL`, falling back to the conventional local port.
//! The E2E job exports the compose-discovered URL before running `--include-ignored`.

use catga_core::{CatgaError, ErrorCode};

/// Returns the NATS URL tests should connect to.
pub fn server_url() -> String {
    std::env::var("CATGA_NATS_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "nats://127.0.0.1:4222".to_owned())
}

/// Wraps an I/O failure in a `CatgaError` with test context.
pub fn test_error(context: &'static str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, context).with_details(error.to_string())
}
