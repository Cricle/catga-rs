//! Shared environment validation for Redis real-service integration tests.

use std::env;

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// Returns the configured Redis URL, or skips only when running outside CI.
///
/// A CI job without its required service URL is a configuration error rather
/// than a successful no-op, so the E2E contract cannot silently be skipped.
pub fn redis_url() -> CatgaResult<Option<String>> {
    match env::var("CATGA_REDIS_URL") {
        Ok(url) if !url.trim().is_empty() => Ok(Some(url)),
        Ok(_) | Err(env::VarError::NotPresent) if env::var_os("CI").is_none() => Ok(None),
        Ok(_) | Err(env::VarError::NotPresent) => Err(CatgaError::new(
            ErrorCode::Unavailable,
            "CATGA_REDIS_URL must be configured when CI executes Redis E2E tests",
        )),
        Err(error) => Err(CatgaError::new(
            ErrorCode::Validation,
            format!("could not read CATGA_REDIS_URL: {error}"),
        )),
    }
}
