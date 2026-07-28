//! Shared environment validation for Redis real-service integration tests.

use std::env;

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// Returns the configured Redis URL, or skips when the current job does not provide Redis.
///
/// E2E workflows set `CATGA_REQUIRE_EXTERNAL_SERVICES`; a missing URL there is
/// a configuration error rather than a successful no-op. Quality and coverage
/// jobs intentionally omit that marker because they do not start Redis.
pub fn redis_url() -> CatgaResult<Option<String>> {
    match env::var("CATGA_REDIS_URL") {
        Ok(url) if !url.trim().is_empty() => Ok(Some(url)),
        Ok(_) | Err(env::VarError::NotPresent)
            if !env::var_os("CATGA_REQUIRE_EXTERNAL_SERVICES")
                .is_some_and(|value| !value.is_empty()) =>
        {
            Ok(None)
        }
        Ok(_) | Err(env::VarError::NotPresent) => Err(CatgaError::new(
            ErrorCode::Unavailable,
            "CATGA_REDIS_URL must be configured when Redis E2E is required",
        )),
        Err(error) => Err(CatgaError::new(
            ErrorCode::Validation,
            format!("could not read CATGA_REDIS_URL: {error}"),
        )),
    }
}
