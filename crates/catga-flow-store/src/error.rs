//! Error conversion at database boundaries.

use catga_core::{CatgaError, ErrorCode};

/// Converts a database failure into a retryable Catga availability failure.
pub(crate) fn database_error(operation: &str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(
        ErrorCode::Unavailable,
        format!("SQL FlowStore {operation} failed: {error}"),
    )
}

/// Returns whether MySQL rejected an insert because its unique key already exists.
///
/// SQLx maps MySQL's duplicate-key admission (`1062` / SQLSTATE `23000`) to its portable
/// [`sqlx::error::ErrorKind::UniqueViolation`] classification. Handling that error explicitly
/// preserves idempotent-create semantics without `INSERT IGNORE`, which would also turn
/// unrelated invalid-data failures into warnings.
pub(crate) fn is_mysql_duplicate_key(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|database| database.kind() == sqlx::error::ErrorKind::UniqueViolation)
}
