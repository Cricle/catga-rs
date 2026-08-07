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
#[cfg(any(feature = "mysql", feature = "postgres"))]
pub(crate) fn is_mysql_duplicate_key(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|database| database.kind() == sqlx::error::ErrorKind::UniqueViolation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use catga_core::ErrorCode;

    #[test]
    fn database_error_contains_operation_and_message() {
        let error = database_error("insert flow", "connection refused");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        let message = error.message();
        assert!(message.contains("insert flow"));
        assert!(message.contains("connection refused"));
        assert!(message.contains("SQL FlowStore"));
    }

    #[test]
    fn database_error_handles_empty_operation() {
        let error = database_error("", "timeout");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        let message = error.message();
        assert!(message.contains("timeout"));
    }

    #[test]
    fn database_error_handles_unicode_message() {
        let error = database_error("query", "connection error: \u{4e2d}\u{6587}");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert!(error.message().contains("\u{4e2d}\u{6587}"));
    }
}
