use catga_core::ErrorCode;

#[test]
fn database_error_contains_operation_and_message() {
    let error = super::database_error("insert flow", "connection refused");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    let message = error.message();
    assert!(message.contains("insert flow"));
    assert!(message.contains("connection refused"));
    assert!(message.contains("SQL FlowStore"));
}

#[test]
fn database_error_handles_empty_operation() {
    let error = super::database_error("", "timeout");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    let message = error.message();
    assert!(message.contains("timeout"));
}

#[test]
fn database_error_handles_unicode_message() {
    let error = super::database_error("query", "connection error: \u{4e2d}\u{6587}");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(error.message().contains("\u{4e2d}\u{6587}"));
}
