use catga_nats::rpc::{map_error, validate_subject};
use catga_core::ErrorCode;

#[test]
fn validate_subject_accepts_valid_subject() {
    assert!(validate_subject("service.foo").is_ok());
    assert!(validate_subject("a").is_ok());
    assert!(validate_subject("FOO.BAR").is_ok());
}

#[test]
fn validate_subject_rejects_empty_subject() {
    let result = validate_subject("");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty"));
}

#[test]
fn validate_subject_rejects_whitespace_only_subject() {
    let result = validate_subject("   ");
    assert!(result.is_err());
}

#[test]
fn validate_subject_rejects_tab_only() {
    let result = validate_subject("\t");
    assert!(result.is_err());
}

#[test]
fn validate_subject_rejects_newline_only() {
    let result = validate_subject("\n");
    assert!(result.is_err());
}

#[test]
fn validate_subject_accepts_deeply_nested() {
    assert!(validate_subject("a.b.c.d.e.f.g.h").is_ok());
}

#[test]
fn validate_subject_accepts_long_subject() {
    let long = "a".repeat(1000);
    assert!(validate_subject(&long).is_ok());
}

#[test]
fn map_error_creates_transient_error() {
    let error = map_error("connection lost");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("connection lost"));
}

#[test]
fn map_error_handles_empty_string() {
    let error = map_error("");
    assert_eq!(error.code(), ErrorCode::Transient);
}

#[test]
fn map_error_includes_nats_prefix() {
    let error = map_error("nats: connection refused");
    assert!(error.to_string().contains("nats"));
}
