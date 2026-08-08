//! Unit tests for RPC helper functions.

use catga_core::{CatgaError, ErrorCode};

fn validate_destination(destination: &str) -> Result<(), CatgaError> {
    if destination.is_empty() || destination.trim().is_empty() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "Redis request destination must not be empty",
        ));
    }
    Ok(())
}

fn validate_timeout(timeout_millis: u64) -> Result<(), CatgaError> {
    if timeout_millis == 0 {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "Redis request timeout must be greater than zero",
        ));
    }
    Ok(())
}

fn parse_reply_to(reply_to: &str) -> Option<&str> {
    if reply_to.starts_with("catga.reply.") {
        Some(reply_to)
    } else {
        None
    }
}

#[test]
fn validate_destination_accepts_valid() {
    assert!(validate_destination("request.queue").is_ok());
    assert!(validate_destination("a").is_ok());
}

#[test]
fn validate_destination_rejects_empty() {
    let result = validate_destination("");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty"));
}

#[test]
fn validate_destination_rejects_whitespace() {
    assert!(validate_destination("   ").is_err());
}

#[test]
fn validate_timeout_accepts_positive() {
    assert!(validate_timeout(1).is_ok());
    assert!(validate_timeout(1000).is_ok());
    assert!(validate_timeout(u64::MAX).is_ok());
}

#[test]
fn validate_timeout_rejects_zero() {
    let result = validate_timeout(0);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("timeout"));
}

#[test]
fn parse_reply_to_valid_prefix() {
    assert_eq!(
        parse_reply_to("catga.reply.550e8400-e29b-41d4-a716-446655440000"),
        Some("catga.reply.550e8400-e29b-41d4-a716-446655440000")
    );
}

#[test]
fn parse_reply_to_rejects_invalid_prefix() {
    assert_eq!(parse_reply_to("invalid.reply.123"), None);
    assert_eq!(parse_reply_to(""), None);
    assert_eq!(parse_reply_to("catga.other.123"), None);
}

#[test]
fn parse_reply_to_requires_dot() {
    assert_eq!(parse_reply_to("catga-reply-123"), None);
}
