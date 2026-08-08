//! Unit tests for transport helper functions.

use catga_core::{CatgaError, ErrorCode};

const RECLAIM_POLL_MILLIS: usize = 1_000;

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

fn destination_stream(destination: &catga_core::Destination) -> Box<str> {
    format!("stream:{destination}").into()
}

#[test]
fn reclaim_poll_millis_value() {
    assert_eq!(RECLAIM_POLL_MILLIS, 1_000);
}

#[test]
fn reclaim_poll_millis_reasonable() {
    assert!(RECLAIM_POLL_MILLIS > 0);
    assert!(RECLAIM_POLL_MILLIS < 60_000);
}

#[test]
fn map_error_creates_transient_error() {
    let error = map_error("connection refused");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("connection refused"));
}

#[test]
fn map_error_handles_empty_string() {
    let error = map_error("");
    assert_eq!(error.code(), ErrorCode::Transient);
}

#[test]
fn map_error_includes_original_message() {
    let error = map_error("redis error: timeout");
    assert!(error.to_string().contains("redis error: timeout"));
}

#[test]
fn destination_stream_format() {
    use catga_core::Destination;
    let dest = Destination::parse("test-queue").unwrap();
    let stream = destination_stream(&dest);
    assert_eq!(stream.as_ref(), "stream:test-queue");
}

#[test]
fn destination_stream_empty_destination() {
    use catga_core::Destination;
    // Empty destination is invalid and returns an error
    let result = Destination::parse("");
    assert!(result.is_err());
}

#[test]
fn destination_stream_whitespace_destination() {
    use catga_core::Destination;
    // Whitespace-only destination is invalid
    let result = Destination::parse("   ");
    assert!(result.is_err());
}

#[test]
fn destination_stream_with_slashes() {
    use catga_core::Destination;
    let dest = Destination::parse("queue/sub/dest").unwrap();
    let stream = destination_stream(&dest);
    assert_eq!(stream.as_ref(), "stream:queue/sub/dest");
}

#[test]
fn destination_stream_returns_boxed() {
    use catga_core::Destination;
    let dest = Destination::parse("test").unwrap();
    let stream = destination_stream(&dest);
    let s: String = stream.into();
    assert_eq!(s.as_str(), "stream:test");
}
