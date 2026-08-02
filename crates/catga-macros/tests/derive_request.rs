//! Tests for the catga_request derive macro.

use catga_core::{Message, Request};
use catga_macros::catga_request;

#[catga_request(response = String)]
struct GetUser(String);

// Test complex type paths
#[catga_request(response = std::time::Duration)]
struct GetTimeout;

// Test generic types
#[catga_request(response = Result<String, std::io::Error>)]
struct GetUserComplex;

#[test]
fn implements_message() {
    let msg = GetUser("123".into());
    assert!(msg.message_type().ends_with("GetUser"));
    assert_eq!(msg.0, "123");
}

#[test]
fn implements_request_with_response() {
    fn assert_request<T: Request>() {}
    assert_request::<GetUser>();
}

#[test]
fn response_type_is_string() {
    fn assert_response_type<R: Request>() -> Option<R::Response> {
        None
    }
    let _ = assert_response_type::<GetUser>();
}

#[test]
fn complex_type_path() {
    fn assert_response_type<R: Request>() -> Option<R::Response> {
        None
    }
    // Duration is the response type
    let _: Option<std::time::Duration> = assert_response_type::<GetTimeout>();
}

#[test]
fn generic_type_result() {
    fn assert_response_type<R: Request>() -> Option<R::Response> {
        None
    }
    // Result<String, Error> is the response type
    let _: Option<Result<String, std::io::Error>> = assert_response_type::<GetUserComplex>();
}
