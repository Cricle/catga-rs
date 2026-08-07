//! Unit tests for the catga-axum crate.
//!
//! Tests HTTP layer logic: client requests, compatibility helpers, and error conversions.

use std::sync::Arc;

use axum::response::IntoResponse;
use catga_core::{
    CatgaError, CatgaResult, DefaultMessageTypeId, EndpointValidation, ErrorCode, Event, Message,
    Registry, Request, validate_required,
};
use http::{HeaderMap, Method, StatusCode};
use reqwest::Method as ReqwestMethod;
use serde::{Deserialize, Serialize};

use crate::client::{
    CorrelationHttpClient, DEFAULT_FORWARD_PATH_PREFIX,
    DEFAULT_HTTP_CLUSTER_FORWARD_RESPONSE_LIMIT_BYTES, HttpClusterForwarder,
};
use crate::compat::{event_route_with_method, mediator_route_with_method};
use crate::{
    CatgaHttpError, CatgaHttpResult, EndpointKind, EndpointMetadata, EndpointMethod,
    IntoCatgaHttpResponse,
};

// Test types for EndpointMetadata tests
#[derive(Deserialize, Serialize)]
struct TestRequest;

impl Message for TestRequest {}

impl Request for TestRequest {
    type Response = ();
    type TypeId = DefaultMessageTypeId;
}

#[derive(Clone, Deserialize, Serialize)]
struct TestEvent;

impl Message for TestEvent {}

impl Event for TestEvent {
    type TypeId = DefaultMessageTypeId;
}

// ---------------------------------------------------------------------------
// Client tests
// ---------------------------------------------------------------------------

mod client_tests {
    use super::*;

    #[test]
    fn test_correlation_http_client_new() {
        let client = reqwest::Client::new();
        let _ = CorrelationHttpClient::new(client);
    }

    #[test]
    fn test_correlation_http_client_request() {
        let client = reqwest::Client::new();
        let correlation_client = CorrelationHttpClient::new(client);
        let headers = HeaderMap::new();
        let _request =
            correlation_client.request(ReqwestMethod::POST, "http://localhost:8080/test", headers);
    }

    #[test]
    fn test_correlation_http_client_post() {
        let client = reqwest::Client::new();
        let correlation_client = CorrelationHttpClient::new(client);
        let headers = HeaderMap::new();
        let _request = correlation_client.post("http://localhost:8080/test", headers);
    }

    #[test]
    fn test_http_cluster_forwarder_new() {
        let client = reqwest::Client::new();
        let _forwarder = HttpClusterForwarder::new(client);
    }

    #[test]
    fn test_http_cluster_forwarder_with_response_limit() {
        let client = reqwest::Client::new();
        let limit = std::num::NonZeroUsize::new(2048).expect("2048 is non-zero");
        let _forwarder = HttpClusterForwarder::with_response_limit(client, limit);
    }

    #[test]
    fn test_http_cluster_forwarder_with_path_prefix() {
        let client = reqwest::Client::new();
        let _forwarder = HttpClusterForwarder::new(client).with_path_prefix("/custom/api");
    }

    #[test]
    fn test_http_cluster_forwarder_with_custom_builder() {
        let client = reqwest::Client::new();
        let _forwarder = HttpClusterForwarder::new(client).with_path_builder(|leader, req_type| {
            format!("{}/v2/custom/{}/endpoint", leader, req_type)
        });
    }

    #[test]
    fn test_default_forward_path_prefix() {
        assert_eq!(DEFAULT_FORWARD_PATH_PREFIX, "/api/catga/forward");
    }

    #[test]
    fn test_default_response_limit_bytes() {
        assert_eq!(
            DEFAULT_HTTP_CLUSTER_FORWARD_RESPONSE_LIMIT_BYTES,
            1024 * 1024
        );
    }
}

// ---------------------------------------------------------------------------
// Compatibility layer tests
// ---------------------------------------------------------------------------

mod compat_tests {
    use super::*;

    #[test]
    fn test_mediator_route_with_method_invalid_path_root() {
        let mediator = Arc::new(catga_core::Mediator::new(Registry::new()));

        // Path "/" is invalid
        let result = mediator_route_with_method::<TestRequest>(EndpointMethod::Post, "/", mediator);
        assert!(result.is_err());
    }

    #[test]
    fn test_mediator_route_with_method_invalid_path_no_slash() {
        let mediator = Arc::new(catga_core::Mediator::new(Registry::new()));

        // Path without leading slash is invalid
        let result =
            mediator_route_with_method::<TestRequest>(EndpointMethod::Post, "api/test", mediator);
        assert!(result.is_err());
    }

    #[test]
    fn test_event_route_with_method_invalid_path_root() {
        let mediator = Arc::new(catga_core::Mediator::new(Registry::new()));

        // Path "/" is invalid for events
        let result = event_route_with_method::<TestEvent>(EndpointMethod::Post, "/", mediator);
        assert!(result.is_err());
    }

    #[test]
    fn test_event_route_with_method_invalid_path_no_slash() {
        let mediator = Arc::new(catga_core::Mediator::new(Registry::new()));

        // Path without leading slash is invalid
        let result =
            event_route_with_method::<TestEvent>(EndpointMethod::Post, "api/test", mediator);
        assert!(result.is_err());
    }
}

// ---------------------------------------------------------------------------
// Error conversion tests
// ---------------------------------------------------------------------------

mod error_conversion_tests {
    use super::*;

    #[test]
    fn test_catga_http_error_from_validation_error() {
        let mut validation = EndpointValidation::new();
        validation.add(validate_required(None, "name"));
        let result: CatgaHttpResult<()> = validation.into_result().map_err(Into::into);
        assert!(result.is_err());
    }

    #[test]
    fn test_catga_http_error_into_response_validation() {
        let error = CatgaError::new(ErrorCode::Validation, "validation failed");
        let http_error = CatgaHttpError::from(error);
        let response = http_error.into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn test_catga_http_error_into_response_internal() {
        let error = CatgaError::new(ErrorCode::Internal, "internal error");
        let http_error = CatgaHttpError::from(error);
        let response = http_error.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_catga_http_error_into_response_not_found() {
        let error = CatgaError::new(ErrorCode::NotFound, "not found");
        let http_error = CatgaHttpError::from(error);
        let response = http_error.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_catga_http_error_into_response_conflict() {
        let error = CatgaError::new(ErrorCode::Conflict, "conflict");
        let http_error = CatgaHttpError::from(error);
        let response = http_error.into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn test_into_catga_http_response_ok_with_content() {
        let result: CatgaResult<String> = Ok("test".to_string());
        let response = result.into_catga_response(StatusCode::OK);
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_into_catga_http_response_ok_no_content() {
        let result: CatgaResult<String> = Ok("test".to_string());
        let response = result.into_catga_response(StatusCode::NO_CONTENT);
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[test]
    fn test_into_catga_http_response_ok_with_created_status() {
        let result: CatgaResult<String> = Ok("test".to_string());
        let response = result.into_catga_response(StatusCode::CREATED);
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[test]
    fn test_into_catga_http_response_err() {
        let result: CatgaResult<String> = Err(CatgaError::new(ErrorCode::NotFound, "not found"));
        let response = result.into_catga_response(StatusCode::OK);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_into_catga_created_valid_location() {
        let result: CatgaResult<String> = Ok("created".to_string());
        let response = result.into_catga_created("/api/resources/123");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert!(response.headers().contains_key(http::header::LOCATION));
    }

    #[test]
    fn test_into_catga_created_invalid_location() {
        let result: CatgaResult<String> = Ok("created".to_string());
        let response = result.into_catga_created("\ninvalid location");
        // Invalid location header should result in internal error
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_into_catga_created_err() {
        let result: CatgaResult<String> = Err(CatgaError::new(ErrorCode::Internal, "failed"));
        let response = result.into_catga_created("/api/resources/123");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}

// ---------------------------------------------------------------------------
// Endpoint metadata tests
// ---------------------------------------------------------------------------

mod endpoint_metadata_tests {
    use super::*;

    #[test]
    fn test_endpoint_method_filter_get() {
        use axum::routing::MethodFilter;
        assert_eq!(EndpointMethod::Get.filter(), MethodFilter::GET);
    }

    #[test]
    fn test_endpoint_method_filter_post() {
        use axum::routing::MethodFilter;
        assert_eq!(EndpointMethod::Post.filter(), MethodFilter::POST);
    }

    #[test]
    fn test_endpoint_method_filter_put() {
        use axum::routing::MethodFilter;
        assert_eq!(EndpointMethod::Put.filter(), MethodFilter::PUT);
    }

    #[test]
    fn test_endpoint_method_filter_patch() {
        use axum::routing::MethodFilter;
        assert_eq!(EndpointMethod::Patch.filter(), MethodFilter::PATCH);
    }

    #[test]
    fn test_endpoint_method_filter_delete() {
        use axum::routing::MethodFilter;
        assert_eq!(EndpointMethod::Delete.filter(), MethodFilter::DELETE);
    }

    #[test]
    fn test_endpoint_method_as_http_method() {
        assert_eq!(EndpointMethod::Get.as_http_method(), Method::GET);
        assert_eq!(EndpointMethod::Post.as_http_method(), Method::POST);
        assert_eq!(EndpointMethod::Put.as_http_method(), Method::PUT);
        assert_eq!(EndpointMethod::Patch.as_http_method(), Method::PATCH);
        assert_eq!(EndpointMethod::Delete.as_http_method(), Method::DELETE);
    }

    #[test]
    fn test_endpoint_kind_tag() {
        assert_eq!(EndpointKind::Command.tag(), "Commands");
        assert_eq!(EndpointKind::Query.tag(), "Queries");
        assert_eq!(EndpointKind::Event.tag(), "Events");
    }

    #[test]
    fn test_endpoint_metadata_command() {
        let meta = EndpointMetadata::command::<TestRequest>("/test");
        assert_eq!(meta.kind(), EndpointKind::Command);
        assert_eq!(meta.path(), "/test");
        assert!(meta.description().is_none());
    }

    #[test]
    fn test_endpoint_metadata_query() {
        let meta = EndpointMetadata::query::<TestRequest>("/query");
        assert_eq!(meta.kind(), EndpointKind::Query);
        assert_eq!(meta.path(), "/query");
    }

    #[test]
    fn test_endpoint_metadata_event() {
        let meta = EndpointMetadata::event::<TestEvent>("/event");
        assert_eq!(meta.kind(), EndpointKind::Event);
        assert_eq!(meta.path(), "/event");
    }

    #[test]
    fn test_endpoint_metadata_with_operation_id() {
        let meta =
            EndpointMetadata::command::<TestRequest>("/test").with_operation_id("custom_operation");
        assert_eq!(meta.operation_id(), "custom_operation");
    }

    #[test]
    fn test_endpoint_metadata_with_description() {
        let meta =
            EndpointMetadata::command::<TestRequest>("/test").with_description("Test endpoint");
        assert_eq!(meta.description(), Some("Test endpoint"));
    }

    #[test]
    fn test_endpoint_metadata_response_statuses_command() {
        let meta = EndpointMetadata::command::<TestRequest>("/test");
        let statuses = meta.response_statuses();
        assert!(statuses.contains(&StatusCode::OK));
        assert!(statuses.contains(&StatusCode::UNPROCESSABLE_ENTITY));
        assert!(statuses.contains(&StatusCode::NOT_FOUND));
        assert!(statuses.contains(&StatusCode::CONFLICT));
    }

    #[test]
    fn test_endpoint_metadata_response_statuses_query() {
        let meta = EndpointMetadata::query::<TestRequest>("/test");
        let statuses = meta.response_statuses();
        assert!(statuses.contains(&StatusCode::OK));
        assert!(statuses.contains(&StatusCode::NOT_FOUND));
        assert_eq!(statuses.len(), 2);
    }

    #[test]
    fn test_endpoint_metadata_response_statuses_event() {
        let meta = EndpointMetadata::event::<TestEvent>("/test");
        let statuses = meta.response_statuses();
        assert_eq!(statuses, &[StatusCode::NO_CONTENT]);
    }

    #[test]
    fn test_endpoint_metadata_method() {
        let meta =
            EndpointMetadata::command_with_method::<TestRequest>(EndpointMethod::Get, "/test");
        assert_eq!(meta.method(), Method::GET);
    }

    #[test]
    fn test_endpoint_metadata_clone() {
        let meta1 = EndpointMetadata::command::<TestRequest>("/test");
        let meta2 = meta1;
        assert_eq!(meta1.path(), meta2.path());
        assert_eq!(meta1.kind(), meta2.kind());
    }

    #[test]
    fn test_endpoint_metadata_debug() {
        let meta = EndpointMetadata::command::<TestRequest>("/test");
        let debug_str = format!("{:?}", meta);
        assert!(debug_str.contains("EndpointMetadata"));
    }

    #[test]
    fn test_endpoint_kind_eq() {
        assert_eq!(EndpointKind::Command, EndpointKind::Command);
        assert_eq!(EndpointKind::Query, EndpointKind::Query);
        assert_eq!(EndpointKind::Event, EndpointKind::Event);
        assert_ne!(EndpointKind::Command, EndpointKind::Query);
    }

    #[test]
    fn test_endpoint_method_eq() {
        assert_eq!(EndpointMethod::Get, EndpointMethod::Get);
        assert_eq!(EndpointMethod::Post, EndpointMethod::Post);
        assert_ne!(EndpointMethod::Get, EndpointMethod::Post);
    }

    #[test]
    fn test_endpoint_method_copy() {
        let method = EndpointMethod::Post;
        let _copied = method;
    }

    #[test]
    fn test_endpoint_kind_copy() {
        let kind = EndpointKind::Command;
        let _copied = kind;
    }
}

// ---------------------------------------------------------------------------
// Correlation tests
// ---------------------------------------------------------------------------

mod correlation_tests {
    use super::*;

    #[test]
    fn test_propagate_correlation_header_no_existing() {
        let mut headers = HeaderMap::new();
        crate::propagate_correlation_header(&mut headers);
        // Without any context, no header should be added
        // (This tests the function doesn't panic)
        let _ = headers;
    }

    #[test]
    fn test_propagate_correlation_header_preserves_existing() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::CORRELATION_ID_HEADER,
            "existing-correlation-id"
                .parse()
                .expect("valid header value"),
        );
        crate::propagate_correlation_header(&mut headers);
        assert_eq!(
            headers
                .get(crate::CORRELATION_ID_HEADER)
                .expect("header should be present"),
            "existing-correlation-id"
        );
    }

    #[test]
    fn test_propagate_trace_context_headers_no_existing() {
        let mut headers = HeaderMap::new();
        crate::propagate_trace_context_headers(&mut headers);
        let _ = headers;
    }

    #[test]
    fn test_propagate_trace_context_headers_preserves_existing() {
        let mut headers = HeaderMap::new();
        headers.insert(
            catga_core::TRACEPARENT_HEADER,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
                .parse()
                .expect("valid traceparent"),
        );
        crate::propagate_trace_context_headers(&mut headers);
        assert!(headers.contains_key(catga_core::TRACEPARENT_HEADER));
    }

    #[test]
    fn test_correlation_id_from_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::CORRELATION_ID_HEADER,
            "12345".parse().expect("valid header value"),
        );
        let id = crate::correlation_id(&headers);
        assert_eq!(id, 12345);
    }

    #[test]
    fn test_correlation_id_generates_when_missing() {
        let headers = HeaderMap::new();
        let id1 = crate::correlation_id(&headers);
        let id2 = crate::correlation_id(&headers);
        // Each call should generate a unique ID
        assert!(id2 > id1);
    }

    #[test]
    fn test_correlation_id_invalid_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::CORRELATION_ID_HEADER,
            "not-a-number".parse().expect("valid header value"),
        );
        let id = crate::correlation_id(&headers);
        // Should generate a new ID when parsing fails
        assert!(id > 0);
    }
}

// ---------------------------------------------------------------------------
// Validation re-export tests
// ---------------------------------------------------------------------------

mod validation_tests {
    use super::*;

    #[test]
    fn test_validation_re_exports() {
        // Test that all validation functions are accessible
        let mut validation = EndpointValidation::new();
        validation.add(crate::validate_required(None, "test"));
        assert!(!validation.is_valid());
    }

    #[test]
    fn test_validate_required_re_exported() {
        assert!(crate::validate_required(None, "field").is_some());
        assert!(crate::validate_required(Some("value"), "field").is_none());
    }

    #[test]
    fn test_validate_min_length_re_exported() {
        assert!(crate::validate_min_length(Some("ab"), 3, "field").is_some());
        assert!(crate::validate_min_length(Some("abc"), 3, "field").is_none());
    }

    #[test]
    fn test_validate_max_length_re_exported() {
        assert!(crate::validate_max_length(Some("abcdef"), 5, "field").is_some());
        assert!(crate::validate_max_length(Some("abc"), 5, "field").is_none());
    }

    #[test]
    fn test_validate_positive_re_exported() {
        assert!(crate::validate_positive(0i32, "field").is_some());
        assert!(crate::validate_positive(1i32, "field").is_none());
    }

    #[test]
    fn test_validate_range_re_exported() {
        assert!(crate::validate_range(5i32, 10, 20, "field").is_some());
        assert!(crate::validate_range(15i32, 10, 20, "field").is_none());
    }

    #[test]
    fn test_validate_not_empty_re_exported() {
        assert!(crate::validate_not_empty::<i32>(None, "field").is_some());
        assert!(crate::validate_not_empty::<i32>(Some(&[][..]), "field").is_some());
        assert!(crate::validate_not_empty(Some(&[1][..]), "field").is_none());
    }

    #[test]
    fn test_validate_min_count_re_exported() {
        assert!(crate::validate_min_count(Some(&[1][..]), 2, "field").is_some());
        assert!(crate::validate_min_count(Some(&[1, 2][..]), 2, "field").is_none());
    }
}

// ---------------------------------------------------------------------------
// Constants tests
// ---------------------------------------------------------------------------

mod constants_tests {
    #[test]
    fn test_max_raft_message_bytes() {
        assert_eq!(crate::MAX_RAFT_MESSAGE_BYTES, 1024 * 1024);
    }

    #[test]
    fn test_raft_message_path() {
        assert_eq!(crate::RAFT_MESSAGE_PATH, "/api/catga/raft");
    }
}
