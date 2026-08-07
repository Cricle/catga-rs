//! Tests for CatgaError to HTTP response mapping behavior.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    routing::post,
};
use catga_axum::{
    CatgaHttpError, CatgaHttpResult, EndpointKind, EndpointMetadata, EndpointMethod,
    EndpointValidation, IntoCatgaHttpResponse, MediatorState, StatusCode,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Handler, Mediator, Message, Registry, Request,
    current_correlation_id, current_correlation_value,
};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test messages
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize)]
struct EchoRequest {
    value: String,
}

impl Message for EchoRequest {}
impl Request for EchoRequest {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct EchoHandler;

#[async_trait]
impl Handler<EchoRequest> for EchoHandler {
    async fn handle(&self, request: EchoRequest) -> CatgaResult<String> {
        Ok(request.value)
    }
}

fn test_mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry.register_request::<EchoRequest, _>(EchoHandler).unwrap();
    Arc::new(Mediator::new(registry))
}

// ---------------------------------------------------------------------------
// CatgaHttpError basic conversion tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catga_http_error_converts_validation_error_to_422() {
    let error = CatgaError::new(ErrorCode::Validation, "field is required");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn catga_http_error_converts_not_found_to_404() {
    let error = CatgaError::new(ErrorCode::NotFound, "resource not found");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn catga_http_error_converts_conflict_to_409() {
    let error = CatgaError::new(ErrorCode::Conflict, "already exists");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn catga_http_error_converts_unauthorized_to_401() {
    let error = CatgaError::new(ErrorCode::Unauthorized, "invalid credentials");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn catga_http_error_converts_forbidden_to_403() {
    let error = CatgaError::new(ErrorCode::Forbidden, "access denied");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn catga_http_error_converts_internal_to_500() {
    let error = CatgaError::new(ErrorCode::Internal, "something went wrong");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn catga_http_error_converts_transient_to_503() {
    let error = CatgaError::new(ErrorCode::Transient, "service temporarily unavailable");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn catga_http_error_converts_timeout_to_408() {
    let error = CatgaError::new(ErrorCode::Timeout, "request timed out");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
}

#[tokio::test]
async fn catga_http_error_converts_unsupported_to_501() {
    let error = CatgaError::new(ErrorCode::Unsupported, "operation not supported");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn catga_http_error_body_contains_code_and_message() {
    let error = CatgaError::new(ErrorCode::Validation, "test validation message");
    let response = CatgaHttpError::from(error).into_response();

    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(parsed["code"], "validation");
    assert_eq!(parsed["message"], "test validation message");
}

// ---------------------------------------------------------------------------
// CatgaHttpError with details
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catga_http_error_includes_details_when_present() {
    let error = CatgaError::new(ErrorCode::Validation, "validation failed")
        .with_details("field 'email' is invalid format");
    let response = CatgaHttpError::from(error).into_response();

    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Details are not in the JSON body - they're for internal logging only
    assert_eq!(parsed["code"], "validation");
    assert_eq!(parsed["message"], "validation failed");
}

// ---------------------------------------------------------------------------
// ErrorCode http_status_u16 tests (exhaustive for each variant)
// ---------------------------------------------------------------------------

#[test]
fn http_status_validation_is_422() {
    assert_eq!(ErrorCode::Validation.http_status_u16(), 422);
}

#[test]
fn http_status_handler_failed_is_400() {
    assert_eq!(ErrorCode::HandlerFailed.http_status_u16(), 400);
}

#[test]
fn http_status_handler_not_found_is_404() {
    assert_eq!(ErrorCode::HandlerNotFound.http_status_u16(), 404);
}

#[test]
fn http_status_pipeline_failed_is_400() {
    assert_eq!(ErrorCode::PipelineFailed.http_status_u16(), 400);
}

#[test]
fn http_status_persistence_failed_is_503() {
    assert_eq!(ErrorCode::PersistenceFailed.http_status_u16(), 503);
}

#[test]
fn http_status_lock_failed_is_503() {
    assert_eq!(ErrorCode::LockFailed.http_status_u16(), 503);
}

#[test]
fn http_status_transport_failed_is_503() {
    assert_eq!(ErrorCode::TransportFailed.http_status_u16(), 503);
}

#[test]
fn http_status_serialization_failed_is_400() {
    assert_eq!(ErrorCode::SerializationFailed.http_status_u16(), 400);
}

#[test]
fn http_status_not_found_is_404() {
    assert_eq!(ErrorCode::NotFound.http_status_u16(), 404);
}

#[test]
fn http_status_conflict_is_409() {
    assert_eq!(ErrorCode::Conflict.http_status_u16(), 409);
}

#[test]
fn http_status_unauthorized_is_401() {
    assert_eq!(ErrorCode::Unauthorized.http_status_u16(), 401);
}

#[test]
fn http_status_forbidden_is_403() {
    assert_eq!(ErrorCode::Forbidden.http_status_u16(), 403);
}

#[test]
fn http_status_cancelled_is_408() {
    assert_eq!(ErrorCode::Cancelled.http_status_u16(), 408);
}

#[test]
fn http_status_timeout_is_408() {
    assert_eq!(ErrorCode::Timeout.http_status_u16(), 408);
}

#[test]
fn http_status_flow_failed_is_400() {
    assert_eq!(ErrorCode::FlowFailed.http_status_u16(), 400);
}

#[test]
fn http_status_flow_cancelled_is_408() {
    assert_eq!(ErrorCode::FlowCancelled.http_status_u16(), 408);
}

#[test]
fn http_status_flow_timeout_is_408() {
    assert_eq!(ErrorCode::FlowTimeout.http_status_u16(), 408);
}

#[test]
fn http_status_flow_compensating_is_400() {
    assert_eq!(ErrorCode::FlowCompensating.http_status_u16(), 400);
}

#[test]
fn http_status_unsupported_is_501() {
    assert_eq!(ErrorCode::Unsupported.http_status_u16(), 501);
}

#[test]
fn http_status_transient_is_503() {
    assert_eq!(ErrorCode::Transient.http_status_u16(), 503);
}

#[test]
fn http_status_unavailable_is_503() {
    assert_eq!(ErrorCode::Unavailable.http_status_u16(), 503);
}

#[test]
fn http_status_internal_is_500() {
    assert_eq!(ErrorCode::Internal.http_status_u16(), 500);
}

// ---------------------------------------------------------------------------
// ErrorCode::as_stable_str tests
// ---------------------------------------------------------------------------

#[test]
fn error_code_as_stable_str_returns_lowercase_string() {
    assert_eq!(ErrorCode::Validation.as_stable_str(), "validation");
    assert_eq!(ErrorCode::NotFound.as_stable_str(), "not_found");
    assert_eq!(ErrorCode::Internal.as_stable_str(), "internal");
}

#[test]
fn error_code_stable_str_is_consistent_across_calls() {
    let code = ErrorCode::Conflict;
    assert_eq!(code.as_stable_str(), code.as_stable_str());
}

// ---------------------------------------------------------------------------
// IntoCatgaHttpResponse trait tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn into_catga_response_maps_success_with_custom_status() {
    let result: CatgaResult<String> = Ok("hello".to_string());
    let response = result.into_catga_response(StatusCode::CREATED);

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(body, r#""hello""#);
}

#[tokio::test]
async fn into_catga_response_maps_error_regardless_of_success_status() {
    let result: CatgaResult<String> = Err(CatgaError::new(ErrorCode::NotFound, "not found"));
    let response = result.into_catga_response(StatusCode::CREATED);

    // Error status wins
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn into_catga_response_returns_no_content_for_unit_type() {
    let result: CatgaResult<()> = Ok(());
    let response = result.into_catga_response(StatusCode::NO_CONTENT);

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn into_catga_created_uses_201_status() {
    let result: CatgaResult<String> = Ok("created".to_string());
    let response = result.into_catga_created("/resources/123");

    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(response.headers().contains_key(http::header::LOCATION));
}

#[tokio::test]
async fn into_catga_created_maps_error_on_success() {
    let result: CatgaResult<String> = Err(CatgaError::new(ErrorCode::Internal, "db error"));
    let response = result.into_catga_created("/resources/123");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn into_catga_created_returns_500_for_invalid_location() {
    let result: CatgaResult<String> = Ok("ok".to_string());
    // Invalid Location header (contains newline)
    let response = result.into_catga_created("/resources\n/invalid");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert!(body.contains("invalid Location header"));
}

// ---------------------------------------------------------------------------
// EndpointMetadata tests
// ---------------------------------------------------------------------------

#[test]
fn endpoint_metadata_command_kind() {
    #[derive(serde::Serialize)]
    struct TestCommand;
    impl Message for TestCommand {}
    impl Request for TestCommand {
        type Response = ();
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::command::<TestCommand>("/test");

    assert_eq!(metadata.kind(), EndpointKind::Command);
    assert_eq!(metadata.path(), "/test");
    assert_eq!(metadata.tag(), "Commands");
}

#[test]
fn endpoint_metadata_query_kind() {
    #[derive(serde::Serialize)]
    struct TestQuery;
    impl Message for TestQuery {}
    impl Request for TestQuery {
        type Response = String;
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::query::<TestQuery>("/test");

    assert_eq!(metadata.kind(), EndpointKind::Query);
    assert_eq!(metadata.path(), "/test");
    assert_eq!(metadata.tag(), "Queries");
}

#[test]
fn endpoint_metadata_event_kind() {
    struct TestEvent;
    impl Message for TestEvent {}
    impl catga_core::Event for TestEvent {
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::event::<TestEvent>("/test");

    assert_eq!(metadata.kind(), EndpointKind::Event);
    assert_eq!(metadata.path(), "/test");
    assert_eq!(metadata.tag(), "Events");
}

#[test]
fn endpoint_metadata_with_custom_operation_id() {
    #[derive(serde::Serialize)]
    struct TestRequest;
    impl Message for TestRequest {}
    impl Request for TestRequest {
        type Response = ();
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::command::<TestRequest>("/test")
        .with_operation_id("custom-op-id");

    assert_eq!(metadata.operation_id(), "custom-op-id");
}

#[test]
fn endpoint_metadata_with_description() {
    #[derive(serde::Serialize)]
    struct TestRequest;
    impl Message for TestRequest {}
    impl Request for TestRequest {
        type Response = ();
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::command::<TestRequest>("/test")
        .with_description("This endpoint does something important");

    assert_eq!(metadata.description(), Some("This endpoint does something important"));
}

#[test]
fn endpoint_metadata_response_statuses_command() {
    #[derive(serde::Serialize)]
    struct TestCommand;
    impl Message for TestCommand {}
    impl Request for TestCommand {
        type Response = ();
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::command::<TestCommand>("/test");
    let statuses = metadata.response_statuses();

    assert!(statuses.contains(&StatusCode::OK));
    assert!(statuses.contains(&StatusCode::UNPROCESSABLE_ENTITY));
    assert!(statuses.contains(&StatusCode::NOT_FOUND));
    assert!(statuses.contains(&StatusCode::CONFLICT));
}

#[test]
fn endpoint_metadata_response_statuses_query() {
    #[derive(serde::Serialize)]
    struct TestQuery;
    impl Message for TestQuery {}
    impl Request for TestQuery {
        type Response = String;
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::query::<TestQuery>("/test");
    let statuses = metadata.response_statuses();

    assert!(statuses.contains(&StatusCode::OK));
    assert!(statuses.contains(&StatusCode::NOT_FOUND));
}

#[test]
fn endpoint_metadata_response_statuses_event() {
    struct TestEvent;
    impl Message for TestEvent {}
    impl catga_core::Event for TestEvent {
        type TypeId = catga_core::DefaultMessageTypeId;
    }

    let metadata = EndpointMetadata::event::<TestEvent>("/test");
    let statuses = metadata.response_statuses();

    assert!(statuses.contains(&StatusCode::NO_CONTENT));
}

// ---------------------------------------------------------------------------
// EndpointMethod tests
// ---------------------------------------------------------------------------

#[test]
fn endpoint_method_get() {
    assert_eq!(EndpointMethod::Get.as_http_method(), Method::GET);
}

#[test]
fn endpoint_method_post() {
    assert_eq!(EndpointMethod::Post.as_http_method(), Method::POST);
}

#[test]
fn endpoint_method_put() {
    assert_eq!(EndpointMethod::Put.as_http_method(), Method::PUT);
}

#[test]
fn endpoint_method_patch() {
    assert_eq!(EndpointMethod::Patch.as_http_method(), Method::PATCH);
}

#[test]
fn endpoint_method_delete() {
    assert_eq!(EndpointMethod::Delete.as_http_method(), Method::DELETE);
}

// ---------------------------------------------------------------------------
// CatgaHttpResult type alias tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catga_http_result_can_return_success() {
    async fn handler() -> CatgaHttpResult<Json<String>> {
        Ok(Json("success".to_string()))
    }

    let app = Router::new()
        .route("/test", post(handler));

    let request = axum::http::Request::post("/test")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn catga_http_result_can_return_error() {
    async fn handler() -> CatgaHttpResult<Json<String>> {
        Err(CatgaHttpError::from(CatgaError::new(
            ErrorCode::NotFound,
            "not found",
        )))
    }

    let app = Router::new()
        .route("/test", post(handler));

    let request = axum::http::Request::post("/test")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
