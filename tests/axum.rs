use std::future::IntoFuture;

use axum::{Json, body::to_bytes, response::IntoResponse, routing::post};
use catga_axum::{CORRELATION_ID_HEADER, CatgaHttpError, HttpClusterForwarder, correlation_id};
use catga_cluster::ClusterForwarder;
use catga_core::{CatgaError, ErrorCode, Request};
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn axum_error_response_uses_stable_status_codes_and_compact_json() {
    let response =
        CatgaHttpError::from(CatgaError::new(ErrorCode::Validation, "bad input")).into_response();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"code":"validation","message":"bad input"}"#
    );

    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::NotFound, "missing"))
            .into_response()
            .status(),
        axum::http::StatusCode::NOT_FOUND
    );
    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::Conflict, "busy"))
            .into_response()
            .status(),
        axum::http::StatusCode::CONFLICT
    );
    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::Transient, "retry"))
            .into_response()
            .status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
}

#[test]
fn axum_correlation_id_uses_the_request_header_or_a_lock_free_fallback() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(CORRELATION_ID_HEADER, "42".parse().unwrap());
    assert_eq!(correlation_id(&headers), 42);

    let first = correlation_id(&axum::http::HeaderMap::new());
    let second = correlation_id(&axum::http::HeaderMap::new());
    assert!(second > first);
}

#[derive(Deserialize, Serialize)]
struct ForwardRequest {
    value: u32,
}

impl catga_core::Message for ForwardRequest {}

impl Request for ForwardRequest {
    type Response = u32;
}

#[tokio::test]
async fn http_cluster_forwarder_posts_a_typed_request_to_the_leader() {
    let app = axum::Router::new().route(
        "/api/catga/forward/ForwardRequest",
        post(|Json(request): Json<ForwardRequest>| async move { Json(request.value + 1) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let result = HttpClusterForwarder::new(reqwest::Client::new())
        .forward(ForwardRequest { value: 41 }, &endpoint)
        .await
        .unwrap();
    server.abort();

    assert_eq!(result, 42);
}
