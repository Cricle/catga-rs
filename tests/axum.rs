use axum::{body::to_bytes, response::IntoResponse};
use catga_axum::CatgaHttpError;
use catga_core::{CatgaError, ErrorCode};

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
