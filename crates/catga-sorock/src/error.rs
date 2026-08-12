//! Maps sorock, tonic, and redb failures into [`CatgaError`].
//!
//! sorock 0.12 surfaces failures as [`anyhow::Error`] internally, as
//! [`tonic::Status`] on the client side (including when a server handler fails,
//! for example when no leader is known yet), and as redb errors at the storage
//! layer. The mapping functions below classify them into stable
//! [`ErrorCode`] categories so callers can make retry decisions without
//! depending on backend types.

use catga_core::{CatgaError, ErrorCode};

/// Maps a gRPC status into a [`CatgaError`].
///
/// `Unavailable` maps to [`ErrorCode::Unavailable`] and `DeadlineExceeded` to
/// [`ErrorCode::Timeout`] (both retryable). `Unknown` maps to
/// [`ErrorCode::Transient`] and `Cancelled` to [`ErrorCode::Cancelled`]:
/// sorock 0.12 aborts in-flight requests when no leader is known — the
/// service handler unwraps the internal `leader is unknown` error, and the
/// aborted handler surfaces to clients as `Unknown` or `Cancelled` depending
/// on how the stream teardown raced the reply. Both are safe to retry once an
/// election completes.
pub fn status_to_catga(status: &tonic::Status) -> CatgaError {
    let code = match status.code() {
        tonic::Code::Unavailable => ErrorCode::Unavailable,
        tonic::Code::DeadlineExceeded => ErrorCode::Timeout,
        tonic::Code::Cancelled => ErrorCode::Cancelled,
        tonic::Code::NotFound => ErrorCode::NotFound,
        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition => ErrorCode::Conflict,
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => ErrorCode::Validation,
        tonic::Code::Unauthenticated => ErrorCode::Unauthorized,
        tonic::Code::PermissionDenied => ErrorCode::Forbidden,
        tonic::Code::Unknown | tonic::Code::Aborted | tonic::Code::ResourceExhausted => {
            ErrorCode::Transient
        }
        _ => ErrorCode::Internal,
    };
    CatgaError::new(
        code,
        format!("sorock gRPC request failed: {}", status.message()),
    )
    .with_details(format!("grpc code: {:?}", status.code()))
}

/// Maps a tonic transport error into a retryable [`CatgaError`].
pub fn transport_to_catga(error: &tonic::transport::Error) -> CatgaError {
    CatgaError::new(
        ErrorCode::TransportFailed,
        format!("sorock transport failed: {error}"),
    )
}

/// Maps a redb database error into a [`CatgaError`].
///
/// Opening or creating the storage file is not blindly retryable (the path may
/// be wrong or locked by another process), so this maps to
/// [`ErrorCode::PersistenceFailed`].
pub fn database_to_catga(error: &redb::DatabaseError) -> CatgaError {
    CatgaError::new(
        ErrorCode::PersistenceFailed,
        format!("sorock redb storage failed: {error}"),
    )
}

/// Maps an opaque sorock [`anyhow::Error`] into a [`CatgaError`].
///
/// sorock 0.12 does not expose its internal error enum outside the crate, so
/// the stable text of well-known failures is matched to preserve categories;
/// everything else maps to [`ErrorCode::Internal`].
pub fn anyhow_to_catga(error: &anyhow::Error) -> CatgaError {
    let text = error.to_string();
    let code = if text.contains("leader is unknown") {
        ErrorCode::Unavailable
    } else if text.contains("not found") {
        ErrorCode::NotFound
    } else {
        ErrorCode::Internal
    };
    CatgaError::new(code, format!("sorock node failed: {text}"))
}
