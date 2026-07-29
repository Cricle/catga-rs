//! Task-scoped cooperative cancellation for mediator dispatch.

use std::future::Future;

use tokio_util::sync::CancellationToken;

use crate::{CatgaError, CatgaResult, ErrorCode};

tokio::task_local! {
    static DISPATCH_CANCELLATION: CancellationToken;
}

/// Returns the cancellation token scoped to the current mediator dispatch.
///
/// The returned token is a cheap shared handle. It is present only inside a
/// `*_with_cancellation` mediator call, never in an ordinary dispatch, and it
/// is not stored in a message or a global registry.
///
/// ```
/// use catga_core::current_cancellation;
///
/// // Outside a scoped dispatch, no token is available.
/// assert!(current_cancellation().is_none());
/// ```
pub fn current_cancellation() -> Option<CancellationToken> {
    DISPATCH_CANCELLATION.try_with(Clone::clone).ok()
}

/// Runs `future` with `cancellation` available through [`current_cancellation`].
///
/// ```
/// use tokio_util::sync::CancellationToken;
/// use catga_core::{current_cancellation, scope_cancellation};
///
/// # async fn run() {
/// let token = CancellationToken::new();
/// let result = scope_cancellation(token.clone(), async {
///     current_cancellation().expect("token is scoped")
/// }).await;
/// assert!(!result.is_cancelled());
/// # }
/// ```
pub async fn scope_cancellation<T>(
    cancellation: CancellationToken,
    future: impl Future<Output = T>,
) -> T {
    DISPATCH_CANCELLATION.scope(cancellation, future).await
}

/// Awaits one Catga operation while treating cancellation as a structured outcome.
///
/// A pre-cancelled token prevents the operation from starting. Later cancellation
/// drops the operation future and returns [`ErrorCode::Cancelled`], preserving
/// Rust's normal ownership-based cancellation model without retaining a task.
pub async fn until_cancelled<T>(
    cancellation: CancellationToken,
    operation: impl Future<Output = CatgaResult<T>>,
) -> CatgaResult<T> {
    if cancellation.is_cancelled() {
        return Err(cancelled_error());
    }
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error()),
        result = operation => result,
    }
}

fn cancelled_error() -> CatgaError {
    CatgaError::new(ErrorCode::Cancelled, "mediator dispatch was cancelled")
}
