use crate::MessageMetadata;

tokio::task_local! {
    static CORRELATION_ID: u64;
}

/// Supplies metadata for requests that participate in ambient correlation propagation.
pub trait Correlated {
    /// Returns the message metadata that carries this request's correlation identity.
    fn metadata(&self) -> MessageMetadata;
}

/// Returns the correlation identifier scoped to the current asynchronous task chain.
pub fn current_correlation_id() -> Option<u64> {
    CORRELATION_ID.try_with(|id| *id).ok()
}

pub(crate) async fn scope<T>(correlation_id: u64, future: impl Future<Output = T>) -> T {
    CORRELATION_ID.scope(correlation_id, future).await
}
