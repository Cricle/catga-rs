//! Shared raw-connection fixture for direct Redis keyspace inspection.

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// Opens a raw multiplexed connection for direct keyspace inspection.
pub async fn raw_connection(url: &str) -> CatgaResult<redis::aio::MultiplexedConnection> {
    let client = redis::Client::open(url)
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))
}
