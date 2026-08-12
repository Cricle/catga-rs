//! Shared Redis error mapping for direct keyspace inspection.

use catga_core::{CatgaError, ErrorCode};

/// Maps a raw Redis error into the test error type.
pub fn map_redis_error(error: redis::RedisError) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
