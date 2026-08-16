//! Pluggable codec implementation for Raft messages.
//!
//! This module provides the `RaftCodec` trait and several codec implementations
//! for encoding/decoding Raft messages.
//!
//! # Supported Codecs
//!
//! - `ProstCodec`: Uses prost for protobuf encoding (requires proto definitions)
//! - `BincodeCodec`: Uses bincode for fast binary encoding
//! - `JsonCodec`: Uses JSON for human-readable encoding
//!
//! # Usage
//!
//! ```rust
//! use catga_raft::transport::{RaftCodec, BincodeCodec};
//!
//! let codec = BincodeCodec;
//! let data = vec![1, 2, 3];
//! let encoded = codec.encode(&data).unwrap();
//! let decoded: Vec<u8> = codec.decode(&encoded).unwrap();
//! assert_eq!(data, decoded);
//! ```

use crate::{CatgaRaftError, CatgaRaftResult};

/// Trait for encoding and decoding Raft messages.
///
/// Implementors must be thread-safe (Send + Sync) as they may be accessed
/// from multiple threads concurrently.
pub trait RaftCodec: Send + Sync {
    /// Encode a value to bytes.
    fn encode<T: serde::Serialize>(&self, value: &T) -> CatgaRaftResult<Vec<u8>>;

    /// Decode bytes to a value.
    fn decode<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> CatgaRaftResult<T>;

    /// Encode a Raft message.
    ///
    /// Default implementation uses `encode`.
    fn encode_message<T: serde::Serialize>(&self, msg: &T) -> CatgaRaftResult<Vec<u8>> {
        self.encode(msg)
    }

    /// Decode a Raft message.
    ///
    /// Default implementation uses `decode`.
    fn decode_message<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> CatgaRaftResult<T> {
        self.decode(data)
    }

    /// Encode a snapshot.
    ///
    /// Default implementation uses `encode`.
    fn encode_snapshot<T: serde::Serialize>(&self, snapshot: &T) -> CatgaRaftResult<Vec<u8>> {
        self.encode(snapshot)
    }

    /// Decode a snapshot.
    ///
    /// Default implementation uses `decode`.
    fn decode_snapshot<T: serde::de::DeserializeOwned>(
        &self,
        data: &[u8],
    ) -> CatgaRaftResult<T> {
        self.decode(data)
    }
}

/// Prost-based codec using protobuf.
///
/// This codec uses the `prost` crate for protobuf encoding/decoding.
/// Requires proto definitions and generated code.
pub struct ProstCodec;

impl RaftCodec for ProstCodec {
    fn encode<T: serde::Serialize>(&self, value: &T) -> CatgaRaftResult<Vec<u8>> {
        // For prost, we would use the generated protobuf types
        // This is a placeholder that requires proto generation
        Err(CatgaRaftError::Codec(
            "ProstCodec requires protobuf code generation".to_string(),
        ))
    }

    fn decode<T: serde::de::DeserializeOwned>(&self, _data: &[u8]) -> CatgaRaftResult<T> {
        Err(CatgaRaftError::Codec(
            "ProstCodec requires protobuf code generation".to_string(),
        ))
    }
}

/// Bincode-based codec for fast binary encoding.
///
/// This codec uses the `bincode` crate for compact, fast binary encoding.
/// Good for internal use where protobuf overhead is not desired.
pub struct BincodeCodec;

impl RaftCodec for BincodeCodec {
    fn encode<T: serde::Serialize>(&self, value: &T) -> CatgaRaftResult<Vec<u8>> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| CatgaRaftError::Codec(format!("bincode encode error: {}", e)))
    }

    fn decode<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> CatgaRaftResult<T> {
        bincode::serde::decode_from_slice(data, bincode::config::standard())
            .map(|(value, _)| value)
            .map_err(|e| CatgaRaftError::Codec(format!("bincode decode error: {}", e)))
    }
}

/// JSON-based codec for human-readable encoding.
///
/// This codec uses `serde_json` for JSON encoding/decoding.
/// Good for debugging and interoperability with other systems.
pub struct JsonCodec;

impl RaftCodec for JsonCodec {
    fn encode<T: serde::Serialize>(&self, value: &T) -> CatgaRaftResult<Vec<u8>> {
        serde_json::to_vec(value)
            .map_err(|e| CatgaRaftError::Codec(format!("JSON encode error: {}", e)))
    }

    fn decode<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> CatgaRaftResult<T> {
        serde_json::from_slice(data)
            .map_err(|e| CatgaRaftError::Codec(format!("JSON decode error: {}", e)))
    }
}
