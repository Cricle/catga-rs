//! MemoryPack codec module.
//!
//! This module provides MemoryPack serialization support for Catga messages
//! with support for RPC responses, envelopes, and various numeric types.

/// API types for MemoryPack including RPC response handling.
pub mod api;
/// Core MemoryPack codec implementation.
pub mod codec;
/// Envelope serialization support for MemoryPack.
pub mod envelope;
/// MemoryPack error types.
pub mod error;
/// Decode limits and validation for MemoryPack.
pub mod limits;
/// MemoryPack reader for deserialization.
pub mod reader;
/// MemoryPack serializer for serialization.
pub mod serializer;
/// State codec support for MemoryPack.
pub mod state;
/// MemoryPack trait definitions (Serialize, Deserialize).
pub mod traits;
/// Variable-length integer encoding for MemoryPack.
pub mod varint;
/// MemoryPack writer for serialization.
pub mod writer;

// Re-export from submodules
pub use super::bincode::{BincodeCodec, MAX_BINCODE_FRAME_BYTES};
pub use api::MemoryPackRequestClient;
pub use api::MemoryPackRpcResponse;
pub use api::MemoryPackSnapshotCodec;
pub use codec::MemoryPackCodec;
pub use error::MemoryPackError;
pub use limits::MemoryPackDecodeLimits;
pub use reader::MemoryPackReader;
pub use serializer::MemoryPackSerializer;
pub use traits::{MemoryPackDeserialize, MemoryPackSerialize};
pub use writer::MemoryPackWriter;

// Re-export MemoryPackable from the derive crate
pub use catga_memorypack_derive::MemoryPackable;
