#![deny(missing_docs)]
//! Catga's vendored, bounded implementation of crates.io `memorypack` 1.2.2.
//!
//! [`MemoryPackCodec`] is the Catga transport adapter. It applies
//! [`MemoryPackDecodeLimits`] before decoding untrusted frames and requires that every frame is
//! fully consumed. The upstream static traits and derive macro are re-exported from this crate,
//! so application models do not depend on a second `memorypack` package.
//!
//! ```
//! use catga_codec_memorypack::MemoryPackCodec;
//!
//! let codec = MemoryPackCodec::default();
//! let frame = codec.encode_value(&42_u64)?;
//! let value: u64 = codec.decode_value(&frame)?;
//! assert_eq!(value, 42);
//! # Ok::<(), catga_core::CatgaError>(())
//! ```

extern crate self as catga_codec_memorypack;

mod api;
mod codec;
mod envelope;
mod limits;
mod reader;
mod writer;

/// Error types produced by MemoryPack operations.
pub mod error;
/// High-level serializer helpers.
pub mod serializer;
/// Per-frame object-reference state.
pub mod state;
/// Serialization traits and standard type implementations.
pub mod traits;
/// Compact integer wire helpers.
pub mod varint;

pub use api::{
    MemoryPackRequestClient, MemoryPackRequestClientFactory, MemoryPackRpcResponse,
    MemoryPackScheduledOutbox, MemoryPackSnapshotCodec,
};
pub use codec::MemoryPackCodec;
pub use error::MemoryPackError;
pub use limits::MemoryPackDecodeLimits;
pub use reader::MemoryPackReader;
pub use serializer::MemoryPackSerializer;
pub use state::{MemoryPackReaderOptionalState, MemoryPackWriterOptionalState};
pub use traits::{MemoryPackDeserialize, MemoryPackDeserializeZeroCopy, MemoryPackSerialize};
pub use writer::MemoryPackWriter;

/// The core typed transport specialized for [`MemoryPackCodec`].
///
/// This alias reuses Catga's shared
/// acknowledgement, batching, destination, and transport-context implementation.
pub type MemoryPackTransport<T> = catga_core::TypedTransport<T, MemoryPackCodec>;

/// A typed MemoryPack delivery that retains its backend acknowledgement token.
pub type MemoryPackDelivery<M> = catga_core::TypedDelivery<M>;

/// The acknowledgement result produced by [`MemoryPackTransport::process_next`].
pub type MemoryPackProcessOutcome = catga_core::TypedProcessOutcome;

pub use traits::{NullableString, NullableVec};

pub use traits::MultiDimArray;

#[cfg(feature = "derive")]
pub use catga_memorypack_derive::MemoryPackable;

/// Common MemoryPack types and traits for application models.
pub mod prelude {
    pub use crate::error::MemoryPackError;
    pub use crate::reader::MemoryPackReader;
    pub use crate::serializer::MemoryPackSerializer;
    pub use crate::traits::{MemoryPackDeserialize, MemoryPackSerialize};
    pub use crate::writer::MemoryPackWriter;

    #[cfg(feature = "derive")]
    pub use catga_memorypack_derive::MemoryPackable;
}
