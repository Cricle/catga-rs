//! Catga's vendored, bounded implementation of crates.io `memorypack` 1.2.2.
//!
//! [`MemoryPackCodec`] is the Catga transport adapter. It applies
//! [`MemoryPackDecodeLimits`] before decoding untrusted frames and requires that every frame is
//! fully consumed. The upstream static traits and derive macro are re-exported from this crate,
//! so application models do not depend on a second `memorypack` package.

extern crate self as catga_codec_memorypack;

mod codec;
mod limits;
mod reader;
mod writer;

pub mod error;
pub mod serializer;
pub mod state;
pub mod traits;
pub mod varint;

pub use codec::MemoryPackCodec;
pub use error::MemoryPackError;
pub use limits::MemoryPackDecodeLimits;
pub use reader::MemoryPackReader;
pub use serializer::MemoryPackSerializer;
pub use state::{MemoryPackReaderOptionalState, MemoryPackWriterOptionalState};
pub use traits::{MemoryPackDeserialize, MemoryPackDeserializeZeroCopy, MemoryPackSerialize};
pub use writer::MemoryPackWriter;

pub use traits::{NullableString, NullableVec};

pub use traits::MultiDimArray;

#[cfg(feature = "derive")]
pub use memorypack_derive::MemoryPackable;

pub mod prelude {
    pub use crate::error::MemoryPackError;
    pub use crate::reader::MemoryPackReader;
    pub use crate::serializer::MemoryPackSerializer;
    pub use crate::traits::{MemoryPackDeserialize, MemoryPackSerialize};
    pub use crate::writer::MemoryPackWriter;

    #[cfg(feature = "derive")]
    pub use memorypack_derive::MemoryPackable;
}
