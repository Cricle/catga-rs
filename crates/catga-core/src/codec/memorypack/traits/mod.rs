mod collections;
mod multidim;
mod options;
mod primitives;
mod smart_ptrs;
mod strings;
mod tuples;

#[cfg(any(
    feature = "uuid",
    feature = "rust_decimal",
    feature = "half",
    feature = "num-bigint"
))]
mod extended;

#[cfg(feature = "chrono")]
mod datetime;

#[cfg(any(feature = "glam", feature = "num-complex"))]
mod math;

#[allow(unused_imports)]
pub use {
    collections::*, multidim::*, options::*, primitives::*, smart_ptrs::*, strings::*, tuples::*,
};

use super::error::MemoryPackError;
use super::reader::MemoryPackReader;
use super::writer::MemoryPackWriter;

/// Encodes a value into a [`MemoryPackWriter`].
pub trait MemoryPackSerialize {
    /// Appends this value's MemoryPack representation to `writer`.
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError>;
}

/// Decodes an owned value from a [`MemoryPackReader`].
pub trait MemoryPackDeserialize: Sized {
    /// Decodes one value from `reader`.
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError>;
}

/// Decodes a value that may borrow directly from a received frame.
pub trait MemoryPackDeserializeZeroCopy<'a>: Sized {
    /// Decodes one value from `reader` without requiring owned string or byte allocations.
    fn deserialize(reader: &mut MemoryPackReader<'a>) -> Result<Self, MemoryPackError>;
}
