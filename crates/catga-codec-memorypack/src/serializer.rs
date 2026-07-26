use crate::error::MemoryPackError;
use crate::limits::MemoryPackDecodeLimits;
use crate::reader::MemoryPackReader;
use crate::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use crate::writer::MemoryPackWriter;

/// MemoryPack serializer
pub struct MemoryPackSerializer;

impl MemoryPackSerializer {
    /// Serialize a value to a byte vector
    #[inline]
    pub fn serialize<T: MemoryPackSerialize>(value: &T) -> Result<Vec<u8>, MemoryPackError> {
        let mut writer = MemoryPackWriter::with_capacity(64);
        value.serialize(&mut writer)?;
        Ok(writer.into_bytes())
    }

    /// Serialize a value to an existing writer
    #[inline]
    pub fn serialize_to<T: MemoryPackSerialize>(
        value: &T,
        writer: &mut MemoryPackWriter,
    ) -> Result<(), MemoryPackError> {
        value.serialize(writer)
    }

    /// Deserializes one complete frame using Catga's default resource budgets.
    ///
    /// For a tighter or larger application-specific budget, use [`Self::deserialize_bounded`].
    #[inline]
    pub fn deserialize<T: MemoryPackDeserialize>(data: &[u8]) -> Result<T, MemoryPackError> {
        Self::deserialize_bounded(data, MemoryPackDecodeLimits::default())
    }

    /// Deserializes one exact received frame while enforcing resource budgets.
    ///
    /// This method rejects trailing bytes and must be used when the caller needs a budget other
    /// than [`MemoryPackDecodeLimits::default`].
    #[inline]
    pub fn deserialize_bounded<T: MemoryPackDeserialize>(
        data: &[u8],
        limits: MemoryPackDecodeLimits,
    ) -> Result<T, MemoryPackError> {
        let mut reader = MemoryPackReader::new_bounded(data, limits)?;
        let value = T::deserialize(&mut reader)?;
        if reader.position() != data.len() as u64 {
            return Err(MemoryPackError::TrailingBytes);
        }
        Ok(value)
    }

    /// Deserialize a value from an existing reader
    #[inline]
    pub fn deserialize_from<T: MemoryPackDeserialize>(
        reader: &mut MemoryPackReader,
    ) -> Result<T, MemoryPackError> {
        T::deserialize(reader)
    }

    /// Deserializes one complete zero-copy frame using Catga's default resource budgets.
    #[inline]
    pub fn deserialize_zero_copy<'a, T>(data: &'a [u8]) -> Result<T, MemoryPackError>
    where
        T: crate::traits::MemoryPackDeserializeZeroCopy<'a>,
    {
        let mut reader = MemoryPackReader::new_bounded(data, MemoryPackDecodeLimits::default())?;
        let value = T::deserialize(&mut reader)?;
        if reader.position() != data.len() as u64 {
            return Err(MemoryPackError::TrailingBytes);
        }
        Ok(value)
    }
}
