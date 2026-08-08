use super::super::error::MemoryPackError;
use super::super::reader::MemoryPackReader;
use super::super::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use super::super::writer::MemoryPackWriter;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Dense multidimensional array represented by row-major data and its shape.
pub struct MultiDimArray<T> {
    /// Length of each dimension, in row-major order.
    pub dimensions: Vec<usize>,
    /// Row-major element storage matching [`Self::dimensions`].
    pub data: Vec<T>,
}

impl<T> MultiDimArray<T> {
    /// Builds an array after validating that its dimensions describe exactly `data.len()` items.
    #[inline]
    pub fn new(dimensions: Vec<usize>, data: Vec<T>) -> Result<Self, MemoryPackError> {
        Self::validate_shape(&dimensions, data.len())?;
        Ok(Self { dimensions, data })
    }

    #[inline]
    /// Returns the number of dimensions.
    pub fn rank(&self) -> usize {
        self.dimensions.len()
    }

    #[inline]
    fn total_elements(&self) -> usize {
        self.data.len()
    }

    fn validate_shape(dimensions: &[usize], data_len: usize) -> Result<(), MemoryPackError> {
        let total = dimensions.iter().try_fold(1_usize, |total, dimension| {
            total
                .checked_mul(*dimension)
                .ok_or(MemoryPackError::LimitExceeded {
                    resource: "multidimensional array elements",
                    limit: usize::MAX,
                })
        })?;
        if total != data_len {
            return Err(MemoryPackError::DeserializationError(
                "multidimensional array dimensions do not match data length".into(),
            ));
        }
        Ok(())
    }
}

impl<T: MemoryPackSerialize> MemoryPackSerialize for MultiDimArray<T> {
    #[inline]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let rank_plus_one = self
            .rank()
            .checked_add(1)
            .and_then(|rank| u8::try_from(rank).ok())
            .ok_or_else(|| {
                MemoryPackError::SerializationError(
                    "multidimensional array rank exceeds the u8 wire range".into(),
                )
            })?;
        writer.write_u8(rank_plus_one)?;

        for &dim in &self.dimensions {
            writer.write_i32(MemoryPackWriter::checked_i32_length(dim)?)?;
        }

        writer.write_i32(MemoryPackWriter::checked_i32_length(self.total_elements())?)?;

        for item in &self.data {
            item.serialize(writer)?;
        }

        Ok(())
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for MultiDimArray<T> {
    #[inline]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let rank_plus_1 = reader.read_u8()?;
        let rank = (rank_plus_1 as usize).saturating_sub(1);

        if rank == 0 {
            return Err(MemoryPackError::DeserializationError(
                "Invalid array rank".into(),
            ));
        }

        let rank_i32 = i32::try_from(rank).map_err(|_| MemoryPackError::InvalidLength(i32::MAX))?;
        let rank = reader
            .validate_collection_len(rank_i32)?
            .ok_or_else(|| MemoryPackError::DeserializationError("Invalid array rank".into()))?;
        reader.reserve_allocation(rank.checked_mul(std::mem::size_of::<usize>()).ok_or(
            MemoryPackError::LimitExceeded {
                resource: "cumulative allocation bytes",
                limit: usize::MAX,
            },
        )?)?;
        let mut dimensions = Vec::with_capacity(rank);
        for _ in 0..rank {
            let dim = reader.read_i32()?;
            if dim < 0 {
                return Err(MemoryPackError::InvalidLength(dim));
            }
            dimensions.push(dim as usize);
        }

        let total = reader.read_i32()?;
        if total < 0 {
            return Err(MemoryPackError::InvalidLength(total));
        }

        let total_usize = reader
            .validate_collection_len(total)?
            .ok_or_else(|| MemoryPackError::DeserializationError("Invalid array length".into()))?;
        reader.reserve_allocation(total_usize.checked_mul(std::mem::size_of::<T>()).ok_or(
            MemoryPackError::LimitExceeded {
                resource: "cumulative allocation bytes",
                limit: usize::MAX,
            },
        )?)?;
        let mut data = Vec::with_capacity(total_usize);

        for _ in 0..total_usize {
            data.push(T::deserialize(reader)?);
        }

        MultiDimArray::new(dimensions, data)
    }
}

#[cfg(feature = "serde")]
impl<T: serde::Serialize> serde::Serialize for MultiDimArray<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MultiDimArray", 2)?;
        state.serialize_field("dimensions", &self.dimensions)?;
        state.serialize_field("data", &self.data)?;
        state.end()
    }
}

