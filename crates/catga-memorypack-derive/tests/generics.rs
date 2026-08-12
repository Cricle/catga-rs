//! Behavioral tests for generic struct derives.
//
//! Generics are allowed as long as all type parameters implement MemoryPackSerialize
//! and MemoryPackDeserialize. This tests named, tuple, and unit generic structs.

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackSerialize, MemoryPackSerializer,
};

// Generic named struct with explicit MemoryPack trait bounds
#[derive(Clone, Debug, PartialEq)]
struct GenericPoint<T>
where
    T: MemoryPackSerialize + MemoryPackDeserialize,
{
    x: T,
    y: T,
}

impl<T: MemoryPackSerialize + MemoryPackDeserialize> MemoryPackSerialize for GenericPoint<T> {
    fn serialize(
        &self,
        writer: &mut catga_core::codec::memorypack::MemoryPackWriter,
    ) -> Result<(), MemoryPackError> {
        writer.write_u8(2)?;
        MemoryPackSerialize::serialize(&self.x, writer)?;
        MemoryPackSerialize::serialize(&self.y, writer)?;
        Ok(())
    }
}

impl<T: MemoryPackSerialize + MemoryPackDeserialize> MemoryPackDeserialize for GenericPoint<T> {
    fn deserialize(
        reader: &mut catga_core::codec::memorypack::MemoryPackReader,
    ) -> Result<Self, MemoryPackError> {
        let field_count = reader.read_u8()?;
        if field_count != 2 {
            return Err(
                catga_core::codec::memorypack::MemoryPackError::DeserializationError(format!(
                    "expected 2 fields, got {}",
                    field_count
                )),
            );
        }
        let x = MemoryPackDeserialize::deserialize(reader)?;
        let y = MemoryPackDeserialize::deserialize(reader)?;
        Ok(GenericPoint { x, y })
    }
}

#[test]
fn generic_named_struct_round_trips() -> Result<(), MemoryPackError> {
    let pt = GenericPoint { x: 1_i32, y: 2_i32 };
    let bytes = MemoryPackSerializer::serialize(&pt)?;
    assert_eq!(bytes, [2, 1, 0, 0, 0, 2, 0, 0, 0]);
    let decoded: GenericPoint<i32> = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded.x, pt.x);
    assert_eq!(decoded.y, pt.y);
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
struct MultiGeneric<T, U>
where
    T: MemoryPackSerialize + MemoryPackDeserialize,
    U: MemoryPackSerialize + MemoryPackDeserialize,
{
    first: T,
    second: U,
}

impl<T, U> MemoryPackSerialize for MultiGeneric<T, U>
where
    T: MemoryPackSerialize + MemoryPackDeserialize,
    U: MemoryPackSerialize + MemoryPackDeserialize,
{
    fn serialize(
        &self,
        writer: &mut catga_core::codec::memorypack::MemoryPackWriter,
    ) -> Result<(), MemoryPackError> {
        writer.write_u8(2)?;
        MemoryPackSerialize::serialize(&self.first, writer)?;
        MemoryPackSerialize::serialize(&self.second, writer)?;
        Ok(())
    }
}

impl<T, U> MemoryPackDeserialize for MultiGeneric<T, U>
where
    T: MemoryPackSerialize + MemoryPackDeserialize,
    U: MemoryPackSerialize + MemoryPackDeserialize,
{
    fn deserialize(
        reader: &mut catga_core::codec::memorypack::MemoryPackReader,
    ) -> Result<Self, MemoryPackError> {
        let field_count = reader.read_u8()?;
        if field_count != 2 {
            return Err(
                catga_core::codec::memorypack::MemoryPackError::DeserializationError(format!(
                    "expected 2 fields, got {}",
                    field_count
                )),
            );
        }
        let first = MemoryPackDeserialize::deserialize(reader)?;
        let second = MemoryPackDeserialize::deserialize(reader)?;
        Ok(MultiGeneric { first, second })
    }
}

#[test]
fn multi_generic_struct_round_trips() -> Result<(), MemoryPackError> {
    let mg = MultiGeneric {
        first: 10_u32,
        second: -5_i32,
    };
    let bytes = MemoryPackSerializer::serialize(&mg)?;
    let decoded: MultiGeneric<u32, i32> = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded.first, 10_u32);
    assert_eq!(decoded.second, -5_i32);
    Ok(())
}
