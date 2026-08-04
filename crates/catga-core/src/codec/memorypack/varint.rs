use super::error::MemoryPackError;
use crate::codec::memorypack::reader::MemoryPackReader;
use crate::codec::memorypack::writer::MemoryPackWriter;

/// Internal wire type codes used for varint encoding.
pub(crate) mod codes {
    /// Maximum value that fits in a single byte (0-127).
    pub const MAX_SINGLE_VALUE: i8 = 127;
    /// Minimum value that fits in a single byte (-120 to -1).
    pub const MIN_SINGLE_VALUE: i8 = -120;

    /// Wire type code for 8-bit unsigned integers.
    pub const BYTE: i8 = -121;
    /// Wire type code for 8-bit signed integers.
    pub const SBYTE: i8 = -122;
    /// Wire type code for 16-bit unsigned integers.
    pub const UINT16: i8 = -123;
    /// Wire type code for 16-bit signed integers.
    pub const INT16: i8 = -124;
    /// Wire type code for 32-bit unsigned integers.
    pub const UINT32: i8 = -125;
    /// Wire type code for 32-bit signed integers.
    pub const INT32: i8 = -126;
    /// Wire type code for 64-bit unsigned integers.
    pub const UINT64: i8 = -127;
    /// Wire type code for 64-bit signed integers.
    pub const INT64: i8 = -128;
}

/// Re-exports the INT64 type code for use in tests.
pub use codes::INT64;

/// Writes an `i64` using MemoryPack's signed compact-integer encoding.
pub fn write_varint(writer: &mut MemoryPackWriter, value: i64) -> Result<(), MemoryPackError> {
    if value >= 0 {
        if value <= codes::MAX_SINGLE_VALUE as i64 {
            writer.write_i8(value as i8)?;
        } else if value <= i16::MAX as i64 {
            writer.write_i8(codes::INT16)?;
            writer.write_i16(value as i16)?;
        } else if value <= i32::MAX as i64 {
            writer.write_i8(codes::INT32)?;
            writer.write_i32(value as i32)?;
        } else {
            writer.write_i8(codes::INT64)?;
            writer.write_i64(value)?;
        }
    } else if value >= codes::MIN_SINGLE_VALUE as i64 {
        writer.write_i8(value as i8)?;
    } else if value >= i8::MIN as i64 {
        writer.write_i8(codes::SBYTE)?;
        writer.write_i8(value as i8)?;
    } else if value >= i16::MIN as i64 {
        writer.write_i8(codes::INT16)?;
        writer.write_i16(value as i16)?;
    } else if value >= i32::MIN as i64 {
        writer.write_i8(codes::INT32)?;
        writer.write_i32(value as i32)?;
    } else {
        writer.write_i8(codes::INT64)?;
        writer.write_i64(value)?;
    }
    Ok(())
}

/// Reads one `i64` encoded with MemoryPack's signed compact-integer encoding.
pub fn read_varint(reader: &mut MemoryPackReader) -> Result<i64, MemoryPackError> {
    let type_code = reader.read_i8()?;

    match type_code {
        codes::BYTE => Ok(reader.read_u8()? as i64),
        codes::SBYTE => Ok(reader.read_i8()? as i64),
        codes::UINT16 => Ok(reader.read_u16()? as i64),
        codes::INT16 => Ok(reader.read_i16()? as i64),
        codes::UINT32 => Ok(reader.read_u32()? as i64),
        codes::INT32 => Ok(reader.read_i32()? as i64),
        codes::UINT64 => Ok(reader.read_u64()? as i64),
        codes::INT64 => reader.read_i64(),
        _ => Ok(type_code as i64),
    }
}
