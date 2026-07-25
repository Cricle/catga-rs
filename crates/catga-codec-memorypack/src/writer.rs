use catga_core::CatgaResult;

use crate::{MemoryPackLimits, error};

const NULL_HEADER: u8 = 0xff;
const MAX_FIXED_OBJECT_MEMBERS: u8 = 249;

/// Bounded writer for the MemoryPack primitives used by Catga records.
pub struct MemoryPackWriter {
    output: Vec<u8>,
    object_depth: usize,
    limits: MemoryPackLimits,
}

impl MemoryPackWriter {
    /// Creates an empty writer governed by `limits`.
    pub fn new(limits: MemoryPackLimits) -> Self {
        Self {
            output: Vec::new(),
            object_depth: 0,
            limits,
        }
    }

    /// Writes a fixed object header with `member_count` members.
    pub fn write_object_header(&mut self, member_count: u8) -> CatgaResult<()> {
        if member_count > MAX_FIXED_OBJECT_MEMBERS {
            return Err(error::invalid(
                "MemoryPack object member count is not fixed-size",
            ));
        }
        self.charge_object()?;
        self.append(&[member_count])
    }

    /// Writes the null object header.
    pub fn write_null_object(&mut self) -> CatgaResult<()> {
        self.append(&[NULL_HEADER])
    }

    /// Closes the current non-null fixed object scope.
    pub fn finish_object(&mut self) -> CatgaResult<()> {
        self.object_depth = self
            .object_depth
            .checked_sub(1)
            .ok_or_else(|| error::invalid("MemoryPack object completion has no active object"))?;
        Ok(())
    }

    /// Writes a boolean as exactly zero or one.
    pub fn write_bool(&mut self, value: bool) -> CatgaResult<()> {
        self.write_u8(u8::from(value))
    }

    /// Writes an unsigned byte.
    pub fn write_u8(&mut self, value: u8) -> CatgaResult<()> {
        self.append(&[value])
    }

    /// Writes a little-endian unsigned 16-bit integer.
    pub fn write_u16(&mut self, value: u16) -> CatgaResult<()> {
        self.append(&value.to_le_bytes())
    }

    /// Writes a little-endian signed 32-bit integer.
    pub fn write_i32(&mut self, value: i32) -> CatgaResult<()> {
        self.append(&value.to_le_bytes())
    }

    /// Writes a little-endian signed 64-bit integer.
    pub fn write_i64(&mut self, value: i64) -> CatgaResult<()> {
        self.append(&value.to_le_bytes())
    }

    /// Writes a little-endian unsigned 64-bit integer.
    pub fn write_u64(&mut self, value: u64) -> CatgaResult<()> {
        self.append(&value.to_le_bytes())
    }

    /// Writes a raw signed value previously produced by `DateTime.ToBinary`.
    pub fn write_datetime_binary(&mut self, value: i64) -> CatgaResult<()> {
        self.write_i64(value)
    }

    /// Writes a nullable string in MemoryPack's UTF-8 representation.
    pub fn write_string(&mut self, value: Option<&str>) -> CatgaResult<()> {
        let Some(value) = value else {
            return self.write_i32(-1);
        };
        if value.is_empty() {
            return self.write_i32(0);
        }
        let byte_count = value.len();
        if byte_count > self.limits.max_string_bytes {
            return Err(error::limit("MemoryPack string exceeds its byte budget"));
        }
        let utf16_unit_count = value.encode_utf16().count();
        if utf16_unit_count > self.limits.max_collection_items {
            return Err(error::limit(
                "MemoryPack UTF-16 string length exceeds its unit budget",
            ));
        }
        let byte_count = i32::try_from(byte_count)
            .map_err(|_| error::limit("MemoryPack UTF-8 length exceeds i32"))?;
        let utf16_units = i32::try_from(utf16_unit_count)
            .map_err(|_| error::limit("MemoryPack UTF-8 character length exceeds i32"))?;
        self.write_i32(!byte_count)?;
        self.write_i32(utf16_units)?;
        self.append(value.as_bytes())
    }

    /// Writes a nullable byte array.
    pub fn write_bytes(&mut self, value: Option<&[u8]>) -> CatgaResult<()> {
        let Some(value) = value else {
            return self.write_i32(-1);
        };
        self.write_collection_length(value.len())?;
        self.append(value)
    }

    /// Writes a nullable little-endian signed 32-bit integer array.
    pub fn write_i32_array(&mut self, value: Option<&[i32]>) -> CatgaResult<()> {
        let Some(value) = value else {
            return self.write_i32(-1);
        };
        self.write_collection_length(value.len())?;
        for item in value {
            self.write_i32(*item)?;
        }
        Ok(())
    }

    /// Returns the completed frame after all output budgets have been enforced.
    pub fn finish(self) -> CatgaResult<Vec<u8>> {
        if self.object_depth != 0 {
            return Err(error::invalid("MemoryPack frame has an unclosed object"));
        }
        Ok(self.output)
    }

    fn write_collection_length(&mut self, length: usize) -> CatgaResult<()> {
        if length > self.limits.max_collection_items {
            return Err(error::limit(
                "MemoryPack collection exceeds its item budget",
            ));
        }
        let length = i32::try_from(length)
            .map_err(|_| error::limit("MemoryPack collection length exceeds i32"))?;
        self.write_i32(length)
    }

    fn charge_object(&mut self) -> CatgaResult<()> {
        self.object_depth = self
            .object_depth
            .checked_add(1)
            .ok_or_else(|| error::limit("MemoryPack nesting counter overflowed"))?;
        if self.object_depth > self.limits.max_nesting_depth {
            Err(error::limit("MemoryPack nesting depth exceeds its budget"))
        } else {
            Ok(())
        }
    }

    fn append(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let next = self
            .output
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| error::limit("MemoryPack output length overflowed"))?;
        if next > self.limits.max_frame_bytes || next > self.limits.max_allocation_bytes {
            return Err(error::limit("MemoryPack output exceeds its byte budget"));
        }
        self.output
            .try_reserve_exact(bytes.len())
            .map_err(|_| error::allocation())?;
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}
