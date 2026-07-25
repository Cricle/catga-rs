use catga_core::CatgaResult;

use crate::{MemoryPackLimits, error};

const NULL_HEADER: u8 = 0xff;
const MAX_FIXED_OBJECT_MEMBERS: u8 = 249;

/// Allocation-bounded reader for the MemoryPack primitives used by Catga records.
pub struct MemoryPackReader<'a> {
    input: &'a [u8],
    cursor: usize,
    allocated: usize,
    object_depth: usize,
    limits: MemoryPackLimits,
}

impl<'a> MemoryPackReader<'a> {
    /// Starts reading one exact frame after validating its total byte budget.
    pub fn new(input: &'a [u8], limits: MemoryPackLimits) -> CatgaResult<Self> {
        if input.len() > limits.max_frame_bytes {
            return Err(error::limit("MemoryPack frame exceeds its byte budget"));
        }
        Ok(Self {
            input,
            cursor: 0,
            allocated: 0,
            object_depth: 0,
            limits,
        })
    }

    /// Reads a nullable fixed object header and validates its exact member count.
    pub fn read_object_header(&mut self, expected_members: u8) -> CatgaResult<bool> {
        if expected_members > MAX_FIXED_OBJECT_MEMBERS {
            return Err(error::invalid(
                "MemoryPack object member count is not fixed-size",
            ));
        }
        match self.read_u8()? {
            NULL_HEADER => Ok(false),
            actual if actual == expected_members => {
                self.object_depth = self
                    .object_depth
                    .checked_add(1)
                    .ok_or_else(|| error::limit("MemoryPack nesting counter overflowed"))?;
                if self.object_depth > self.limits.max_nesting_depth {
                    return Err(error::limit("MemoryPack nesting depth exceeds its budget"));
                }
                Ok(true)
            }
            actual => Err(error::invalid(format!(
                "MemoryPack object has {actual} members; expected {expected_members}"
            ))),
        }
    }

    /// Closes the current non-null fixed object scope.
    pub fn finish_object(&mut self) -> CatgaResult<()> {
        self.object_depth = self
            .object_depth
            .checked_sub(1)
            .ok_or_else(|| error::invalid("MemoryPack object completion has no active object"))?;
        Ok(())
    }

    /// Reads a boolean encoded strictly as zero or one.
    pub fn read_bool(&mut self) -> CatgaResult<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(error::invalid("MemoryPack boolean must be zero or one")),
        }
    }

    /// Reads an unsigned byte.
    pub fn read_u8(&mut self) -> CatgaResult<u8> {
        Ok(self.take(1)?[0])
    }

    /// Reads a little-endian unsigned 16-bit integer.
    pub fn read_u16(&mut self) -> CatgaResult<u16> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    /// Reads a little-endian signed 32-bit integer.
    pub fn read_i32(&mut self) -> CatgaResult<i32> {
        Ok(i32::from_le_bytes(self.read_array()?))
    }

    /// Reads a little-endian signed 64-bit integer.
    pub fn read_i64(&mut self) -> CatgaResult<i64> {
        Ok(i64::from_le_bytes(self.read_array()?))
    }

    /// Reads a little-endian unsigned 64-bit integer.
    pub fn read_u64(&mut self) -> CatgaResult<u64> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    /// Reads the raw signed value produced by `DateTime.ToBinary`.
    pub fn read_datetime_binary(&mut self) -> CatgaResult<i64> {
        self.read_i64()
    }

    /// Reads a nullable MemoryPack string, accepting its UTF-8 and UTF-16 forms.
    pub fn read_string(&mut self) -> CatgaResult<Option<Box<str>>> {
        let header = self.read_i32()?;
        if header == -1 {
            return Ok(None);
        }
        if header >= 0 {
            let units = usize::try_from(header)
                .map_err(|_| error::invalid("invalid MemoryPack UTF-16 length"))?;
            if units > self.limits.max_collection_items {
                return Err(error::limit(
                    "MemoryPack UTF-16 string exceeds its item budget",
                ));
            }
            let byte_count = units
                .checked_mul(2)
                .ok_or_else(|| error::limit("MemoryPack UTF-16 byte length overflowed"))?;
            self.check_string(byte_count)?;
            let bytes = self.take(byte_count)?;
            let decoded = || {
                std::char::decode_utf16(
                    bytes
                        .chunks_exact(2)
                        .map(|pair| u16::from_le_bytes([pair[0], pair[1]])),
                )
            };
            let mut utf8_bytes = 0_usize;
            for character in decoded() {
                let character = character
                    .map_err(|_| error::invalid("MemoryPack string contains invalid UTF-16"))?;
                utf8_bytes = utf8_bytes
                    .checked_add(character.len_utf8())
                    .ok_or_else(|| error::limit("MemoryPack UTF-16 output length overflowed"))?;
            }
            self.check_string(utf8_bytes)?;
            self.charge_allocation(utf8_bytes)?;
            let mut value = String::new();
            value
                .try_reserve_exact(utf8_bytes)
                .map_err(|_| error::allocation())?;
            for character in decoded() {
                value
                    .push(character.map_err(|_| {
                        error::invalid("MemoryPack string contains invalid UTF-16")
                    })?);
            }
            return Ok(Some(value.into_boxed_str()));
        }

        let byte_count_i32 = !header;
        let byte_count = usize::try_from(byte_count_i32)
            .map_err(|_| error::invalid("invalid MemoryPack UTF-8 length"))?;
        let utf16_units = self.read_i32()?;
        if utf16_units < 0 {
            return Err(error::invalid(
                "MemoryPack UTF-8 character length is negative",
            ));
        }
        self.check_string(byte_count)?;
        let bytes = self.take(byte_count)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| error::invalid("MemoryPack string contains invalid UTF-8"))?;
        let actual_units = value.encode_utf16().count();
        let expected_units = usize::try_from(utf16_units)
            .map_err(|_| error::invalid("invalid MemoryPack UTF-8 character length"))?;
        if expected_units > self.limits.max_collection_items {
            return Err(error::limit(
                "MemoryPack UTF-8 string exceeds its item budget",
            ));
        }
        if actual_units != expected_units {
            return Err(error::invalid(
                "MemoryPack UTF-8 character length does not match",
            ));
        }
        self.charge_allocation(byte_count)?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(byte_count)
            .map_err(|_| error::allocation())?;
        owned.push_str(value);
        Ok(Some(owned.into_boxed_str()))
    }

    /// Reads a nullable byte array after validating its item and allocation budgets.
    pub fn read_bytes(&mut self) -> CatgaResult<Option<Box<[u8]>>> {
        let Some(length) = self.read_collection_length()? else {
            return Ok(None);
        };
        self.charge_allocation(length)?;
        let bytes = self.take(length)?;
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(length)
            .map_err(|_| error::allocation())?;
        owned.extend_from_slice(bytes);
        Ok(Some(owned.into_boxed_slice()))
    }

    /// Reads a nullable little-endian signed 32-bit integer array.
    pub fn read_i32_array(&mut self) -> CatgaResult<Option<Box<[i32]>>> {
        let Some(length) = self.read_collection_length()? else {
            return Ok(None);
        };
        let allocation = length
            .checked_mul(std::mem::size_of::<i32>())
            .ok_or_else(|| error::limit("MemoryPack array allocation overflowed"))?;
        self.charge_allocation(allocation)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(length)
            .map_err(|_| error::allocation())?;
        for _ in 0..length {
            values.push(self.read_i32()?);
        }
        Ok(Some(values.into_boxed_slice()))
    }

    /// Succeeds only when every byte in the exact frame was consumed.
    pub fn finish(self) -> CatgaResult<()> {
        if self.object_depth != 0 {
            Err(error::invalid("MemoryPack frame has an unclosed object"))
        } else if self.cursor == self.input.len() {
            Ok(())
        } else {
            Err(error::invalid("MemoryPack frame contains trailing input"))
        }
    }

    fn read_collection_length(&mut self) -> CatgaResult<Option<usize>> {
        let length = self.read_i32()?;
        if length == -1 {
            return Ok(None);
        }
        let length = usize::try_from(length)
            .map_err(|_| error::invalid("MemoryPack collection length is negative"))?;
        if length > self.limits.max_collection_items {
            return Err(error::limit(
                "MemoryPack collection exceeds its item budget",
            ));
        }
        Ok(Some(length))
    }

    fn check_string(&self, byte_count: usize) -> CatgaResult<()> {
        if byte_count > self.limits.max_string_bytes {
            Err(error::limit("MemoryPack string exceeds its byte budget"))
        } else {
            Ok(())
        }
    }

    fn charge_allocation(&mut self, bytes: usize) -> CatgaResult<()> {
        let next = self
            .allocated
            .checked_add(bytes)
            .ok_or_else(|| error::limit("MemoryPack allocation budget overflowed"))?;
        if next > self.limits.max_allocation_bytes {
            return Err(error::limit("MemoryPack allocation exceeds its budget"));
        }
        self.allocated = next;
        Ok(())
    }

    fn read_array<const N: usize>(&mut self) -> CatgaResult<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| error::truncated())
    }

    fn take(&mut self, count: usize) -> CatgaResult<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or_else(error::truncated)?;
        let bytes = self
            .input
            .get(self.cursor..end)
            .ok_or_else(error::truncated)?;
        self.cursor = end;
        Ok(bytes)
    }
}
