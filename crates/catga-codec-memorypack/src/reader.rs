use crate::error::MemoryPackError;
use crate::limits::MemoryPackDecodeLimits;
use crate::state::MemoryPackReaderOptionalState;

use byteorder::{LittleEndian, ReadBytesExt};
use simdutf8::basic;
use std::io::{Cursor, Read};

/// Stateful reader for a single MemoryPack frame.
///
/// Use [`Self::new_bounded`] for untrusted input so collection, allocation, and nesting budgets
/// are applied before materializing values.
pub struct MemoryPackReader<'a> {
    pub(crate) cursor: Cursor<&'a [u8]>,
    /// Optional object-reference state used by reference-preserving schemas.
    pub optional_state: Option<MemoryPackReaderOptionalState>,
    limits: Option<MemoryPackDecodeLimits>,
    allocated_bytes: usize,
    nesting_depth: usize,
}

impl<'a> MemoryPackReader<'a> {
    /// Creates a reader that enforces `limits` before a received frame can allocate memory.
    pub fn new_bounded(
        data: &'a [u8],
        limits: MemoryPackDecodeLimits,
    ) -> Result<Self, MemoryPackError> {
        if data.len() > limits.max_frame_bytes {
            return Err(MemoryPackError::LimitExceeded {
                resource: "frame bytes",
                limit: limits.max_frame_bytes,
            });
        }
        Ok(Self {
            cursor: Cursor::new(data),
            optional_state: None,
            limits: Some(limits),
            allocated_bytes: 0,
            nesting_depth: 0,
        })
    }

    pub(crate) fn validate_collection_len(
        &mut self,
        size: i32,
    ) -> Result<Option<usize>, MemoryPackError> {
        match size {
            -1 | 0 => Ok(None),
            value if value < 0 => Err(MemoryPackError::InvalidLength(value)),
            value => {
                let size = value as usize;
                if let Some(limits) = self.limits
                    && size > limits.max_collection_items
                {
                    return Err(MemoryPackError::LimitExceeded {
                        resource: "collection items",
                        limit: limits.max_collection_items,
                    });
                }
                Ok(Some(size))
            }
        }
    }

    pub(crate) fn reserve_allocation(&mut self, bytes: usize) -> Result<(), MemoryPackError> {
        let Some(limits) = self.limits else {
            return Ok(());
        };
        let next =
            self.allocated_bytes
                .checked_add(bytes)
                .ok_or(MemoryPackError::LimitExceeded {
                    resource: "cumulative allocation bytes",
                    limit: limits.max_allocation_bytes,
                })?;
        if next > limits.max_allocation_bytes {
            return Err(MemoryPackError::LimitExceeded {
                resource: "cumulative allocation bytes",
                limit: limits.max_allocation_bytes,
            });
        }
        self.allocated_bytes = next;
        Ok(())
    }

    /// Records entry into a derived object scope for bounded deserialization.
    pub fn enter_object(&mut self) -> Result<(), MemoryPackError> {
        let next = self
            .nesting_depth
            .checked_add(1)
            .ok_or(MemoryPackError::LimitExceeded {
                resource: "nesting depth",
                limit: self
                    .limits
                    .map_or(usize::MAX, |limits| limits.max_nesting_depth),
            })?;
        if let Some(limits) = self.limits
            && next > limits.max_nesting_depth
        {
            return Err(MemoryPackError::LimitExceeded {
                resource: "nesting depth",
                limit: limits.max_nesting_depth,
            });
        }
        self.nesting_depth = next;
        Ok(())
    }

    /// Leaves a derived object scope after decoding completes or fails.
    pub fn leave_object(&mut self) {
        self.nesting_depth = self.nesting_depth.saturating_sub(1);
    }

    /// Reads an owned string, accepting the UTF-8 and UTF-16 MemoryPack representations.
    pub fn read_string(&mut self) -> Result<String, MemoryPackError> {
        let length_or_marker = self.read_i32()?;

        if length_or_marker == -1 {
            return Ok(String::new());
        }

        if length_or_marker < 0 {
            return self.read_utf8_string(!length_or_marker as usize);
        }

        let char_count = length_or_marker as usize;
        if char_count == 0 {
            return Ok(String::new());
        }

        self.read_utf16_string(char_count)
    }

    fn read_utf8_string(&mut self, byte_count: usize) -> Result<String, MemoryPackError> {
        self.validate_string_bytes(byte_count)?;
        let _char_length = self.read_i32()?;
        let slice = self.read_bytes(byte_count)?;

        Ok(basic::from_utf8(slice)
            .map_err(|_| MemoryPackError::InvalidUtf8)?
            .to_string())
    }

    #[inline]
    /// Reads a zero-copy UTF-8 string slice.
    ///
    /// UTF-16 wire values require allocation and return
    /// [`MemoryPackError::Utf16NotSupportedForZeroCopy`].
    pub fn read_str(&mut self) -> Result<&'a str, MemoryPackError> {
        let length_or_marker = self.read_i32()?;

        if length_or_marker == -1 || length_or_marker == 0 {
            return Ok("");
        }

        if length_or_marker < 0 {
            return self.read_utf8_str(!length_or_marker as usize);
        }

        Err(MemoryPackError::Utf16NotSupportedForZeroCopy)
    }

    #[inline]
    fn read_utf8_str(&mut self, byte_count: usize) -> Result<&'a str, MemoryPackError> {
        self.validate_string_bytes(byte_count)?;
        let _char_length = self.read_i32()?;
        let slice = self.read_bytes(byte_count)?;

        let str_slice = basic::from_utf8(slice).map_err(|_| MemoryPackError::InvalidUtf8)?;

        Ok(str_slice)
    }

    #[inline]
    /// Reads exactly `length` bytes as a zero-copy slice.
    pub fn read_bytes(&mut self, length: usize) -> Result<&'a [u8], MemoryPackError> {
        let pos = self.cursor.position() as usize;
        let buffer = self.cursor.get_ref();

        if pos + length > buffer.len() {
            return Err(MemoryPackError::UnexpectedEndOfBuffer);
        }

        let slice = &buffer[pos..pos + length];
        self.cursor.set_position((pos + length) as u64);
        Ok(slice)
    }

    #[inline]
    /// Reads exactly `length` bytes into an owned vector after checking the allocation budget.
    pub fn read_bytes_vec(&mut self, length: usize) -> Result<Vec<u8>, MemoryPackError> {
        self.reserve_allocation(length)?;
        Ok(self.read_bytes(length)?.to_vec())
    }

    #[inline]
    /// Reads a fixed-size byte array.
    pub fn read_fixed_bytes<const N: usize>(&mut self) -> Result<[u8; N], MemoryPackError> {
        let mut buffer = [0u8; N];
        self.cursor.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    #[inline]
    fn read_utf16_string(&mut self, char_count: usize) -> Result<String, MemoryPackError> {
        let byte_count = char_count
            .checked_mul(2)
            .ok_or(MemoryPackError::LimitExceeded {
                resource: "string bytes",
                limit: self
                    .limits
                    .map_or(usize::MAX, |limits| limits.max_string_bytes),
            })?;
        self.validate_string_bytes(byte_count)?;
        let allocation = char_count
            .checked_mul(3)
            .ok_or(MemoryPackError::LimitExceeded {
                resource: "cumulative allocation bytes",
                limit: self
                    .limits
                    .map_or(usize::MAX, |limits| limits.max_allocation_bytes),
            })?;
        self.reserve_allocation(allocation)?;
        let slice = self.read_bytes(byte_count)?;

        let mut result = String::with_capacity(char_count * 3);
        let mut i = 0;
        while i < byte_count {
            let code_unit = u16::from_le_bytes([slice[i], slice[i + 1]]);
            i += 2;

            if !(0xD800..=0xDFFF).contains(&code_unit) {
                if let Some(c) = char::from_u32(code_unit as u32) {
                    result.push(c);
                } else {
                    return Err(MemoryPackError::InvalidUtf8);
                }
            } else if (0xD800..=0xDBFF).contains(&code_unit) {
                if i + 2 > byte_count {
                    return Err(MemoryPackError::InvalidUtf8);
                }

                let low = u16::from_le_bytes([slice[i], slice[i + 1]]);

                i += 2;
                if !(0xDC00..=0xDFFF).contains(&low) {
                    return Err(MemoryPackError::InvalidUtf8);
                }

                let code_point =
                    0x10000 + ((code_unit as u32 - 0xD800) << 10) + (low as u32 - 0xDC00);

                if let Some(c) = char::from_u32(code_point) {
                    result.push(c);
                } else {
                    return Err(MemoryPackError::InvalidUtf8);
                }
            } else {
                return Err(MemoryPackError::InvalidUtf8);
            }
        }
        Ok(result)
    }

    #[inline(always)]
    /// Reads a canonical one-byte Boolean value.
    pub fn read_bool(&mut self) -> Result<bool, MemoryPackError> {
        match self.cursor.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(MemoryPackError::DeserializationError(
                "invalid boolean wire value".into(),
            )),
        }
    }

    #[inline(always)]
    /// Reads a little-endian signed 8-bit integer.
    pub fn read_i8(&mut self) -> Result<i8, MemoryPackError> {
        Ok(self.cursor.read_i8()?)
    }

    #[inline(always)]
    /// Reads an unsigned 8-bit integer.
    pub fn read_u8(&mut self) -> Result<u8, MemoryPackError> {
        Ok(self.cursor.read_u8()?)
    }

    #[inline(always)]
    /// Reads a little-endian signed 16-bit integer.
    pub fn read_i16(&mut self) -> Result<i16, MemoryPackError> {
        Ok(self.cursor.read_i16::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian unsigned 16-bit integer.
    pub fn read_u16(&mut self) -> Result<u16, MemoryPackError> {
        Ok(self.cursor.read_u16::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian signed 32-bit integer.
    pub fn read_i32(&mut self) -> Result<i32, MemoryPackError> {
        Ok(self.cursor.read_i32::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian unsigned 32-bit integer.
    pub fn read_u32(&mut self) -> Result<u32, MemoryPackError> {
        Ok(self.cursor.read_u32::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian signed 64-bit integer.
    pub fn read_i64(&mut self) -> Result<i64, MemoryPackError> {
        Ok(self.cursor.read_i64::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian unsigned 64-bit integer.
    pub fn read_u64(&mut self) -> Result<u64, MemoryPackError> {
        Ok(self.cursor.read_u64::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian IEEE-754 single-precision value.
    pub fn read_f32(&mut self) -> Result<f32, MemoryPackError> {
        Ok(self.cursor.read_f32::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian IEEE-754 double-precision value.
    pub fn read_f64(&mut self) -> Result<f64, MemoryPackError> {
        Ok(self.cursor.read_f64::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian signed 128-bit integer.
    pub fn read_i128(&mut self) -> Result<i128, MemoryPackError> {
        Ok(self.cursor.read_i128::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads a little-endian unsigned 128-bit integer.
    pub fn read_u128(&mut self) -> Result<u128, MemoryPackError> {
        Ok(self.cursor.read_u128::<LittleEndian>()?)
    }

    #[inline(always)]
    /// Reads one UTF-16 scalar value, validating any surrogate pair.
    pub fn read_char(&mut self) -> Result<char, MemoryPackError> {
        let code_unit = self.read_u16()?;

        if !(0xD800..=0xDFFF).contains(&code_unit) {
            return char::from_u32(code_unit as u32).ok_or(MemoryPackError::InvalidCodePoint);
        }

        if !(0xD800..=0xDBFF).contains(&code_unit) {
            return Err(MemoryPackError::DeserializationError(
                "UTF-16 low surrogate must follow a high surrogate".into(),
            ));
        }

        let low_surrogate = self.read_u16()?;
        if !(0xDC00..=0xDFFF).contains(&low_surrogate) {
            return Err(MemoryPackError::DeserializationError(
                "UTF-16 high surrogate is not followed by a low surrogate".into(),
            ));
        }

        let code_point =
            0x10000 + (((code_unit as u32 - 0xD800) << 10) | (low_surrogate as u32 - 0xDC00));
        char::from_u32(code_point).ok_or(MemoryPackError::InvalidCodePoint)
    }

    #[inline]
    /// Advances the cursor by `n` bytes without decoding them.
    pub fn skip(&mut self, n: usize) -> Result<(), MemoryPackError> {
        let position = usize::try_from(self.cursor.position())
            .map_err(|_| MemoryPackError::UnexpectedEndOfBuffer)?;
        let next = position
            .checked_add(n)
            .filter(|next| *next <= self.cursor.get_ref().len())
            .ok_or(MemoryPackError::UnexpectedEndOfBuffer)?;
        self.cursor.set_position(next as u64);
        Ok(())
    }

    #[inline]
    /// Moves the cursor backward by `n` bytes.
    pub fn rewind(&mut self, n: usize) -> Result<(), MemoryPackError> {
        let position = self.cursor.position();
        let rewind = u64::try_from(n).map_err(|_| MemoryPackError::UnexpectedEndOfBuffer)?;
        let next = position
            .checked_sub(rewind)
            .ok_or(MemoryPackError::UnexpectedEndOfBuffer)?;
        self.cursor.set_position(next);
        Ok(())
    }

    #[inline]
    /// Returns the current byte offset within the frame.
    pub fn position(&self) -> u64 {
        self.cursor.position()
    }

    fn validate_string_bytes(&mut self, bytes: usize) -> Result<(), MemoryPackError> {
        if let Some(limits) = self.limits
            && bytes > limits.max_string_bytes
        {
            return Err(MemoryPackError::LimitExceeded {
                resource: "string bytes",
                limit: limits.max_string_bytes,
            });
        }
        self.reserve_allocation(bytes)
    }
}
