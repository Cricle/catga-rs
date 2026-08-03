use crate::codec::memorypack::MemoryPackError;

/// Resource budgets applied before a MemoryPack value is decoded from an untrusted frame.
///
/// Catga uses [`crate::MemoryPackReader::new_bounded`] and
/// [`crate::MemoryPackSerializer::deserialize_bounded`] at transport boundaries so a compact,
/// malformed frame cannot request an unbounded allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPackDecodeLimits {
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_allocation_bytes: usize,
    pub(crate) max_string_bytes: usize,
    pub(crate) max_collection_items: usize,
    pub(crate) max_nesting_depth: usize,
}

impl MemoryPackDecodeLimits {
    /// Creates non-zero budgets for one received MemoryPack frame.
    pub fn new(
        max_frame_bytes: usize,
        max_allocation_bytes: usize,
        max_string_bytes: usize,
        max_collection_items: usize,
        max_nesting_depth: usize,
    ) -> Result<Self, MemoryPackError> {
        if [
            max_frame_bytes,
            max_allocation_bytes,
            max_string_bytes,
            max_collection_items,
            max_nesting_depth,
        ]
        .contains(&0)
        {
            return Err(MemoryPackError::InvalidLimit(
                "MemoryPack decode limits must be non-zero".into(),
            ));
        }
        if max_string_bytes > max_allocation_bytes {
            return Err(MemoryPackError::InvalidLimit(
                "MemoryPack string limit exceeds the allocation limit".into(),
            ));
        }
        Ok(Self {
            max_frame_bytes,
            max_allocation_bytes,
            max_string_bytes,
            max_collection_items,
            max_nesting_depth,
        })
    }

    /// Returns the maximum accepted received frame length in bytes.
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }
}

impl Default for MemoryPackDecodeLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_allocation_bytes: 1024 * 1024,
            max_string_bytes: 256 * 1024,
            max_collection_items: 65_536,
            max_nesting_depth: 32,
        }
    }
}
