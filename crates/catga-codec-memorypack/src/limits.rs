use catga_core::{CatgaResult, ErrorCode};

use crate::error::invalid;

/// Resource budgets applied before a MemoryPack reader or writer allocates memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPackLimits {
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_allocation_bytes: usize,
    pub(crate) max_string_bytes: usize,
    pub(crate) max_collection_items: usize,
    pub(crate) max_nesting_depth: usize,
}

impl MemoryPackLimits {
    /// Creates non-zero budgets for one exact MemoryPack frame.
    pub fn new(
        max_frame_bytes: usize,
        max_allocation_bytes: usize,
        max_string_bytes: usize,
        max_collection_items: usize,
        max_nesting_depth: usize,
    ) -> CatgaResult<Self> {
        if [
            max_frame_bytes,
            max_allocation_bytes,
            max_string_bytes,
            max_collection_items,
            max_nesting_depth,
        ]
        .contains(&0)
        {
            return Err(invalid("MemoryPack limits must be non-zero"));
        }
        if max_string_bytes > max_frame_bytes || max_string_bytes > max_allocation_bytes {
            return Err(catga_core::CatgaError::new(
                ErrorCode::Validation,
                "MemoryPack string budget exceeds the frame or allocation budget",
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
}

impl Default for MemoryPackLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_allocation_bytes: 1024 * 1024,
            max_string_bytes: 256 * 1024,
            max_collection_items: 65_536,
            max_nesting_depth: 8,
        }
    }
}
