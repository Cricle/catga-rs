//! Shared typed request fixtures for request/reply edge tests.

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Handler, Message, Request};

/// A typed doubling request used by request/reply edge tests.
pub struct DoubleRequest(pub u64);

/// Compile-time marker for [`DoubleRequest`].

impl Message for DoubleRequest {}

impl Request for DoubleRequest {
    type Response = u64;
}

impl MemoryPackSerialize for DoubleRequest {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.write_u64(self.0)
    }
}

impl MemoryPackDeserialize for DoubleRequest {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        reader.read_u64().map(Self)
    }
}

/// A handler that doubles its request value.
pub struct DoublingHandler;

#[async_trait::async_trait]
impl Handler<DoubleRequest> for DoublingHandler {
    async fn handle(&self, request: DoubleRequest) -> CatgaResult<u64> {
        Ok(request.0 * 2)
    }
}

/// A handler that always rejects its request with a validation failure.
pub struct RejectingHandler;

#[async_trait::async_trait]
impl Handler<DoubleRequest> for RejectingHandler {
    async fn handle(&self, _: DoubleRequest) -> CatgaResult<u64> {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "rejected by test handler",
        ))
    }
}
