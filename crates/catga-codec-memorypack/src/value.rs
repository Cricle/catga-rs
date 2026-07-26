use catga_core::{CatgaResult, PayloadDecoder, PayloadEncoder};

use crate::{MemoryPackLimits, MemoryPackReader, MemoryPackWriter};

/// Explicit MemoryPack schema for one application-owned value type.
///
/// Implementations declare their fields and MemoryPack layout directly. Catga never infers a
/// schema through runtime type lookup or C# reflection.
pub trait MemoryPackValueCodec<T>: Send + Sync {
    /// Writes `value` to the supplied bounded MemoryPack frame writer.
    ///
    /// Implementations must close every non-null object scope they open before returning.
    fn encode(&self, value: &T, writer: &mut MemoryPackWriter) -> CatgaResult<()>;

    /// Reads one value from the supplied bounded MemoryPack frame reader.
    ///
    /// Implementations must close every non-null object scope they open before returning.
    fn decode(&self, reader: &mut MemoryPackReader<'_>) -> CatgaResult<T>;

    /// Encodes one value into an exact, allocation-bounded MemoryPack frame.
    fn encode_value(&self, value: &T, limits: MemoryPackLimits) -> CatgaResult<Vec<u8>> {
        encode_value(self, value, limits)
    }

    /// Decodes one exact, allocation-bounded MemoryPack frame.
    ///
    /// Trailing input and unclosed object scopes are rejected after the schema decoder returns.
    fn decode_value(&self, bytes: &[u8], limits: MemoryPackLimits) -> CatgaResult<T> {
        decode_value(self, bytes, limits)
    }
}

/// Adapts one explicit MemoryPack schema to Catga's typed payload codec contracts.
///
/// The adapter owns both the schema and its fixed frame limits, so transport call sites cannot
/// accidentally bypass the MemoryPack allocation, collection, string, or nesting budgets.
pub struct MemoryPackPayloadCodec<C> {
    schema: C,
    limits: MemoryPackLimits,
}

impl<C> MemoryPackPayloadCodec<C> {
    /// Creates a payload adapter using `schema` and fixed `limits` for every frame.
    pub const fn new(schema: C, limits: MemoryPackLimits) -> Self {
        Self { schema, limits }
    }

    /// Returns the explicit schema used for typed payload frames.
    pub const fn schema(&self) -> &C {
        &self.schema
    }

    /// Returns the fixed frame limits applied to every payload frame.
    pub const fn limits(&self) -> MemoryPackLimits {
        self.limits
    }
}

impl<T, C> PayloadEncoder<T> for MemoryPackPayloadCodec<C>
where
    C: MemoryPackValueCodec<T>,
{
    fn encode_payload(&self, value: &T) -> CatgaResult<Vec<u8>> {
        self.schema.encode_value(value, self.limits)
    }
}

impl<T, C> PayloadDecoder<T> for MemoryPackPayloadCodec<C>
where
    C: MemoryPackValueCodec<T>,
{
    fn decode_payload(&self, bytes: &[u8]) -> CatgaResult<T> {
        self.schema.decode_value(bytes, self.limits)
    }
}

/// Encodes `value` with an explicit schema into an exact, allocation-bounded MemoryPack frame.
pub fn encode_value<T, C>(codec: &C, value: &T, limits: MemoryPackLimits) -> CatgaResult<Vec<u8>>
where
    C: MemoryPackValueCodec<T> + ?Sized,
{
    let mut writer = MemoryPackWriter::new(limits);
    codec.encode(value, &mut writer)?;
    writer.finish()
}

/// Decodes `bytes` with an explicit schema from one exact, allocation-bounded MemoryPack frame.
///
/// This helper rejects trailing input and unclosed object scopes after decoding the value.
pub fn decode_value<T, C>(codec: &C, bytes: &[u8], limits: MemoryPackLimits) -> CatgaResult<T>
where
    C: MemoryPackValueCodec<T> + ?Sized,
{
    let mut reader = MemoryPackReader::new(bytes, limits)?;
    let value = codec.decode(&mut reader)?;
    reader.finish()?;
    Ok(value)
}
