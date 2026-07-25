use catga_core::CatgaResult;

use crate::{
    MemoryPackLimits, MemoryPackReader, MemoryPackWriter,
    fixtures::{
        DEAD_LETTER_MESSAGE_MEMBERS, FLOW_STATE_MEMBERS, FOR_EACH_PROGRESS_MEMBERS,
        INBOX_MESSAGE_MEMBERS, NATS_STORED_SNAPSHOT_MEMBERS, OUTBOX_MESSAGE_MEMBERS,
        STORED_SNAPSHOT_METADATA_MEMBERS,
    },
};

trait RecordCodec: Sized {
    const MEMBERS: u8;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self>;
    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()>;
}

fn decode_record<T: RecordCodec>(bytes: &[u8], limits: MemoryPackLimits) -> CatgaResult<Option<T>> {
    let mut reader = MemoryPackReader::new(bytes, limits)?;
    if !reader.read_object_header(T::MEMBERS)? {
        reader.finish()?;
        return Ok(None);
    }
    let value = T::read(&mut reader)?;
    reader.finish_object()?;
    reader.finish()?;
    Ok(Some(value))
}

fn encode_record<T: RecordCodec>(
    value: Option<&T>,
    limits: MemoryPackLimits,
) -> CatgaResult<Vec<u8>> {
    let mut writer = MemoryPackWriter::new(limits);
    match value {
        Some(value) => {
            writer.write_object_header(T::MEMBERS)?;
            value.write(&mut writer)?;
            writer.finish_object()?;
        }
        None => writer.write_null_object()?,
    }
    writer.finish()
}

macro_rules! record_api {
    ($type:ty, $name:literal) => {
        impl $type {
            #[doc = concat!("Decodes one exact nullable `", $name, "` formatter frame.")]
            pub fn decode(bytes: &[u8], limits: MemoryPackLimits) -> CatgaResult<Option<Self>> {
                decode_record(bytes, limits)
            }

            #[doc = concat!("Encodes one exact nullable `", $name, "` formatter frame.")]
            pub fn encode(value: Option<&Self>, limits: MemoryPackLimits) -> CatgaResult<Vec<u8>> {
                encode_record(value, limits)
            }
        }
    };
}

/// Stable nine-member payload emitted by the Catga `FlowState` formatter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowStateRecord {
    /// Stable flow identity.
    pub id: Option<Box<str>>,
    /// Registered flow type name.
    pub flow_type: Option<Box<str>>,
    /// Source lifecycle enum discriminant.
    pub status: u8,
    /// Source current-step value.
    pub current_step: i32,
    /// Optimistic concurrency version.
    pub version: i64,
    /// Current owner identity.
    pub owner: Option<Box<str>>,
    /// Raw `DateTime.ToBinary` heartbeat value.
    pub heartbeat_binary: i64,
    /// Opaque application state bytes.
    pub data: Option<Box<[u8]>>,
    /// Persisted flow error text.
    pub error: Option<Box<str>>,
}

impl RecordCodec for FlowStateRecord {
    const MEMBERS: u8 = FLOW_STATE_MEMBERS;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self> {
        Ok(Self {
            id: reader.read_string()?,
            flow_type: reader.read_string()?,
            status: reader.read_u8()?,
            current_step: reader.read_i32()?,
            version: reader.read_i64()?,
            owner: reader.read_string()?,
            heartbeat_binary: reader.read_datetime_binary()?,
            data: reader.read_bytes()?,
            error: reader.read_string()?,
        })
    }

    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_string(self.id.as_deref())?;
        writer.write_string(self.flow_type.as_deref())?;
        writer.write_u8(self.status)?;
        writer.write_i32(self.current_step)?;
        writer.write_i64(self.version)?;
        writer.write_string(self.owner.as_deref())?;
        writer.write_datetime_binary(self.heartbeat_binary)?;
        writer.write_bytes(self.data.as_deref())?;
        writer.write_string(self.error.as_deref())
    }
}

record_api!(FlowStateRecord, "FlowState");

/// Stable thirteen-member payload emitted by the Catga outbox formatter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxMessageRecord {
    /// Message identity.
    pub id: i64,
    /// Source message type name.
    pub message_type: Option<Box<str>>,
    /// Opaque message payload.
    pub payload: Option<Box<[u8]>>,
    /// Raw `DateTime.ToBinary` creation value.
    pub created_at_binary: i64,
    /// Raw nullable-date slot, represented by the formatter as a binary value.
    pub scheduled_at_binary: i64,
    /// Source outbox-state enum discriminant.
    pub status: u8,
    /// Number of recorded retries.
    pub retry_count: i32,
    /// Configured retry limit.
    pub max_retries: i32,
    /// Last delivery error.
    pub last_error: Option<Box<str>>,
    /// Source priority enum discriminant.
    pub priority: u8,
    /// Source boolean formatter slot.
    pub flag: bool,
    /// Source correlation identity.
    pub correlation_id: i64,
    /// Source metadata JSON.
    pub metadata_json: Option<Box<str>>,
}

impl RecordCodec for OutboxMessageRecord {
    const MEMBERS: u8 = OUTBOX_MESSAGE_MEMBERS;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self> {
        Ok(Self {
            id: reader.read_i64()?,
            message_type: reader.read_string()?,
            payload: reader.read_bytes()?,
            created_at_binary: reader.read_datetime_binary()?,
            scheduled_at_binary: reader.read_datetime_binary()?,
            status: reader.read_u8()?,
            retry_count: reader.read_i32()?,
            max_retries: reader.read_i32()?,
            last_error: reader.read_string()?,
            priority: reader.read_u8()?,
            flag: reader.read_bool()?,
            correlation_id: reader.read_i64()?,
            metadata_json: reader.read_string()?,
        })
    }

    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_i64(self.id)?;
        writer.write_string(self.message_type.as_deref())?;
        writer.write_bytes(self.payload.as_deref())?;
        writer.write_datetime_binary(self.created_at_binary)?;
        writer.write_datetime_binary(self.scheduled_at_binary)?;
        writer.write_u8(self.status)?;
        writer.write_i32(self.retry_count)?;
        writer.write_i32(self.max_retries)?;
        writer.write_string(self.last_error.as_deref())?;
        writer.write_u8(self.priority)?;
        writer.write_bool(self.flag)?;
        writer.write_i64(self.correlation_id)?;
        writer.write_string(self.metadata_json.as_deref())
    }
}

record_api!(OutboxMessageRecord, "OutboxMessage");

/// Stable thirteen-member payload emitted by the Catga inbox formatter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboxMessageRecord {
    /// Message identity.
    pub id: i64,
    /// Source message type name.
    pub message_type: Option<Box<str>>,
    /// Opaque message payload.
    pub payload: Option<Box<[u8]>>,
    /// Raw `DateTime.ToBinary` receipt value.
    pub received_at_binary: i64,
    /// Source inbox-state enum discriminant.
    pub status: u8,
    /// Raw `DateTime.ToBinary` processing value.
    pub processed_at_binary: i64,
    /// Opaque formatter result bytes.
    pub processing_result: Option<Box<[u8]>>,
    /// Source mode enum discriminant.
    pub mode: u8,
    /// First source boolean formatter slot.
    pub flag: bool,
    /// Raw `DateTime.ToBinary` expiration value.
    pub expires_at_binary: i64,
    /// Second source boolean formatter slot.
    pub secondary_flag: bool,
    /// Source correlation identity.
    pub correlation_id: i64,
    /// Source metadata JSON.
    pub metadata_json: Option<Box<str>>,
}

impl RecordCodec for InboxMessageRecord {
    const MEMBERS: u8 = INBOX_MESSAGE_MEMBERS;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self> {
        Ok(Self {
            id: reader.read_i64()?,
            message_type: reader.read_string()?,
            payload: reader.read_bytes()?,
            received_at_binary: reader.read_datetime_binary()?,
            status: reader.read_u8()?,
            processed_at_binary: reader.read_datetime_binary()?,
            processing_result: reader.read_bytes()?,
            mode: reader.read_u8()?,
            flag: reader.read_bool()?,
            expires_at_binary: reader.read_datetime_binary()?,
            secondary_flag: reader.read_bool()?,
            correlation_id: reader.read_i64()?,
            metadata_json: reader.read_string()?,
        })
    }

    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_i64(self.id)?;
        writer.write_string(self.message_type.as_deref())?;
        writer.write_bytes(self.payload.as_deref())?;
        writer.write_datetime_binary(self.received_at_binary)?;
        writer.write_u8(self.status)?;
        writer.write_datetime_binary(self.processed_at_binary)?;
        writer.write_bytes(self.processing_result.as_deref())?;
        writer.write_u8(self.mode)?;
        writer.write_bool(self.flag)?;
        writer.write_datetime_binary(self.expires_at_binary)?;
        writer.write_bool(self.secondary_flag)?;
        writer.write_i64(self.correlation_id)?;
        writer.write_string(self.metadata_json.as_deref())
    }
}

record_api!(InboxMessageRecord, "InboxMessage");

/// Stable eight-member payload emitted by the Catga dead-letter formatter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterMessageRecord {
    /// Message identity.
    pub id: i64,
    /// Source message type name.
    pub message_type: Option<Box<str>>,
    /// Opaque message payload.
    pub payload: Option<Box<[u8]>>,
    /// Source exception type name.
    pub exception_type: Option<Box<str>>,
    /// Exception message.
    pub error: Option<Box<str>>,
    /// Persisted source stack trace.
    pub stack_trace: Option<Box<str>>,
    /// Number of delivery attempts.
    pub retry_count: i32,
    /// Raw `DateTime.ToBinary` dead-letter time.
    pub dead_lettered_at_binary: i64,
}

impl RecordCodec for DeadLetterMessageRecord {
    const MEMBERS: u8 = DEAD_LETTER_MESSAGE_MEMBERS;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self> {
        Ok(Self {
            id: reader.read_i64()?,
            message_type: reader.read_string()?,
            payload: reader.read_bytes()?,
            exception_type: reader.read_string()?,
            error: reader.read_string()?,
            stack_trace: reader.read_string()?,
            retry_count: reader.read_i32()?,
            dead_lettered_at_binary: reader.read_datetime_binary()?,
        })
    }

    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_i64(self.id)?;
        writer.write_string(self.message_type.as_deref())?;
        writer.write_bytes(self.payload.as_deref())?;
        writer.write_string(self.exception_type.as_deref())?;
        writer.write_string(self.error.as_deref())?;
        writer.write_string(self.stack_trace.as_deref())?;
        writer.write_i32(self.retry_count)?;
        writer.write_datetime_binary(self.dead_lettered_at_binary)
    }
}

record_api!(DeadLetterMessageRecord, "DeadLetterMessage");

/// Stable six-member payload emitted by the Catga stored-snapshot metadata formatter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSnapshotMetadataRecord {
    /// Stable flow identity.
    pub flow_id: Option<Box<str>>,
    /// Source state type name.
    pub state_type: Option<Box<str>>,
    /// Source snapshot-format enum discriminant.
    pub format: u8,
    /// Raw `DateTime.ToBinary` creation value.
    pub created_at_binary: i64,
    /// Raw `DateTime.ToBinary` expiration value.
    pub expires_at_binary: i64,
    /// Persisted snapshot payload length.
    pub payload_length: i32,
}

impl RecordCodec for StoredSnapshotMetadataRecord {
    const MEMBERS: u8 = STORED_SNAPSHOT_METADATA_MEMBERS;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self> {
        Ok(Self {
            flow_id: reader.read_string()?,
            state_type: reader.read_string()?,
            format: reader.read_u8()?,
            created_at_binary: reader.read_datetime_binary()?,
            expires_at_binary: reader.read_datetime_binary()?,
            payload_length: reader.read_i32()?,
        })
    }

    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_string(self.flow_id.as_deref())?;
        writer.write_string(self.state_type.as_deref())?;
        writer.write_u8(self.format)?;
        writer.write_datetime_binary(self.created_at_binary)?;
        writer.write_datetime_binary(self.expires_at_binary)?;
        writer.write_i32(self.payload_length)
    }
}

record_api!(StoredSnapshotMetadataRecord, "StoredSnapshotMetadata");

/// Stable five-member payload emitted by the Catga NATS stored-snapshot formatter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsStoredSnapshotRecord {
    /// NATS snapshot key.
    pub key: Option<Box<str>>,
    /// Snapshot version.
    pub version: i64,
    /// Raw `DateTime.ToBinary` creation value.
    pub created_at_binary: i64,
    /// Source state type name.
    pub state_type: Option<Box<str>>,
    /// Opaque snapshot payload.
    pub payload: Option<Box<[u8]>>,
}

impl RecordCodec for NatsStoredSnapshotRecord {
    const MEMBERS: u8 = NATS_STORED_SNAPSHOT_MEMBERS;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self> {
        Ok(Self {
            key: reader.read_string()?,
            version: reader.read_i64()?,
            created_at_binary: reader.read_datetime_binary()?,
            state_type: reader.read_string()?,
            payload: reader.read_bytes()?,
        })
    }

    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_string(self.key.as_deref())?;
        writer.write_i64(self.version)?;
        writer.write_datetime_binary(self.created_at_binary)?;
        writer.write_string(self.state_type.as_deref())?;
        writer.write_bytes(self.payload.as_deref())
    }
}

record_api!(NatsStoredSnapshotRecord, "NatsStoredSnapshot");

/// Stable four-member payload emitted by the Catga `ForEachProgress` formatter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForEachProgressRecord {
    /// Current source item index.
    pub current_index: i32,
    /// Total source item count.
    pub total_items: i32,
    /// Completed source item indices.
    pub completed: Option<Box<[i32]>>,
    /// Failed source item indices.
    pub failed: Option<Box<[i32]>>,
}

impl RecordCodec for ForEachProgressRecord {
    const MEMBERS: u8 = FOR_EACH_PROGRESS_MEMBERS;

    fn read(reader: &mut MemoryPackReader<'_>) -> CatgaResult<Self> {
        Ok(Self {
            current_index: reader.read_i32()?,
            total_items: reader.read_i32()?,
            completed: reader.read_i32_array()?,
            failed: reader.read_i32_array()?,
        })
    }

    fn write(&self, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_i32(self.current_index)?;
        writer.write_i32(self.total_items)?;
        writer.write_i32_array(self.completed.as_deref())?;
        writer.write_i32_array(self.failed.as_deref())
    }
}

record_api!(ForEachProgressRecord, "ForEachProgress");
