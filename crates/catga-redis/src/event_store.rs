//! Redis Streams-backed optimistic event persistence.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, EventStore, EventStream,
    StoredEvent, VersionInfo, telemetry,
};
use redis::{
    AsyncCommands, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
    streams::{StreamId, StreamRangeReply},
};

use crate::transport::map_error;

const APPEND: &str = r#"
local current = redis.call('GET', KEYS[1])
if current == false then current = -1 else current = tonumber(current) end
if ARGV[1] ~= '' and tonumber(ARGV[1]) ~= current then
    return {err = 'CATGA_VERSION_CONFLICT'}
end
local count = tonumber(ARGV[2])
for i = 1, count do
    current = current + 1
    local offset = 3 + (i - 1) * 2
    redis.call('XADD', KEYS[2], tostring(current + 1) .. '-0', 'version', current, 'payload', ARGV[offset], 'timestamp', ARGV[offset + 1])
end
redis.call('SET', KEYS[1], current)
redis.call('SADD', KEYS[3], ARGV[3 + count * 2])
return current
"#;

/// Redis Streams event store with atomic version checks and batched appends.
pub struct RedisEventStore {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: PostcardCodec,
}

impl RedisEventStore {
    /// Connects to Redis and namespaces all event streams beneath `prefix`.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new().set_response_timeout(None),
            )
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
            codec: PostcardCodec,
        })
    }

    fn version_key(&self, stream_id: &str) -> String {
        format!("{}:version:{stream_id}", self.prefix)
    }

    fn stream_key(&self, stream_id: &str) -> String {
        format!("{}:stream:{stream_id}", self.prefix)
    }

    fn ids_key(&self) -> String {
        format!("{}:ids", self.prefix)
    }

    async fn entries(&self, stream_id: &str) -> CatgaResult<Vec<StoredEvent>> {
        let mut connection = self.connection.clone();
        let reply: StreamRangeReply = connection
            .xrange_all(self.stream_key(stream_id))
            .await
            .map_err(map_error)?;
        reply
            .ids
            .iter()
            .map(|entry| self.decode_entry(entry))
            .collect()
    }

    async fn entries_from(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<Vec<StoredEvent>> {
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self.connection.clone();
        let start = stream_entry_id(from_version)?;
        let reply: StreamRangeReply = connection
            .xrange_count(self.stream_key(stream_id), start, "+", max_count)
            .await
            .map_err(map_error)?;
        reply
            .ids
            .iter()
            .map(|entry| self.decode_entry(entry))
            .collect()
    }

    async fn entries_to(&self, stream_id: &str, to_version: i64) -> CatgaResult<Vec<StoredEvent>> {
        if to_version < 0 {
            return Ok(Vec::new());
        }
        let mut connection = self.connection.clone();
        let end = stream_entry_id(u64::try_from(to_version).unwrap_or(u64::MAX))?;
        let reply: StreamRangeReply = connection
            .xrange(self.stream_key(stream_id), "-", end)
            .await
            .map_err(map_error)?;
        reply
            .ids
            .iter()
            .map(|entry| self.decode_entry(entry))
            .collect()
    }

    fn decode_entry(&self, entry: &StreamId) -> CatgaResult<StoredEvent> {
        let version = entry.get::<i64>("version").ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "Redis event entry is missing version")
        })?;
        let payload = entry.get::<Vec<u8>>("payload").ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "Redis event entry is missing payload")
        })?;
        let timestamp = entry.get::<u64>("timestamp").ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "Redis event entry is missing timestamp",
            )
        })?;
        Ok(StoredEvent::new(
            version,
            Arc::new(self.codec.decode(&payload)?),
            from_unix_millis(timestamp),
        ))
    }
}

#[async_trait]
impl EventStore for RedisEventStore {
    async fn append(
        &self,
        stream_id: &str,
        events: Vec<Envelope>,
        expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        telemetry::record_persistence("redis", "event_store", "append", async {
            if stream_id.is_empty() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "event stream id must not be empty",
                ));
            }
            if events.is_empty() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "event batch must not be empty",
                ));
            }
            let payloads: CatgaResult<Vec<_>> = events
                .iter()
                .map(|event| self.codec.encode(event))
                .collect();
            let payloads = payloads?;
            let mut connection = self.connection.clone();
            let script = Script::new(APPEND);
            let version_key = self.version_key(stream_id);
            let stream_key = self.stream_key(stream_id);
            let ids_key = self.ids_key();
            let mut invocation = script.key(&version_key);
            invocation
                .key(&stream_key)
                .key(&ids_key)
                .arg(expected_version.map_or_else(String::new, |version| version.to_string()))
                .arg(payloads.len());
            let timestamp = unix_millis(SystemTime::now());
            for payload in payloads {
                invocation.arg(payload).arg(timestamp);
            }
            invocation.arg(stream_id);
            invocation
                .invoke_async(&mut connection)
                .await
                .map_err(map_append_error)
        })
        .await
    }

    async fn read(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventStream> {
        telemetry::record_persistence("redis", "event_store", "read", async {
            let version = self.version(stream_id).await?;
            let events = self
                .entries_from(stream_id, from_version, max_count)
                .await?;
            Ok(EventStream::new(stream_id, version, events))
        })
        .await
    }

    async fn version(&self, stream_id: &str) -> CatgaResult<i64> {
        telemetry::record_persistence("redis", "event_store", "version", async {
            let mut connection = self.connection.clone();
            let version: Option<i64> = connection
                .get(self.version_key(stream_id))
                .await
                .map_err(map_error)?;
            Ok(version.unwrap_or(-1))
        })
        .await
    }

    async fn read_to_version(&self, stream_id: &str, to_version: i64) -> CatgaResult<EventStream> {
        telemetry::record_persistence("redis", "event_store", "read_to_version", async {
            let events = self.entries_to(stream_id, to_version).await?;
            let version = events.last().map_or(-1, StoredEvent::version);
            Ok(EventStream::new(stream_id, version, events))
        })
        .await
    }

    async fn read_to_time(
        &self,
        stream_id: &str,
        upper_bound: SystemTime,
    ) -> CatgaResult<EventStream> {
        telemetry::record_persistence("redis", "event_store", "read_to_time", async {
            let events: Vec<_> = self
                .entries(stream_id)
                .await?
                .into_iter()
                .filter(|event| event.timestamp() <= upper_bound)
                .collect();
            let version = events.last().map_or(-1, StoredEvent::version);
            Ok(EventStream::new(stream_id, version, events))
        })
        .await
    }

    async fn version_history(&self, stream_id: &str) -> CatgaResult<Vec<VersionInfo>> {
        telemetry::record_persistence("redis", "event_store", "version_history", async {
            self.entries(stream_id)
                .await?
                .into_iter()
                .map(|event| {
                    Ok(VersionInfo::new(
                        event.version(),
                        event.timestamp(),
                        event.envelope().message_type(),
                    ))
                })
                .collect()
        })
        .await
    }

    async fn stream_ids(&self) -> CatgaResult<Vec<String>> {
        telemetry::record_persistence("redis", "event_store", "stream_ids", async {
            let mut connection = self.connection.clone();
            let mut ids: Vec<String> = connection
                .smembers(self.ids_key())
                .await
                .map_err(map_error)?;
            ids.sort_unstable();
            Ok(ids)
        })
        .await
    }
}

fn unix_millis(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn from_unix_millis(millis: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis)
}

fn stream_entry_id(version: u64) -> CatgaResult<String> {
    let id = version.checked_add(1).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Validation,
            "event stream version exceeds Redis ID range",
        )
    })?;
    Ok(format!("{id}-0"))
}

fn map_append_error(error: redis::RedisError) -> CatgaError {
    if error.to_string().contains("CATGA_VERSION_CONFLICT") {
        CatgaError::new(ErrorCode::Conflict, "event stream version conflict")
    } else {
        map_error(error)
    }
}
