//! Redis Streams-backed optimistic event persistence.

use std::{
    collections::BinaryHeap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, EventPage, EventStore,
    EventStream, StoredEvent, StreamIdsPage, VersionHistoryPage, VersionInfo, telemetry,
    validate_event_store_page_size,
};
use redis::{
    AsyncCommands, Script,
    aio::ConnectionManager,
    cmd,
    streams::{StreamId, StreamRangeReply},
};

use crate::{RedisCommandOptions, transport::map_error};

const APPEND: &str = r#"
local current = redis.call('GET', KEYS[1])
if current == false then current = '-1' end
if ARGV[1] ~= '' and ARGV[1] ~= current then
    return {err = 'CATGA_VERSION_CONFLICT'}
end

local function increment(value)
    if value == '-1' then return '0' end
    local digits = { string.byte(value, 1, #value) }
    local carry = 1
    for index = #digits, 1, -1 do
        local digit = digits[index] - string.byte('0') + carry
        if digit == 10 then
            digits[index] = string.byte('0')
        else
            digits[index] = string.byte('0') + digit
            carry = 0
            break
        end
    end
    if carry == 1 then table.insert(digits, 1, string.byte('1')) end
    return string.char(unpack(digits))
end

local count = tonumber(ARGV[2])
local final = current
for i = 1, count do
    if final == '9223372036854775807' then
        return {err = 'CATGA_VERSION_EXHAUSTED'}
    end
    final = increment(final)
end
for i = 1, count do
    current = increment(current)
    local offset = 3 + (i - 1) * 2
    redis.call('XADD', KEYS[2], increment(current) .. '-0', 'version', current, 'payload', ARGV[offset], 'timestamp', ARGV[offset + 1])
end
redis.call('SET', KEYS[1], current)
redis.call('SADD', KEYS[3], ARGV[3 + count * 2])
return current
"#;

/// Redis Streams event store with atomic version checks and batched appends.
pub struct RedisEventStore {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: MemoryPackCodec,
    /// Pre-hashed append script; avoids recomputing the SHA-1 digest on every append.
    append_script: Script,
}

impl RedisEventStore {
    /// Connects to Redis and namespaces all event streams beneath `prefix`.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        Self::connect_with_options(server, prefix, RedisCommandOptions::default()).await
    }

    /// Connects with an explicit timeout policy for ordinary Redis commands.
    ///
    /// The policy bounds later persistence commands after the connection is established.
    pub async fn connect_with_options(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
        command_options: RedisCommandOptions,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(command_options.connection_manager_config())
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
            codec: MemoryPackCodec::default(),
            append_script: Script::new(APPEND),
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

    async fn entries_between(
        &self,
        stream_id: &str,
        from_version: u64,
        to_version: i64,
        max_count: usize,
    ) -> CatgaResult<Vec<StoredEvent>> {
        if to_version < 0 || max_count == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self.connection.clone();
        let start = stream_entry_id(from_version)?;
        let end = stream_entry_id(u64::try_from(to_version).unwrap_or(u64::MAX))?;
        let reply: StreamRangeReply = connection
            .xrange_count(self.stream_key(stream_id), start, end, max_count)
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
                let mut connection = self.connection.clone();
                let version: Option<i64> = connection
                    .get(self.version_key(stream_id))
                    .await
                    .map_err(map_error)?;
                return Ok(version.unwrap_or(-1));
            }
            let payloads: CatgaResult<Vec<_>> = events
                .iter()
                .map(|event| self.codec.encode(event))
                .collect();
            let payloads = payloads?;
            let mut connection = self.connection.clone();
            let version_key = self.version_key(stream_id);
            let stream_key = self.stream_key(stream_id);
            let ids_key = self.ids_key();
            let mut invocation = self.append_script.key(&version_key);
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

    async fn read_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        validate_event_store_page_size(max_count)?;
        telemetry::record_persistence("redis", "event_store", "read_page", async {
            let version = self.version(stream_id).await?;
            let events = self
                .entries_from(stream_id, from_version, max_count)
                .await?;
            let next_version = events.last().and_then(|event| {
                (event.version() < version)
                    .then(|| u64::try_from(event.version().saturating_add(1)).ok())
                    .flatten()
            });
            Ok(EventPage::new(
                EventStream::new(stream_id, version, events),
                next_version,
            ))
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

    async fn read_to_version_page(
        &self,
        stream_id: &str,
        from_version: u64,
        to_version: i64,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        validate_event_store_page_size(max_count)?;
        telemetry::record_persistence("redis", "event_store", "read_to_version_page", async {
            let events = self
                .entries_between(stream_id, from_version, to_version, max_count)
                .await?;
            let version = events.last().map_or(-1, StoredEvent::version);
            let stream_version = self.version(stream_id).await?;
            let next_version = events.last().and_then(|event| {
                (event.version() < to_version && event.version() < stream_version)
                    .then(|| u64::try_from(event.version().saturating_add(1)).ok())
                    .flatten()
            });
            Ok(EventPage::new(
                EventStream::new(stream_id, version, events),
                next_version,
            ))
        })
        .await
    }

    async fn read_to_time_page(
        &self,
        stream_id: &str,
        from_version: u64,
        upper_bound: SystemTime,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        validate_event_store_page_size(max_count)?;
        telemetry::record_persistence("redis", "event_store", "read_to_time_page", async {
            let stream_version = self.version(stream_id).await?;
            let scanned = self
                .entries_from(stream_id, from_version, max_count)
                .await?;
            let next_version = scanned.last().and_then(|event| {
                (event.version() < stream_version)
                    .then(|| u64::try_from(event.version().saturating_add(1)).ok())
                    .flatten()
            });
            let events: Vec<_> = scanned
                .into_iter()
                .filter(|event| event.timestamp() <= upper_bound)
                .collect();
            let version = events.last().map_or(-1, StoredEvent::version);
            Ok(EventPage::new(
                EventStream::new(stream_id, version, events),
                next_version,
            ))
        })
        .await
    }

    async fn version_history_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        validate_event_store_page_size(max_count)?;
        telemetry::record_persistence("redis", "event_store", "version_history_page", async {
            let stream_version = self.version(stream_id).await?;
            let events = self
                .entries_from(stream_id, from_version, max_count)
                .await?;
            let next_version = events.last().and_then(|event| {
                (event.version() < stream_version)
                    .then(|| u64::try_from(event.version().saturating_add(1)).ok())
                    .flatten()
            });
            let entries = events
                .into_iter()
                .map(|event| {
                    VersionInfo::new(
                        event.version(),
                        event.timestamp(),
                        event.envelope().message_type(),
                    )
                })
                .collect();
            Ok(VersionHistoryPage::new(entries, next_version))
        })
        .await
    }

    async fn stream_ids_page(
        &self,
        after: Option<&str>,
        max_count: usize,
    ) -> CatgaResult<StreamIdsPage> {
        validate_event_store_page_size(max_count)?;
        telemetry::record_persistence("redis", "event_store", "stream_ids_page", async {
            let mut connection = self.connection.clone();
            let mut scan_cursor = 0_u64;
            let mut ids = BinaryHeap::with_capacity(max_count);
            let mut has_more = false;
            loop {
                let (next_cursor, scanned): (u64, Vec<String>) = cmd("SSCAN")
                    .arg(self.ids_key())
                    .arg(scan_cursor)
                    .arg("COUNT")
                    .arg(max_count)
                    .query_async(&mut connection)
                    .await
                    .map_err(map_error)?;
                for id in scanned {
                    if after.is_some_and(|cursor| id.as_str() <= cursor) {
                        continue;
                    }
                    if ids.len() < max_count {
                        ids.push(id);
                    } else {
                        has_more = true;
                        let largest = ids.peek().map(String::as_str);
                        if largest.is_some_and(|largest| id.as_str() < largest) {
                            let _ = ids.pop();
                            ids.push(id);
                        }
                    }
                }
                if next_cursor == 0 {
                    break;
                }
                scan_cursor = next_cursor;
            }
            let mut ids = ids.into_vec();
            ids.sort_unstable();
            let next_stream_id = has_more.then(|| ids.last().cloned()).flatten();
            Ok(StreamIdsPage::new(ids, next_stream_id))
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
    let message = error.to_string();
    if message.contains("CATGA_VERSION_CONFLICT") {
        CatgaError::new(ErrorCode::Conflict, "event stream version conflict")
    } else if message.contains("CATGA_VERSION_EXHAUSTED") {
        CatgaError::new(ErrorCode::Internal, "event stream version is exhausted")
    } else {
        map_error(message)
    }
}
