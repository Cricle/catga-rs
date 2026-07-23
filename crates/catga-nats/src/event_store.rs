//! JetStream-backed optimistic event persistence.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::{
    header,
    jetstream::{
        self,
        context::Publish,
        kv,
        stream::{self, DirectGetErrorKind, Stream},
    },
};
use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, EventStore, EventStream,
    StoredEvent, VersionInfo,
};
use futures::TryStreamExt;

const VERSION: &str = "Catga-Version";
const TIMESTAMP: &str = "Catga-Timestamp";

/// JetStream event store that uses subject-sequence preconditions for optimistic writes.
pub struct NatsEventStore {
    context: jetstream::Context,
    stream: Stream,
    ids: kv::Store,
    subject_prefix: Box<str>,
    codec: PostcardCodec,
}

impl NatsEventStore {
    /// Connects and provisions a direct-read JetStream stream for one event-store namespace.
    pub async fn connect(
        server: &str,
        stream_name: impl Into<Box<str>>,
        subject_prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = async_nats::connect(server).await.map_err(map_error)?;
        let context = jetstream::new(client);
        let stream_name = stream_name.into();
        let subject_prefix = subject_prefix.into();
        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.to_string(),
                subjects: vec![format!("{subject_prefix}.>")],
                allow_direct: true,
                ..Default::default()
            })
            .await
            .map_err(map_error)?;
        let bucket = format!("{stream_name}_IDS");
        let ids = match context.get_key_value(&bucket).await {
            Ok(store) => store,
            Err(_) => context
                .create_key_value(kv::Config {
                    bucket,
                    history: 1,
                    ..Default::default()
                })
                .await
                .map_err(map_error)?,
        };
        Ok(Self {
            context,
            stream,
            ids,
            subject_prefix,
            codec: PostcardCodec,
        })
    }

    fn subject(&self, stream_id: &str) -> CatgaResult<String> {
        if stream_id.is_empty()
            || stream_id
                .split('.')
                .any(|part| part.is_empty() || part == "*" || part == ">")
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "event stream id is not a valid NATS subject suffix",
            ));
        }
        Ok(format!("{}.{}", self.subject_prefix, stream_id))
    }

    async fn entries(&self, stream_id: &str) -> CatgaResult<Vec<StoredEvent>> {
        let subject = self.subject(stream_id)?;
        let first = match self.stream.direct_get_first_for_subject(&subject).await {
            Ok(message) => message,
            Err(error) if error.kind() == DirectGetErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(map_error(error)),
        };
        let mut entries = Vec::new();
        let mut current = first;
        loop {
            let sequence = message_sequence(&current)?;
            entries.push(self.decode_message(&current)?);
            current = match self
                .stream
                .direct_get_next_for_subject(&subject, Some(sequence))
                .await
            {
                Ok(message) => message,
                Err(error) if error.kind() == DirectGetErrorKind::NotFound => break,
                Err(error) => return Err(map_error(error)),
            };
        }
        Ok(entries)
    }

    async fn current(&self, stream_id: &str) -> CatgaResult<Option<(i64, u64)>> {
        let subject = self.subject(stream_id)?;
        match self.stream.direct_get_last_for_subject(subject).await {
            Ok(message) => Ok(Some((
                message_version(&message)?,
                message_sequence(&message)?,
            ))),
            Err(error) if error.kind() == DirectGetErrorKind::NotFound => Ok(None),
            Err(error) => Err(map_error(error)),
        }
    }

    fn decode_message(&self, message: &async_nats::Message) -> CatgaResult<StoredEvent> {
        Ok(StoredEvent::new(
            message_version(message)?,
            Arc::new(self.codec.decode(&message.payload)?),
            from_unix_millis(message_timestamp(message)?),
        ))
    }
}

#[async_trait]
impl EventStore for NatsEventStore {
    async fn append(
        &self,
        stream_id: &str,
        events: Vec<Envelope>,
        expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        if events.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "event batch must not be empty",
            ));
        }
        let subject = self.subject(stream_id)?;
        let current = self.current(stream_id).await?;
        if expected_version.is_some_and(|expected| current.map_or(-1, |value| value.0) != expected)
        {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "event stream version conflict",
            ));
        }
        let mut version = current.map_or(-1, |value| value.0);
        let mut previous_sequence = current.map_or(0, |value| value.1);
        let timestamp = unix_millis(SystemTime::now());
        for event in events {
            version = version.saturating_add(1);
            let payload = self.codec.encode(&event)?;
            let version_text = version.to_string();
            let timestamp_text = timestamp.to_string();
            let mut publish = Publish::build()
                .payload(payload.into())
                .header(VERSION, version_text.as_str())
                .header(TIMESTAMP, timestamp_text.as_str());
            if expected_version.is_some() {
                publish = publish.expected_last_subject_sequence(previous_sequence);
            }
            let ack = self
                .context
                .send_publish(subject.clone(), publish)
                .await
                .map_err(map_append_error)?
                .await
                .map_err(map_append_error)?;
            previous_sequence = ack.sequence;
        }
        self.ids
            .put(stream_id, "".into())
            .await
            .map_err(map_error)?;
        Ok(version)
    }

    async fn read(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventStream> {
        let version = self.version(stream_id).await?;
        let events = self
            .entries(stream_id)
            .await?
            .into_iter()
            .filter(|event| event.version() >= i64::try_from(from_version).unwrap_or(i64::MAX))
            .take(max_count)
            .collect();
        Ok(EventStream::new(stream_id, version, events))
    }

    async fn version(&self, stream_id: &str) -> CatgaResult<i64> {
        Ok(self.current(stream_id).await?.map_or(-1, |value| value.0))
    }

    async fn read_to_version(&self, stream_id: &str, to_version: i64) -> CatgaResult<EventStream> {
        let events: Vec<_> = self
            .entries(stream_id)
            .await?
            .into_iter()
            .filter(|event| event.version() <= to_version)
            .collect();
        let version = events.last().map_or(-1, StoredEvent::version);
        Ok(EventStream::new(stream_id, version, events))
    }

    async fn read_to_time(
        &self,
        stream_id: &str,
        upper_bound: SystemTime,
    ) -> CatgaResult<EventStream> {
        let events: Vec<_> = self
            .entries(stream_id)
            .await?
            .into_iter()
            .filter(|event| event.timestamp() <= upper_bound)
            .collect();
        let version = events.last().map_or(-1, StoredEvent::version);
        Ok(EventStream::new(stream_id, version, events))
    }

    async fn version_history(&self, stream_id: &str) -> CatgaResult<Vec<VersionInfo>> {
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
    }

    async fn stream_ids(&self) -> CatgaResult<Vec<String>> {
        let mut ids: Vec<_> = self
            .ids
            .keys()
            .await
            .map_err(map_error)?
            .try_collect()
            .await
            .map_err(map_error)?;
        ids.sort_unstable();
        Ok(ids)
    }
}

fn message_header<N>(message: &async_nats::Message, name: N) -> CatgaResult<&str>
where
    N: async_nats::header::IntoHeaderName + std::fmt::Display,
{
    let label = name.to_string();
    message
        .headers
        .as_ref()
        .and_then(|headers| headers.get(name))
        .map(|value| value.as_str())
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("JetStream event is missing {label}"),
            )
        })
}
fn message_version(message: &async_nats::Message) -> CatgaResult<i64> {
    message_header(message, VERSION)?.parse().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "JetStream event has an invalid version",
        )
    })
}
fn message_timestamp(message: &async_nats::Message) -> CatgaResult<u64> {
    message_header(message, TIMESTAMP)?.parse().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "JetStream event has an invalid timestamp",
        )
    })
}
fn message_sequence(message: &async_nats::Message) -> CatgaResult<u64> {
    message_header(message, header::NATS_SEQUENCE)?
        .parse()
        .map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "JetStream event has an invalid sequence",
            )
        })
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
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
fn map_append_error(error: impl std::fmt::Display) -> CatgaError {
    let message = error.to_string();
    if message.contains("expected last subject sequence") || message.contains("wrong last sequence")
    {
        CatgaError::new(ErrorCode::Conflict, "event stream version conflict")
    } else {
        CatgaError::new(ErrorCode::Transient, message)
    }
}
