//! JetStream-backed optimistic event persistence.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::{
    Message,
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
    StoredEvent, VersionInfo, telemetry,
};
use futures::{
    Stream as FuturesStream, StreamExt, TryStream, TryStreamExt, stream as futures_stream,
};

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
        let stream_name = stream_name.into();
        let subject_prefix = subject_prefix.into();
        validate_subject_prefix(&subject_prefix)?;
        let client = async_nats::connect(server).await.map_err(map_error)?;
        let context = jetstream::new(client);
        let mut stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.to_string(),
                subjects: vec![format!("{subject_prefix}.>")],
                allow_direct: true,
                ..Default::default()
            })
            .await
            .map_err(map_error)?;
        let mut stream_config = stream.info().await.map_err(map_error)?.config.clone();
        if !stream_subjects_cover_prefix(&stream_config.subjects, &subject_prefix) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "existing JetStream stream subjects do not cover the event subject prefix",
            ));
        }
        if !stream_config.allow_direct {
            stream_config.allow_direct = true;
            context
                .update_stream(&stream_config)
                .await
                .map_err(map_error)?;
        }
        let stream = context.get_stream(&stream_name).await.map_err(map_error)?;
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
        self.entries_from(stream_id, 0, usize::MAX).await
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

    async fn entries_from(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<Vec<StoredEvent>> {
        let from_version = i64::try_from(from_version).unwrap_or(i64::MAX);
        let subject = self.subject(stream_id)?;
        let events = self.subject_messages(subject).try_filter_map(|message| {
            futures::future::ready(
                self.decode_message(&message)
                    .map(|event| (event.version() >= from_version).then_some(event)),
            )
        });
        take_at_most(events, max_count).await
    }

    fn subject_messages(
        &self,
        subject: String,
    ) -> impl TryStream<Ok = Message, Error = CatgaError> + '_ {
        futures_stream::try_unfold(None, move |sequence| {
            let subject = subject.clone();
            async move {
                let message = match sequence {
                    Some(sequence) => match next_direct_sequence(sequence) {
                        Some(sequence) => {
                            self.stream
                                .direct_get_next_for_subject(&subject, Some(sequence))
                                .await
                        }
                        None => return Ok(None),
                    },
                    None => self.stream.direct_get_first_for_subject(&subject).await,
                };
                match message {
                    Ok(message) => {
                        let sequence = message_sequence(&message)?;
                        Ok(Some((message.into(), Some(sequence))))
                    }
                    Err(error) if error.kind() == DirectGetErrorKind::NotFound => Ok(None),
                    Err(error) => Err(map_error(error)),
                }
            }
        })
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
        telemetry::record_persistence("nats", "event_store", "append", async {
            if events.is_empty() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "event batch must not be empty",
                ));
            }
            let subject = self.subject(stream_id)?;
            let current = self.current(stream_id).await?;
            if expected_version
                .is_some_and(|expected| current.map_or(-1, |value| value.0) != expected)
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
        })
        .await
    }

    async fn read(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventStream> {
        telemetry::record_persistence("nats", "event_store", "read", async {
            let version = self.version(stream_id).await?;
            let events = self
                .entries_from(stream_id, from_version, max_count)
                .await?;
            Ok(EventStream::new(stream_id, version, events))
        })
        .await
    }

    async fn version(&self, stream_id: &str) -> CatgaResult<i64> {
        telemetry::record_persistence("nats", "event_store", "version", async {
            Ok(self.current(stream_id).await?.map_or(-1, |value| value.0))
        })
        .await
    }

    async fn read_to_version(&self, stream_id: &str, to_version: i64) -> CatgaResult<EventStream> {
        telemetry::record_persistence("nats", "event_store", "read_to_version", async {
            let events: Vec<_> = self
                .entries(stream_id)
                .await?
                .into_iter()
                .filter(|event| event.version() <= to_version)
                .collect();
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
        telemetry::record_persistence("nats", "event_store", "read_to_time", async {
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
        telemetry::record_persistence("nats", "event_store", "version_history", async {
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
        telemetry::record_persistence("nats", "event_store", "stream_ids", async {
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
        })
        .await
    }
}

fn validate_subject_prefix(subject_prefix: &str) -> CatgaResult<()> {
    if subject_prefix.is_empty()
        || subject_prefix
            .split('.')
            .any(|token| token.is_empty() || token == "*" || token == ">")
    {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "event subject prefix must contain only literal NATS subject tokens",
        ));
    }
    Ok(())
}

fn stream_subjects_cover_prefix(stream_subjects: &[String], subject_prefix: &str) -> bool {
    stream_subjects
        .iter()
        .any(|subject| subject_filter_covers_prefix(subject, subject_prefix))
}

fn subject_filter_covers_prefix(filter: &str, subject_prefix: &str) -> bool {
    let mut filter_tokens: Vec<_> = filter.split('.').collect();
    if filter_tokens.pop() != Some(">") {
        return false;
    }
    let prefix_tokens: Vec<_> = subject_prefix.split('.').collect();
    filter_tokens.len() <= prefix_tokens.len()
        && filter_tokens
            .iter()
            .zip(prefix_tokens)
            .all(|(filter_token, prefix_token)| {
                *filter_token == "*" || *filter_token == prefix_token
            })
}

fn next_direct_sequence(sequence: u64) -> Option<u64> {
    sequence.checked_add(1)
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
    message_header(message, async_nats::header::NATS_SEQUENCE)?
        .parse()
        .map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "JetStream event has an invalid sequence",
            )
        })
}
async fn take_at_most<T, E, S>(stream: S, max_count: usize) -> Result<Vec<T>, E>
where
    S: FuturesStream<Item = Result<T, E>>,
{
    futures::pin_mut!(stream);
    let mut values = Vec::new();
    for _ in 0..max_count {
        let Some(value) = stream.next().await else {
            break;
        };
        values.push(value?);
    }
    Ok(values)
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use futures::{StreamExt, stream};

    use super::{
        next_direct_sequence, stream_subjects_cover_prefix, take_at_most, validate_subject_prefix,
    };

    #[test]
    fn direct_next_sequence_is_strictly_after_the_current_sequence() {
        assert_eq!(next_direct_sequence(41), Some(42));
        assert_eq!(next_direct_sequence(u64::MAX), None);
    }

    #[test]
    fn subject_prefix_requires_literal_subject_tokens() {
        assert!(validate_subject_prefix("catga.events").is_ok());
        assert!(validate_subject_prefix("").is_err());
        assert!(validate_subject_prefix("catga.*").is_err());
        assert!(validate_subject_prefix("catga.>").is_err());
        assert!(validate_subject_prefix("catga..events").is_err());
    }

    #[test]
    fn stream_subjects_must_cover_every_stream_id_below_the_prefix() {
        assert!(stream_subjects_cover_prefix(
            &["catga.events.>".to_string()],
            "catga.events"
        ));
        assert!(stream_subjects_cover_prefix(
            &[">".to_string()],
            "catga.events"
        ));
        assert!(!stream_subjects_cover_prefix(
            &["catga.other.>".to_string()],
            "catga.events"
        ));
        assert!(!stream_subjects_cover_prefix(
            &["catga.events.*".to_string()],
            "catga.events"
        ));
    }

    #[tokio::test]
    async fn bounded_collector_does_not_poll_past_limit() {
        let polled = Arc::new(AtomicUsize::new(0));
        let source = stream::iter(0..8).map({
            let polled = Arc::clone(&polled);
            move |value| {
                polled.fetch_add(1, Ordering::Relaxed);
                Ok::<_, ()>(value)
            }
        });

        let values = take_at_most(source, 2)
            .await
            .expect("integer source does not fail");

        assert_eq!(values, [0, 1]);
        assert_eq!(polled.load(Ordering::Relaxed), 2);
    }
}
