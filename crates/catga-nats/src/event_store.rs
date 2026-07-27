//! JetStream-backed optimistic event persistence.

use std::{
    collections::BinaryHeap,
    future::Future,
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
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, EventPage, EventStore,
    EventStream, MAX_EVENT_STORE_PAGE_SIZE, StoredEvent, StreamIdsPage, VersionHistoryPage,
    VersionInfo, telemetry, validate_event_store_page_size,
};
use futures::{
    Stream as FuturesStream, StreamExt, TryStream, TryStreamExt, stream as futures_stream,
};
use serde::Serialize;

const VERSION: &str = "Catga-Version";
const TIMESTAMP: &str = "Catga-Timestamp";
const MAX_EVENT_STORE_HISTORY_SCAN: usize = MAX_EVENT_STORE_PAGE_SIZE;

/// JetStream-backed event store with optimistic, subject-sequence writes.
///
/// The store places each event stream below a caller-selected NATS subject prefix and uses
/// JetStream's expected-last-subject-sequence precondition to reject stale appends. It also keeps
/// an internal one-history KV bucket named `<stream_name>_IDS` to enumerate stream identifiers.
///
/// Construct the store with [`Self::connect`]. Connection provisioning reuses compatible
/// pre-existing JetStream resources, including when another connector creates the identifier
/// bucket concurrently.
pub struct NatsEventStore {
    client: async_nats::Client,
    context: jetstream::Context,
    stream: Stream,
    ids: kv::Store,
    stream_name: Box<str>,
    subject_prefix: Box<str>,
    codec: MemoryPackCodec,
}

impl NatsEventStore {
    /// Connects to NATS and provisions one direct-read event-store namespace.
    ///
    /// `server` is the NATS server URL. `stream_name` names the JetStream stream and the
    /// associated `<stream_name>_IDS` identifier bucket. `subject_prefix` is the literal NATS
    /// subject prefix under which this store writes event streams.
    ///
    /// An existing stream must cover `subject_prefix`; when necessary, this method enables direct
    /// reads on it. The identifier bucket is created with one revision of history. If another
    /// connector creates that bucket after this connection's initial lookup, this method reopens
    /// the bucket and completes successfully.
    ///
    /// Returns [`CatgaError`] with [`ErrorCode::Validation`] for an invalid subject prefix or an
    /// incompatible existing stream, and maps NATS and JetStream failures to transient errors.
    pub async fn connect(
        server: &str,
        stream_name: impl Into<Box<str>>,
        subject_prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let stream_name = stream_name.into();
        let subject_prefix = subject_prefix.into();
        validate_subject_prefix(&subject_prefix)?;
        let client = async_nats::connect(server).await.map_err(map_error)?;
        let context = jetstream::new(client.clone());
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
        let ids = get_or_create_with_reopen(
            || context.get_key_value(&bucket),
            || {
                context.create_key_value(kv::Config {
                    bucket: bucket.to_string(),
                    history: 1,
                    ..Default::default()
                })
            },
        )
        .await
        .map_err(map_error)?;
        Ok(Self {
            client,
            context,
            stream,
            ids,
            stream_name,
            subject_prefix,
            codec: MemoryPackCodec::default(),
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
        let events = self
            .subject_messages(subject)
            .and_then(|message| futures::future::ready(self.decode_message(&message)));
        take_matching_at_most(events, max_count, |event| event.version() >= from_version).await
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
                            self.direct_get_next_for_subject(&subject, Some(sequence))
                                .await?
                        }
                        None => return Ok(None),
                    },
                    None => self.direct_get_next_for_subject(&subject, None).await?,
                };
                match message {
                    Some(message) => {
                        let sequence = message_sequence(&message)?;
                        Ok(Some((message, Some(sequence))))
                    }
                    None => Ok(None),
                }
            }
        })
    }

    async fn direct_get_next_for_subject(
        &self,
        subject: &str,
        sequence: Option<u64>,
    ) -> CatgaResult<Option<Message>> {
        let payload =
            serde_json::to_vec(&DirectGetNextRequest { subject, sequence }).map_err(map_error)?;
        let message = self
            .client
            .request(
                format!("$JS.API.DIRECT.GET.{}", self.stream_name),
                payload.into(),
            )
            .await
            .map_err(map_error)?;
        match (message.status, message.description.as_deref()) {
            (Some(async_nats::StatusCode::NOT_FOUND), Some(_)) => Ok(None),
            (Some(async_nats::StatusCode::TIMEOUT), Some(_)) => {
                Err(CatgaError::new(ErrorCode::Transient, "invalid subject"))
            }
            (Some(status), Some(description)) => Err(CatgaError::new(
                ErrorCode::Transient,
                format!("JetStream direct get failed with {status}: {description}"),
            )),
            _ => Ok(Some(message)),
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

#[derive(Serialize)]
struct DirectGetNextRequest<'a> {
    #[serde(rename = "next_by_subj")]
    subject: &'a str,
    #[serde(rename = "seq", skip_serializing_if = "Option::is_none")]
    sequence: Option<u64>,
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
            let subject = self.subject(stream_id)?;
            let current = self.current(stream_id).await?;
            if events.is_empty() {
                return Ok(current.map_or(-1, |value| value.0));
            }
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

    async fn read_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        validate_event_store_page_size(max_count)?;
        telemetry::record_persistence("nats", "event_store", "read_page", async {
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
        telemetry::record_persistence("nats", "event_store", "version", async {
            Ok(self.current(stream_id).await?.map_or(-1, |value| value.0))
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
        telemetry::record_persistence("nats", "event_store", "read_to_version_page", async {
            let stream_version = self.version(stream_id).await?;
            let events: Vec<_> = self
                .entries_from(stream_id, from_version, max_count)
                .await?
                .into_iter()
                .take_while(|event| event.version() <= to_version)
                .collect();
            let version = events.last().map_or(-1, StoredEvent::version);
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
        telemetry::record_persistence("nats", "event_store", "read_to_time_page", async {
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
        telemetry::record_persistence("nats", "event_store", "version_history_page", async {
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
        telemetry::record_persistence("nats", "event_store", "stream_ids_page", async {
            let keys = self.ids.keys().await.map_err(map_error)?;
            futures::pin_mut!(keys);
            let mut ids = BinaryHeap::with_capacity(max_count);
            let mut has_more = false;
            while let Some(key) = keys.try_next().await.map_err(map_error)? {
                if after.is_some_and(|cursor| key.as_str() <= cursor) {
                    continue;
                }
                if ids.len() < max_count {
                    ids.push(key);
                } else {
                    has_more = true;
                    let largest = ids.peek().map(String::as_str);
                    if largest.is_some_and(|largest| key.as_str() < largest) {
                        let _ = ids.pop();
                        ids.push(key);
                    }
                }
            }
            let mut ids = ids.into_vec();
            ids.sort_unstable();
            let next_stream_id = has_more.then(|| ids.last().cloned()).flatten();
            Ok(StreamIdsPage::new(ids, next_stream_id))
        })
        .await
    }
}

async fn get_or_create_with_reopen<T, GetError, CreateError, Get, Create, GetFuture, CreateFuture>(
    mut get: Get,
    create: Create,
) -> Result<T, GetError>
where
    Get: FnMut() -> GetFuture,
    Create: FnOnce() -> CreateFuture,
    GetFuture: Future<Output = Result<T, GetError>>,
    CreateFuture: Future<Output = Result<T, CreateError>>,
{
    match get().await {
        Ok(store) => Ok(store),
        Err(_) => match create().await {
            Ok(store) => Ok(store),
            Err(_) => get().await,
        },
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
async fn take_matching_at_most<T, S, F>(
    stream: S,
    max_count: usize,
    mut matches: F,
) -> CatgaResult<Vec<T>>
where
    S: FuturesStream<Item = CatgaResult<T>>,
    F: FnMut(&T) -> bool,
{
    futures::pin_mut!(stream);
    let mut values = Vec::with_capacity(max_count);
    for _ in 0..MAX_EVENT_STORE_HISTORY_SCAN {
        let Some(value) = stream.next().await else {
            return Ok(values);
        };
        let value = value?;
        if matches(&value) {
            values.push(value);
            if values.len() == max_count {
                return Ok(values);
            }
        }
    }
    Err(CatgaError::new(
        ErrorCode::Unavailable,
        "NATS event history scan limit reached before filling page",
    ))
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
