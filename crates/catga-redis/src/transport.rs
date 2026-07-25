use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Delivery, Destination,
    DestinationTransport, Envelope, EnvelopeCodec, ErrorCode, HealthCheckable, MessageTransport,
    OperationTracker, Stoppable, Waitable, telemetry,
};
use dashmap::{DashMap, DashSet};
use redis::{
    AsyncCommands, AsyncConnectionConfig,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    streams::{StreamId, StreamPendingCountReply, StreamReadOptions, StreamReadReply},
};
use tokio_util::sync::CancellationToken;

use crate::{RedisConfig, acknowledgement::RedisAcknowledger};

/// Redis Streams-backed at-least-once transport with explicit acknowledgement.
pub struct RedisTransport {
    client: redis::Client,
    commands: ConnectionManager,
    stream: Box<str>,
    group: Box<str>,
    consumer: Box<str>,
    codec: PostcardCodec,
    in_flight: Arc<InFlight>,
    operations: OperationTracker,
    acceptance: AcceptanceGate,
}

impl RedisTransport {
    /// Connects and idempotently provisions the configured stream and consumer group.
    pub async fn connect(config: RedisConfig) -> CatgaResult<Self> {
        let client = redis::Client::open(config.server.as_ref()).map_err(map_error)?;
        let manager_config = ConnectionManagerConfig::new().set_response_timeout(None);
        let mut commands = client
            .get_connection_manager_with_config(manager_config)
            .await
            .map_err(map_error)?;

        match commands
            .xgroup_create_mkstream(config.stream.as_ref(), config.group.as_ref(), "0")
            .await
        {
            Ok(()) => {}
            Err(error) if error.code() == Some("BUSYGROUP") => {}
            Err(error) => return Err(map_error(error)),
        }

        Ok(Self {
            client,
            commands,
            stream: config.stream,
            group: config.group,
            consumer: config.consumer,
            codec: PostcardCodec,
            in_flight: Arc::new(InFlight::new()),
            operations: OperationTracker::default(),
            acceptance: AcceptanceGate::default(),
        })
    }

    async fn blocking_connection(&self) -> CatgaResult<MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new().set_response_timeout(None),
            )
            .await
            .map_err(map_error)
    }

    async fn ensure_consumer_group(&self, stream: &str) -> CatgaResult<()> {
        let mut commands = self.commands.clone();
        match commands
            .xgroup_create_mkstream(stream, self.group.as_ref(), "0")
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if error.code() == Some("BUSYGROUP") => Ok(()),
            Err(error) => Err(map_error(error)),
        }
    }

    /// Returns the broker-maintained count for one recovered pending entry.
    ///
    /// The exact entry-id range and count of one keep the query bounded, independent of the
    /// consumer group's pending backlog. A missing entry means the broker state changed between
    /// recovery and inspection, so returning a transient error leaves the unacknowledged work
    /// available for a later safe retry instead of guessing a retry limit.
    async fn pending_attempts(&self, stream: &str, entry_id: &str) -> CatgaResult<u32> {
        let mut connection = self.commands.clone();
        let pending: StreamPendingCountReply = connection
            .xpending_count(stream, self.group.as_ref(), entry_id, entry_id, 1)
            .await
            .map_err(map_error)?;
        let entry = pending
            .ids
            .into_iter()
            .find(|pending| pending.id == entry_id)
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Transient,
                    "Redis pending entry disappeared before its delivery count was read",
                )
            })?;
        Ok(match u32::try_from(entry.times_delivered) {
            Ok(attempts) => attempts.max(1),
            Err(_) => u32::MAX,
        })
    }

    async fn receive_stream(&self, stream: Box<str>) -> CatgaResult<Delivery> {
        let _receiving = self.in_flight.begin_receive(stream.as_ref());
        let mut connection = self.blocking_connection().await?;
        let pending = if let Some(_recovery) = self.in_flight.try_start_recovery(stream.as_ref()) {
            read_entry(
                &mut connection,
                stream.as_ref(),
                self.group.as_ref(),
                self.consumer.as_ref(),
                "0",
                false,
            )
            .await?
        } else {
            None
        };
        let (entry, attempts) = match pending {
            Some(entry) => {
                let attempts = self
                    .pending_attempts(stream.as_ref(), entry.id.as_str())
                    .await?;
                (entry, attempts)
            }
            None => (
                read_entry(
                    &mut connection,
                    stream.as_ref(),
                    self.group.as_ref(),
                    self.consumer.as_ref(),
                    ">",
                    true,
                )
                .await?
                .ok_or_else(|| {
                    CatgaError::new(
                        ErrorCode::Transient,
                        "Redis stream read returned no entries",
                    )
                })?,
                1,
            ),
        };
        let payload = entry.get::<Vec<u8>>("payload").ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "Redis stream entry is missing payload")
        })?;
        let envelope = self.codec.decode(&payload)?;
        self.in_flight.insert(stream.as_ref(), entry.id.as_str());

        Ok(Delivery::with_acknowledger(
            envelope,
            Box::new(RedisAcknowledger {
                connection: self.commands.clone(),
                stream,
                group: self.group.clone(),
                entry_id: entry.id.into_boxed_str(),
                in_flight: Arc::clone(&self.in_flight),
                _operation: self.operations.begin_operation(),
            }),
        )
        .with_attempts(attempts))
    }
}

#[async_trait]
impl MessageTransport for RedisTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("redis", "stream", async {
            self.acceptance.ensure_accepting()?;
            let payload = self.codec.encode(&envelope)?;
            let mut connection = self.commands.clone();
            let _: Option<String> = connection
                .xadd(self.stream.as_ref(), "*", &[("payload", payload)])
                .await
                .map_err(map_error)?;
            Ok(())
        })
        .await
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("redis", "stream", async {
            self.receive_stream(self.stream.clone()).await
        })
        .await
    }
}

#[async_trait]
impl DestinationTransport for RedisTransport {
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("redis", "destination_stream", async {
            self.acceptance.ensure_accepting()?;
            let payload = self.codec.encode(&envelope)?;
            let stream = destination_stream(destination);
            let mut connection = self.commands.clone();
            let _: Option<String> = connection
                .xadd(stream.as_ref(), "*", &[("payload", payload)])
                .await
                .map_err(map_error)?;
            Ok(())
        })
        .await
    }

    async fn receive_from(&self, destination: &Destination) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("redis", "destination_stream", async {
            let stream = destination_stream(destination);
            self.ensure_consumer_group(stream.as_ref()).await?;
            self.receive_stream(stream).await
        })
        .await
    }
}

fn destination_stream(destination: &Destination) -> Box<str> {
    format!("stream:{destination}").into_boxed_str()
}

impl Stoppable for RedisTransport {
    fn stop_accepting(&self) {
        self.acceptance.stop_accepting();
    }

    fn is_accepting(&self) -> bool {
        self.acceptance.is_accepting()
    }
}

#[async_trait]
impl AsyncInitializable for RedisTransport {
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl HealthCheckable for RedisTransport {
    fn is_healthy(&self) -> bool {
        true
    }

    fn health_status(&self) -> Option<&str> {
        Some("Redis transport is ready")
    }
}

#[async_trait]
impl Waitable for RedisTransport {
    async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        self.operations.wait_for_completion(cancellation).await
    }

    fn pending_operations(&self) -> usize {
        self.operations.pending_operations()
    }
}

async fn read_entry(
    connection: &mut MultiplexedConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    entry_id: &str,
    block: bool,
) -> CatgaResult<Option<StreamId>> {
    let options = StreamReadOptions::default().group(group, consumer).count(1);
    let options = if block { options.block(0) } else { options };
    let reply: Option<StreamReadReply> = connection
        .xread_options::<_, _, Option<StreamReadReply>>(&[stream], &[entry_id], &options)
        .await
        .map_err(map_error)?;
    Ok(reply.and_then(|reply| {
        reply
            .keys
            .into_iter()
            .next()
            .and_then(|stream| stream.ids.into_iter().next())
    }))
}

pub(crate) struct InFlight {
    streams: DashMap<Box<str>, Arc<InFlightStream>>,
}

struct InFlightStream {
    entries: DashSet<Box<str>>,
    active_receivers: AtomicUsize,
    recovery_gate: AtomicBool,
}

impl InFlight {
    fn new() -> Self {
        Self {
            streams: DashMap::new(),
        }
    }

    fn stream(&self, stream: &str) -> Arc<InFlightStream> {
        self.streams
            .entry(stream.into())
            .or_insert_with(|| {
                Arc::new(InFlightStream {
                    entries: DashSet::new(),
                    active_receivers: AtomicUsize::new(0),
                    recovery_gate: AtomicBool::new(false),
                })
            })
            .clone()
    }

    fn begin_receive(&self, stream: &str) -> ReceiveGuard {
        let in_flight = self.stream(stream);
        in_flight.active_receivers.fetch_add(1, Ordering::SeqCst);
        ReceiveGuard { in_flight }
    }

    fn try_start_recovery(&self, stream: &str) -> Option<RecoveryGuard> {
        let in_flight = self.stream(stream);
        if in_flight.active_receivers.load(Ordering::SeqCst) != 1
            || in_flight
                .recovery_gate
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return None;
        }
        let can_recover =
            in_flight.active_receivers.load(Ordering::SeqCst) == 1 && in_flight.entries.is_empty();
        if can_recover {
            Some(RecoveryGuard { in_flight })
        } else {
            in_flight.recovery_gate.store(false, Ordering::SeqCst);
            None
        }
    }

    fn insert(&self, stream: &str, entry_id: &str) {
        self.stream(stream).entries.insert(entry_id.into());
    }

    pub(crate) fn release(&self, stream: &str, entry_id: &str) {
        if let Some(in_flight) = self.streams.get(stream) {
            in_flight.entries.remove(entry_id);
        }
    }
}

struct ReceiveGuard {
    in_flight: Arc<InFlightStream>,
}

impl Drop for ReceiveGuard {
    fn drop(&mut self) {
        self.in_flight
            .active_receivers
            .fetch_sub(1, Ordering::SeqCst);
    }
}

struct RecoveryGuard {
    in_flight: Arc<InFlightStream>,
}

impl Drop for RecoveryGuard {
    fn drop(&mut self) {
        self.in_flight.recovery_gate.store(false, Ordering::SeqCst);
    }
}

pub(crate) fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
