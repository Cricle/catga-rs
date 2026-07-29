use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Delivery, Destination,
    DestinationTransport, Envelope, EnvelopeCodec, ErrorCode, HealthCheckable, MessageTransport,
    OperationTracker, Stoppable, Waitable, telemetry,
};
use dashmap::{DashMap, DashSet};
use redis::{
    AsyncCommands, AsyncConnectionConfig,
    aio::{ConnectionManager, MultiplexedConnection},
    streams::{
        StreamClaimReply, StreamId, StreamPendingCountReply, StreamReadOptions, StreamReadReply,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{RedisConfig, RedisPendingReclaimOptions, acknowledgement::RedisAcknowledger};

const RECLAIM_POLL_MILLIS: usize = 1_000;

/// Redis Streams-backed at-least-once transport with explicit acknowledgement.
///
/// `C` controls the envelope wire format. It defaults to [`MemoryPackCodec`] so existing
/// construction calls keep Catga's standard bounded MemoryPack framing. Use a `*_with_codec`
/// constructor to explicitly select another [`EnvelopeCodec`] for a stream.
pub struct RedisTransport<C = MemoryPackCodec>
where
    C: EnvelopeCodec,
{
    client: redis::Client,
    commands: ConnectionManager,
    stream: Box<str>,
    group: Box<str>,
    consumer: Box<str>,
    reclaim_options: RedisPendingReclaimOptions,
    codec: C,
    in_flight: Arc<InFlight>,
    operations: OperationTracker,
    acceptance: AcceptanceGate,
}

impl RedisTransport<MemoryPackCodec> {
    /// Connects with the default bounded cross-consumer pending-delivery recovery policy.
    pub async fn connect(config: RedisConfig) -> CatgaResult<Self> {
        Self::connect_with_codec(config, MemoryPackCodec::default()).await
    }

    /// Connects and idempotently provisions the configured stream and consumer group.
    ///
    /// `reclaim_options` controls how an idle delivery left by another consumer can be moved to
    /// this consumer. Recovery is bounded to one claimed entry per Redis command and never
    /// materializes the group pending list in memory.
    pub async fn connect_with_reclaim_options(
        config: RedisConfig,
        reclaim_options: RedisPendingReclaimOptions,
    ) -> CatgaResult<Self> {
        Self::connect_with_reclaim_options_with_codec(
            config,
            reclaim_options,
            MemoryPackCodec::default(),
        )
        .await
    }

    /// Builds a transport from an application-owned Redis client.
    ///
    /// This preserves the client's configured TLS, authentication, reconnection, and
    /// observability behavior while provisioning the stream and consumer group in `config`.
    /// `config.server` is not opened by this constructor; it remains part of [`RedisConfig`] for
    /// compatibility with [`Self::connect`]. The transport uses the default bounded
    /// cross-consumer pending-delivery recovery policy.
    pub async fn from_client(client: redis::Client, config: RedisConfig) -> CatgaResult<Self> {
        Self::from_client_with_codec(client, config, MemoryPackCodec::default()).await
    }

    /// Builds a transport from an application-owned Redis client with an explicit recovery policy.
    ///
    /// Like [`Self::from_client`], this reuses the supplied client's TLS, authentication,
    /// reconnection, and observability configuration instead of opening `config.server`. It
    /// still idempotently provisions `config.stream` and `config.group`, and applies
    /// `reclaim_options` to bounded recovery of deliveries abandoned by other consumers.
    pub async fn connect_with_client(
        client: redis::Client,
        config: RedisConfig,
        reclaim_options: RedisPendingReclaimOptions,
    ) -> CatgaResult<Self> {
        Self::connect_with_client_with_codec(
            client,
            config,
            reclaim_options,
            MemoryPackCodec::default(),
        )
        .await
    }
}

impl<C> RedisTransport<C>
where
    C: EnvelopeCodec,
{
    /// Connects with the supplied envelope codec and the default bounded pending-delivery
    /// recovery policy.
    ///
    /// All peers that publish to or receive from this stream must use the same codec contract.
    /// The codec is retained by value, so no global serializer registry or dynamic dispatch is
    /// required on the hot path.
    pub async fn connect_with_codec(config: RedisConfig, codec: C) -> CatgaResult<Self> {
        let client = redis::Client::open(config.server.as_ref()).map_err(map_error)?;
        Self::from_client_with_codec(client, config, codec).await
    }

    /// Connects with an explicit bounded recovery policy and supplied envelope codec.
    ///
    /// `reclaim_options` only controls Redis Stream recovery; it does not alter the selected
    /// codec or its frame validation policy.
    pub async fn connect_with_reclaim_options_with_codec(
        config: RedisConfig,
        reclaim_options: RedisPendingReclaimOptions,
        codec: C,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(config.server.as_ref()).map_err(map_error)?;
        Self::connect_with_client_with_codec(client, config, reclaim_options, codec).await
    }

    /// Builds a transport from an application-owned Redis client and supplied envelope codec.
    ///
    /// This preserves the client's configured TLS, authentication, reconnection, and
    /// observability behavior while provisioning the stream and consumer group in `config`.
    /// The default bounded cross-consumer pending-delivery recovery policy is applied.
    pub async fn from_client_with_codec(
        client: redis::Client,
        config: RedisConfig,
        codec: C,
    ) -> CatgaResult<Self> {
        Self::connect_with_client_with_codec(
            client,
            config,
            RedisPendingReclaimOptions::default(),
            codec,
        )
        .await
    }

    /// Builds a transport from an application-owned Redis client, explicit recovery policy, and
    /// supplied envelope codec.
    ///
    /// Like [`Self::from_client_with_codec`], this reuses the supplied client's TLS,
    /// authentication, reconnection, and observability configuration instead of opening
    /// `config.server`. It idempotently provisions `config.stream` and `config.group` before
    /// retaining `codec` for every envelope encode and decode operation.
    pub async fn connect_with_client_with_codec(
        client: redis::Client,
        config: RedisConfig,
        reclaim_options: RedisPendingReclaimOptions,
        codec: C,
    ) -> CatgaResult<Self> {
        Self::initialize(client, config, reclaim_options, codec).await
    }

    async fn initialize(
        client: redis::Client,
        config: RedisConfig,
        reclaim_options: RedisPendingReclaimOptions,
        codec: C,
    ) -> CatgaResult<Self> {
        let manager_config = crate::config::command_connection_manager_config();
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
            reclaim_options,
            codec,
            in_flight: Arc::new(InFlight::new()),
            operations: OperationTracker::default(),
            acceptance: AcceptanceGate::default(),
        })
    }

    /// Opens a dedicated connection without response timeout for one blocking `XREAD`.
    ///
    /// Each concurrent receiver owns its connection because Redis serializes commands per
    /// connection: a pending `XREAD BLOCK` would otherwise hold up every other receiver sharing
    /// the same multiplexed connection. The connection is established per receive so abandoned
    /// or cancelled blocking reads never leave a reused connection in an inconsistent state.
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

    async fn reclaim_idle_entry(
        &self,
        connection: &mut MultiplexedConnection,
        stream: &str,
    ) -> CatgaResult<Option<StreamId>> {
        for _ in 0..self.reclaim_options.max_scans() {
            let cursor = self.in_flight.reclaim_cursor(stream);
            let pending: StreamPendingCountReply = connection
                .xpending_count(stream, self.group.as_ref(), cursor.as_ref(), "+", 1)
                .await
                .map_err(map_error)?;
            let Some(pending) = pending.ids.into_iter().next() else {
                self.in_flight.set_reclaim_cursor(stream, "-".into());
                return Ok(None);
            };
            self.in_flight
                .set_reclaim_cursor(stream, format!("({}", pending.id).into());
            let idle_millis = u64::try_from(pending.last_delivered_ms)
                .map_or(u64::MAX, |idle_millis| idle_millis);
            if pending.consumer == self.consumer.as_ref()
                || idle_millis < self.reclaim_options.minimum_idle_millis()
            {
                continue;
            }
            let claimed: StreamClaimReply = connection
                .xclaim(
                    stream,
                    self.group.as_ref(),
                    self.consumer.as_ref(),
                    self.reclaim_options.minimum_idle_millis(),
                    &[pending.id.as_str()],
                )
                .await
                .map_err(map_error)?;
            if let Some(entry) = claimed.ids.into_iter().next() {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn receive_stream(&self, stream: Box<str>) -> CatgaResult<Delivery> {
        let _receiving = self.in_flight.begin_receive(stream.as_ref());
        let mut connection = self.blocking_connection().await?;
        let (entry, attempts) = loop {
            let pending =
                if let Some(_recovery) = self.in_flight.try_start_recovery(stream.as_ref()) {
                    let owned_pending = if self.in_flight.can_read_owned_pending(stream.as_ref()) {
                        read_entry(
                            &mut connection,
                            stream.as_ref(),
                            self.group.as_ref(),
                            self.consumer.as_ref(),
                            "0",
                            None,
                        )
                        .await?
                    } else {
                        None
                    };
                    match owned_pending {
                        Some(entry) => Some(entry),
                        None => {
                            self.reclaim_idle_entry(&mut connection, stream.as_ref())
                                .await?
                        }
                    }
                } else {
                    None
                };
            if let Some(entry) = pending {
                let attempts = self
                    .pending_attempts(stream.as_ref(), entry.id.as_str())
                    .await?;
                break (entry, attempts);
            }
            if let Some(entry) = read_entry(
                &mut connection,
                stream.as_ref(),
                self.group.as_ref(),
                self.consumer.as_ref(),
                ">",
                Some(RECLAIM_POLL_MILLIS),
            )
            .await?
            {
                break (entry, 1);
            }
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
                consumer: self.consumer.clone(),
                entry_id: entry.id.into_boxed_str(),
                in_flight: Arc::clone(&self.in_flight),
                _operation: self.operations.begin_operation(),
            }),
        )
        .with_attempts(attempts))
    }
}

#[async_trait]
impl<C> MessageTransport for RedisTransport<C>
where
    C: EnvelopeCodec,
{
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
impl<C> DestinationTransport for RedisTransport<C>
where
    C: EnvelopeCodec,
{
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

impl<C> Stoppable for RedisTransport<C>
where
    C: EnvelopeCodec,
{
    fn stop_accepting(&self) {
        self.acceptance.stop_accepting();
    }

    fn is_accepting(&self) -> bool {
        self.acceptance.is_accepting()
    }
}

#[async_trait]
impl<C> AsyncInitializable for RedisTransport<C>
where
    C: EnvelopeCodec,
{
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl<C> HealthCheckable for RedisTransport<C>
where
    C: EnvelopeCodec,
{
    fn is_healthy(&self) -> bool {
        true
    }

    fn health_status(&self) -> Option<&str> {
        Some("Redis transport is ready")
    }
}

#[async_trait]
impl<C> Waitable for RedisTransport<C>
where
    C: EnvelopeCodec,
{
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
    block_millis: Option<usize>,
) -> CatgaResult<Option<StreamId>> {
    let options = StreamReadOptions::default().group(group, consumer).count(1);
    let options = if let Some(block_millis) = block_millis {
        options.block(block_millis)
    } else {
        options
    };
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
    reclaim_cursors: DashMap<Box<str>, Box<str>>,
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
            reclaim_cursors: DashMap::new(),
        }
    }

    fn stream(&self, stream: &str) -> Arc<InFlightStream> {
        if let Some(existing) = self.streams.get(stream) {
            return existing.clone();
        }
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
        if in_flight
            .recovery_gate
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        Some(RecoveryGuard { in_flight })
    }

    fn insert(&self, stream: &str, entry_id: &str) {
        self.stream(stream).entries.insert(entry_id.into());
    }

    fn can_read_owned_pending(&self, stream: &str) -> bool {
        self.streams
            .get(stream)
            .is_none_or(|in_flight| in_flight.entries.is_empty())
    }

    fn reclaim_cursor(&self, stream: &str) -> Box<str> {
        self.reclaim_cursors
            .get(stream)
            .map(|cursor| cursor.value().clone())
            .unwrap_or_else(|| "-".into())
    }

    fn set_reclaim_cursor(&self, stream: &str, cursor: Box<str>) {
        self.reclaim_cursors.insert(stream.into(), cursor);
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
