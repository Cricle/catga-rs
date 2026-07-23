use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    CatgaError, CatgaResult, Delivery, Envelope, EnvelopeCodec, ErrorCode, MessageTransport,
};
use redis::{
    AsyncCommands, AsyncConnectionConfig,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    streams::{StreamId, StreamReadOptions, StreamReadReply},
};

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
}

#[async_trait]
impl MessageTransport for RedisTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let payload = self.codec.encode(&envelope)?;
        let mut connection = self.commands.clone();
        let _: Option<String> = connection
            .xadd(self.stream.as_ref(), "*", &[("payload", payload)])
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        let _receiving = self.in_flight.begin_receive();
        let mut connection = self.blocking_connection().await?;
        let pending = if let Some(_recovery) = self.in_flight.try_start_recovery() {
            read_entry(
                &mut connection,
                self.stream.as_ref(),
                self.group.as_ref(),
                self.consumer.as_ref(),
                "0",
                false,
            )
            .await?
        } else {
            None
        };
        let entry = match pending {
            Some(entry) => entry,
            None => read_entry(
                &mut connection,
                self.stream.as_ref(),
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
        };
        let payload = entry.get::<Vec<u8>>("payload").ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "Redis stream entry is missing payload")
        })?;
        let envelope = self.codec.decode(&payload)?;
        self.in_flight.insert(entry.id.as_str());

        Ok(Delivery::with_acknowledger(
            envelope,
            Box::new(RedisAcknowledger {
                connection: self.commands.clone(),
                stream: self.stream.clone(),
                group: self.group.clone(),
                entry_id: entry.id.into_boxed_str(),
                in_flight: Arc::clone(&self.in_flight),
            }),
        ))
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
    entries: Mutex<HashSet<Box<str>>>,
    active_receivers: AtomicUsize,
    recovery_gate: AtomicBool,
}

impl InFlight {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashSet::new()),
            active_receivers: AtomicUsize::new(0),
            recovery_gate: AtomicBool::new(false),
        }
    }

    fn begin_receive(&self) -> ReceiveGuard<'_> {
        self.active_receivers.fetch_add(1, Ordering::SeqCst);
        ReceiveGuard { in_flight: self }
    }

    fn try_start_recovery(&self) -> Option<RecoveryGuard<'_>> {
        if self.active_receivers.load(Ordering::SeqCst) != 1
            || self
                .recovery_gate
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return None;
        }
        let can_recover = self.active_receivers.load(Ordering::SeqCst) == 1
            && self
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty();
        if can_recover {
            Some(RecoveryGuard { in_flight: self })
        } else {
            self.recovery_gate.store(false, Ordering::SeqCst);
            None
        }
    }

    fn insert(&self, entry_id: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(entry_id.into());
    }

    pub(crate) fn release(&self, entry_id: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(entry_id);
    }
}

struct ReceiveGuard<'a> {
    in_flight: &'a InFlight,
}

impl Drop for ReceiveGuard<'_> {
    fn drop(&mut self) {
        self.in_flight
            .active_receivers
            .fetch_sub(1, Ordering::SeqCst);
    }
}

struct RecoveryGuard<'a> {
    in_flight: &'a InFlight,
}

impl Drop for RecoveryGuard<'_> {
    fn drop(&mut self) {
        self.in_flight.recovery_gate.store(false, Ordering::SeqCst);
    }
}

pub(crate) fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
