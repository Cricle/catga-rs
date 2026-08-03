//! Contract coverage for Redis Streams envelope codec injection.

use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, Destination, DestinationTransport, Envelope, EnvelopeCodec, ErrorCode,
    MessageMetadata, MessageTransport,
};
use catga_redis::{RedisConfig, RedisPendingReclaimOptions, RedisTransport};

const FRAME_PREFIX: &[u8] = b"catga-redis-test-codec-v1\0";

/// A deliberately non-default envelope frame used to prove transport codec injection.
///
/// The prefix makes frames incompatible with `MemoryPackCodec` at the transport boundary while
/// reusing its envelope representation so the test focuses solely on transport delegation.
#[derive(Clone, Default)]
struct TaggedCodec {
    encoded: Arc<AtomicUsize>,
    decoded: Arc<AtomicUsize>,
}

impl EnvelopeCodec for TaggedCodec {
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>> {
        self.encoded.fetch_add(1, Ordering::Relaxed);
        let encoded = MemoryPackCodec::default().encode(envelope)?;
        let mut frame = Vec::with_capacity(FRAME_PREFIX.len() + encoded.len());
        frame.extend_from_slice(FRAME_PREFIX);
        frame.extend_from_slice(&encoded);
        Ok(frame)
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope> {
        self.decoded.fetch_add(1, Ordering::Relaxed);
        let payload = bytes.strip_prefix(FRAME_PREFIX).ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "test codec frame prefix is missing")
        })?;
        MemoryPackCodec::default().decode(payload)
    }
}

fn config(url: &str, suffix: &str) -> RedisConfig {
    RedisConfig {
        server: url.into(),
        stream: format!("catga-codec-injection-stream-{suffix}").into(),
        group: format!("catga-codec-injection-group-{suffix}").into(),
        consumer: format!("catga-codec-injection-consumer-{suffix}").into(),
    }
}

#[test]
fn codec_aware_constructors_are_public() {
    let client = redis::Client::open("redis://127.0.0.1:1")
        .expect("test Redis URL must be syntactically valid");
    let config = config("redis://unused.invalid", "constructors");
    let reclaim_options = RedisPendingReclaimOptions::default();

    std::mem::drop(RedisTransport::connect_with_codec(
        config.clone(),
        TaggedCodec::default(),
    ));
    std::mem::drop(RedisTransport::connect_with_reclaim_options_with_codec(
        config.clone(),
        reclaim_options.clone(),
        TaggedCodec::default(),
    ));
    std::mem::drop(RedisTransport::from_client_with_codec(
        client.clone(),
        config.clone(),
        TaggedCodec::default(),
    ));
    std::mem::drop(RedisTransport::connect_with_client_with_codec(
        client,
        config,
        reclaim_options,
        TaggedCodec::default(),
    ));
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn injected_codec_frames_primary_and_destination_redis_streams() -> CatgaResult<()> {
    let url = env::var("CATGA_REDIS_URL").map_err(|_| {
        CatgaError::new(
            ErrorCode::Unavailable,
            "CATGA_REDIS_URL must be set for the Redis codec injection contract",
        )
    })?;
    let codec = TaggedCodec::default();
    let transport = RedisTransport::connect_with_codec(
        config(&url, &uuid::Uuid::new_v4().to_string()),
        codec.clone(),
    )
    .await?;
    let envelope = Envelope::new(
        42,
        "catga.codec.injection",
        vec![1, 2, 3],
        MessageMetadata::new(42, None),
    );

    transport.publish(envelope.clone()).await?;
    let delivery = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "Redis stream delivery timed out"))??;

    assert_eq!(delivery.envelope(), &envelope);
    transport.ack(delivery).await?;

    let destination =
        Destination::parse(format!("catga-codec-injection:{}", uuid::Uuid::new_v4()))?;
    let directed = Envelope::new(
        43,
        "catga.codec.injection.destination",
        vec![4, 5, 6],
        MessageMetadata::new(43, None),
    );
    transport.send_to(&destination, directed.clone()).await?;
    let delivery =
        tokio::time::timeout(Duration::from_secs(1), transport.receive_from(&destination))
            .await
            .map_err(|_| {
                CatgaError::new(ErrorCode::Timeout, "Redis destination delivery timed out")
            })??;
    assert_eq!(delivery.envelope(), &directed);
    transport.ack(delivery).await?;

    assert_eq!(codec.encoded.load(Ordering::Relaxed), 2);
    assert_eq!(codec.decoded.load(Ordering::Relaxed), 2);
    Ok(())
}
