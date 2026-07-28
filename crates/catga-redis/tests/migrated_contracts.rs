#![allow(missing_docs)]

//! Service-gated public-contract coverage migrated from crate-local tests.

use std::time::Duration;

use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DeadLetterStore, Envelope, EnvelopeCodec, ErrorCode, MessageMetadata,
};
use catga_flow::{FlowState, FlowStore};
use catga_redis::{RedisDeadLetters, RedisFlows, RedisIdempotency};
use redis::AsyncCommands;

#[path = "support/service_url.rs"]
mod service_url;

fn map_redis_error(error: redis::RedisError) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

#[tokio::test]
async fn redis_dead_letters_decode_legacy_hash_records() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = format!("catga-test-dead-letter-{}", uuid::Uuid::new_v4());
    let store = RedisDeadLetters::connect(&url, prefix.clone()).await?;
    let codec = MemoryPackCodec::default();
    let envelope = Envelope::new(
        4,
        "tests.dead-letter",
        vec![1],
        MessageMetadata::new(4, None),
    );
    let client = redis::Client::open(url).map_err(map_redis_error)?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(map_redis_error)?;
    let fields = [
        ("payload", codec.encode(&envelope)?),
        ("reason", b"old failure".to_vec()),
        ("attempts", b"2".to_vec()),
    ];
    let _: () = connection
        .hset_multiple(format!("{prefix}:details:1"), &fields)
        .await
        .map_err(map_redis_error)?;
    let _: usize = connection
        .rpush(format!("{prefix}:queue"), 1_u64)
        .await
        .map_err(map_redis_error)?;

    let letters = store.list(1).await?;
    let letter = letters
        .into_iter()
        .next()
        .expect("legacy Redis hash record must be returned");
    assert_eq!(letter.diagnostics().error_code(), ErrorCode::Internal);
    assert_eq!(letter.diagnostics().stage(), "legacy");
    Ok(())
}

#[tokio::test]
async fn redis_idempotency_accepts_submillisecond_and_maximum_retention() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    RedisIdempotency::with_retention(
        &url,
        format!("catga-test-idempotency-{}", uuid::Uuid::new_v4()),
        Duration::from_nanos(1),
    )
    .await?;
    RedisIdempotency::with_retention(
        &url,
        format!("catga-test-idempotency-{}", uuid::Uuid::new_v4()),
        Duration::from_millis(100 * 365 * 24 * 60 * 60 * 1_000),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn redis_idempotency_rejects_retention_above_its_maximum_before_connecting() {
    let result = RedisIdempotency::with_retention(
        "redis://127.0.0.1:1",
        "catga-test-idempotency",
        Duration::from_millis(100 * 365 * 24 * 60 * 60 * 1_000 + 1),
    )
    .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
}

#[tokio::test]
async fn redis_flows_use_stable_hashed_record_and_type_index_keys() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = format!("catga-test-flow-{}", uuid::Uuid::new_v4());
    let store = RedisFlows::connect(&url, prefix.clone()).await?;
    let state = FlowState::new("payment-42", "payment", [], "node-a");
    assert!(store.create(state).await?);

    let client = redis::Client::open(url).map_err(map_redis_error)?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(map_redis_error)?;
    let keys: Vec<String> = connection
        .keys(format!("{prefix}:*"))
        .await
        .map_err(map_redis_error)?;
    assert!(
        keys.iter()
            .any(|key| key.starts_with(&format!("{prefix}:flow:")))
    );
    assert!(
        keys.iter()
            .any(|key| key.starts_with(&format!("{prefix}:flow-type:")))
    );
    assert!(keys.iter().all(|key| !key.contains("payment-42")));
    Ok(())
}

#[tokio::test]
async fn redis_flows_do_not_claim_terminal_flows() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = format!("catga-test-flow-{}", uuid::Uuid::new_v4());
    let store = RedisFlows::connect(&url, prefix).await?;
    let state = FlowState::new("done-flow", "payment", [], "node-a").done(0);
    assert!(store.create(state).await?);
    assert!(
        store
            .try_claim("payment", "node-b", Duration::from_secs(86_400))
            .await?
            .is_none()
    );
    Ok(())
}

#[cfg(feature = "streams-rpc")]
mod streams_rpc {
    use std::{future::pending, sync::Arc, time::Instant};

    use super::*;
    use catga_core::{Delivery, Destination, DestinationTransport, MessageTransport};
    use catga_redis::RedisStreamsRequestClient;

    struct NeverSendingTransport;

    #[async_trait::async_trait]
    impl MessageTransport for NeverSendingTransport {
        async fn publish(&self, _: Envelope) -> CatgaResult<()> {
            pending().await
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    #[async_trait::async_trait]
    impl DestinationTransport for NeverSendingTransport {
        async fn send_to(&self, _: &Destination, _: Envelope) -> CatgaResult<()> {
            pending().await
        }

        async fn receive_from(&self, _: &Destination) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    #[tokio::test]
    async fn request_timeout_budget_bounds_a_never_resolving_durable_send() -> CatgaResult<()> {
        let Some(url) = service_url::redis_url()? else {
            return Ok(());
        };
        let client = RedisStreamsRequestClient::new(Arc::new(NeverSendingTransport), &url)?;
        let request = Envelope::new(
            1,
            "catga.test.request",
            Vec::new(),
            MessageMetadata::new(1, Some(1)),
        );
        let started = Instant::now();

        let error = client
            .request_to("orders", request, Duration::from_millis(20))
            .await
            .expect_err("a durable send that never resolves must exhaust the request budget");

        assert_eq!(error.code(), ErrorCode::Timeout);
        assert!(started.elapsed() < Duration::from_secs(1));
        Ok(())
    }
}
