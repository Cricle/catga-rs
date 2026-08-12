//! Raw KV bucket provisioning with a byte cap, so tests can force broker-side write
//! rejections deterministically regardless of the server's payload limit.

use std::time::Duration;

use crate::nats_server::{server_url, test_error};
use async_nats::jetstream::{self, kv, stream};
use catga_core::CatgaResult;

/// Provisions a raw KV bucket whose stream rejects writes beyond `max_bytes`.
pub async fn raw_kv_with_byte_cap(bucket: &str, max_bytes: i64) -> CatgaResult<kv::Store> {
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw KV capped client", error))?;
    let context = jetstream::new(client);
    context
        .create_stream(stream::Config {
            name: format!("KV_{bucket}"),
            subjects: vec![format!("$KV.{bucket}.>")],
            max_messages_per_subject: 1,
            discard: stream::DiscardPolicy::New,
            allow_rollup: true,
            deny_delete: true,
            allow_direct: true,
            max_bytes,
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create raw KV capped bucket", error))?;
    for _ in 0..20 {
        if let Ok(store) = context.get_key_value(bucket).await {
            return Ok(store);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    context
        .get_key_value(bucket)
        .await
        .map_err(|error| test_error("open raw KV capped bucket", error))
}
