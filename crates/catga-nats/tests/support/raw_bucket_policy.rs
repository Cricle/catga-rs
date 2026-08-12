//! Raw KV bucket provisioning with an explicit max-age policy.
//!
//! Only bucket-policy reset tests need this; it stays separate so every other test binary
//! compiles exactly the fixtures it uses.

use std::time::Duration;

use crate::nats_server::{server_url, test_error};
use async_nats::jetstream::{self, kv, stream};
use catga_core::CatgaResult;

/// Provisions a raw KV bucket whose records expire after `max_age`.
pub async fn raw_kv_with_max_age(bucket: &str, max_age: Duration) -> CatgaResult<kv::Store> {
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw KV policy client", error))?;
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
            max_age,
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create raw KV policy bucket", error))?;
    for _ in 0..20 {
        if let Ok(store) = context.get_key_value(bucket).await {
            return Ok(store);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    context
        .get_key_value(bucket)
        .await
        .map_err(|error| test_error("open raw KV policy bucket", error))
}
