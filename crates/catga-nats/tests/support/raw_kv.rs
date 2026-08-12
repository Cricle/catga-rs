//! Raw JetStream KV access for corrupt- and legacy-record injection tests.
//!
//! Stores decode whatever the broker holds; these helpers let a test place bytes the public API
//! would never write (malformed frames, superseded wire versions) behind the exact keys a store
//! reads, so decode and recovery contracts are exercised for real.

use std::time::Duration;

use crate::nats_server::{server_url, test_error};
use async_nats::jetstream::{self, kv, stream};
use catga_core::CatgaResult;

/// Opens (or provisions) the raw KV bucket the store under test uses.
pub async fn raw_kv(bucket: &str) -> CatgaResult<kv::Store> {
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw KV fixture client", error))?;
    let context = jetstream::new(client);
    if let Ok(store) = context.get_key_value(bucket).await {
        return Ok(store);
    }
    context
        .create_stream(stream::Config {
            name: format!("KV_{bucket}"),
            subjects: vec![format!("$KV.{bucket}.>")],
            max_messages_per_subject: 1,
            discard: stream::DiscardPolicy::New,
            allow_rollup: true,
            deny_delete: true,
            allow_direct: true,
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create raw KV fixture bucket", error))?;
    for _ in 0..20 {
        if let Ok(store) = context.get_key_value(bucket).await {
            return Ok(store);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    context
        .get_key_value(bucket)
        .await
        .map_err(|error| test_error("open raw KV fixture bucket", error))
}
