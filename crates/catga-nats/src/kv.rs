//! Compatibility provisioning for JetStream KV buckets.

use std::time::Duration;

use async_nats::jetstream::{self, kv, stream};

/// Opens a bucket or provisions the documented KV stream shape.
pub(crate) async fn open_or_create(
    context: &jetstream::Context,
    bucket: &str,
) -> Result<kv::Store, String> {
    if let Ok(store) = context.get_key_value(bucket).await {
        return Ok(store);
    }

    // Avoid `Context::create_key_value` here: recent async-nats versions query account metadata
    // that older NATS servers do not return in the shape expected by the client. A KV bucket is a
    // regular JetStream stream with this stable configuration.
    let _ = context
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
        .await;

    for _ in 0..20 {
        if let Ok(store) = context.get_key_value(bucket).await {
            return Ok(store);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    context
        .get_key_value(bucket)
        .await
        .map_err(|error| error.to_string())
}
