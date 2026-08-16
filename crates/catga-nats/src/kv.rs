//! Compatibility provisioning for JetStream KV buckets.

use std::time::Duration;

use async_nats::jetstream::{self, kv, stream};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, ResilienceExecutor, ResilienceOptions, RetryJitter,
};
use tokio_util::sync::CancellationToken;

/// Maximum read/compare/write attempts for a contested KV entry.
pub(crate) const MAX_CAS_RETRIES: usize = 8;

/// Additional visibility probes after a freshly provisioned bucket was not yet readable.
const BUCKET_PROBE_RETRIES: u32 = 20;

/// Fixed pause between bucket visibility probes.
const BUCKET_PROBE_DELAY: Duration = Duration::from_millis(10);

/// Reports exhaustion of a bounded KV revision compare-and-set retry loop.
pub(crate) fn cas_error(component: &str, operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("NATS {component} {operation} compare-and-set did not stabilize"),
    )
}

/// Opens a bucket or provisions the documented KV stream shape.
///
/// A freshly provisioned bucket can lag behind the stream creation that backs it, so the store
/// lookup is driven through the core bounded-retry policy: one initial probe plus
/// `BUCKET_PROBE_RETRIES` retries with a fixed `BUCKET_PROBE_DELAY` pause, matching the
/// historical 20x10ms polling cadence. Every caller normalizes the failure to
/// [`ErrorCode::Transient`] through its own mapping.
pub(crate) async fn open_or_create(
    context: &jetstream::Context,
    bucket: &str,
) -> CatgaResult<kv::Store> {
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

    let executor = ResilienceExecutor::with_jitter(
        ResilienceOptions {
            max_retries: BUCKET_PROBE_RETRIES,
            retry_delay: BUCKET_PROBE_DELAY,
            // Far above any broker round trip; bounds a wedged attempt instead of the policy.
            timeout: Duration::from_secs(30),
            ..ResilienceOptions::default()
        },
        RetryJitter::fixed(BUCKET_PROBE_DELAY),
    )?;
    executor
        .execute(CancellationToken::new(), |_| async {
            context
                .get_key_value(bucket)
                .await
                .map_err(CatgaError::transient)
        })
        .await
}
