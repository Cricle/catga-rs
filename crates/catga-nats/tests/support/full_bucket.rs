//! Adaptive bucket filler for broker write-failure tests.
//!
//! Byte-capped buckets reject new writes once full; the byte check runs before a superseded
//! revision is discarded, so even same-subject rewrites and delete markers fail once the
//! free space is smaller than one message. Filling adaptively — coarse passes first, then
//! ever smaller ones — is deterministic regardless of the server's exact storage accounting.

use crate::nats_server::test_error;
use async_nats::jetstream::kv;
use catga_core::CatgaResult;

/// Writes junk entries until even a tiny write is rejected, leaving no room for more writes.
pub async fn fill_bucket(store: &kv::Store) -> CatgaResult<()> {
    for payload in [256_usize, 64, 8] {
        for index in 0u32..512 {
            let key = format!("junk.{payload}.{index:04}");
            if store.put(key, vec![0xA5; payload].into()).await.is_err() {
                break;
            }
        }
    }
    if store
        .put("junk.probe".to_owned(), vec![0xA5; 8].into())
        .await
        .is_ok()
    {
        return Err(test_error(
            "fill capped bucket",
            "the byte cap never rejected a write",
        ));
    }
    Ok(())
}
