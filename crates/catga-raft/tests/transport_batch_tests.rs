//! Integration tests for `catga_raft::transport::batch`.
//!
//! Covers the public batching primitives of the transport layer:
//! `MessageBatch`, `BatchSender`, and the module-level constants
//! (`DEFAULT_BATCH_SIZE`, `DEFAULT_FLUSH_INTERVAL_MS`, `MAX_PENDING_BATCHES`).
//!
//! Note: the current `BatchSender` API is infallible (every fallible-looking
//! method always returns `Ok`), so instead of error cases the tests focus on
//! construction/defaults, happy paths, and edge cases (zero batch size, zero
//! flush interval, flushing unknown peers, empty flushes, and concurrent use).
//!
//! To keep tests deterministic, most of them use a very long flush interval so
//! the time-based flush never fires; the tests that exercise time-based
//! flushing use `Duration::ZERO`, which is deterministic as well.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use catga_raft::transport::batch::{
    DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL_MS, MAX_PENDING_BATCHES, BatchSender, MessageBatch,
};

/// Shorthand for building a `Bytes` payload from a string.
fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// A flush interval long enough that the time-based flush never triggers
/// during a test run.
const LONG_INTERVAL: Duration = Duration::from_secs(600);

// ============================================================================
// MessageBatch tests
// ============================================================================

#[test]
fn message_batch_new_is_empty() {
    let batch = MessageBatch::new(7);
    assert_eq!(batch.peer_id, 7);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    assert_eq!(batch.total_size(), 0);
    // A freshly created batch has a non-negative age.
    assert!(batch.age() <= Duration::from_secs(1));
}

#[test]
fn message_batch_push_len_total_size_and_clone() {
    let mut batch = MessageBatch::new(1);

    batch.push(b("abc"));
    assert!(!batch.is_empty());
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.total_size(), 3);

    batch.push(Bytes::new()); // empty payloads count as messages but add no bytes
    batch.push(b("de"));
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.total_size(), 5);

    // Clone preserves all messages and metadata.
    let clone = batch.clone();
    assert_eq!(clone.peer_id, batch.peer_id);
    assert_eq!(clone.len(), batch.len());
    assert_eq!(clone.total_size(), batch.total_size());
    assert_eq!(clone.messages, batch.messages);
}

#[test]
fn message_batch_age_grows_over_time() {
    let batch = MessageBatch::new(2);
    std::thread::sleep(Duration::from_millis(10));
    assert!(
        batch.age() >= Duration::from_millis(5),
        "batch age must grow as time elapses"
    );
}

// ============================================================================
// Constants and construction
// ============================================================================

#[test]
fn transport_batch_constants_have_documented_values() {
    assert_eq!(DEFAULT_BATCH_SIZE, 64);
    assert_eq!(DEFAULT_FLUSH_INTERVAL_MS, 1);
    assert_eq!(MAX_PENDING_BATCHES, 1024);
}

#[test]
fn batch_sender_default_matches_constants() {
    let sender = BatchSender::default();
    assert_eq!(sender.flush_interval(), Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS));
    assert_eq!(sender.max_batch_size(), DEFAULT_BATCH_SIZE);
    assert_eq!(sender.total_pending(), 0);
    assert_eq!(sender.pending_peers(), 0);
    assert_eq!(sender.pending_count(42), 0);

    // Debug impl reports the type name and live counters.
    let dbg = format!("{sender:?}");
    assert!(dbg.contains("BatchSender"));
    assert!(dbg.contains("flush_interval"));
}

#[test]
fn batch_sender_new_and_with_config_expose_their_settings() {
    let a = BatchSender::new(Duration::from_millis(5), 16);
    assert_eq!(a.flush_interval(), Duration::from_millis(5));
    assert_eq!(a.max_batch_size(), 16);

    let b = BatchSender::with_config(Duration::from_millis(7), 32, MAX_PENDING_BATCHES);
    assert_eq!(b.flush_interval(), Duration::from_millis(7));
    assert_eq!(b.max_batch_size(), 32);
    assert_eq!(b.total_pending(), 0);
}

// ============================================================================
// send / accumulation
// ============================================================================

#[tokio::test]
async fn batch_sender_send_accumulates_pending_per_peer() {
    let sender = BatchSender::new(LONG_INTERVAL, 1000);

    sender.send(1, b("m1")).await.unwrap();
    sender.send(1, b("m2")).await.unwrap();
    sender.send(2, b("m3")).await.unwrap();

    assert_eq!(sender.pending_count(1), 2);
    assert_eq!(sender.pending_count(2), 1);
    assert_eq!(sender.pending_count(3), 0, "unknown peers have no pending messages");
    assert_eq!(sender.total_pending(), 3);
    assert_eq!(sender.pending_peers(), 2);
}

#[tokio::test]
async fn batch_sender_size_threshold_forces_flush() {
    let sender = BatchSender::new(LONG_INTERVAL, 3);

    sender.send(1, b("a")).await.unwrap();
    sender.send(1, b("b")).await.unwrap();
    assert_eq!(sender.pending_count(1), 2, "below the threshold messages accumulate");

    // The third message reaches max_batch_size and forces a flush of peer 1.
    sender.send(1, b("c")).await.unwrap();
    assert_eq!(sender.pending_count(1), 0);
    assert_eq!(sender.total_pending(), 0);
}

#[tokio::test]
async fn batch_sender_zero_max_size_flushes_every_send() {
    let sender = BatchSender::new(LONG_INTERVAL, 0);

    // With max_batch_size == 0, `len >= max_batch_size` is always true once a
    // message is pending, so every send immediately flushes.
    for i in 0..4u8 {
        sender.send(5, Bytes::from(vec![i])).await.unwrap();
        assert_eq!(sender.pending_count(5), 0);
    }
    assert_eq!(sender.total_pending(), 0);
    assert_eq!(sender.pending_peers(), 0);
}

#[tokio::test]
async fn batch_sender_zero_flush_interval_drains_on_next_send() {
    let sender = BatchSender::new(Duration::ZERO, 1000);

    sender.send(9, b("first")).await.unwrap();
    // No time has been required to elapse for the second send: the time-based
    // flush is deterministic with a zero interval and drains `first` before
    // the new message is buffered.
    sender.send(9, b("second")).await.unwrap();

    assert_eq!(sender.pending_count(9), 1, "only the most recent message should remain pending");
    assert_eq!(sender.total_pending(), 1);
}

// ============================================================================
// flush / flush_by_peer
// ============================================================================

#[tokio::test]
async fn batch_sender_flush_returns_batch_count_and_empties_all() {
    let sender = BatchSender::new(LONG_INTERVAL, 1000);

    sender.send_batch(1, vec![b("a"), b("b")]).await.unwrap();
    sender.send_batch(2, vec![b("c")]).await.unwrap();
    sender.send(3, b("d")).await.unwrap();
    assert_eq!(sender.total_pending(), 4);

    // flush() counts batches (one per peer), not messages.
    let flushed = sender.flush().await.unwrap();
    assert_eq!(flushed, 3);
    assert_eq!(sender.total_pending(), 0);
    assert_eq!(sender.pending_peers(), 0);

    // Flushing again with nothing pending returns zero batches.
    assert_eq!(sender.flush().await.unwrap(), 0);
}

#[tokio::test]
async fn batch_sender_flush_by_peer_isolates_peers() {
    let sender = BatchSender::new(LONG_INTERVAL, 1000);

    sender.send(1, b("x")).await.unwrap();
    sender.send(2, b("y")).await.unwrap();

    sender.flush_by_peer(1).await.unwrap();
    assert_eq!(sender.pending_count(1), 0);
    assert_eq!(sender.pending_count(2), 1, "flushing one peer must not touch the others");
    assert_eq!(sender.pending_peers(), 1);

    // Flushing a peer with no pending messages is a no-op that succeeds.
    sender.flush_by_peer(999).await.unwrap();
    assert_eq!(sender.pending_count(2), 1);
}

#[tokio::test]
async fn batch_sender_send_batch_empty_vec_is_a_noop() {
    let sender = BatchSender::new(LONG_INTERVAL, 1000);

    sender.send_batch(4, Vec::new()).await.unwrap();
    assert_eq!(sender.total_pending(), 0);
    assert_eq!(sender.pending_peers(), 0);
}

#[tokio::test]
async fn batch_sender_flush_resets_time_since_last_flush() {
    let sender = BatchSender::new(LONG_INTERVAL, 1000);
    sender.send(1, b("msg")).await.unwrap();

    sender.flush().await.unwrap();
    assert!(
        sender.time_since_last_flush() < Duration::from_millis(500),
        "flush must update the last-flush timestamp"
    );
    // The configured interval is unaffected by flushing.
    assert_eq!(sender.flush_interval(), LONG_INTERVAL);
}

// ============================================================================
// Concurrency
// ============================================================================

#[tokio::test]
async fn batch_sender_is_safe_under_concurrent_sends() {
    let sender = Arc::new(BatchSender::new(LONG_INTERVAL, 10_000));
    let mut handles = Vec::new();

    // Four tasks, each sending 100 messages to its own peer.
    for peer in 1u64..=4 {
        let s = Arc::clone(&sender);
        handles.push(tokio::spawn(async move {
            for i in 0..100u32 {
                s.send(peer, Bytes::from(i.to_le_bytes().to_vec()))
                    .await
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(sender.pending_peers(), 4);
    assert_eq!(sender.total_pending(), 400);
    for peer in 1u64..=4 {
        assert_eq!(sender.pending_count(peer), 100);
    }

    assert_eq!(sender.flush().await.unwrap(), 4);
    assert_eq!(sender.total_pending(), 0);
}
