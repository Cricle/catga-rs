//! Integration tests for `src/pipeline.rs` (TiKV-style pipeline batching).
//!
//! Coverage notes:
//! - `Proposal` is crate-private, so the contents of flushed batches are
//!   observed through `batch_receiver` and the public counters
//!   (`pending_count` / `inflight_count`).
//! - The flusher task wakes on every sub-batch propose, so tests that assert
//!   on synchronous accumulation either run on the default current-thread
//!   runtime (the flusher cannot preempt between sync calls) or observe the
//!   batch receiver instead of the pending queue.
//! - `pipeline_perf.rs` covers throughput; this file focuses on construction,
//!   defaults, deterministic state transitions, error paths, and the
//!   in-flight accounting regression (the "halt bug").

use std::time::Duration;

use catga_raft::{CatgaRaftError, PipelineConfig, PipelineManager};

/// Config with a long flush interval so the periodic tick never interferes;
/// only explicit flushes, batch-size flushes and direct flusher wakes fire.
fn size_only_config(batch_size: usize, max_inflight: usize) -> PipelineConfig {
    PipelineConfig {
        batch_size,
        flush_interval: Duration::from_secs(60),
        max_inflight,
    }
}

/// Polls `condition` every 10ms until it holds or the deadline passes.
async fn eventually<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Construction and defaults
// ---------------------------------------------------------------------------

/// `PipelineConfig::default` must match the documented defaults.
#[test]
fn pipeline_config_default_values() {
    let config = PipelineConfig::default();
    assert_eq!(config.batch_size, 64);
    assert_eq!(config.flush_interval, Duration::from_millis(1));
    assert_eq!(config.max_inflight, 1024);
}

/// A default-constructed manager starts idle with empty queues.
#[test]
fn pipeline_manager_default_state() {
    let manager = PipelineManager::default();
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(manager.inflight_count(), 0);
    // Not started yet, so proposing must fail with NotLeader.
    let result = manager.propose(b"hello".to_vec());
    assert!(matches!(result, Err(CatgaRaftError::NotLeader)));
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

/// Proposing on a manager that was never started must return NotLeader.
#[test]
fn pipeline_propose_before_start_is_not_leader() {
    let manager = PipelineManager::new(size_only_config(8, 16));
    assert!(matches!(
        manager.propose(vec![1, 2, 3]),
        Err(CatgaRaftError::NotLeader)
    ));
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(manager.inflight_count(), 0);
}

/// After `stop()`, new proposals must be rejected with NotLeader, and the
/// already-accepted proposal must survive via the flusher's final flush:
/// shipped into a batch (in flight), never dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_propose_after_stop_is_not_leader() {
    let manager = PipelineManager::new(size_only_config(8, 16));
    manager.start();
    manager.propose(b"before-stop".to_vec()).unwrap();
    manager.stop();

    assert!(matches!(
        manager.propose(b"after-stop".to_vec()),
        Err(CatgaRaftError::NotLeader)
    ));

    // stop() wakes the flusher for one final flush, so the accepted proposal
    // must end up in flight (queued in a batch) with nothing left pending.
    let flushed = eventually(Duration::from_secs(2), || {
        manager.pending_count() == 0 && manager.inflight_count() == 1
    })
    .await;
    assert!(
        flushed,
        "accepted proposal lost on stop: pending={}, inflight={}",
        manager.pending_count(),
        manager.inflight_count()
    );
}

/// Once in-flight reaches `max_inflight`, proposals fail fast with
/// `Backpressure` and recover after the owner drains and acknowledges the
/// batch, freeing capacity.
#[tokio::test]
async fn pipeline_inflight_limit_backpressure_and_recovery() {
    let manager = PipelineManager::new(size_only_config(1, 1));
    manager.start();
    let rx = manager.batch_receiver();

    // batch_size == 1: each propose flushes immediately, inflight -> 1.
    manager.propose(vec![1]).unwrap();
    assert_eq!(manager.inflight_count(), 1);
    assert_eq!(manager.pending_count(), 0);

    // At the limit: rejected with Backpressure, nothing queued.
    assert!(matches!(
        manager.propose(vec![2]),
        Err(CatgaRaftError::Backpressure)
    ));
    assert_eq!(manager.inflight_count(), 1);
    assert_eq!(manager.pending_count(), 0);

    // Free capacity the way the owner loop does: drain the batch, then ack.
    let batch = rx.try_recv().expect("flushed batch must be queued");
    manager.batch_completed(batch.len());
    assert_eq!(manager.inflight_count(), 0);
    manager.propose(vec![3]).unwrap();
    assert_eq!(manager.inflight_count(), 1);

    manager.stop();
}

/// When the batch channel is full, propose must fail fast with
/// `Backpressure`, queue nothing, and leave the in-flight counter untouched
/// — the old code silently dropped the whole batch *after* incrementing
/// inflight, losing data and leaking capacity.
#[tokio::test]
async fn pipeline_channel_full_propose_fails_fast_without_side_effects() {
    // Channel capacity = max_inflight = 2 batches; batch_size 1 so each
    // propose flushes synchronously into the channel.
    let manager = PipelineManager::new(size_only_config(1, 2));
    manager.start();
    let rx = manager.batch_receiver();

    manager.propose(vec![1]).unwrap();
    manager.propose(vec![2]).unwrap();
    assert_eq!(manager.inflight_count(), 2);

    // Acknowledge the batches (as the owner loop would) *without* receiving
    // them, so the channel stays full while in-flight has room: this
    // isolates the channel-full rejection from the in-flight limit.
    manager.batch_completed(2);
    assert_eq!(manager.inflight_count(), 0);

    let err = manager.propose(vec![3]).unwrap_err();
    assert!(
        matches!(err, CatgaRaftError::Backpressure),
        "channel-full propose must fail with Backpressure, got {err:?}"
    );
    assert_eq!(
        manager.inflight_count(),
        0,
        "a failed propose must not change the in-flight counter"
    );
    assert_eq!(
        manager.pending_count(),
        0,
        "a failed propose must not queue anything"
    );

    // Nothing was dropped: the two accepted batches are still in the channel.
    let first = rx.try_recv().expect("first batch must be intact");
    let second = rx.try_recv().expect("second batch must be intact");
    assert_eq!(first.into_items(), vec![(Vec::new(), vec![1u8])]);
    assert_eq!(second.into_items(), vec![(Vec::new(), vec![2u8])]);

    manager.stop();
}

// ---------------------------------------------------------------------------
// In-flight accounting (halt-bug regressions)
// ---------------------------------------------------------------------------

/// `batch_completed` decrements the in-flight counter by the consumed
/// batch's length, exactly mirroring what the owner loop does per batch.
#[tokio::test]
async fn pipeline_inflight_decrements_after_batch_consumption() {
    // batch_size == 1: every propose flushes synchronously, so counters are
    // deterministic on the current-thread runtime.
    let manager = PipelineManager::new(size_only_config(1, 16));
    manager.start();
    let rx = manager.batch_receiver();

    manager.propose(vec![1]).unwrap();
    manager.propose(vec![2]).unwrap();
    manager.propose(vec![3]).unwrap();
    assert_eq!(manager.inflight_count(), 3);

    // Consume one batch like the owner loop: len() -> batch_completed(len).
    let batch = rx.try_recv().expect("first batch must be queued");
    let len = batch.len();
    assert_eq!(batch.into_items(), vec![(Vec::new(), vec![1u8])]);
    manager.batch_completed(len);
    assert_eq!(manager.inflight_count(), 3 - len);

    // Drain and acknowledge the rest: inflight returns to exactly zero.
    for expected in [2u8, 3u8] {
        let batch = rx.try_recv().expect("batch must be queued");
        let len = batch.len();
        assert_eq!(batch.into_items(), vec![(Vec::new(), vec![expected])]);
        manager.batch_completed(len);
    }
    assert_eq!(manager.inflight_count(), 0);
    assert!(rx.try_recv().is_err(), "no phantom batches");

    manager.stop();
}

/// Regression (halt bug): with the owner loop acknowledging every consumed
/// batch, proposing far more than `max_inflight` entries *in total* must
/// keep working. The old code only ever incremented `inflight`, so after
/// `max_inflight` cumulative proposals every further propose failed forever.
#[tokio::test]
async fn pipeline_survives_more_than_max_inflight_total_proposals() {
    let max_inflight = 8usize;
    let manager = PipelineManager::new(size_only_config(1, max_inflight));
    manager.start();
    let rx = manager.batch_receiver();

    // 16x the in-flight window, consuming each batch the way the owner does.
    for i in 0..(max_inflight * 16) {
        manager.propose(vec![(i % 256) as u8]).unwrap_or_else(|e| {
            panic!("propose #{i} failed with {e} despite consumed batches (halt bug)")
        });
        while let Ok(batch) = rx.try_recv() {
            manager.batch_completed(batch.len());
        }
    }
    assert_eq!(manager.inflight_count(), 0);

    // And a trailing propose still goes through.
    manager.propose(b"still-alive".to_vec()).unwrap();
    assert_eq!(manager.inflight_count(), 1);

    manager.stop();
}

/// `batch_completed` uses saturating subtraction: over-completing must not
/// underflow the in-flight counter.
#[test]
fn pipeline_batch_completed_saturates_at_zero() {
    let manager = PipelineManager::new(size_only_config(8, 16));
    manager.batch_completed(100);
    assert_eq!(manager.inflight_count(), 0);
    manager.batch_completed(0);
    assert_eq!(manager.inflight_count(), 0);
}

// ---------------------------------------------------------------------------
// Happy paths, flush behavior and latency
// ---------------------------------------------------------------------------

/// Pending proposals accumulate below batch_size and a manual `flush()`
/// moves them to in-flight exactly once.
///
/// Runs on the current-thread runtime without await points between the
/// proposes, so the flusher task cannot preempt the accumulation.
#[tokio::test]
async fn pipeline_manual_flush_moves_pending_to_inflight() {
    let manager = PipelineManager::new(size_only_config(100, 100));
    manager.start();

    for i in 0..3u8 {
        manager.propose(vec![i]).unwrap();
        assert_eq!(manager.pending_count(), (i + 1) as usize);
    }
    assert_eq!(manager.inflight_count(), 0);

    manager.flush().unwrap();
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(manager.inflight_count(), 3);

    // Flushing again with nothing pending is a no-op.
    manager.flush().unwrap();
    assert_eq!(manager.inflight_count(), 3);

    manager.stop();
}

/// `flush()` on an empty pending queue must not change any counter.
#[tokio::test]
async fn pipeline_flush_empty_is_noop() {
    let manager = PipelineManager::new(size_only_config(8, 16));
    manager.start();
    manager.flush().unwrap();
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(manager.inflight_count(), 0);
    manager.stop();
}

/// Proposals that reach the batch size are flushed from the pending queue
/// into the in-flight counter synchronously.
#[tokio::test]
async fn pipeline_propose_flushes_when_batch_size_reached() {
    let manager = PipelineManager::new(size_only_config(2, 16));
    manager.start();

    manager.propose(vec![1]).unwrap();
    assert_eq!(manager.pending_count(), 1);
    assert_eq!(manager.inflight_count(), 0);

    // Second proposal hits batch_size and triggers a synchronous flush.
    manager.propose(vec![2]).unwrap();
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(manager.inflight_count(), 2);

    manager.stop();
}

/// Regression (tail latency): a solo write below batch_size must not wait a
/// full `flush_interval` for the periodic tick — `propose` wakes the flusher
/// directly, and the flushed batch pings the owner-facing flush notify.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_solo_write_flushes_well_before_flush_interval() {
    let config = PipelineConfig {
        batch_size: 64,
        // Deliberately huge: the periodic tick cannot possibly rescue us.
        flush_interval: Duration::from_secs(30),
        max_inflight: 1024,
    };
    let manager = PipelineManager::new(config);
    manager.start();
    let rx = manager.batch_receiver();

    let started = std::time::Instant::now();
    manager.propose(b"solo".to_vec()).unwrap();

    // Generous bound: in practice this lands within a few milliseconds; the
    // broken behavior needed the full 30s interval.
    let batch = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("solo propose must be flushed by the flusher wake, not the interval tick");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "solo write waited {:?} to be flushed",
        started.elapsed()
    );
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.into_items(), vec![(Vec::new(), b"solo".to_vec())]);
    assert_eq!(manager.inflight_count(), 1);

    manager.stop();
}
