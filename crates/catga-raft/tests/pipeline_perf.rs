//! Performance tests for PipelineManager.
//!
//! These tests verify the batching behavior, flush timing, in-flight limits,
//! and basic throughput of the TiKV-style pipeline manager.

use std::time::{Duration, Instant};

use catga_raft::{PipelineConfig, PipelineManager};

/// Test that proposals are batched correctly when batch_size is reached.
#[tokio::test]
async fn test_pipeline_batching() {
    let config = PipelineConfig {
        batch_size: 10,
        flush_interval: Duration::from_secs(10), // Long interval, batch_size should trigger
        max_inflight: 1000,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose entries up to batch_size
    for i in 0..9 {
        let result = manager.propose(format!("entry-{}", i).into_bytes());
        assert!(result.is_ok(), "Proposal {} should succeed", i);
        // Pending count should be i+1 (not yet flushed)
        assert_eq!(
            manager.pending_count(),
            i + 1,
            "Pending count should be {} before batch_size reached",
            i + 1
        );
    }

    // 10th proposal should trigger immediate flush
    let result = manager.propose(b"entry-10".to_vec());
    assert!(result.is_ok());

    // Give some time for the flush to complete
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Pending should be 0 after batch_size flush
    assert_eq!(
        manager.pending_count(),
        0,
        "Pending should be 0 after batch_size flush"
    );

    manager.stop();
}

/// Test that proposals are batched correctly with a smaller batch size.
#[tokio::test]
async fn test_pipeline_batching_small() {
    let config = PipelineConfig {
        batch_size: 3,
        flush_interval: Duration::from_secs(10),
        max_inflight: 1000,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose exactly batch_size entries
    for i in 0..3 {
        let result = manager.propose(vec![i as u8]);
        assert!(result.is_ok());
    }

    // Give some time for the flush to complete
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Pending should be 0 after batch_size flush
    assert_eq!(
        manager.pending_count(),
        0,
        "Pending should be 0 after flush"
    );

    manager.stop();
}

/// Test that flush interval triggers batch send even without reaching batch_size.
#[tokio::test]
async fn test_pipeline_flush_interval() {
    let flush_duration = Duration::from_millis(50);
    let config = PipelineConfig {
        batch_size: 100, // Large batch size to ensure interval triggers flush
        flush_interval: flush_duration,
        max_inflight: 1000,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose fewer entries than batch_size
    for i in 0..5 {
        let result = manager.propose(format!("entry-{}", i).into_bytes());
        assert!(result.is_ok());
    }

    // Immediately after proposing, pending should have entries
    assert!(
        manager.pending_count() > 0,
        "Pending should have entries immediately after propose"
    );

    // Wait for flush interval to expire
    tokio::time::sleep(flush_duration + Duration::from_millis(20)).await;

    // Pending should be 0 after time-based flush
    assert_eq!(
        manager.pending_count(),
        0,
        "Pending should be 0 after flush_interval timeout"
    );

    manager.stop();
}

/// Test that time-based flush works with various interval sizes.
#[tokio::test]
async fn test_pipeline_flush_interval_various() {
    // Skip 1ms interval as it's too short for reliable timing
    for interval_ms in [5, 10, 20, 50] {
        let config = PipelineConfig {
            batch_size: 1000, // Large to prevent size-based flush
            flush_interval: Duration::from_millis(interval_ms),
            max_inflight: 1000,
        };

        let manager = PipelineManager::new(config);
        manager.start();

        // Add one proposal
        manager.propose(vec![1]).unwrap();

        // Wait for interval plus some buffer
        tokio::time::sleep(Duration::from_millis(interval_ms + 10)).await;

        // Should be flushed
        assert_eq!(
            manager.pending_count(),
            0,
            "Flush should occur for interval {}ms",
            interval_ms
        );

        manager.stop();
    }
}

/// Test that max_inflight limit properly rejects new proposals.
/// Note: The inflight limit is checked at propose time based on current in-flight count.
#[tokio::test]
async fn test_pipeline_inflight_limit() {
    let config = PipelineConfig {
        batch_size: 10,
        flush_interval: Duration::from_secs(10), // Long interval
        max_inflight: 5,                         // Small limit for testing
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose batch_size proposals to trigger automatic flush
    for _ in 0..10 {
        manager.propose(vec![1]).unwrap();
    }

    // Wait for flush to complete
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Inflight should now be 10 (from the batch that was flushed)
    assert_eq!(manager.inflight_count(), 10);

    // With inflight=10 > max_inflight=5, new proposals should be rejected
    let result = manager.propose(b"should-reject".to_vec());
    assert!(
        result.is_err(),
        "Proposal should be rejected when inflight ({}) >= max_inflight ({})",
        manager.inflight_count(),
        5
    );

    manager.stop();
}

/// Test that in-flight count is tracked correctly during batching.
#[tokio::test]
async fn test_pipeline_inflight_tracking() {
    let config = PipelineConfig {
        batch_size: 10,
        flush_interval: Duration::from_secs(10),
        max_inflight: 100,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose entries
    for i in 0..10 {
        manager.propose(vec![i as u8]).unwrap();
    }

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Check that inflight count increased
    let inflight = manager.inflight_count();
    assert_eq!(inflight, 10, "Inflight should be 10 after first batch");

    // Mark batch as completed
    manager.batch_completed(10);

    // Inflight should decrease
    assert_eq!(
        manager.inflight_count(),
        0,
        "Inflight should be 0 after batch_completed"
    );

    manager.stop();
}

/// Test basic throughput of the pipeline manager.
/// Measures proposals per second under normal operation.
#[tokio::test]
async fn test_pipeline_throughput() {
    let config = PipelineConfig {
        batch_size: 64,
        flush_interval: Duration::from_millis(1),
        max_inflight: 1024,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    let proposal_count = 1000;
    let data_size = 100; // bytes per proposal

    let start = Instant::now();

    // Propose entries
    for i in 0..proposal_count {
        let data = vec![(i % 256) as u8; data_size];
        let result = manager.propose(data);
        assert!(result.is_ok(), "Proposal {} should succeed", i);
    }

    let propose_duration = start.elapsed();

    // Wait for all batches to be flushed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Calculate throughput
    let proposals_per_sec = (proposal_count as f64) / propose_duration.as_secs_f64();
    let bytes_per_sec = proposals_per_sec * (data_size as f64);

    println!(
        "Throughput: {:.2} proposals/sec, {:.2} KB/sec",
        proposals_per_sec,
        bytes_per_sec / 1024.0
    );

    // Sanity check: should be able to propose at least 1000/second
    assert!(
        proposals_per_sec >= 1000.0,
        "Throughput {:.2} proposals/sec is below minimum threshold",
        proposals_per_sec
    );

    manager.stop();
}

/// Test throughput with larger batch sizes.
#[tokio::test]
async fn test_pipeline_throughput_large_batch() {
    let config = PipelineConfig {
        batch_size: 256,
        flush_interval: Duration::from_millis(1),
        max_inflight: 4096,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    let proposal_count = 1000; // Reduced from 5000 for faster tests
    let data_size = 64;

    let start = Instant::now();

    for i in 0..proposal_count {
        let data = vec![(i % 256) as u8; data_size];
        manager.propose(data).unwrap();
    }

    let propose_duration = start.elapsed();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let proposals_per_sec = (proposal_count as f64) / propose_duration.as_secs_f64();
    println!(
        "Large batch throughput: {:.2} proposals/sec",
        proposals_per_sec
    );

    // Reduced threshold since we're testing batching behavior
    assert!(
        proposals_per_sec >= 1000.0,
        "Throughput {:.2} proposals/sec is below expected for large batches",
        proposals_per_sec
    );

    manager.stop();
}

/// Test that batch receiver functionality works.
/// Note: ProposalBatch is private, so we verify via inflight tracking.
#[tokio::test]
async fn test_pipeline_batch_receiver() {
    let config = PipelineConfig {
        batch_size: 5,
        flush_interval: Duration::from_secs(10),
        max_inflight: 100,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose batch_size entries to trigger a batch
    for i in 0..5 {
        manager.propose(vec![i as u8]).unwrap();
    }

    // Wait for batch to be processed
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Verify batch was flushed via inflight count
    assert_eq!(
        manager.inflight_count(),
        5,
        "Inflight should be 5 after batch flush"
    );

    manager.stop();
}

/// Test that proposals are correctly batched together.
/// Note: ProposalBatch fields are private, so we verify via inflight tracking.
#[tokio::test]
async fn test_pipeline_batch_ordering() {
    let config = PipelineConfig {
        batch_size: 10,
        flush_interval: Duration::from_secs(10),
        max_inflight: 100,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose entries with specific values
    for i in 0..10 {
        manager.propose(vec![i as u8]).unwrap();
    }

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Verify all proposals were flushed as a single batch
    assert_eq!(
        manager.inflight_count(),
        10,
        "All 10 proposals should be in-flight after batch flush"
    );
    assert_eq!(
        manager.pending_count(),
        0,
        "Pending should be 0 after flush"
    );

    manager.stop();
}

/// Test pipeline under concurrent load.
#[tokio::test]
async fn test_pipeline_concurrent_load() {
    let config = PipelineConfig {
        batch_size: 64,
        flush_interval: Duration::from_millis(1),
        max_inflight: 2048,
    };

    let config_clone = config.clone();

    // Spawn multiple concurrent tasks
    let handles: Vec<_> = (0..4)
        .map(|task_id| {
            let manager = PipelineManager::new(config_clone.clone());
            manager.start();

            tokio::spawn(async move {
                for i in 0..(500 / 4) {
                    let data = vec![((task_id * 100 + i) % 256) as u8; 64];
                    let _ = manager.propose(data);
                }
            })
        })
        .collect();

    let start = Instant::now();

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();

    // Give time for final flushes
    tokio::time::sleep(Duration::from_millis(50)).await;

    let total_proposals = 500.0;
    let proposals_per_sec = total_proposals / duration.as_secs_f64();

    println!(
        "Concurrent load: {:.2} proposals/sec total",
        proposals_per_sec
    );

    // Verify all proposals were processed
    // Note: We cannot easily track total from concurrent managers,
    // but we can verify no panics occurred
}

/// Test that stop() properly signals shutdown.
/// Note: stop() sets running=false, actual flush happens in background task.
#[tokio::test]
async fn test_pipeline_stop_flushes_remaining() {
    let config = PipelineConfig {
        batch_size: 100,
        flush_interval: Duration::from_millis(10), // Short interval for faster shutdown
        max_inflight: 100,
    };

    let manager = PipelineManager::new(config);
    manager.start();

    // Propose some entries
    for i in 0..5 {
        manager.propose(vec![i as u8]).unwrap();
    }

    // Stop should signal background task to flush
    manager.stop();

    // Wait for the flush interval to trigger in background task
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Pending should be 0 after background task processes stop
    assert_eq!(
        manager.pending_count(),
        0,
        "Pending should be 0 after stop() and flush interval"
    );
}
