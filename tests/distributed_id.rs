//! Lock-free Snowflake distributed-ID contract tests.

use std::{collections::HashSet, sync::Arc};

use catga_core::{DistributedIdGenerator, SnowflakeIdGenerator, SnowflakeLayout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snowflake_ids_are_monotonic_per_caller_unique_under_concurrency_and_parseable() {
    let generator = Arc::new(SnowflakeIdGenerator::new(7, SnowflakeLayout::default()).unwrap());
    let first = generator.next_id().unwrap();
    let second = generator.next_id().unwrap();
    assert!(second > first);
    let metadata = generator.parse(second);
    assert_eq!(metadata.worker_id(), 7);
    assert!(metadata.timestamp_millis() >= SnowflakeLayout::default().epoch_millis());

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let generator = Arc::clone(&generator);
        tasks.spawn(async move {
            let mut ids = [0; 128];
            generator.fill(&mut ids).unwrap();
            ids
        });
    }
    let mut ids = HashSet::new();
    while let Some(task) = tasks.join_next().await {
        ids.extend(task.unwrap());
    }
    assert_eq!(ids.len(), 1024);
}

#[test]
fn snowflake_layout_rejects_invalid_bit_allocation_and_worker_ids() {
    assert!(SnowflakeLayout::new(40, 10, 12, 1_704_067_200_000).is_err());
    assert!(SnowflakeIdGenerator::new(256, SnowflakeLayout::default()).is_err());
}
