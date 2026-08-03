//! Manual SQLite DSL step-progress performance benchmark.
//!
//! Run only when measuring performance:
//! `cargo test -p catga-flow-store --features sqlite --test sqlite_dsl_progress_performance -- --ignored --nocapture`
//!
//! The timed interval excludes temporary database setup, migration, fixture construction, and a
//! warm-up cycle. It measures complete create, versioned update, read, and delete lifecycles for
//! distinct step-progress records. Correctness assertions remain active while measuring, but the
//! benchmark intentionally has no host-dependent timing threshold.
#![cfg(feature = "sqlite")]

use std::time::Instant;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::{DslStepProgress, DslStepProgressStore};
use catga_flow_store::SqlDslStepProgressStore;

const RECORD_COUNT: usize = 128;
const PAYLOAD_BYTES: usize = 256;

/// Measures complete SQLite DSL-progress lifecycles without a timing threshold.
#[tokio::test]
#[ignore = "manual performance benchmark; run with --ignored --nocapture"]
async fn sqlite_dsl_progress_lifecycle_benchmark() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("dsl-progress-performance.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let initial_payload = vec![0xA5; PAYLOAD_BYTES];
    let updated_payload = vec![0x5A; PAYLOAD_BYTES];
    warm_up(&store, &initial_payload, &updated_payload).await?;
    let records = (0..RECORD_COUNT)
        .map(|index| {
            DslStepProgress::new(
                format!("sqlite-dsl-progress-benchmark-{index}"),
                index as u32,
                initial_payload.clone(),
            )
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    for initial in records {
        let flow_id = initial.flow_id().to_owned();
        let step_index = initial.step_index();
        let expected_version = initial.version();
        let next = initial.clone().next_version(updated_payload.clone())?;

        assert!(store.create(initial.clone()).await?);
        assert!(!store.create(initial).await?);
        assert!(store.update(expected_version, next.clone()).await?);
        assert_eq!(store.get(&flow_id, step_index).await?, Some(next));
        assert!(store.delete(&flow_id, step_index).await?);
        assert!(store.get(&flow_id, step_index).await?.is_none());
    }
    let elapsed = started.elapsed();
    let elapsed_per_record = elapsed / (RECORD_COUNT as u32);
    let records_per_second = (RECORD_COUNT as f64) / elapsed.as_secs_f64();

    println!(
        "sqlite_dsl_progress_lifecycle: records={RECORD_COUNT}, payload_bytes={PAYLOAD_BYTES}, total={elapsed:?}, per_record={elapsed_per_record:?}, records_per_second={records_per_second:.2}"
    );
    Ok(())
}

async fn warm_up(
    store: &SqlDslStepProgressStore,
    initial_payload: &[u8],
    updated_payload: &[u8],
) -> CatgaResult<()> {
    let initial = DslStepProgress::new("sqlite-dsl-progress-benchmark-warmup", 0, initial_payload);
    let next = initial.clone().next_version(updated_payload)?;

    assert!(store.create(initial.clone()).await?);
    assert!(store.update(initial.version(), next.clone()).await?);
    assert_eq!(
        store.get(initial.flow_id(), initial.step_index()).await?,
        Some(next)
    );
    assert!(
        store
            .delete(initial.flow_id(), initial.step_index())
            .await?
    );
    Ok(())
}

fn temporary_directory() -> CatgaResult<tempfile::TempDir> {
    tempfile::tempdir().map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            "create SQLite performance-test directory",
        )
        .with_details(error.to_string())
    })
}
