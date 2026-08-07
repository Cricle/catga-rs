//! Contract coverage for [`FlowStore::create_batch`].
//!
//! The SQLite cases run locally without any external service and exercise the transactional
//! batch implementation; the memory case exercises the trait's sequential default. Backend
//! specific service coverage lives in `cross_backend.rs`.

use catga_core::flow::{FlowState, FlowStore, MAX_FLOW_STORE_BATCH};
use catga_core::memory::MemoryFlows;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::SqlFlowStore;

fn batch_state(tag: &str, sequence: usize) -> FlowState {
    FlowState::new(
        format!("batch-{tag}-{sequence}").as_str(),
        "batch-contract",
        format!("payload-{sequence}").into_bytes(),
        "batch-node",
    )
}

async fn connect_sqlite() -> CatgaResult<(tempfile::TempDir, SqlFlowStore)> {
    let directory = tempfile::tempdir().map_err(|error| {
        CatgaError::new(ErrorCode::Internal, format!("create temp dir: {error}"))
    })?;
    let url = format!("sqlite:{}", directory.path().join("batch.db").display());
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    Ok((directory, store))
}

#[tokio::test]
async fn sqlite_create_batch_creates_every_flow_in_one_unit() -> CatgaResult<()> {
    let (_directory, store) = connect_sqlite().await?;
    let states: Vec<FlowState> = (0..16)
        .map(|sequence| batch_state("all", sequence))
        .collect();

    let created = store.create_batch(states.clone()).await?;

    assert_eq!(created.len(), states.len());
    assert!(created.iter().all(|was_created| *was_created));
    for state in &states {
        let persisted = store.get(state.id()).await?.ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "batched flow was not persisted")
        })?;
        assert_eq!(persisted.data(), state.data());
    }
    Ok(())
}

#[tokio::test]
async fn sqlite_create_batch_reports_conflicts_positionally_and_keeps_new_rows() -> CatgaResult<()>
{
    let (_directory, store) = connect_sqlite().await?;
    let existing = batch_state("mixed", 0);
    assert!(store.create(existing.clone()).await?);

    // Position 0 duplicates the existing flow; positions 1 and 2 are new.
    let states = vec![
        existing.clone(),
        batch_state("mixed", 1),
        batch_state("mixed", 2),
    ];
    let created = store.create_batch(states).await?;

    assert_eq!(created, vec![false, true, true]);
    assert!(store.get("batch-mixed-1").await?.is_some());
    assert!(store.get("batch-mixed-2").await?.is_some());
    Ok(())
}

#[tokio::test]
async fn sqlite_create_batch_is_idempotent_for_an_already_created_set() -> CatgaResult<()> {
    let (_directory, store) = connect_sqlite().await?;
    let states: Vec<FlowState> = (0..8)
        .map(|sequence| batch_state("idem", sequence))
        .collect();

    assert!(store.create_batch(states.clone()).await?.iter().all(|c| *c));
    let replayed = store.create_batch(states).await?;

    assert_eq!(replayed.len(), 8);
    assert!(replayed.iter().all(|was_created| !*was_created));
    Ok(())
}

#[tokio::test]
async fn sqlite_create_batch_empty_input_returns_empty_output() -> CatgaResult<()> {
    let (_directory, store) = connect_sqlite().await?;
    assert!(store.create_batch(Vec::new()).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn create_batch_rejects_a_batch_above_the_supported_maximum() -> CatgaResult<()> {
    let (_directory, store) = connect_sqlite().await?;
    let states: Vec<FlowState> = (0..=MAX_FLOW_STORE_BATCH)
        .map(|sequence| batch_state("oversize", sequence))
        .collect();

    let error = store
        .create_batch(states)
        .await
        .expect_err("an oversized batch must be rejected before any write");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[tokio::test]
async fn memory_create_batch_uses_the_sequential_default() -> CatgaResult<()> {
    let store = MemoryFlows::default();
    let states: Vec<FlowState> = (0..8)
        .map(|sequence| batch_state("memory", sequence))
        .collect();

    let created = store.create_batch(states.clone()).await?;

    assert!(created.iter().all(|was_created| *was_created));
    let replayed = store.create_batch(states).await?;
    assert!(replayed.iter().all(|was_created| !*was_created));
    Ok(())
}
