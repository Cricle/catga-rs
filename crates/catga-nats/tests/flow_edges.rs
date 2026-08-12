//! Flow-store edge contracts: delete-marked states, type-index corruption and recovery,
//! and broker write failures.
//!
//! The paged type index is a hint structure: every claim reloads the authoritative state
//! record, so missing or corrupt index pieces must degrade predictably — claims drain,
//! recreates either recover or report a transient error, and oversized pages are loud.

#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_capped_kv.rs"]
mod raw_capped_kv;
#[path = "support/raw_kv.rs"]
mod raw_kv;
#[path = "support/record_frames.rs"]
mod record_frames;

use std::time::Duration;

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter, MemoryPackable,
};
use catga_core::flow::{FlowState, FlowStore};
use catga_core::{CatgaResult, ErrorCode};
use catga_nats::NatsFlows;
use names::unique;
use nats_server::{server_url, test_error};
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;
use record_frames::framed;
use serde::{Deserialize, Serialize};

/// Twin of the store-internal flow type index cursor.
#[derive(Clone, Copy, Debug, Default, Deserialize, MemoryPackable, Serialize)]
struct TypeIndex {
    tail_page: u64,
    scan_page: u64,
    scan_offset: u32,
}

fn encode<T: MemoryPackSerialize>(value: &T) -> Vec<u8> {
    MemoryPackSerializer::serialize(value).expect("test record must serialize")
}

fn flow_key(id: &str) -> String {
    format!(
        "f{}",
        hex::encode(catga_core::hash::sha256_digest(id.as_bytes()))
    )
}

fn type_metadata_key(flow_type: &str) -> String {
    format!(
        "m{}",
        hex::encode(catga_core::hash::sha256_digest(flow_type.as_bytes()))
    )
}

fn type_page_key(flow_type: &str, page: u64) -> String {
    format!(
        "p{}.{page}",
        hex::encode(catga_core::hash::sha256_digest(flow_type.as_bytes()))
    )
}

fn type_marker_key(flow_type: &str, id: &str) -> String {
    format!(
        "i{}.{}",
        hex::encode(catga_core::hash::sha256_digest(flow_type.as_bytes())),
        hex::encode(catga_core::hash::sha256_digest(id.as_bytes()))
    )
}

fn running(id: &str, flow_type: &str, data: &[u8]) -> FlowState {
    FlowState::new(id, flow_type, data.to_vec(), "owner-a")
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn delete_marked_states_read_as_missing() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_GONE");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    assert!(flows.create(running("flow-gone", "order", b"")).await?);

    raw_kv(&bucket)
        .await?
        .delete(flow_key("flow-gone"))
        .await
        .map_err(|error| test_error("delete flow state", error))?;

    assert_eq!(flows.get("flow-gone").await?, None);
    assert!(
        !flows
            .update(0, running("flow-gone", "order", b"").next_version()?)
            .await?
    );
    assert!(!flows.heartbeat("flow-gone", "owner-a", 0).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn terminal_flows_are_pruned_from_the_type_index() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_PRUNE");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    assert!(flows.create(running("flow-done", "order", b"")).await?);

    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    assert!(
        index
            .entry(&type_marker_key("order", "flow-done"))
            .await
            .map_err(|error| test_error("read flow marker", error))?
            .is_some()
    );

    // Completing the flow removes its index marker.
    let done = running("flow-done", "order", b"").done(1).next_version()?;
    assert!(flows.update(0, done).await?);
    let marker = index
        .entry(&type_marker_key("order", "flow-done"))
        .await
        .map_err(|error| test_error("read pruned flow marker", error))?
        .expect("a pruned marker remains as a delete marker");
    assert!(matches!(
        marker.operation,
        async_nats::jetstream::kv::Operation::Delete | async_nats::jetstream::kv::Operation::Purge
    ));

    // A duplicate create of the completed flow is fenced without touching the index.
    assert!(!flows.create(running("flow-done", "order", b"")).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn pruning_tolerates_missing_markers_and_deleted_pages() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_PRUNEEDGE");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    let index = raw_kv(&format!("{bucket}_IDX")).await?;

    // A flow created terminal was never indexed; its duplicate create prunes a
    // marker that never existed.
    let terminal = running("flow-terminal", "order", b"").done(0);
    assert!(flows.create(terminal.clone()).await?);
    assert!(!flows.create(terminal).await?);

    // A type mismatch on the same identity is fenced before any index work.
    assert!(
        !flows
            .create(running("flow-terminal", "refund", b""))
            .await?
    );

    // A deleted marker is a no-op prune.
    assert!(flows.create(running("flow-m", "order", b"")).await?);
    index
        .delete(type_marker_key("order", "flow-m"))
        .await
        .map_err(|error| test_error("delete flow marker", error))?;
    let done = running("flow-m", "order", b"").done(1).next_version()?;
    assert!(flows.update(0, done).await?);

    // A deleted page is skipped during pruning; the marker still goes away.
    assert!(flows.create(running("flow-p", "order", b"")).await?);
    index
        .delete(type_page_key("order", 0))
        .await
        .map_err(|error| test_error("delete flow index page", error))?;
    let done = running("flow-p", "order", b"").done(1).next_version()?;
    assert!(flows.update(0, done).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn claims_drain_when_index_metadata_or_pages_are_delete_marked() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_DRAIN");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    assert!(flows.create(running("flow-c", "order", b"")).await?);
    let index = raw_kv(&format!("{bucket}_IDX")).await?;

    // Without index metadata the type reads as having no claimable work.
    index
        .delete(type_metadata_key("order"))
        .await
        .map_err(|error| test_error("delete flow index metadata", error))?;
    assert!(
        flows
            .try_claim("order", "worker", Duration::from_secs(3600))
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn claims_skip_delete_marked_pages_and_fresh_heartbeats() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_SKIP");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    assert!(flows.create(running("flow-s", "order", b"")).await?);
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    index
        .delete(type_page_key("order", 0))
        .await
        .map_err(|error| test_error("delete flow index page", error))?;

    // The scan cursor walks past the missing page and drains.
    assert!(
        flows
            .try_claim("order", "worker", Duration::ZERO)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn an_oversized_index_page_is_an_internal_error() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_BADPAGE");
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    let oversized: Vec<Box<str>> = (0..33).map(|n| format!("flow-{n}").into()).collect();
    index
        .create(
            type_page_key("order", 0),
            framed(&encode(&oversized)).into(),
        )
        .await
        .map_err(|error| test_error("inject oversized flow index page", error))?;
    index
        .create(
            type_metadata_key("order"),
            framed(&encode(&TypeIndex::default())).into(),
        )
        .await
        .map_err(|error| test_error("inject flow index metadata", error))?;

    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    assert!(matches!(
        flows.try_claim("order", "worker", Duration::ZERO).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn recreating_index_components_on_delete_marked_keys_fails_transiently() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_RECREATE");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    assert!(flows.create(running("flow-r", "order", b"")).await?);
    let index = raw_kv(&format!("{bucket}_IDX")).await?;

    // Delete-marking the shared type metadata makes the next index write exhaust its
    // create retries against the marker's revision.
    index
        .delete(type_metadata_key("order"))
        .await
        .map_err(|error| test_error("delete flow index metadata", error))?;
    assert!(matches!(
        flows.create(running("flow-r2", "order", b"")).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn broker_write_failures_are_transient_not_conflicts() -> CatgaResult<()> {
    // The bucket budget admits a small record but not a larger create or rewrite.
    let bucket = unique("CATGA_FLOW_CAPPED");
    raw_kv_with_byte_cap(&bucket, 2_500).await?;
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;

    // The create publish itself is rejected.
    assert!(matches!(
        flows.create(running("flow-big", "order", &vec![0xEE; 4_000])).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));

    // A rewrite that crosses the byte budget reports the storage failure.
    assert!(
        flows
            .create(running("flow-small", "order", &vec![0x01; 800]))
            .await?
    );
    assert!(matches!(
        flows
            .update(0, running("flow-small", "order", &vec![0x02; 2_000]).next_version()?)
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

/// Twin of the store-internal per-flow index marker.
#[derive(Clone, Copy, Debug, Deserialize, MemoryPackable, Serialize)]
struct IndexMarker {
    page: u64,
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn delete_marked_markers_exhaust_index_create_retries() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_MARKDEL");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    let index = raw_kv(&format!("{bucket}_IDX")).await?;

    // A delete-marked marker can never be superseded by the revision-zero recreate.
    index
        .create(
            type_marker_key("order", "flow-dm"),
            framed(&encode(&IndexMarker { page: 0 })).into(),
        )
        .await
        .map_err(|error| test_error("seed flow marker", error))?;
    index
        .delete(type_marker_key("order", "flow-dm"))
        .await
        .map_err(|error| test_error("delete flow marker", error))?;
    assert!(matches!(
        flows.create(running("flow-dm", "order", b"")).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn delete_marked_pages_exhaust_index_create_retries() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_PAGEDEL");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;
    let index = raw_kv(&format!("{bucket}_IDX")).await?;

    // A delete-marked first page can never be superseded by the revision-zero recreate.
    index
        .create(
            type_page_key("order", 0),
            framed(&encode(&vec![Box::<str>::from("flow-seed")])).into(),
        )
        .await
        .map_err(|error| test_error("seed flow index page", error))?;
    index
        .delete(type_page_key("order", 0))
        .await
        .map_err(|error| test_error("delete flow index page", error))?;
    assert!(matches!(
        flows.create(running("flow-dp", "order", b"")).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn the_type_index_pages_over_after_thirty_two_flows() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_PAGING");
    let flows = NatsFlows::connect(&server_url(), bucket.as_str()).await?;

    // The thirty-third flow of one type rolls the tail page forward.
    for index in 0..33_u32 {
        let id = format!("flow-page-{index}");
        assert!(flows.create(running(&id, "paged", b"")).await?);
    }
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    assert!(
        index
            .entry(&type_page_key("paged", 1))
            .await
            .map_err(|error| test_error("read second flow index page", error))?
            .is_some()
    );

    // Every flow across both pages is claimable.
    let mut claimed = 0_usize;
    while let Some(state) = flows.try_claim("paged", "worker", Duration::ZERO).await? {
        let expected = state.version();
        let done = state.done(0).next_version()?;
        assert!(flows.update(expected, done).await?);
        claimed += 1;
    }
    assert_eq!(claimed, 33);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn index_contention_settles_every_writer_and_claimer() -> CatgaResult<()> {
    let bucket = unique("CATGA_FLOW_STORM");
    let flows = std::sync::Arc::new(NatsFlows::connect(&server_url(), bucket.as_str()).await?);

    // Creators race the shared type index: metadata, markers, and the single first page.
    // A create that exhausts its bounded CAS retries reports Transient and is retried.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..24_u32 {
        let flows = std::sync::Arc::clone(&flows);
        tasks.spawn(async move {
            let id = format!("flow-storm-{index}");
            for _ in 0..4 {
                match flows.create(running(&id, "storm", b"")).await {
                    Err(error) if error.code() == ErrorCode::Transient => continue,
                    outcome => return outcome,
                }
            }
            flows.create(running(&id, "storm", b"")).await
        });
    }
    let mut created = 0_usize;
    while let Some(result) = tasks.join_next().await {
        match result.expect("create task must not panic") {
            Ok(true) => created += 1,
            Ok(false) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    assert_eq!(created, 24);

    // Claimers race the scan cursor and the same candidate records.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..16_u32 {
        let flows = std::sync::Arc::clone(&flows);
        tasks.spawn(async move {
            let owner = format!("owner-{index}");
            flows.try_claim("storm", &owner, Duration::ZERO).await
        });
    }
    let mut claimed = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result.expect("claim task must not panic") {
            Ok(Some(state)) => claimed.push(state),
            Ok(None) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }

    // Completing the claimed flows races their pruning compare-and-sets.
    let mut tasks = tokio::task::JoinSet::new();
    for state in claimed {
        let flows = std::sync::Arc::clone(&flows);
        tasks.spawn(async move {
            let expected = state.version();
            let done = state.done(0).next_version()?;
            flows.update(expected, done).await
        });
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("update task must not panic") {
            Ok(_) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    Ok(())
}
