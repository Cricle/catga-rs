//! Suspended-flow edge contracts: mutation fences, wait-correlation index corruption,
//! broker write failures, and timeout-poll message handling.
//!
//! The correlation index is rebuilt from authoritative continuation records, so corrupt
//! index entries must be loud internal errors while missing or stale entries clean up
//! silently. Timeout polls must settle every delivery exactly once: empty and
//! non-suspended records ack away, future deadlines nack with the remaining delay, and
//! over-limit deliveries nack for immediate redelivery.

#[path = "support/full_bucket.rs"]
mod full_bucket;
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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter, MemoryPackable,
};
use catga_core::flow::{
    FlowContinuation, FlowState, SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowStore,
    WaitCondition, WaitPolicy,
};
use catga_core::{CatgaResult, ErrorCode};
use catga_nats::NatsSuspendedFlows;
use names::unique;
use nats_server::{server_url, test_error};
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;
use record_frames::framed;
use serde::{Deserialize, Serialize};

/// Twin of the store-internal wait-correlation index record.
#[derive(Clone, Debug, Deserialize, MemoryPackable, Serialize)]
struct WaitCorrelationIndex {
    correlation_id: Box<str>,
    flow_ids: Vec<Box<str>>,
}

fn encode<T: MemoryPackSerialize>(value: &T) -> Vec<u8> {
    MemoryPackSerializer::serialize(value).expect("test record must serialize")
}

fn correlation_key(correlation_id: &str) -> String {
    format!(
        "c{}",
        hex::encode(catga_core::hash::sha256_digest(correlation_id.as_bytes()))
    )
}

fn waiting(flow_id: &str, correlation_id: &str, timeout: Duration) -> FlowContinuation {
    let state = FlowState::new(flow_id, "order", Vec::<u8>::new(), "owner-a").suspended();
    FlowContinuation::waiting(
        state,
        "wait-step",
        WaitCondition::new(
            correlation_id,
            WaitPolicy::All,
            1,
            SystemTime::now(),
            timeout,
        ),
    )
}

async fn connect(bucket: &str) -> CatgaResult<NatsSuspendedFlows> {
    NatsSuspendedFlows::connect(&server_url(), bucket).await
}

async fn inject_index(
    bucket: &str,
    correlation_id: &str,
    index: &WaitCorrelationIndex,
) -> CatgaResult<()> {
    raw_kv(&format!("{bucket}_IDX"))
        .await?
        .put(
            correlation_key(correlation_id),
            framed(&encode(index)).into(),
        )
        .await
        .map_err(|error| test_error("inject wait-correlation index", error))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn mutations_fence_missing_deleted_and_stale_records() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_FENCE");
    let store = connect(&bucket).await?;

    // Missing records are misses for every mutation.
    assert!(
        !store
            .record_wait_success("missing", 0, "child", vec![1])
            .await?
    );
    assert!(!store.heartbeat("missing", "owner-a", 0).await?);

    // Deleted records are misses too.
    let continuation = waiting("flow-del", "corr-del", Duration::from_secs(600));
    assert!(store.create(continuation).await?);
    assert!(store.delete("flow-del", 0).await?);
    assert!(!store.delete("flow-del", 0).await?);
    assert!(
        !store
            .record_wait_failure(
                "flow-del",
                0,
                "child",
                catga_core::CatgaError::new(ErrorCode::Internal, "gone"),
            )
            .await?
    );

    // A stale expected version is fenced.
    let continuation = waiting("flow-ver", "corr-ver", Duration::from_secs(600));
    assert!(store.create(continuation).await?);
    assert!(
        !store
            .record_wait_success("flow-ver", 9, "child", vec![1])
            .await?
    );
    // A result for the same child twice is an idempotent no-op.
    assert!(
        store
            .record_wait_success("flow-ver", 0, "child", vec![1])
            .await?
    );
    assert!(
        store
            .record_wait_success("flow-ver", 0, "child", vec![1])
            .await?
    );
    // A suspended flow has no owner: heartbeats are fenced for everyone.
    assert!(!store.heartbeat("flow-ver", "owner-b", 0).await?);
    assert!(!store.heartbeat("flow-ver", "owner-a", 0).await?);

    // A running continuation with a live owner heartbeats normally.
    let running = FlowContinuation::new(
        FlowState::new("flow-run", "order", Vec::<u8>::new(), "owner-a"),
        "step",
    );
    assert!(store.create(running).await?);
    assert!(store.heartbeat("flow-run", "owner-a", 0).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_correlation_indexes_are_internal_errors() -> CatgaResult<()> {
    let mut cases: Vec<(&str, WaitCorrelationIndex)> = vec![
        (
            "mismatched identity",
            WaitCorrelationIndex {
                correlation_id: "other".into(),
                flow_ids: vec!["flow-1".into()],
            },
        ),
        (
            "no candidates",
            WaitCorrelationIndex {
                correlation_id: "corr".into(),
                flow_ids: vec![],
            },
        ),
        (
            "too many candidates",
            WaitCorrelationIndex {
                correlation_id: "corr".into(),
                flow_ids: (0..17).map(|n| format!("flow-{n}").into()).collect(),
            },
        ),
        (
            "duplicate candidates",
            WaitCorrelationIndex {
                correlation_id: "corr".into(),
                flow_ids: vec!["flow-1".into(), "flow-1".into()],
            },
        ),
        (
            "empty candidate",
            WaitCorrelationIndex {
                correlation_id: "corr".into(),
                flow_ids: vec!["".into()],
            },
        ),
    ];
    for (name, index) in cases.drain(..) {
        let bucket = unique("CATGA_SUSP_CORRUPT");
        let store = connect(&bucket).await?;
        inject_index(&bucket, "corr", &index).await?;
        assert!(
            matches!(
                store.get_by_wait_correlation("corr").await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name} must surface an internal decode error"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn recreating_a_delete_marked_correlation_index_exhausts_its_retries() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_RECREATE");
    let store = connect(&bucket).await?;

    let first = waiting("flow-a", "corr-recycle", Duration::from_secs(600));
    assert!(store.create(first).await?);
    // Deleting the flow removes the single-candidate index with a delete marker.
    assert!(store.delete("flow-a", 0).await?);

    // A create-key write cannot supersede the delete marker, so registration fails.
    let second = waiting("flow-b", "corr-recycle", Duration::from_secs(600));
    assert!(matches!(
        store.create(second).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn unregistering_tolerates_missing_and_stale_index_entries() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_UNREG");
    let store = connect(&bucket).await?;
    let index_bucket = format!("{bucket}_IDX");

    // A delete-marked previous correlation cleans up as a no-op.
    let continuation = waiting("flow-x", "corr-x", Duration::from_secs(600));
    assert!(store.create(continuation.clone()).await?);
    raw_kv(&index_bucket)
        .await?
        .delete(correlation_key("corr-x"))
        .await
        .map_err(|error| test_error("delete correlation index", error))?;
    let moved = FlowContinuation::waiting(
        continuation.state().clone().next_version()?,
        continuation.step_name(),
        WaitCondition::new(
            "corr-x2",
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(600),
        ),
    );
    assert!(store.update(0, moved).await?);

    // An index that no longer lists the flow is also a no-op.
    let second = waiting("flow-y", "corr-y", Duration::from_secs(600));
    assert!(store.create(second.clone()).await?);
    inject_index(
        &bucket,
        "corr-y",
        &WaitCorrelationIndex {
            correlation_id: "corr-y".into(),
            flow_ids: vec!["flow-z".into()],
        },
    )
    .await?;
    let moved = FlowContinuation::waiting(
        second.state().clone().next_version()?,
        second.step_name(),
        WaitCondition::new(
            "corr-y2",
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(600),
        ),
    );
    assert!(store.update(0, moved).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn broker_write_failures_are_transient() -> CatgaResult<()> {
    // The store bucket admits the initial record but not a larger rewrite: the byte
    // check runs before the superseded revision is discarded.
    let bucket = unique("CATGA_SUSP_CAPPED");
    raw_kv_with_byte_cap(&bucket, 1_200).await?;
    let store = connect(&bucket).await?;
    let continuation = FlowContinuation::new(
        FlowState::new("flow-cap", "order", vec![0x01; 200], "owner-a"),
        "step",
    );
    assert!(store.create(continuation).await?);
    let grown = FlowContinuation::new(
        FlowState::new("flow-cap", "order", vec![0x02; 500], "owner-a").next_version()?,
        "step",
    );
    assert!(matches!(
        store.update(0, grown).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));

    // The index bucket rejects an oversized correlation registration outright.
    let indexed_bucket = unique("CATGA_SUSP_CAPI");
    raw_kv_with_byte_cap(&format!("{indexed_bucket}_IDX"), 400).await?;
    let indexed = connect(&indexed_bucket).await?;
    let oversized = waiting("flow-wide", &"c".repeat(600), Duration::from_secs(600));
    assert!(matches!(
        indexed.create(oversized).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn timeout_polls_settle_every_delivery_shape() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_POLL");
    let store = connect(&bucket).await?;

    // A non-suspended record carries no deadline and is acknowledged away.
    let running = FlowContinuation::new(
        FlowState::new("flow-run", "order", Vec::<u8>::new(), "owner-a"),
        "step",
    );
    assert!(store.create(running).await?);
    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(SystemTime::now(), 4, 8)?)
        .await?;
    assert!(receipts.is_empty());

    // A future deadline is negatively acknowledged with the remaining delay.
    let future = waiting("flow-future", "corr-future", Duration::from_secs(600));
    assert!(store.create(future).await?);
    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(SystemTime::now(), 4, 8)?)
        .await?;
    assert!(receipts.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn timeout_polls_bound_receipts_and_validate_the_clock() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_BOUND");
    let store = connect(&bucket).await?;
    for (flow, correlation) in [("flow-t1", "corr-t1"), ("flow-t2", "corr-t2")] {
        let mut continuation = waiting(flow, correlation, Duration::from_millis(1));
        // Backdate the wait so its deadline has already passed.
        continuation = continuation.with_wait(WaitCondition::new(
            correlation,
            WaitPolicy::All,
            1,
            SystemTime::now() - Duration::from_secs(10),
            Duration::from_millis(1),
        ));
        assert!(store.create(continuation).await?);
    }

    // The first over-limit delivery is nacked for immediate redelivery.
    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(SystemTime::now(), 1, 8)?)
        .await?;
    assert_eq!(receipts.len(), 1);
    store.ack_timed_out(&receipts[0]).await?;

    // Poll clock bounds are validation errors.
    let before_epoch = UNIX_EPOCH
        .checked_sub(Duration::from_secs(1))
        .expect("pre-epoch time");
    assert!(matches!(
        store
            .poll_timed_out(&TimedOutFlowPoll::new(before_epoch, 1, 8)?)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn timeout_polls_acknowledge_empty_messages() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_EMPTY");
    let store = connect(&bucket).await?;

    // A message the KV writer never produced lands in the stream and is acked away.
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw timeout publisher", error))?;
    client
        .publish(format!("$KV.{bucket}.orphan"), Vec::<u8>::new().into())
        .await
        .map_err(|error| test_error("publish empty timeout message", error))?;
    client
        .flush()
        .await
        .map_err(|error| test_error("flush empty timeout message", error))?;

    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(SystemTime::now(), 4, 8)?)
        .await?;
    assert!(receipts.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn unregistering_a_purged_correlation_is_a_noop() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_PURGED");
    let store = connect(&bucket).await?;
    let continuation = waiting("flow-purged", "corr-purged", Duration::from_secs(600));
    assert!(store.create(continuation).await?);

    // Purging the index entry entirely (not a delete marker) leaves nothing to clean up.
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw index client", error))?;
    let stream = async_nats::jetstream::new(client)
        .get_stream(format!("KV_{bucket}_IDX"))
        .await
        .map_err(|error| test_error("open wait-correlation index stream", error))?;
    stream
        .purge()
        .filter(format!(
            "$KV.{bucket}_IDX.{}",
            correlation_key("corr-purged")
        ))
        .await
        .map_err(|error| test_error("purge wait-correlation index entry", error))?;

    assert!(store.delete("flow-purged", 0).await?);
    assert!(store.get("flow-purged").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn concurrent_deletes_conflict_then_read_the_terminal_marker() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_DELRACE");
    let store = std::sync::Arc::new(connect(&bucket).await?);
    let continuation = waiting("flow-race", "corr-race", Duration::from_secs(600));
    assert!(store.create(continuation).await?);

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let store = std::sync::Arc::clone(&store);
        tasks.spawn(async move { store.delete("flow-race", 0).await });
    }
    let mut deleted = 0_usize;
    while let Some(result) = tasks.join_next().await {
        match result.expect("delete task must not panic") {
            Ok(true) => deleted += 1,
            Ok(false) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    assert_eq!(deleted, 1);
    assert!(store.get("flow-race").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_full_bucket_fails_rewrites_transiently_but_still_deletes() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_CAPFILL");
    let raw = raw_kv_with_byte_cap(&bucket, 4_096).await?;
    let store = connect(&bucket).await?;
    let continuation = waiting("flow-full", "corr-full", Duration::from_secs(600));
    assert!(store.create(continuation).await?);
    full_bucket::fill_bucket(&raw).await?;

    // A changed rewrite no longer fits and the broker proves it never committed.
    assert!(matches!(
        store
            .record_wait_success("flow-full", 0, "child", vec![1])
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    // The failed rewrite left the original continuation intact.
    assert!(store.get("flow-full").await?.is_some());
    // Delete markers are exempt from the byte cap: settling stays possible.
    assert!(store.delete("flow-full", 0).await?);
    assert!(store.get("flow-full").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn correlation_and_result_contention_settles_every_candidate() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUSP_STORM");
    let store = std::sync::Arc::new(connect(&bucket).await?);

    // Creators race to extend one shared correlation index. A create that exhausts its
    // bounded CAS retries reports Transient and is retried.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..8_u32 {
        let store = std::sync::Arc::clone(&store);
        tasks.spawn(async move {
            let flow_id = format!("flow-c{index}");
            for _ in 0..4 {
                match store
                    .create(waiting(&flow_id, "corr-storm", Duration::from_secs(600)))
                    .await
                {
                    Err(error) if error.code() == ErrorCode::Transient => continue,
                    outcome => return outcome,
                }
            }
            store
                .create(waiting(&flow_id, "corr-storm", Duration::from_secs(600)))
                .await
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
    assert_eq!(created, 8);

    // Concurrent result recording races the per-record compare-and-set; once the single
    // expected result lands, every duplicate is an idempotent no-op.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..16_u32 {
        let store = std::sync::Arc::clone(&store);
        tasks.spawn(async move {
            let flow_id = format!("flow-c{}", index % 8);
            let child_id = format!("child-{index}");
            store
                .record_wait_success(&flow_id, 0, &child_id, vec![index as u8])
                .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("record task must not panic") {
            Ok(_) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }

    // Claims race to move two candidates off the shared correlation index.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..2_u32 {
        for round in 0..4_u32 {
            let store = std::sync::Arc::clone(&store);
            tasks.spawn(async move {
                let flow_id = format!("flow-c{index}");
                let Some(expected) = store.get(&flow_id).await? else {
                    return Ok(false);
                };
                let moved = FlowContinuation::waiting(
                    expected.state().clone().next_version()?,
                    expected.step_name(),
                    WaitCondition::new(
                        format!("corr-moved-{index}-{round}"),
                        WaitPolicy::All,
                        1,
                        SystemTime::now(),
                        Duration::from_secs(600),
                    ),
                );
                store.claim(&expected, moved).await
            });
        }
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("claim task must not panic") {
            Ok(_) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    Ok(())
}
