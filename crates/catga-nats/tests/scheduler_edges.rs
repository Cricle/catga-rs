//! Scheduler edge contracts: validation fences, malformed identities, and corrupt index records.
//!
//! These tests pin the behavior a crash-recovering deployment relies on: malformed schedule
//! identities are ignored, corrupt broker records surface as internal errors, and claim leases
//! obey their documented bounds.

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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter, MemoryPackable,
};
use catga_core::flow::{DueFlowScheduler, FlowScheduler};
use catga_core::{CatgaResult, ErrorCode};
use catga_nats::NatsFlowScheduler;
use names::unique;
use nats_server::{server_url, test_error};
use raw_kv::raw_kv;
use record_frames::framed;
use serde::{Deserialize, Serialize};

/// Twin of the store-internal schedule record; field order is the wire contract.
#[derive(Clone, Debug, Deserialize, MemoryPackable, Serialize)]
struct ScheduleRecord {
    version: u8,
    schedule_id: Box<str>,
    flow_id: Box<str>,
    state_id: Box<str>,
    due_at_millis: u64,
    owner: Option<Box<str>>,
    lease_until_millis: Option<u64>,
}

/// Twin of the store-internal schedule index cursor.
#[derive(Clone, Copy, Debug, Default, Deserialize, MemoryPackable, Serialize)]
struct ScheduleIndex {
    tail_page: u64,
    scan_page: u64,
    scan_offset: u32,
}

fn encode<T: MemoryPackSerialize>(value: &T) -> Vec<u8> {
    MemoryPackSerializer::serialize(value).expect("test record must serialize")
}

fn scheduler_target_key(flow_id: &str, state_id: &str) -> String {
    let mut target = Vec::with_capacity(16 + flow_id.len() + state_id.len());
    target.extend_from_slice(&(flow_id.len() as u64).to_be_bytes());
    target.extend_from_slice(flow_id.as_bytes());
    target.extend_from_slice(&(state_id.len() as u64).to_be_bytes());
    target.extend_from_slice(state_id.as_bytes());
    format!("r{}", hex::encode(catga_core::hash::sha256_digest(&target)))
}

/// Twin of the store-internal per-key index marker.
#[derive(Clone, Copy, Debug, Deserialize, MemoryPackable, Serialize)]
struct IndexMarker {
    page: u64,
}

fn scheduler_marker_key(record_key: &str) -> String {
    format!(
        "i{}",
        hex::encode(catga_core::hash::sha256_digest(record_key.as_bytes()))
    )
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn claim_due_validates_lease_and_clock_bounds() -> CatgaResult<()> {
    let scheduler = NatsFlowScheduler::connect(&server_url(), unique("CATGA_SCHED_BOUNDS")).await?;

    assert!(matches!(
        scheduler
            .claim_due("worker", SystemTime::now(), Duration::ZERO, 1)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        scheduler
            .renew_due("worker", "any", SystemTime::now(), Duration::ZERO)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    // A pre-epoch poll instant cannot become a millisecond deadline.
    assert!(matches!(
        scheduler
            .claim_due(
                "worker",
                UNIX_EPOCH.checked_sub(Duration::from_secs(1)).expect("pre-epoch time"),
                Duration::from_secs(1),
                1,
            )
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        scheduler
            .renew_due(
                "worker",
                "any",
                UNIX_EPOCH.checked_sub(Duration::from_secs(1)).expect("pre-epoch time"),
                Duration::from_secs(1),
            )
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    // A lease whose millisecond length overflows u64 is rejected, not wrapped.
    assert!(matches!(
        scheduler
            .claim_due("worker", SystemTime::now(), Duration::MAX, 1)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    // A zero limit is a valid no-op.
    assert!(
        scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(1), 0)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn claim_due_on_an_empty_index_returns_no_work() -> CatgaResult<()> {
    let scheduler = NatsFlowScheduler::connect(&server_url(), unique("CATGA_SCHED_EMPTY")).await?;
    let claimed = scheduler
        .claim_due("worker", SystemTime::now(), Duration::from_secs(30), 4)
        .await?;
    assert!(claimed.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn malformed_schedule_identities_are_ignored_not_errors() -> CatgaResult<()> {
    let scheduler = NatsFlowScheduler::connect(&server_url(), unique("CATGA_SCHED_IDS")).await?;
    for invalid in [
        "",
        "not-a-schedule",
        "r0123:still-not-valid",
        "rzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz:550e8400-e29b-41d4-a716-446655440000",
        "r0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef:not-a-uuid",
    ] {
        assert!(!scheduler.cancel_resume(invalid).await?);
        assert!(!scheduler.ack_due("worker", invalid).await?);
        assert!(!scheduler.release_due("worker", invalid).await?);
        assert!(
            !scheduler
                .renew_due("worker", invalid, SystemTime::now(), Duration::from_secs(1))
                .await?
        );
    }
    // Well-formed but unknown identities are also misses.
    let unknown = format!("r{}:550e8400-e29b-41d4-a716-446655440000", "0".repeat(64));
    assert!(!scheduler.cancel_resume(&unknown).await?);
    assert!(!scheduler.ack_due("worker", &unknown).await?);
    assert!(!scheduler.release_due("worker", &unknown).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn rescheduling_after_cancel_revives_the_deleted_record() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_REVIVE");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket).await?;
    let due = SystemTime::now() + Duration::from_secs(600);

    let first_id = scheduler.schedule_resume("flow-r", "state-r", due).await?;
    assert!(scheduler.cancel_resume(&first_id).await?);
    // A second cancel sees the deleted record as a miss.
    assert!(!scheduler.cancel_resume(&first_id).await?);

    let second_id = scheduler.schedule_resume("flow-r", "state-r", due).await?;
    assert_ne!(first_id, second_id);
    let claimed = scheduler
        .claim_due(
            "worker",
            SystemTime::now() + Duration::from_secs(601),
            Duration::from_secs(30),
            1,
        )
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].schedule_id(), second_id.as_ref());
    assert_eq!(claimed[0].flow_id(), "flow-r");
    assert_eq!(claimed[0].state_id(), "state-r");
    assert!(scheduler.ack_due("worker", &second_id).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn renew_rejects_an_expired_lease_and_ack_fences_other_owners() -> CatgaResult<()> {
    let scheduler = NatsFlowScheduler::connect(&server_url(), unique("CATGA_SCHED_RENEW")).await?;
    let past = SystemTime::now() - Duration::from_secs(1);
    let schedule_id = scheduler.schedule_resume("flow-l", "state-l", past).await?;

    let claimed = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::from_millis(50), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert!(matches!(
        scheduler.ack_due("worker-b", &schedule_id).await,
        Ok(false)
    ));
    assert!(matches!(
        scheduler.release_due("worker-b", &schedule_id).await,
        Ok(false)
    ));
    assert!(matches!(
        scheduler
            .renew_due(
                "worker-b",
                &schedule_id,
                SystemTime::now(),
                Duration::from_secs(30)
            )
            .await,
        Ok(false)
    ));

    // Let the lease lapse: renewal by the former owner must fail.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !scheduler
            .renew_due(
                "worker-a",
                &schedule_id,
                SystemTime::now(),
                Duration::from_secs(30)
            )
            .await?
    );

    // An expired lease is claimable by another owner, who can then renew it.
    let reclaimed = scheduler
        .claim_due("worker-b", SystemTime::now(), Duration::from_secs(60), 1)
        .await?;
    assert_eq!(reclaimed.len(), 1);
    assert!(
        scheduler
            .renew_due(
                "worker-b",
                &schedule_id,
                SystemTime::now(),
                Duration::from_secs(60)
            )
            .await?
    );
    assert!(scheduler.release_due("worker-b", &schedule_id).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_schedule_records_surface_as_internal_errors() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_CORRUPT");
    let store = raw_kv(&bucket).await?;
    let flow_id = unique("flow-corrupt");
    let key = scheduler_target_key(&flow_id, "state-corrupt");
    let schedule_id: Box<str> = format!("{key}:550e8400-e29b-41d4-a716-446655440000").into();

    // An unsupported record version is an internal error, not silent data.
    let unsupported = ScheduleRecord {
        version: 2,
        schedule_id: schedule_id.clone(),
        flow_id: flow_id.as_str().into(),
        state_id: "state-corrupt".into(),
        due_at_millis: 1,
        owner: None,
        lease_until_millis: None,
    };
    store
        .create(&key, framed(&encode(&unsupported)).into())
        .await
        .map_err(|error| test_error("inject unsupported schedule record", error))?;
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    let page: Vec<Box<str>> = vec![key.as_str().into()];
    index
        .create("p0", framed(&encode(&page)).into())
        .await
        .map_err(|error| test_error("inject corrupt schedule index page", error))?;
    index
        .create("m", framed(&encode(&ScheduleIndex::default())).into())
        .await
        .map_err(|error| test_error("inject corrupt schedule index cursor", error))?;
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    assert!(matches!(
        scheduler.claim_due("worker", SystemTime::now() + Duration::from_secs(5), Duration::from_secs(30), 1).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    assert!(matches!(
        scheduler.cancel_resume(&schedule_id).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_index_pages_surface_as_internal_errors() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_BADIDX");
    let index_bucket = format!("{bucket}_IDX");
    let index = raw_kv(&index_bucket).await?;

    // A page holding more than the bounded 32 entries violates the index invariant.
    let oversized: Vec<Box<str>> = (0..33).map(|n| format!("r{n:064}").into()).collect();
    index
        .create("p0", framed(&encode(&oversized)).into())
        .await
        .map_err(|error| test_error("inject oversized index page", error))?;
    index
        .create("m", framed(&encode(&ScheduleIndex::default())).into())
        .await
        .map_err(|error| test_error("inject schedule index cursor", error))?;

    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    assert!(matches!(
        scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(30), 1)
            .await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_cursor_past_the_live_page_entries_advances_and_recovers() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_ADVANCE");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    let due = SystemTime::now() - Duration::from_secs(1);
    let schedule_id = scheduler
        .schedule_resume("flow-adv", "state-adv", due)
        .await?;

    // Move the scan cursor past the single live entry, simulating a drained page.
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    let entry = index
        .entry("m")
        .await
        .map_err(|error| test_error("read schedule index cursor", error))?
        .expect("index cursor must exist after scheduling");
    index
        .update(
            "m",
            framed(&encode(&ScheduleIndex {
                tail_page: 0,
                scan_page: 0,
                scan_offset: 7,
            }))
            .into(),
            entry.revision,
        )
        .await
        .map_err(|error| test_error("move schedule index cursor", error))?;

    // The out-of-range offset yields no candidate; the cursor wraps and the next poll claims.
    let claimed = scheduler
        .claim_due("worker", SystemTime::now(), Duration::from_secs(30), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].schedule_id(), schedule_id.as_ref());
    assert!(scheduler.ack_due("worker", &schedule_id).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn scheduling_the_same_target_twice_conflicts() -> CatgaResult<()> {
    let scheduler = NatsFlowScheduler::connect(&server_url(), unique("CATGA_SCHED_DUP")).await?;
    let due = SystemTime::now() + Duration::from_secs(600);
    let duplicate_id = scheduler
        .schedule_resume("flow-dup", "state-dup", due)
        .await?;
    assert!(matches!(
        scheduler.schedule_resume("flow-dup", "state-dup", due).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    // A claimed schedule cannot be cancelled, and an acknowledged one cannot be acked twice.
    let live = scheduler
        .schedule_resume("flow-live", "state-live", due)
        .await?;
    let claimed = scheduler
        .claim_due("worker", due, Duration::from_secs(30), 8)
        .await?;
    assert_eq!(claimed.len(), 2);
    assert!(!scheduler.cancel_resume(&live).await?);
    assert!(!scheduler.cancel_resume(&duplicate_id).await?);
    assert!(scheduler.ack_due("worker", &live).await?);
    assert!(!scheduler.ack_due("worker", &live).await?);
    assert!(scheduler.ack_due("worker", &duplicate_id).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_deadlines_that_overflow_unix_time_are_validation_errors() -> CatgaResult<()> {
    let scheduler = NatsFlowScheduler::connect(&server_url(), unique("CATGA_SCHED_OFLOW")).await?;
    // The lease fits u64 milliseconds, but now + lease does not.
    let lease = Duration::from_millis(u64::MAX - 1_000_000_000_000);
    assert!(matches!(
        scheduler.claim_due("worker", SystemTime::now(), lease, 1).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        scheduler
            .renew_due("worker", "any", SystemTime::now(), lease)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_delete_marked_index_cursor_halts_claims() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_NOCURSOR");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    let due = SystemTime::now() - Duration::from_secs(1);
    scheduler.schedule_resume("flow-m", "state-m", due).await?;

    // Deleting the cursor key leaves a delete marker; claims see the index as exhausted.
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    index
        .delete("m")
        .await
        .map_err(|error| test_error("delete schedule index cursor", error))?;
    assert!(
        scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(30), 4)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_marker_pointing_at_a_full_stale_page_is_forwarded_to_the_tail() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_FORWARD");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    let due = SystemTime::now() + Duration::from_secs(600);

    // Fill the first index page (32 entries) and spill one more into the tail page.
    for n in 0..33 {
        scheduler
            .schedule_resume(&format!("flow-f{n}"), &format!("state-f{n}"), due)
            .await?;
    }

    // Corrupt the last schedule's marker to point at the full first page.
    let record_key = scheduler_target_key("flow-f32", "state-f32");
    let marker_key = scheduler_marker_key(&record_key);
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    let marker = index
        .entry(&marker_key)
        .await
        .map_err(|error| test_error("read schedule index marker", error))?
        .expect("marker must exist after scheduling");
    index
        .update(
            &marker_key,
            framed(&encode(&IndexMarker { page: 0 })).into(),
            marker.revision,
        )
        .await
        .map_err(|error| test_error("corrupt schedule index marker", error))?;

    // Re-scheduling the same target forwards the marker past the full page, finds the
    // live record in the tail page, and still conflicts at the record level.
    assert!(matches!(
        scheduler.schedule_resume("flow-f32", "state-f32", due).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn an_exhausted_tail_page_rejects_new_index_entries() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_TAILMAX");
    let index_bucket = format!("{bucket}_IDX");
    let index = raw_kv(&index_bucket).await?;

    // Pin the tail at u64::MAX with a full page parked there.
    let full_page: Vec<Box<str>> = (0..32).map(|n| format!("r{n:064}").into()).collect();
    let full_page_key = format!("p{}", u64::MAX);
    index
        .create(&full_page_key, framed(&encode(&full_page)).into())
        .await
        .map_err(|error| test_error("inject full tail page", error))?;
    index
        .create(
            "m",
            framed(&encode(&ScheduleIndex {
                tail_page: u64::MAX,
                scan_page: 0,
                scan_offset: 0,
            }))
            .into(),
        )
        .await
        .map_err(|error| test_error("inject exhausted tail cursor", error))?;
    let record_key = scheduler_target_key("flow-tail", "state-tail");
    index
        .create(
            &scheduler_marker_key(&record_key),
            framed(&encode(&IndexMarker { page: u64::MAX })).into(),
        )
        .await
        .map_err(|error| test_error("inject tail marker", error))?;

    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    assert!(matches!(
        scheduler
            .schedule_resume("flow-tail", "state-tail", SystemTime::now() + Duration::from_secs(60))
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn an_oversized_schedule_record_reports_a_transient_broker_error() -> CatgaResult<()> {
    let scheduler = NatsFlowScheduler::connect(&server_url(), unique("CATGA_SCHED_BIG")).await?;
    // A multi-megabyte flow identifier exceeds the broker payload limit during the
    // record create, which is a broker failure rather than a revision conflict.
    let oversized = "f".repeat(2 * 1024 * 1024);
    assert!(matches!(
        scheduler
            .schedule_resume(&oversized, "state-big", SystemTime::now() + Duration::from_secs(60))
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn delete_marked_index_metadata_exhausts_create_retries() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_METADEL");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    scheduler
        .schedule_resume(
            "flow-a",
            "state-a",
            SystemTime::now() - Duration::from_secs(1),
        )
        .await?;

    // Delete-marking the shared cursor metadata makes every recreate conflict with the
    // marker's revision until the bounded retries exhaust.
    raw_kv(&format!("{bucket}_IDX"))
        .await?
        .delete("m")
        .await
        .map_err(|error| test_error("delete scheduler index metadata", error))?;
    assert!(matches!(
        scheduler
            .schedule_resume("flow-b", "state-b", SystemTime::now())
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn scheduling_and_claiming_stay_consistent_under_contention() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_STORM");
    let scheduler = Arc::new(NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?);

    // Thirty-two racers create the shared index metadata, markers, and pages at once.
    let due = SystemTime::now() - Duration::from_secs(1);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32_u32 {
        let scheduler = Arc::clone(&scheduler);
        tasks.spawn(async move {
            let flow_id = format!("flow-{index}");
            scheduler.schedule_resume(&flow_id, "state", due).await
        });
    }
    let mut scheduled = 0_usize;
    while let Some(result) = tasks.join_next().await {
        match result.expect("schedule task must not panic") {
            Ok(_) => scheduled += 1,
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    assert!(scheduled > 0);

    // Sixteen claimers race the scan cursor and the same candidate records in the
    // background while the foreground drains every remaining schedule. Leases are short
    // because claim_due is not atomic: a transient cursor failure discards the
    // already-committed claims of that call, which only become reclaimable after expiry.
    let claimed = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..16_u32 {
        let scheduler = Arc::clone(&scheduler);
        let claimed = Arc::clone(&claimed);
        tasks.spawn(async move {
            for _ in 0..4 {
                match scheduler
                    .claim_due(
                        format!("owner-{index}").as_str(),
                        SystemTime::now(),
                        Duration::from_millis(900),
                        8,
                    )
                    .await
                {
                    Ok(batch) => {
                        claimed
                            .lock()
                            .await
                            .extend(batch.iter().map(|resume| resume.schedule_id().to_owned()));
                    }
                    Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
                }
            }
        });
    }
    for _ in 0..80 {
        match scheduler
            .claim_due(
                "owner-foreground",
                SystemTime::now(),
                Duration::from_millis(900),
                8,
            )
            .await
        {
            Ok(batch) => {
                claimed
                    .lock()
                    .await
                    .extend(batch.iter().map(|resume| resume.schedule_id().to_owned()));
            }
            // Cursor contention can exhaust the bounded CAS retries; wait out the leases
            // of any claims that call dropped, then keep draining.
            Err(error) => {
                assert_eq!(error.code(), ErrorCode::Transient);
                tokio::time::sleep(Duration::from_millis(1_200)).await;
            }
        }
        if claimed.lock().await.len() == scheduled {
            break;
        }
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("claim task must not panic");
    }
    assert_eq!(claimed.lock().await.len(), scheduled);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn broker_write_failures_are_transient_and_verified() -> CatgaResult<()> {
    // A bucket that admits nothing rejects the schedule create outright.
    let bucket = unique("CATGA_SCHED_CAP0");
    raw_capped_kv::raw_kv_with_byte_cap(&bucket, 1).await?;
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    assert!(matches!(
        scheduler
            .schedule_resume("flow-wide", "state", SystemTime::now())
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));

    // A bucket that fills up later rejects the claim rewrite: stamping an owner grows the
    // record, and a growing rewrite no longer fits. Shrinking writes and delete markers
    // stay admissible, so only the claim path can surface a storage failure.
    let bucket = unique("CATGA_SCHED_CAPFILL");
    let raw = raw_capped_kv::raw_kv_with_byte_cap(&bucket, 4_096).await?;
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    scheduler
        .schedule_resume(
            "flow-cap",
            "state",
            SystemTime::now() - Duration::from_secs(1),
        )
        .await?;
    full_bucket::fill_bucket(&raw).await?;
    assert!(matches!(
        scheduler
            .claim_due(
                "owner-with-a-much-longer-name",
                SystemTime::now(),
                Duration::from_secs(600),
                1,
            )
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn concurrent_settlements_conflict_and_only_one_wins() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_SETTLERACE");
    let scheduler = Arc::new(NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?);
    let schedule_id = scheduler
        .schedule_resume(
            "flow-settle",
            "state",
            SystemTime::now() - Duration::from_secs(1),
        )
        .await?;
    let claimed = scheduler
        .claim_due("owner", SystemTime::now(), Duration::from_secs(600), 1)
        .await?;
    assert_eq!(claimed.len(), 1);

    // Eight acknowledgers race one claimed record: exactly one delete lands.
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let scheduler = Arc::clone(&scheduler);
        let schedule_id = schedule_id.clone();
        tasks.spawn(async move { scheduler.ack_due("owner", &schedule_id).await });
    }
    let mut acked = 0_usize;
    while let Some(result) = tasks.join_next().await {
        match result.expect("ack task must not panic") {
            Ok(true) => acked += 1,
            Ok(false) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    assert_eq!(acked, 1);

    // Reschedule, reclaim, and race releases: exactly one lease rewrite lands.
    let schedule_id = scheduler
        .schedule_resume(
            "flow-settle",
            "state",
            SystemTime::now() - Duration::from_secs(1),
        )
        .await?;
    let mut released = 0_usize;
    for _ in 0..64 {
        let batch = scheduler
            .claim_due("owner", SystemTime::now(), Duration::from_secs(600), 1)
            .await?;
        if !batch.is_empty() {
            break;
        }
    }
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let scheduler = Arc::clone(&scheduler);
        let schedule_id = schedule_id.clone();
        tasks.spawn(async move { scheduler.release_due("owner", &schedule_id).await });
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("release task must not panic") {
            Ok(true) => released += 1,
            Ok(false) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    assert_eq!(released, 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn delete_marked_markers_exhaust_index_create_retries() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_MARKERDEL");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;

    // Delete-marking the per-target marker makes every revision-zero recreate conflict with
    // the marker's revision until the bounded retries exhaust.
    let index = raw_kv(&format!("{bucket}_IDX")).await?;
    let marker = scheduler_marker_key(&scheduler_target_key("flow-marked", "state"));
    index
        .create(
            marker.as_str(),
            framed(&encode(&IndexMarker { page: 0 })).into(),
        )
        .await
        .map_err(|error| test_error("seed scheduler index marker", error))?;
    index
        .delete(marker.as_str())
        .await
        .map_err(|error| test_error("delete scheduler index marker", error))?;
    assert!(matches!(
        scheduler
            .schedule_resume("flow-marked", "state", SystemTime::now())
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_full_index_bucket_fails_marker_creates_transiently() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_IDXFULL");
    let raw = raw_capped_kv::raw_kv_with_byte_cap(&format!("{bucket}_IDX"), 4_096).await?;
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    scheduler
        .schedule_resume(
            "flow-seed",
            "state",
            SystemTime::now() - Duration::from_secs(1),
        )
        .await?;
    full_bucket::fill_bucket(&raw).await?;

    // The shared cursor metadata already exists, so the next schedule must create a fresh
    // marker — and the full index bucket rejects that create outright.
    assert!(matches!(
        scheduler
            .schedule_resume("flow-blocked", "state", SystemTime::now())
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_delete_marked_scan_page_yields_no_candidates() -> CatgaResult<()> {
    let bucket = unique("CATGA_SCHED_PAGEDELSCAN");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    scheduler
        .schedule_resume(
            "flow-hidden",
            "state",
            SystemTime::now() - Duration::from_secs(1),
        )
        .await?;

    // Delete-marking the page the scan cursor points at leaves no candidate to examine; the
    // cursor advances and the claim drains empty instead of failing.
    raw_kv(&format!("{bucket}_IDX"))
        .await?
        .delete("p0")
        .await
        .map_err(|error| test_error("delete scheduler index page", error))?;
    assert!(
        scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(30), 4)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_sealed_bucket_surfaces_deletes_as_transient_failures() -> CatgaResult<()> {
    // Sealing the stream rejects every further write, so the acknowledgement delete marker
    // cannot land; the store verifies the record survived and reports a transient failure.
    let bucket = unique("CATGA_SCHED_SEALED");
    let scheduler = NatsFlowScheduler::connect(&server_url(), bucket.as_str()).await?;
    let schedule_id = scheduler
        .schedule_resume(
            "flow-acked",
            "state",
            SystemTime::now() - Duration::from_secs(1),
        )
        .await?;
    let claimed = scheduler
        .claim_due("worker", SystemTime::now(), Duration::from_secs(600), 1)
        .await?;
    assert_eq!(claimed.len(), 1);

    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw stream client", error))?;
    let context = async_nats::jetstream::new(client);
    let mut config = context
        .get_stream(format!("KV_{bucket}"))
        .await
        .map_err(|error| test_error("open scheduler bucket stream", error))?
        .get_info()
        .await
        .map_err(|error| test_error("read scheduler bucket stream info", error))?
        .config;
    config.sealed = true;
    context
        .update_stream(config)
        .await
        .map_err(|error| test_error("seal the scheduler bucket stream", error))?;

    assert!(matches!(
        scheduler.ack_due("worker", &schedule_id).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}
