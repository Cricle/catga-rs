//! Strict unit tests for [`SorockApp`]: state-machine adaptation, the
//! snapshot store protocol, and error propagation, driven directly through
//! sorock's public `RaftApp` trait without a running cluster.

#[path = "common/failing_machine.rs"]
mod failing_machine;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use catga_core::{CatgaError, ErrorCode};
use catga_sorock::SorockApp;
use failing_machine::FailingMachine;
use futures::StreamExt;
use recording_machine::RecordingMachine;
use sorock::process::{RaftApp, SnapshotStream};

/// sorock seeds every fresh log with an implicit snapshot at index 1.
const GENESIS: u64 = 1;

/// `SorockApp` streams snapshots in chunks of at most 256 KiB.
const MAX_CHUNK: usize = 256 * 1024;

/// A `SorockApp` over a [`RecordingMachine`] plus its observation counters.
struct AppFixture {
    app: SorockApp<RecordingMachine>,
    sum: Arc<AtomicU64>,
    applied: Arc<AtomicU64>,
    snapshot_calls: Arc<AtomicU64>,
}

fn recording_app(snapshot_interval: u64) -> AppFixture {
    let sum = Arc::new(AtomicU64::new(0));
    let applied = Arc::new(AtomicU64::new(0));
    let snapshot_calls = Arc::new(AtomicU64::new(0));
    let machine = RecordingMachine::new(
        Arc::clone(&sum),
        Arc::clone(&applied),
        Arc::clone(&snapshot_calls),
    );
    AppFixture {
        app: SorockApp::new(machine, snapshot_interval),
        sum,
        applied,
        snapshot_calls,
    }
}

fn failing_app(
    fail_apply: bool,
    fail_snapshot: bool,
    fail_restore: bool,
    snapshot_interval: u64,
) -> SorockApp<FailingMachine> {
    SorockApp::new(
        FailingMachine {
            fail_apply,
            fail_snapshot,
            fail_restore,
        },
        snapshot_interval,
    )
}

/// Little-endian u64 command understood by [`RecordingMachine`].
fn command(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn stream_of(chunks: Vec<anyhow::Result<Bytes>>) -> SnapshotStream {
    Box::pin(futures::stream::iter(chunks))
}

async fn collect(mut st: SnapshotStream) -> Vec<u8> {
    let mut buf = Vec::new();
    while let Some(chunk) = st.next().await {
        buf.extend_from_slice(&chunk.expect("snapshot chunks must be ok"));
    }
    buf
}

/// Extracts the `CatgaError` code `SorockApp` wrapped into `anyhow::Error`.
fn catga_code(error: &anyhow::Error) -> ErrorCode {
    error
        .downcast_ref::<CatgaError>()
        .expect("app errors must wrap the machine's CatgaError")
        .code()
}

#[tokio::test]
async fn process_read_is_acknowledged_without_touching_the_machine() {
    let fixture = recording_app(0);

    let out = fixture
        .app
        .process_read(b"arbitrary read request")
        .await
        .expect("reads are always acknowledged");

    assert!(out.is_empty());
    assert_eq!(fixture.sum.load(Ordering::Acquire), 0);
    assert_eq!(fixture.applied.load(Ordering::Acquire), 0);
    assert_eq!(
        fixture.app.applied_index_handle().load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn process_write_applies_in_order_and_tracks_applied_index() {
    let fixture = recording_app(0);

    for (index, value) in [(1_u64, 5_u64), (2, 7)] {
        let out = fixture
            .app
            .process_write(&command(value), index)
            .await
            .expect("writes to a healthy machine must succeed");
        assert!(out.is_empty(), "the consensus contract carries no response");
    }

    assert_eq!(fixture.sum.load(Ordering::Acquire), 12);
    assert_eq!(fixture.applied.load(Ordering::Acquire), 2);
    assert_eq!(
        fixture.app.applied_index_handle().load(Ordering::Acquire),
        2
    );
    assert_eq!(
        fixture.snapshot_calls.load(Ordering::Acquire),
        0,
        "snapshot interval 0 disables snapshotting"
    );
    assert_eq!(
        fixture
            .app
            .get_latest_snapshot()
            .await
            .expect("latest snapshot query must succeed"),
        GENESIS
    );
}

#[tokio::test]
async fn process_write_propagates_machine_apply_error() {
    let app = failing_app(true, false, false, 0);

    let error = app
        .process_write(&command(1), 3)
        .await
        .expect_err("apply failure must propagate");

    assert_eq!(catga_code(&error), ErrorCode::Internal);
    assert!(
        error.to_string().contains("injected apply failure"),
        "unexpected error text: {error}"
    );
    assert_eq!(
        app.applied_index_handle().load(Ordering::Acquire),
        0,
        "a failed write must not advance the applied index"
    );
}

#[tokio::test]
async fn process_write_snapshots_at_interval_boundaries() {
    let fixture = recording_app(2);

    for index in 1..=4_u64 {
        fixture
            .app
            .process_write(&command(index), index)
            .await
            .expect("writes must succeed");
    }

    assert_eq!(fixture.sum.load(Ordering::Acquire), 10);
    assert_eq!(
        fixture.snapshot_calls.load(Ordering::Acquire),
        2,
        "interval 2 snapshots exactly at indices 2 and 4"
    );
    assert_eq!(
        fixture
            .app
            .get_latest_snapshot()
            .await
            .expect("latest snapshot query must succeed"),
        4
    );

    // Snapshot at index 2 captured sum 1 + 2 = 3; at index 4 the full 10.
    let at_two = collect(
        fixture
            .app
            .open_snapshot(2)
            .await
            .expect("snapshot at index 2 must exist"),
    )
    .await;
    assert_eq!(at_two, 3_u64.to_le_bytes());
    let at_four = collect(
        fixture
            .app
            .open_snapshot(4)
            .await
            .expect("snapshot at index 4 must exist"),
    )
    .await;
    assert_eq!(at_four, 10_u64.to_le_bytes());

    let error = fixture
        .app
        .open_snapshot(3)
        .await
        .err()
        .expect("index 3 is not a snapshot boundary");
    assert_eq!(
        error.to_string(),
        "snapshot at index 3 is not in the snapshot store"
    );
}

#[tokio::test]
async fn process_write_propagates_snapshot_error() {
    let app = failing_app(false, true, false, 1);

    let error = app
        .process_write(&command(1), 1)
        .await
        .expect_err("snapshot failure must propagate");

    assert_eq!(catga_code(&error), ErrorCode::PersistenceFailed);
    assert!(
        error.to_string().contains("injected snapshot failure"),
        "unexpected error text: {error}"
    );
    assert_eq!(
        app.applied_index_handle().load(Ordering::Acquire),
        1,
        "the entry applied before its snapshot failed"
    );
    assert_eq!(
        app.get_latest_snapshot()
            .await
            .expect("latest snapshot query must succeed"),
        GENESIS,
        "a failed snapshot must not be stored"
    );
}

#[tokio::test]
async fn install_snapshot_genesis_is_a_noop() {
    let fixture = recording_app(0);

    // Even if bytes were stored under the genesis index, installing it must
    // not touch the machine: the genesis snapshot is defined as empty.
    fixture
        .app
        .save_snapshot(
            stream_of(vec![Ok(Bytes::from(42_u64.to_le_bytes().to_vec()))]),
            GENESIS,
        )
        .await
        .expect("saving snapshot bytes must succeed");

    fixture
        .app
        .install_snapshot(GENESIS)
        .await
        .expect("installing the genesis snapshot is always ok");

    assert_eq!(fixture.sum.load(Ordering::Acquire), 0);
    assert_eq!(
        fixture.app.applied_index_handle().load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn snapshot_save_open_install_round_trip_restores_the_machine() {
    let fixture = recording_app(0);
    let payload = 42_u64.to_le_bytes();
    let chunks: Vec<anyhow::Result<Bytes>> = payload
        .chunks(3)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();

    fixture
        .app
        .save_snapshot(stream_of(chunks), 5)
        .await
        .expect("saving a chunked snapshot must succeed");
    assert_eq!(
        fixture
            .app
            .get_latest_snapshot()
            .await
            .expect("latest snapshot query must succeed"),
        5
    );

    let reopened = collect(
        fixture
            .app
            .open_snapshot(5)
            .await
            .expect("the stored snapshot must open"),
    )
    .await;
    assert_eq!(reopened, payload, "save/open must round-trip bytes exactly");

    fixture
        .app
        .install_snapshot(5)
        .await
        .expect("installing a well-formed snapshot must succeed");
    assert_eq!(fixture.sum.load(Ordering::Acquire), 42);
    assert_eq!(
        fixture.app.applied_index_handle().load(Ordering::Acquire),
        5
    );
}

#[tokio::test]
async fn save_snapshot_propagates_stream_errors_and_stores_nothing() {
    let fixture = recording_app(0);
    let broken = stream_of(vec![
        Ok(Bytes::from_static(b"partial")),
        Err(anyhow::anyhow!("stream broken")),
    ]);

    let error = fixture
        .app
        .save_snapshot(broken, 7)
        .await
        .expect_err("a broken chunk stream must fail the save");
    assert!(
        error.to_string().contains("stream broken"),
        "unexpected error text: {error}"
    );

    let error = fixture
        .app
        .open_snapshot(7)
        .await
        .err()
        .expect("a failed save must not leave a snapshot behind");
    assert_eq!(
        error.to_string(),
        "snapshot at index 7 is not in the snapshot store"
    );
    assert_eq!(
        fixture
            .app
            .get_latest_snapshot()
            .await
            .expect("latest snapshot query must succeed"),
        GENESIS
    );
}

#[tokio::test]
async fn open_snapshot_chunks_large_snapshots() {
    let fixture = recording_app(0);
    let payload: Vec<u8> = (0..600_000_u32).map(|i| (i % 251) as u8).collect();
    assert!(payload.len() > 2 * MAX_CHUNK);

    fixture
        .app
        .save_snapshot(stream_of(vec![Ok(Bytes::from(payload.clone()))]), 9)
        .await
        .expect("saving a large snapshot must succeed");

    let mut st = fixture
        .app
        .open_snapshot(9)
        .await
        .expect("the large snapshot must open");
    let mut chunks = 0_usize;
    let mut reassembled = Vec::new();
    while let Some(chunk) = st.next().await {
        let chunk = chunk.expect("snapshot chunks must be ok");
        assert!(
            chunk.len() <= MAX_CHUNK,
            "chunk of {} bytes exceeds the 256 KiB streaming bound",
            chunk.len()
        );
        chunks += 1;
        reassembled.extend_from_slice(&chunk);
    }

    assert_eq!(chunks, 3, "600000 bytes stream as three 256 KiB chunks");
    assert_eq!(reassembled, payload);
}

#[tokio::test]
async fn install_snapshot_with_truncated_bytes_fails_restore_with_validation() {
    let fixture = recording_app(0);
    let truncated = Bytes::from_static(&[1_u8, 2, 3]);

    fixture
        .app
        .save_snapshot(stream_of(vec![Ok(truncated.clone())]), 4)
        .await
        .expect("the store keeps snapshot bytes verbatim");

    let reopened = collect(
        fixture
            .app
            .open_snapshot(4)
            .await
            .expect("stored bytes open as-is"),
    )
    .await;
    assert_eq!(reopened, truncated, "no integrity check happens at rest");

    let error = fixture
        .app
        .install_snapshot(4)
        .await
        .expect_err("installing a corrupt snapshot must fail");
    assert_eq!(catga_code(&error), ErrorCode::Validation);
    assert!(
        error.to_string().contains("eight bytes"),
        "the machine documents its snapshot format: {error}"
    );
    assert_eq!(fixture.sum.load(Ordering::Acquire), 0);
    assert_eq!(
        fixture.app.applied_index_handle().load(Ordering::Acquire),
        0,
        "a failed install must not advance the applied index"
    );
}

#[tokio::test]
async fn install_snapshot_propagates_machine_restore_error() {
    let app = failing_app(false, false, true, 0);
    app.save_snapshot(
        stream_of(vec![Ok(Bytes::from(1_u64.to_le_bytes().to_vec()))]),
        3,
    )
    .await
    .expect("saving snapshot bytes must succeed");

    let error = app
        .install_snapshot(3)
        .await
        .expect_err("restore failure must propagate");

    assert_eq!(catga_code(&error), ErrorCode::Validation);
    assert!(
        error.to_string().contains("injected restore failure"),
        "unexpected error text: {error}"
    );
    assert_eq!(app.applied_index_handle().load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn install_snapshot_missing_index_fails() {
    let fixture = recording_app(0);

    let error = fixture
        .app
        .install_snapshot(42)
        .await
        .expect_err("installing an unknown snapshot must fail");

    assert_eq!(
        error.to_string(),
        "snapshot at index 42 is not in the snapshot store"
    );
    assert_eq!(
        fixture.app.applied_index_handle().load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn delete_snapshots_before_keeps_boundary_and_later_snapshots() {
    let fixture = recording_app(0);
    for index in [2_u64, 5, 9] {
        fixture
            .app
            .save_snapshot(
                stream_of(vec![Ok(Bytes::from(index.to_le_bytes().to_vec()))]),
                index,
            )
            .await
            .expect("saving snapshot bytes must succeed");
    }

    fixture
        .app
        .delete_snapshots_before(5)
        .await
        .expect("deletion must succeed");

    let error = fixture
        .app
        .open_snapshot(2)
        .await
        .err()
        .expect("snapshots below the boundary are gone");
    assert_eq!(
        error.to_string(),
        "snapshot at index 2 is not in the snapshot store"
    );
    for index in [5_u64, 9] {
        let bytes = collect(
            fixture
                .app
                .open_snapshot(index)
                .await
                .expect("boundary and later snapshots are kept"),
        )
        .await;
        assert_eq!(bytes, index.to_le_bytes());
    }
    assert_eq!(
        fixture
            .app
            .get_latest_snapshot()
            .await
            .expect("latest snapshot query must succeed"),
        9
    );

    fixture
        .app
        .delete_snapshots_before(100)
        .await
        .expect("deleting past the end must succeed");
    assert_eq!(
        fixture
            .app
            .get_latest_snapshot()
            .await
            .expect("latest snapshot query must succeed"),
        GENESIS,
        "an empty store reports the implicit genesis snapshot"
    );
}
