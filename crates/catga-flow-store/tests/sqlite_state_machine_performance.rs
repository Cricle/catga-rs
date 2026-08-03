//! Manual SQLite state-machine storage performance benchmark.
//!
//! Run only when measuring performance:
//! `cargo test -p catga-flow-store --features sqlite --test sqlite_state_machine_performance -- --ignored --nocapture`
//!
//! The timed interval excludes temporary database setup, migration, fixture construction, and a
//! warm-up cycle. It measures complete create, initial load, versioned update, and terminal
//! snapshot load lifecycles for distinct state-machine instances. Correctness assertions remain
//! active while measuring, but the benchmark intentionally has no host-dependent timing
//! threshold.
#![cfg(feature = "sqlite")]

use std::time::Instant;

use catga_core::codec::memorypack::{MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter};
use catga_core::MemoryPackable;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::{StateMachineSnapshot, StateMachineStore};
use catga_flow_store::SqlStateMachineStore;

const RECORD_COUNT: usize = 128;
const PAYLOAD_BYTES: usize = 256;
const OPERATIONS_PER_LIFECYCLE: usize = 4;

#[derive(Clone, Debug, Eq, MemoryPackable, PartialEq)]
struct BenchmarkStateMachine {
    completed: bool,
    payload: Vec<u8>,
}

/// Measures complete SQLite state-machine storage lifecycles without a timing threshold.
#[tokio::test]
#[ignore = "manual performance benchmark; run with --ignored --nocapture"]
async fn sqlite_state_machine_storage_lifecycle_benchmark() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("state-machine-performance.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlStateMachineStore::<BenchmarkStateMachine>::connect_sqlite(&url).await?;
    store.migrate().await?;

    let initial_payload = vec![0xA5; PAYLOAD_BYTES];
    let terminal_payload = vec![0x5A; PAYLOAD_BYTES];
    warm_up(&store, &initial_payload, &terminal_payload).await?;
    let records = (0..RECORD_COUNT)
        .map(|index| {
            StateMachineSnapshot::new(
                format!("sqlite-state-machine-benchmark-{index}"),
                BenchmarkStateMachine {
                    completed: false,
                    payload: initial_payload.clone(),
                },
            )
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    for initial in records {
        let instance_id = initial.instance_id().to_owned();
        let terminal = initial.next_version(BenchmarkStateMachine {
            completed: true,
            payload: terminal_payload.clone(),
        })?;

        assert!(store.create(initial.clone()).await?);
        assert_eq!(store.get(&instance_id).await?, Some(initial.clone()));
        assert!(store.update(initial.version(), terminal.clone()).await?);
        assert_eq!(store.get(&instance_id).await?, Some(terminal));
    }
    let elapsed = started.elapsed();
    let elapsed_per_lifecycle = elapsed / (RECORD_COUNT as u32);
    let operations = RECORD_COUNT * OPERATIONS_PER_LIFECYCLE;
    let operations_per_second = (operations as f64) / elapsed.as_secs_f64();

    println!(
        "sqlite_state_machine_storage_lifecycle: records={RECORD_COUNT}, payload_bytes={PAYLOAD_BYTES}, operations={operations}, total={elapsed:?}, per_lifecycle={elapsed_per_lifecycle:?}, operations_per_second={operations_per_second:.2}"
    );
    Ok(())
}

async fn warm_up(
    store: &SqlStateMachineStore<BenchmarkStateMachine>,
    initial_payload: &[u8],
    terminal_payload: &[u8],
) -> CatgaResult<()> {
    let initial = StateMachineSnapshot::new(
        "sqlite-state-machine-benchmark-warmup",
        BenchmarkStateMachine {
            completed: false,
            payload: initial_payload.to_vec(),
        },
    );
    let terminal = initial.next_version(BenchmarkStateMachine {
        completed: true,
        payload: terminal_payload.to_vec(),
    })?;

    assert!(store.create(initial.clone()).await?);
    assert_eq!(
        store.get(initial.instance_id()).await?,
        Some(initial.clone())
    );
    assert!(store.update(initial.version(), terminal.clone()).await?);
    assert_eq!(store.get(terminal.instance_id()).await?, Some(terminal));
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
