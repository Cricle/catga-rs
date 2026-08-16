//! Tests for the dedicated apply worker that applies committed entries off
//! the raft owner loop ([`ApplyThread::spawn_worker`] + [`ApplySender`]).
//!
//! Coverage:
//! - (a) entries apply through the worker strictly in index order, exactly
//!   once, and the shutdown join drains everything still queued;
//! - (b) a snapshot install discards stale queued entries without gaps:
//!   nothing superseded is applied, the frontier rebases at the snapshot
//!   index, and later entries apply on top in order;
//! - (c) a full apply channel blocks the sender rather than dropping
//!   committed entries (small-capacity knob; production default is 4096).
//!
//! Tests (a) and (b) run on a current-thread runtime for determinism: a
//! spawned worker task cannot run until the test awaits, so entries sent
//! with spare channel capacity are provably *queued* (not yet applied) at
//! each assertion point.

use std::sync::Arc;
use std::time::{Duration, Instant};

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};
use catga_raft::ApplyThread;
use parking_lot::Mutex;

/// One applied log record, serialized into snapshots.
type EntryRec = (u64, Vec<u8>);

/// State machine that records every applied entry in order.
#[derive(Clone, Default)]
struct RecordingMachine {
    applied: Arc<Mutex<Vec<EntryRec>>>,
}

impl RecordingMachine {
    fn entries(&self) -> Vec<EntryRec> {
        self.applied.lock().clone()
    }
}

impl ConsensusStateMachine for RecordingMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.applied.lock().push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

/// State machine whose state round-trips through snapshot/restore, so a
/// snapshot install observably replaces whatever was applied before.
#[derive(Clone, Default)]
struct SnapshottingMachine {
    applied: Arc<Mutex<Vec<EntryRec>>>,
}

impl SnapshottingMachine {
    fn entries(&self) -> Vec<EntryRec> {
        self.applied.lock().clone()
    }
}

fn encode_entries(entries: &[EntryRec]) -> CatgaResult<Vec<u8>> {
    bincode::serde::encode_to_vec(entries, bincode::config::standard())
        .map_err(|e| CatgaError::new(ErrorCode::Internal, format!("encode snapshot: {e}")))
}

fn decode_entries(bytes: &[u8]) -> CatgaResult<Vec<EntryRec>> {
    let (entries, _): (Vec<EntryRec>, usize) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| CatgaError::new(ErrorCode::Internal, format!("decode snapshot: {e}")))?;
    Ok(entries)
}

impl ConsensusStateMachine for SnapshottingMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.applied.lock().push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        encode_entries(&self.entries())
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        *self.applied.lock() = decode_entries(bytes)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// (a) Order and exactly-once through the worker; shutdown drains the queue.
// ---------------------------------------------------------------------------

/// 100 entries sent through the apply channel must reach the state machine
/// strictly in index order, exactly once. Dropping the sender and joining
/// the worker drains every queued entry before the join returns.
#[tokio::test(flavor = "current_thread")]
async fn entries_apply_in_order_through_worker() {
    const COUNT: u64 = 100;
    let machine = RecordingMachine::default();
    let recorder = machine.clone();
    let apply = Arc::new(ApplyThread::new(machine));
    let (sender, handle) = apply.spawn_worker(256);

    // Current-thread runtime: the worker has not run yet (no await yielded),
    // and the 256-deep channel absorbs every send without parking. The
    // entries are therefore provably queued, not applied.
    for index in 1..=COUNT {
        sender
            .send_entry(index, format!("payload-{index}").into_bytes())
            .await
            .expect("send_entry must succeed while the worker is alive");
    }
    assert!(
        recorder.entries().is_empty(),
        "nothing may be applied before the worker runs"
    );

    // Shutdown drain: dropping the sender closes the channel only after the
    // queued entries are consumed; the join must outlive all of them.
    drop(sender);
    handle.await.expect("apply worker must exit cleanly");

    let entries = recorder.entries();
    assert_eq!(entries.len(), COUNT as usize, "exactly once per entry");
    for (position, (index, data)) in entries.iter().enumerate() {
        let expected = (position + 1) as u64;
        assert_eq!(*index, expected, "entries must apply in index order");
        assert_eq!(data, format!("payload-{expected}").as_bytes());
    }
    assert_eq!(
        apply.applied_index(),
        COUNT,
        "applied_index advances only after the entries were applied"
    );
}

// ---------------------------------------------------------------------------
// (b) Snapshot install discards stale queued entries without gaps.
// ---------------------------------------------------------------------------

/// Entries still queued when a snapshot installs are discarded (never
/// applied), the frontier rebases at the snapshot index, and entries sent
/// afterwards apply on top in order — no gaps, no duplicates.
#[tokio::test(flavor = "current_thread")]
async fn snapshot_install_discards_stale_queued_entries() {
    const SNAP_INDEX: u64 = 5;
    let machine = SnapshottingMachine::default();
    let recorder = machine.clone();
    let apply = Arc::new(ApplyThread::new(machine));
    let (sender, handle) = apply.spawn_worker(16);

    // Queue entries 1..=5 while the worker cannot have run yet (see (a)).
    for index in 1..=SNAP_INDEX {
        sender
            .send_entry(index, format!("stale-{index}").into_bytes())
            .await
            .expect("queue stale entry");
    }
    assert!(recorder.entries().is_empty(), "queued, not yet applied");

    // Install a snapshot covering exactly those five entries. This bumps
    // the apply epoch, invalidating everything still queued, and rebases
    // the machine state + frontier synchronously.
    let snap_state: Vec<EntryRec> = (1..=SNAP_INDEX)
        .map(|i| (i, format!("snap-{i}").into_bytes()))
        .collect();
    apply
        .install_snapshot(&encode_entries(&snap_state).expect("encode"), SNAP_INDEX)
        .expect("install snapshot");
    assert_eq!(apply.applied_index(), SNAP_INDEX);

    // Let the worker run: it must consume all five stale messages without
    // applying any of them, then acknowledge the barrier.
    sender.flush().await;
    assert_eq!(
        recorder.entries(),
        snap_state,
        "stale queued entries must be discarded, never applied on top of the snapshot"
    );

    // Entries after the snapshot apply on top, in order, without gaps.
    for index in SNAP_INDEX + 1..=SNAP_INDEX + 2 {
        sender
            .send_entry(index, format!("after-{index}").into_bytes())
            .await
            .expect("send post-snapshot entry");
    }
    sender.flush().await;

    let mut expected = snap_state.clone();
    expected.push((SNAP_INDEX + 1, b"after-6".to_vec()));
    expected.push((SNAP_INDEX + 2, b"after-7".to_vec()));
    assert_eq!(recorder.entries(), expected, "no gaps, no duplicates");
    assert_eq!(apply.applied_index(), SNAP_INDEX + 2);

    drop(sender);
    handle.await.expect("apply worker must exit cleanly");
}

// ---------------------------------------------------------------------------
// (c) Backpressure blocks rather than drops.
// ---------------------------------------------------------------------------

/// A gate that blocks every `apply` call until opened.
struct Gate {
    state: std::sync::Mutex<(bool, Instant)>,
    cond: std::sync::Condvar,
}

impl Gate {
    fn closed() -> Self {
        Self {
            state: std::sync::Mutex::new((false, Instant::now())),
            cond: std::sync::Condvar::new(),
        }
    }

    fn open(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.0 = true;
        self.cond.notify_all();
    }

    /// Blocks until the gate opens; gives up after 10s so a broken test
    /// fails an assertion instead of hanging.
    fn wait(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.0 && state.1.elapsed() < Duration::from_secs(10) {
            state = match self.cond.wait_timeout(state, Duration::from_millis(100)) {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
    }
}

/// State machine stalled on a gate: records the index of every entry that
/// actually made it through.
#[derive(Clone)]
struct GatedMachine {
    gate: Arc<Gate>,
    applied: Arc<Mutex<Vec<u64>>>,
}

impl ConsensusStateMachine for GatedMachine {
    fn apply(&mut self, index: u64, _data: &[u8]) -> CatgaResult<()> {
        self.gate.wait();
        self.applied.lock().push(index);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

/// With the state machine stalled and a 2-deep channel, sending 5 entries
/// must BLOCK once capacity runs out — and once the machine unstalls, all
/// five land exactly once and in order. Nothing is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backpressure_blocks_instead_of_dropping() {
    const COUNT: u64 = 5;
    let gate = Arc::new(Gate::closed());
    let applied = Arc::new(Mutex::new(Vec::<u64>::new()));
    let machine = GatedMachine {
        gate: Arc::clone(&gate),
        applied: Arc::clone(&applied),
    };
    let apply = Arc::new(ApplyThread::new(machine));
    let (sender, handle) = apply.spawn_worker(2);

    // Send all five from a separate task so the main task can observe the
    // block. With capacity 2 and the worker stalled on entry 1, at most
    // three sends can ever complete while the gate is closed.
    let send_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let send_task = {
        let send_done = Arc::clone(&send_done);
        tokio::spawn(async move {
            for index in 1..=COUNT {
                sender
                    .send_entry(index, Vec::new())
                    .await
                    .expect("send_entry must eventually succeed");
            }
            send_done.store(true, std::sync::atomic::Ordering::Release);
        })
    };

    // While the machine is stalled the sender must sit blocked on the full
    // channel — not return, and not drop entries.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !send_done.load(std::sync::atomic::Ordering::Acquire),
        "send must block while the state machine is stalled (no drops)"
    );
    assert!(
        applied.lock().is_empty(),
        "the stalled machine must not have applied anything yet"
    );

    // Unstall the machine: the queue drains and the blocked sends complete.
    gate.open();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !send_done.load(std::sync::atomic::Ordering::Acquire) {
        assert!(Instant::now() < deadline, "blocked sends never completed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    send_task.await.expect("sender task must finish");

    drop(apply); // Sender already moved into the task; worker drains next.
    handle.await.expect("apply worker must exit cleanly");

    assert_eq!(
        *applied.lock(),
        (1..=COUNT).collect::<Vec<_>>(),
        "every entry lands exactly once, in order — nothing dropped"
    );
}
