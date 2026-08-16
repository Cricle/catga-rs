//! Integration tests for the `apply` module (`ApplyThread`).
//!
//! `ApplyThread` is exercised directly through its public API:
//! construction defaults, commit-index tracking, batch `advance`,
//! single `apply_entry`, error propagation, and shared state-machine
//! access from multiple threads.

use std::sync::Arc;

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};
use catga_raft::ApplyThread;

// ============================================================================
// Test state machine
// ============================================================================

/// Records every applied `(index, data)` pair; optionally fails at a
/// configured index to exercise error paths.
#[derive(Default)]
struct RecordingStateMachine {
    applied: Vec<(u64, Vec<u8>)>,
    fail_at: Option<u64>,
}

impl RecordingStateMachine {
    fn failing_at(index: u64) -> Self {
        Self {
            applied: Vec::new(),
            fail_at: Some(index),
        }
    }
}

impl ConsensusStateMachine for RecordingStateMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        if self.fail_at == Some(index) {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "simulated state-machine failure",
            ));
        }
        self.applied.push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok((self.applied.len() as u64).to_le_bytes().to_vec())
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        if data.len() != 8 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "snapshot payload must be 8 bytes",
            ));
        }
        Ok(())
    }
}

fn entries(range: std::ops::RangeInclusive<u64>) -> Vec<(u64, Vec<u8>)> {
    range
        .map(|i| (i, format!("entry-{}", i).into_bytes()))
        .collect()
}

// ============================================================================
// Construction / defaults
// ============================================================================

#[test]
fn test_apply_thread_new_defaults() {
    let thread = ApplyThread::new(RecordingStateMachine::default());
    assert_eq!(thread.commit_index(), 0);
    assert_eq!(thread.applied_index(), 0);
    assert!(thread.state_machine().lock().applied.is_empty());
}

// ============================================================================
// Commit index tracking
// ============================================================================

#[test]
fn test_apply_thread_update_commit_index() {
    let thread = ApplyThread::new(RecordingStateMachine::default());
    thread.update_commit_index(42);
    assert_eq!(thread.commit_index(), 42);

    // The setter stores the given value unconditionally.
    thread.update_commit_index(7);
    assert_eq!(thread.commit_index(), 7);
}

// ============================================================================
// Batch advance
// ============================================================================

#[test]
fn test_apply_thread_advance_applies_in_order() {
    let thread = ApplyThread::new(RecordingStateMachine::default());
    thread.update_commit_index(3);

    let batch = entries(1..=3);
    let applied = thread.advance(batch.into_iter()).unwrap();

    assert_eq!(applied, 3);
    assert_eq!(thread.applied_index(), 3);

    let sm = thread.state_machine().lock();
    assert_eq!(sm.applied.len(), 3);
    assert_eq!(
        sm.applied,
        vec![
            (1, b"entry-1".to_vec()),
            (2, b"entry-2".to_vec()),
            (3, b"entry-3".to_vec()),
        ]
    );
}

#[test]
fn test_apply_thread_advance_empty_iterator() {
    let thread = ApplyThread::new(RecordingStateMachine::default());

    // Empty batch on a fresh thread applies nothing and reports 0.
    let applied = thread.advance(std::iter::empty()).unwrap();
    assert_eq!(applied, 0);
    assert_eq!(thread.applied_index(), 0);

    // After progress, an empty batch reports the current applied index.
    thread.advance(entries(1..=2).into_iter()).unwrap();
    let applied = thread.advance(std::iter::empty()).unwrap();
    assert_eq!(applied, 2);
    assert_eq!(thread.applied_index(), 2);
}

#[test]
fn test_apply_thread_advance_resumes_after_progress() {
    let thread = ApplyThread::new(RecordingStateMachine::default());

    thread.update_commit_index(3);
    assert_eq!(thread.advance(entries(1..=3).into_iter()).unwrap(), 3);

    thread.update_commit_index(5);
    assert_eq!(thread.advance(entries(4..=5).into_iter()).unwrap(), 5);
    assert_eq!(thread.applied_index(), 5);
    assert_eq!(thread.state_machine().lock().applied.len(), 5);
}

#[test]
fn test_apply_thread_advance_applies_provided_entries_regardless_of_commit_index() {
    // Current behavior: `advance` applies whatever entries it is given;
    // the stored commit index is tracked but not used to filter them.
    let thread = ApplyThread::new(RecordingStateMachine::default());
    assert_eq!(thread.commit_index(), 0);

    let applied = thread.advance(entries(1..=2).into_iter()).unwrap();
    assert_eq!(applied, 2);
    assert_eq!(thread.applied_index(), 2);
    assert_eq!(thread.commit_index(), 0);
}

#[test]
fn test_apply_thread_advance_error_preserves_applied_index() {
    // Entry 2 fails: entries before it reach the state machine, but the
    // persisted applied index is not advanced on error.
    let thread = ApplyThread::new(RecordingStateMachine::failing_at(2));

    let result = thread.advance(entries(1..=3).into_iter());
    assert!(result.is_err());
    assert_eq!(thread.applied_index(), 0);

    let sm = thread.state_machine().lock();
    assert_eq!(sm.applied, vec![(1, b"entry-1".to_vec())]);
}

// ============================================================================
// Single-entry apply
// ============================================================================

#[test]
fn test_apply_thread_apply_entry_updates_index() {
    let thread = ApplyThread::new(RecordingStateMachine::default());

    thread.apply_entry(1, b"hello").unwrap();
    assert_eq!(thread.applied_index(), 1);

    thread.apply_entry(2, b"world").unwrap();
    assert_eq!(thread.applied_index(), 2);

    let sm = thread.state_machine().lock();
    assert_eq!(
        sm.applied,
        vec![(1, b"hello".to_vec()), (2, b"world".to_vec())]
    );
}

#[test]
fn test_apply_thread_apply_entry_keeps_max_index() {
    let thread = ApplyThread::new(RecordingStateMachine::default());

    thread.apply_entry(5, b"late").unwrap();
    assert_eq!(thread.applied_index(), 5);

    // A stale (lower-index) entry is still applied to the state machine,
    // but the applied index never moves backwards (fetch_max semantics).
    thread.apply_entry(2, b"stale").unwrap();
    assert_eq!(thread.applied_index(), 5);
    assert_eq!(thread.state_machine().lock().applied.len(), 2);
}

#[test]
fn test_apply_thread_apply_entry_error_leaves_index_unchanged() {
    let thread = ApplyThread::new(RecordingStateMachine::failing_at(7));

    thread.apply_entry(6, b"ok").unwrap();
    assert_eq!(thread.applied_index(), 6);

    let result = thread.apply_entry(7, b"boom");
    assert!(result.is_err());
    assert_eq!(thread.applied_index(), 6);
    assert_eq!(thread.state_machine().lock().applied.len(), 1);
}

// ============================================================================
// Shared state-machine handle / concurrency
// ============================================================================

#[test]
fn test_apply_thread_state_machine_shared_handle() {
    let thread = ApplyThread::new(RecordingStateMachine::default());

    // The returned handle shares storage with the apply thread.
    let sm = Arc::clone(thread.state_machine());
    thread.apply_entry(1, b"data").unwrap();
    assert_eq!(sm.lock().applied.len(), 1);

    sm.lock().fail_at = Some(2);
    assert!(thread.apply_entry(2, b"data").is_err());
}

#[test]
fn test_apply_thread_concurrent_apply() {
    let thread = Arc::new(ApplyThread::new(RecordingStateMachine::default()));
    const THREADS: u64 = 4;
    const PER_THREAD: u64 = 25;

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let apply_thread = Arc::clone(&thread);
        handles.push(std::thread::spawn(move || {
            for i in 1..=PER_THREAD {
                // Disjoint index ranges per thread.
                let index = t * PER_THREAD + i;
                apply_thread
                    .apply_entry(index, format!("t{}-e{}", t, i).as_bytes())
                    .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(thread.applied_index(), THREADS * PER_THREAD);
    let sm = thread.state_machine().lock();
    assert_eq!(sm.applied.len() as u64, THREADS * PER_THREAD);

    // Every index was applied exactly once, regardless of thread order.
    let mut indices: Vec<u64> = sm.applied.iter().map(|(i, _)| *i).collect();
    indices.sort_unstable();
    indices.dedup();
    assert_eq!(indices.len() as u64, THREADS * PER_THREAD);
}
