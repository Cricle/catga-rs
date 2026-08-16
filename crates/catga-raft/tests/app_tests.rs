//! Integration tests for the application/state-machine adapter layer of catga-raft.
//!
//! Coverage note: the target source file `src/app.rs` defines `CatgaRaftApp`, but that module
//! is **not** declared in `lib.rs`, so it is not part of the crate's public API and cannot be
//! reached from an integration test. These tests therefore exercise the closest public surface
//! with the same responsibilities: [`ApplyThread`], the adapter that bridges a
//! `catga_core::ConsensusStateMachine` into the raft layer while tracking commit/applied
//! indices (the same index bookkeeping `CatgaRaftApp` performs with `set_last_index` /
//! `set_first_index`), including the snapshot/restore hand-off through the wrapped state
//! machine.

use std::sync::Arc;

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};
use catga_raft::ApplyThread;

/// A state machine that records every applied entry and can be configured to fail
/// at a specific index.
struct RecordingStateMachine {
    applied: Vec<(u64, Vec<u8>)>,
    restored: Option<Vec<u8>>,
    fail_apply_at: Option<u64>,
}

impl RecordingStateMachine {
    fn new() -> Self {
        Self {
            applied: Vec::new(),
            restored: None,
            fail_apply_at: None,
        }
    }
}

impl ConsensusStateMachine for RecordingStateMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        if self.fail_apply_at == Some(index) {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "simulated apply failure",
            ));
        }
        self.applied.push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        let mut out = Vec::new();
        for (index, data) in &self.applied {
            out.extend_from_slice(&index.to_le_bytes());
            out.extend_from_slice(data);
            out.push(b'|');
        }
        Ok(out)
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        self.restored = Some(data.to_vec());
        Ok(())
    }
}

// ============================================================================
// Construction / defaults
// ============================================================================

#[test]
fn test_app_apply_thread_defaults() {
    let apply = ApplyThread::new(RecordingStateMachine::new());
    assert_eq!(apply.commit_index(), 0);
    assert_eq!(apply.applied_index(), 0);
}

#[test]
fn test_app_commit_index_tracking() {
    let apply = ApplyThread::new(RecordingStateMachine::new());
    assert_eq!(apply.commit_index(), 0);

    apply.update_commit_index(5);
    assert_eq!(apply.commit_index(), 5);

    apply.update_commit_index(9);
    assert_eq!(apply.commit_index(), 9);
}

// ============================================================================
// Happy paths
// ============================================================================

#[test]
fn test_app_advance_applies_entries_in_order() {
    let apply = ApplyThread::new(RecordingStateMachine::new());

    let entries = vec![
        (1u64, b"one".to_vec()),
        (2u64, b"two".to_vec()),
        (3u64, b"three".to_vec()),
    ];
    let applied = apply.advance(entries.into_iter()).unwrap();

    assert_eq!(applied, 3);
    assert_eq!(apply.applied_index(), 3);

    let sm = apply.state_machine().lock();
    assert_eq!(
        sm.applied,
        vec![
            (1, b"one".to_vec()),
            (2, b"two".to_vec()),
            (3, b"three".to_vec()),
        ]
    );
}

#[test]
fn test_app_advance_empty_iterator() {
    let apply = ApplyThread::new(RecordingStateMachine::new());

    let applied = apply.advance(std::iter::empty()).unwrap();
    assert_eq!(applied, 0);
    assert_eq!(apply.applied_index(), 0);
}

#[test]
fn test_app_advance_after_apply_entry() {
    let apply = ApplyThread::new(RecordingStateMachine::new());

    apply.apply_entry(2, b"x").unwrap();
    assert_eq!(apply.applied_index(), 2);

    let entries = vec![(3u64, b"y".to_vec()), (4u64, b"z".to_vec())];
    let applied = apply.advance(entries.into_iter()).unwrap();

    assert_eq!(applied, 4);
    assert_eq!(apply.applied_index(), 4);
    assert_eq!(apply.state_machine().lock().applied.len(), 3);
}

#[test]
fn test_app_apply_entry_success() {
    let apply = ApplyThread::new(RecordingStateMachine::new());

    apply.apply_entry(7, b"payload").unwrap();
    assert_eq!(apply.applied_index(), 7);

    let sm = apply.state_machine().lock();
    assert_eq!(sm.applied, vec![(7, b"payload".to_vec())]);
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn test_app_apply_entry_never_regresses_index() {
    let apply = ApplyThread::new(RecordingStateMachine::new());

    apply.apply_entry(10, b"high").unwrap();
    apply.apply_entry(4, b"low").unwrap();

    // `apply_entry` uses fetch_max semantics: a replayed lower index must not
    // move the applied index backwards.
    assert_eq!(apply.applied_index(), 10);
    assert_eq!(apply.state_machine().lock().applied.len(), 2);
}

#[test]
fn test_app_state_machine_accessor_is_shared() {
    let apply = ApplyThread::new(RecordingStateMachine::new());

    apply.apply_entry(1, b"ok").unwrap();

    // Mutating through the accessor must affect the same instance used by apply_entry.
    {
        let mut sm = apply.state_machine().lock();
        sm.fail_apply_at = Some(2);
    }

    assert!(apply.apply_entry(2, b"boom").is_err());
    assert_eq!(apply.applied_index(), 1);
}

#[test]
fn test_app_snapshot_restore_via_state_machine() {
    let apply = ApplyThread::new(RecordingStateMachine::new());
    apply.apply_entry(1, b"a").unwrap();
    apply.apply_entry(2, b"b").unwrap();

    let snapshot = apply.state_machine().lock().snapshot().unwrap();
    assert!(!snapshot.is_empty());

    // Restore into a fresh adapter, mirroring the snapshot hand-off that app.rs
    // delegates to the wrapped state machine.
    let restored = ApplyThread::new(RecordingStateMachine::new());
    restored.state_machine().lock().restore(&snapshot).unwrap();
    assert_eq!(
        restored.state_machine().lock().restored.as_deref(),
        Some(snapshot.as_slice())
    );
}

#[test]
fn test_app_apply_thread_concurrent_apply() {
    let apply = Arc::new(ApplyThread::new(RecordingStateMachine::new()));

    // 4 threads x 25 entries each, disjoint index ranges: no fixed ordering needed.
    std::thread::scope(|scope| {
        for t in 0..4u64 {
            let apply = Arc::clone(&apply);
            scope.spawn(move || {
                for i in 0..25u64 {
                    let index = t * 25 + i + 1;
                    apply
                        .apply_entry(index, format!("payload-{index}").as_bytes())
                        .unwrap();
                }
            });
        }
    });

    assert_eq!(apply.applied_index(), 100);
    assert_eq!(apply.state_machine().lock().applied.len(), 100);
}

// ============================================================================
// Error cases
// ============================================================================

#[test]
fn test_app_advance_error_preserves_applied_index() {
    let mut sm = RecordingStateMachine::new();
    sm.fail_apply_at = Some(2);
    let apply = ApplyThread::new(sm);

    let entries = vec![
        (1u64, b"a".to_vec()),
        (2u64, b"b".to_vec()),
        (3u64, b"c".to_vec()),
    ];
    let result = apply.advance(entries.into_iter());

    let err = result.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Internal);

    // The applied index is only persisted after the whole batch succeeds, and the
    // iterator is short-circuited at the failing entry.
    assert_eq!(apply.applied_index(), 0);
    let sm = apply.state_machine().lock();
    assert_eq!(sm.applied, vec![(1, b"a".to_vec())]);
}

#[test]
fn test_app_apply_entry_error_leaves_index() {
    let mut sm = RecordingStateMachine::new();
    sm.fail_apply_at = Some(1);
    let apply = ApplyThread::new(sm);

    let result = apply.apply_entry(1, b"doomed");
    assert!(result.is_err());
    assert_eq!(apply.applied_index(), 0);
    assert!(apply.state_machine().lock().applied.is_empty());
}
