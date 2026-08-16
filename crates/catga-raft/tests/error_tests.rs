//! Integration tests for `catga_raft::error` (source: src/error.rs).
//!
//! The module defines the public `CatgaRaftError` enum (thiserror-based,
//! deriving `Error`, `Debug`, `Clone`) and the `CatgaRaftResult<T>` alias.
//! Both are re-exported from the crate root, so they are exercised here
//! through the public API surface.

use std::error::Error;

use catga_raft::{CatgaRaftError, CatgaRaftResult};

/// Returns one instance of every `CatgaRaftError` variant.
fn all_variants() -> Vec<CatgaRaftError> {
    vec![
        CatgaRaftError::Raft("raft failure".to_string()),
        CatgaRaftError::Storage("storage failure".to_string()),
        CatgaRaftError::Transport("transport failure".to_string()),
        CatgaRaftError::Codec("codec failure".to_string()),
        CatgaRaftError::CircuitBreakerOpen,
        CatgaRaftError::Backpressure,
        CatgaRaftError::NotLeader,
        CatgaRaftError::Timeout,
        CatgaRaftError::NodeNotFound(42),
    ]
}

// ============================================================================
// Construction + Display (thiserror `#[error]` messages)
// ============================================================================

#[test]
fn error_display_string_payload_variants() {
    assert_eq!(
        CatgaRaftError::Raft("bad state".to_string()).to_string(),
        "raft error: bad state"
    );
    assert_eq!(
        CatgaRaftError::Storage("disk full".to_string()).to_string(),
        "storage error: disk full"
    );
    assert_eq!(
        CatgaRaftError::Transport("connection reset".to_string()).to_string(),
        "transport error: connection reset"
    );
    assert_eq!(
        CatgaRaftError::Codec("invalid frame".to_string()).to_string(),
        "codec error: invalid frame"
    );
}

#[test]
fn error_display_unit_variants() {
    assert_eq!(
        CatgaRaftError::CircuitBreakerOpen.to_string(),
        "circuit breaker is open"
    );
    assert_eq!(
        CatgaRaftError::Backpressure.to_string(),
        "backpressure: peer overloaded"
    );
    assert_eq!(CatgaRaftError::NotLeader.to_string(), "not leader");
    assert_eq!(CatgaRaftError::Timeout.to_string(), "timeout");
}

#[test]
fn error_display_node_not_found() {
    assert_eq!(CatgaRaftError::NodeNotFound(7).to_string(), "node not found: 7");
}

#[test]
fn error_empty_message_payload() {
    // Edge case: empty string payloads are allowed and formatted verbatim.
    assert_eq!(CatgaRaftError::Raft(String::new()).to_string(), "raft error: ");
    assert_eq!(
        CatgaRaftError::Codec(String::new()).to_string(),
        "codec error: "
    );
}

#[test]
fn error_node_not_found_extreme_ids() {
    // Edge case: boundary node ids format without overflow or loss.
    assert_eq!(
        CatgaRaftError::NodeNotFound(0).to_string(),
        "node not found: 0"
    );
    assert_eq!(
        CatgaRaftError::NodeNotFound(u64::MAX).to_string(),
        format!("node not found: {}", u64::MAX)
    );
}

// ============================================================================
// Derives: Clone + Debug
// ============================================================================

#[test]
fn error_clone_preserves_variant_and_payload() {
    for original in all_variants() {
        let cloned = original.clone();
        // No PartialEq derive: compare via Display + matching.
        assert_eq!(cloned.to_string(), original.to_string());
        match (&original, &cloned) {
            (CatgaRaftError::Raft(a), CatgaRaftError::Raft(b))
            | (CatgaRaftError::Storage(a), CatgaRaftError::Storage(b))
            | (CatgaRaftError::Transport(a), CatgaRaftError::Transport(b))
            | (CatgaRaftError::Codec(a), CatgaRaftError::Codec(b)) => assert_eq!(a, b),
            (CatgaRaftError::NodeNotFound(a), CatgaRaftError::NodeNotFound(b)) => {
                assert_eq!(a, b)
            }
            _ => {}
        }
    }
}

#[test]
fn error_debug_formatting() {
    // Derived Debug names the variants; payloads are included.
    assert_eq!(format!("{:?}", CatgaRaftError::Timeout), "Timeout");
    assert_eq!(
        format!("{:?}", CatgaRaftError::NodeNotFound(9)),
        "NodeNotFound(9)"
    );
    let dbg = format!("{:?}", CatgaRaftError::Raft("oops".to_string()));
    assert!(dbg.starts_with("Raft(") && dbg.contains("oops"), "got: {dbg}");
}

// ============================================================================
// std::error::Error trait behavior
// ============================================================================

#[test]
fn error_source_is_none_for_all_variants() {
    // No variant carries a #[source], so source() must always be None.
    for err in all_variants() {
        assert!(err.source().is_none(), "unexpected source for {err:?}");
    }
}

#[test]
fn error_converts_into_box_dyn_error() {
    let boxed: Box<dyn Error + Send + Sync> = Box::new(CatgaRaftError::Timeout);
    assert_eq!(boxed.to_string(), "timeout");
    assert!(boxed.source().is_none());
}

#[test]
fn error_is_send_sync_across_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CatgaRaftError>();
    assert_send_sync::<CatgaRaftResult<()>>();

    // Round-trip an error through another thread.
    let handle = std::thread::spawn(|| CatgaRaftError::Backpressure);
    let err = handle.join().expect("worker thread panicked");
    assert!(matches!(err, CatgaRaftError::Backpressure));
}

// ============================================================================
// CatgaRaftResult<T> alias
// ============================================================================

#[test]
fn error_result_alias_ok_and_err() {
    let ok: CatgaRaftResult<u32> = Ok(7);
    assert_eq!(ok.unwrap(), 7);

    let err: CatgaRaftResult<u32> = Err(CatgaRaftError::Storage("io".to_string()));
    let payload = err.unwrap_err();
    assert!(matches!(&payload, CatgaRaftError::Storage(m) if m == "io"));
}

#[test]
fn error_result_propagation_with_question_mark() {
    fn failing_step() -> CatgaRaftResult<()> {
        Err(CatgaRaftError::NotLeader)?;
        Ok(())
    }

    fn caller() -> CatgaRaftResult<u64> {
        failing_step()?;
        Ok(1)
    }

    let result = caller();
    assert!(matches!(result, Err(CatgaRaftError::NotLeader)));
}

#[test]
fn error_pattern_matching_all_variants() {
    // Each variant is discriminable via `matches!` (no PartialEq derive).
    let variants = all_variants();
    assert_eq!(variants.len(), 9);
    assert!(matches!(&variants[0], CatgaRaftError::Raft(_)));
    assert!(matches!(&variants[1], CatgaRaftError::Storage(_)));
    assert!(matches!(&variants[2], CatgaRaftError::Transport(_)));
    assert!(matches!(&variants[3], CatgaRaftError::Codec(_)));
    assert!(matches!(&variants[4], CatgaRaftError::CircuitBreakerOpen));
    assert!(matches!(&variants[5], CatgaRaftError::Backpressure));
    assert!(matches!(&variants[6], CatgaRaftError::NotLeader));
    assert!(matches!(&variants[7], CatgaRaftError::Timeout));
    assert!(matches!(&variants[8], CatgaRaftError::NodeNotFound(42)));
}
