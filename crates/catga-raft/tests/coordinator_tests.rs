//! Integration tests for `CatgaRaftCoordinator` (src/coordinator.rs).
//!
//! The coordinator is fully public: construction via `CatgaRaftCoordinator::new`,
//! mutation via `set_leader` / `set_members`, and the read surface comes from
//! the `catga_core::ConsensusCoordinator` trait implementation
//! (`node_id`, `is_leader`, `leader_endpoint`, `member_endpoints`).
//!
//! These tests exercise the coordinator directly (no runtime startup, no
//! network), so they are fast and deterministic.

use std::sync::Arc;
use std::thread;

use catga_core::ConsensusCoordinator;
use catga_raft::CatgaRaftCoordinator;

/// Compile-time check that the coordinator can be shared across threads and
/// used behind trait objects / `Arc`.
fn assert_send_sync<T: Send + Sync>() {}

/// Helper: collect member endpoints into a plain `Vec<String>` for assertions.
fn members_as_strings(coord: &CatgaRaftCoordinator) -> Vec<String> {
    coord
        .member_endpoints()
        .iter()
        .map(|s| s.to_string())
        .collect()
}

// ============================================================================
// Construction / defaults
// ============================================================================

/// A fresh coordinator reports its node id and has no leader and no members.
#[test]
fn coordinator_new_defaults() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    assert_eq!(coord.node_id(), "node-1");
    assert!(!coord.is_leader(), "a fresh coordinator must not be leader");
    assert!(
        coord.leader_endpoint().is_none(),
        "a fresh coordinator must not know any leader endpoint"
    );
    assert!(
        coord.member_endpoints().is_empty(),
        "a fresh coordinator must have an empty member list"
    );
}

/// Edge case: an empty node id is accepted and reported back unchanged.
#[test]
fn coordinator_empty_node_id() {
    let coord = CatgaRaftCoordinator::new(String::new());
    assert_eq!(coord.node_id(), "");
    assert!(!coord.is_leader());
}

/// The node id is a stable identifier: repeated reads return the same value
/// and do not mutate state.
#[test]
fn coordinator_node_id_is_stable() {
    let coord = CatgaRaftCoordinator::new("node-42".to_string());
    for _ in 0..3 {
        assert_eq!(coord.node_id(), "node-42");
    }
    // Leadership/membership state must be unaffected by reading the id.
    assert!(!coord.is_leader());
    assert!(coord.member_endpoints().is_empty());
}

// ============================================================================
// Leadership state
// ============================================================================

/// Setting a leader endpoint marks the node as leader and stores the endpoint.
#[test]
fn coordinator_set_leader_some() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_leader(Some("127.0.0.1:7001".to_string()));

    assert!(coord.is_leader());
    let ep = coord
        .leader_endpoint()
        .expect("leader endpoint must be set");
    assert_eq!(ep.as_ref(), "127.0.0.1:7001");
}

/// Setting `None` on a fresh coordinator keeps the default (non-leader) state.
#[test]
fn coordinator_set_leader_none_from_fresh() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_leader(None);

    assert!(!coord.is_leader());
    assert!(coord.leader_endpoint().is_none());
}

/// Clearing the leader resets both the flag and the stored endpoint.
#[test]
fn coordinator_clear_leader() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());
    coord.set_leader(Some("127.0.0.1:7001".to_string()));
    assert!(coord.is_leader());

    coord.set_leader(None);

    assert!(!coord.is_leader(), "leader flag must reset when cleared");
    assert!(
        coord.leader_endpoint().is_none(),
        "leader endpoint must be cleared together with the flag"
    );
}

/// A subsequent `set_leader` call overwrites the previous endpoint atomically
/// with the flag (no stale endpoint / flag combinations).
#[test]
fn coordinator_leader_endpoint_overwrite() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_leader(Some("127.0.0.1:7001".to_string()));
    coord.set_leader(Some("127.0.0.1:7002".to_string()));

    assert!(coord.is_leader());
    assert_eq!(
        coord.leader_endpoint().map(|e| e.to_string()),
        Some("127.0.0.1:7002".to_string())
    );
}

/// Documented behavior: `is_leader` reflects "some leader endpoint is known",
/// so setting an endpoint (even one that is not this node) flips it to true.
#[test]
fn coordinator_is_leader_flag_follows_endpoint_presence() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_leader(Some("10.0.0.9:7009".to_string()));
    assert!(coord.is_leader(), "flag is derived from endpoint presence");

    coord.set_leader(None);
    assert!(!coord.is_leader());
}

// ============================================================================
// Membership state
// ============================================================================

/// Member endpoints are exposed in the order they were set.
#[test]
fn coordinator_set_members_basic() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_members(vec![
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
        "127.0.0.1:7003".to_string(),
    ]);

    assert_eq!(
        members_as_strings(&coord),
        vec![
            "127.0.0.1:7001".to_string(),
            "127.0.0.1:7002".to_string(),
            "127.0.0.1:7003".to_string(),
        ]
    );
}

/// `set_members` replaces the previous list; it must not append.
#[test]
fn coordinator_set_members_replaces_previous() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_members(vec!["a:1".to_string(), "b:2".to_string()]);
    coord.set_members(vec!["c:3".to_string()]);

    assert_eq!(members_as_strings(&coord), vec!["c:3".to_string()]);
}

/// Edge case: setting an empty member list clears the membership view.
#[test]
fn coordinator_set_members_empty_clears() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_members(vec!["a:1".to_string()]);
    coord.set_members(Vec::new());

    assert!(coord.member_endpoints().is_empty());
}

/// Leadership and membership state are independent of each other.
#[test]
fn coordinator_leader_and_members_are_independent() {
    let coord = CatgaRaftCoordinator::new("node-1".to_string());

    coord.set_leader(Some("127.0.0.1:7001".to_string()));
    coord.set_members(vec![
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
    ]);

    // Changing membership must not disturb leadership state and vice versa.
    assert!(coord.is_leader());
    assert_eq!(coord.member_endpoints().len(), 2);

    coord.set_leader(None);
    assert_eq!(
        coord.member_endpoints().len(),
        2,
        "members survive leader change"
    );

    coord.set_members(Vec::new());
    assert!(
        coord.leader_endpoint().is_none(),
        "cleared leader stays cleared"
    );
}

// ============================================================================
// Trait-object usage and concurrency
// ============================================================================

/// The coordinator is usable through the public `ConsensusCoordinator` trait
/// object surface, which is how the runtime hands it to other components.
#[test]
fn coordinator_as_trait_object() {
    assert_send_sync::<CatgaRaftCoordinator>();

    let coord = CatgaRaftCoordinator::new("node-obj".to_string());
    coord.set_leader(Some("127.0.0.1:7001".to_string()));

    // Trait methods must be readable through the boxed trait object surface.
    let boxed: Box<dyn ConsensusCoordinator> = Box::new(coord);
    assert_eq!(boxed.node_id(), "node-obj");
    assert!(boxed.is_leader());
    assert_eq!(
        boxed.leader_endpoint().map(|e| e.to_string()),
        Some("127.0.0.1:7001".to_string())
    );
    assert_eq!(boxed.member_endpoints().len(), 0);
}

/// Concurrent readers and writers must not deadlock or lose the final state.
#[test]
fn coordinator_concurrent_access_across_threads() {
    let coord = Arc::new(CatgaRaftCoordinator::new("node-1".to_string()));
    let mut handles = Vec::new();

    for i in 0..4u32 {
        let c = Arc::clone(&coord);
        handles.push(thread::spawn(move || {
            for _ in 0..200 {
                c.set_leader(Some(format!("127.0.0.1:{}", 7000 + i)));
                c.set_members(vec![format!("127.0.0.1:{}", 7000 + i)]);
            }
        }));
    }

    let reader = Arc::clone(&coord);
    handles.push(thread::spawn(move || {
        for _ in 0..400 {
            // Read-only trait calls while writers are active.
            let _ = reader.is_leader();
            let _ = reader.leader_endpoint();
            let _ = reader.member_endpoints().len();
        }
    }));

    for h in handles {
        h.join().expect("coordinator worker thread panicked");
    }

    // After all writers finish, one final consistent state must be visible.
    assert!(coord.is_leader());
    assert!(coord.leader_endpoint().is_some());
    assert_eq!(coord.member_endpoints().len(), 1);
}

/// Async task access (as done inside the runtime) must also be sound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_concurrent_access_async() {
    let coord = Arc::new(CatgaRaftCoordinator::new("node-async".to_string()));

    let mut tasks = Vec::new();
    for i in 0..3usize {
        let c = Arc::clone(&coord);
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                c.set_leader(Some(format!("127.0.0.1:{}", 8000 + i)));
                c.set_members(vec![format!("127.0.0.1:{}", 8000 + i)]);
                assert_eq!(c.node_id(), "node-async");
            }
        }));
    }

    for t in tasks {
        t.await.expect("coordinator async task panicked");
    }

    assert!(coord.is_leader());
    assert_eq!(coord.member_endpoints().len(), 1);
}
