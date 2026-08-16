//! Integration tests for `catga_raft::node::RaftNode`.
//!
//! `RaftNode` (src/node.rs) is a thin, synchronous wrapper around
//! `raft::RawNode` that integrates the crate's `CatgaRaftConfig` and
//! `CatgaRaftResult`/`CatgaRaftError` types. These tests exercise the full
//! public surface of the module: construction (including config validation
//! failures), tick-driven election in a single-node cluster, propose, step,
//! and the shared raw-node handle.
//!
//! All tests are deterministic and use only logical clocks (no sleeps, no
//! network ports).

use std::sync::Arc;

use catga_raft::node::RaftNode;
use catga_raft::{CatgaRaftConfig, CatgaRaftError};
use raft::prelude::{Message, MessageType};
use raft::storage::MemStorage;
use raft::{INVALID_ID, StateRole};
use slog::Logger;

/// A no-op logger suitable for constructing raft nodes in tests.
fn test_logger() -> Logger {
    Logger::root(slog::Discard, slog::o!())
}

/// Config for node 1, the only voter of a single-node cluster.
fn single_node_config() -> CatgaRaftConfig {
    CatgaRaftConfig {
        node_id: 1,
        cluster_id: 42,
        election_tick: 10,
        heartbeat_tick: 3,
        max_size_per_msg: 1024 * 1024,
        max_inflight_msgs: 16,
    }
}

/// In-memory storage pre-seeded with node 1 as the only voter.
fn single_voter_storage() -> MemStorage {
    MemStorage::new_with_conf_state((vec![1u64], vec![]))
}

/// Build a healthy single-node `RaftNode`.
fn new_single_node() -> RaftNode<MemStorage> {
    RaftNode::new(single_node_config(), single_voter_storage(), &test_logger())
        .expect("single-node RaftNode construction should succeed")
}

/// Tick the node until it becomes leader or the tick budget is exhausted.
///
/// raft 0.7 randomizes the election timeout into
/// `[election_tick, 2 * election_tick)`, so `3 * election_tick` ticks is a
/// generous, deterministic budget for a single voter to win its own election.
///
/// With `pre_vote: true` (enabled in `RaftNode::new`) the campaign gains a
/// PreVote phase, but no extra ticks are needed: raft-rs `campaign()` runs
/// `become_pre_candidate` -> `poll(self)` -> (won, single voter) ->
/// `campaign(CAMPAIGN_ELECTION)` -> `become_candidate` -> `become_leader`
/// synchronously inside the very `tick()` that fires MsgHup. Worst case is
/// therefore still `2 * election_tick - 1` ticks, well within the budget.
fn elect_leader(node: &RaftNode<MemStorage>, tick_budget: usize) {
    for _ in 0..tick_budget {
        if node.raw_node().lock().raft.state == StateRole::Leader {
            return;
        }
        node.tick();
    }
}

// ============================================================================
// Construction / defaults
// ============================================================================

#[test]
fn test_node_construction_and_config_roundtrip() {
    let config = single_node_config();
    let node = RaftNode::new(config.clone(), single_voter_storage(), &test_logger())
        .expect("construction should succeed");

    // config() must hand back exactly what the node was built with.
    assert_eq!(node.config().node_id, 1);
    assert_eq!(node.config().cluster_id, 42);
    assert_eq!(node.config().election_tick, 10);
    assert_eq!(node.config().heartbeat_tick, 3);
    assert_eq!(node.config().max_size_per_msg, 1024 * 1024);
    assert_eq!(node.config().max_inflight_msgs, 16);

    // A freshly created node starts as a follower with no known leader.
    let raw = node.raw_node();
    let rn = raw.lock();
    assert_eq!(rn.raft.state, StateRole::Follower);
    assert_eq!(rn.raft.leader_id, INVALID_ID);
    assert_eq!(rn.raft.id, 1);
}

/// Election hardening: `RaftNode::new` must build the raft::Config with
/// `pre_vote` and `check_quorum` enabled. These are node-level raft settings
/// (not fields of `CatgaRaftConfig`), asserted here against the underlying
/// `raft::RaftCore`.
///
/// - `pre_vote` stops a partitioned/stalled node from bumping the cluster
///   term when it rejoins (it must win a term-less pre-election first).
/// - `check_quorum` makes a leader that loses quorum step down within one
///   election timeout instead of accepting writes that can never commit.
#[test]
fn test_node_enables_pre_vote_and_check_quorum() {
    let node = new_single_node();
    let raw = node.raw_node();
    let rn = raw.lock();
    assert!(
        rn.raft.pre_vote,
        "pre_vote must be enabled in RaftNode::new"
    );
    assert!(
        rn.raft.check_quorum,
        "check_quorum must be enabled in RaftNode::new"
    );
}

#[test]
fn test_node_new_rejects_election_tick_not_greater_than_heartbeat() {
    // raft::Config::validate requires election_tick > heartbeat_tick.
    let mut config = single_node_config();
    config.election_tick = 3;
    config.heartbeat_tick = 3;

    let result = RaftNode::new(config, single_voter_storage(), &test_logger());
    assert!(matches!(result, Err(CatgaRaftError::Raft(_))));
}

#[test]
fn test_node_new_rejects_zero_heartbeat_tick() {
    let mut config = single_node_config();
    config.heartbeat_tick = 0;

    let result = RaftNode::new(config, single_voter_storage(), &test_logger());
    assert!(matches!(result, Err(CatgaRaftError::Raft(_))));
}

#[test]
#[should_panic]
fn test_node_new_zero_node_id_panics() {
    // CatgaRaftConfig::default() has node_id = 0. raft::RawNode::new asserts
    // that the node id is non-zero before config validation can report it,
    // so construction must panic rather than return Err.
    let _ = RaftNode::new(
        CatgaRaftConfig::default(),
        single_voter_storage(),
        &test_logger(),
    );
}

// ============================================================================
// Tick / election happy paths
// ============================================================================

#[test]
fn test_node_tick_wins_single_node_election() {
    let config = single_node_config();
    let node = new_single_node();

    elect_leader(&node, 3 * config.election_tick);

    let rn_guard = node.raw_node();
    let rn = rn_guard.lock();
    assert_eq!(rn.raft.state, StateRole::Leader);
    assert_eq!(rn.raft.leader_id, 1);
    // The campaign must have bumped the term at least once (0 -> 1).
    assert!(rn.raft.term >= 1);
    // Pre-vote regression check: the PreVote phase must not inflate the term.
    // A single voter's first election runs pre-vote (term-less) + one real
    // campaign, so the term must land on exactly 1, not higher.
    assert_eq!(rn.raft.term, 1);
}

#[test]
fn test_node_stays_leader_across_extra_ticks() {
    let config = single_node_config();
    let node = new_single_node();
    elect_leader(&node, 3 * config.election_tick);

    // With check_quorum enabled, the leader re-checks quorum activity every
    // election timeout and steps down if quorum is inactive. A single-node
    // cluster never trips this: `quorum_recently_active` counts the leader
    // itself as active, so the singleton quorum is always satisfied and
    // heartbeat ticks must not demote the leader.
    for _ in 0..(config.heartbeat_tick * 3 + 2) {
        node.tick();
    }

    let rn_guard = node.raw_node();
    let rn = rn_guard.lock();
    assert_eq!(rn.raft.state, StateRole::Leader);
    assert_eq!(rn.raft.leader_id, 1);
}

// ============================================================================
// Propose
// ============================================================================

#[test]
fn test_node_propose_appends_entry_when_leader() {
    let config = single_node_config();
    let node = new_single_node();
    elect_leader(&node, 3 * config.election_tick);

    let before = node.raw_node().lock().raft.raft_log.last_index();

    let payload = b"catga-raft-test-payload".to_vec();
    node.propose(payload.clone())
        .expect("leader should accept a proposal");

    let rn_guard = node.raw_node();
    let rn = rn_guard.lock();
    assert_eq!(rn.raft.raft_log.last_index(), before + 1);
    let entries = rn.raft.raft_log.all_entries();
    assert!(
        entries.iter().any(|e| e.data.to_vec() == payload),
        "proposed payload must appear in the raft log"
    );
}

#[test]
fn test_node_propose_without_leader_is_dropped() {
    // No ticks were issued, so the node is still a follower without a known
    // leader; raft returns ProposalDropped, surfaced as CatgaRaftError::Raft.
    let node = new_single_node();

    let result = node.propose(b"should be dropped".to_vec());
    assert!(matches!(result, Err(CatgaRaftError::Raft(_))));

    // Nothing was appended to the log.
    let rn_guard = node.raw_node();
    let rn = rn_guard.lock();
    assert!(rn.raft.raft_log.all_entries().is_empty());
}

// ============================================================================
// Step
// ============================================================================

#[test]
fn test_node_step_rejects_local_message() {
    let node = new_single_node();

    // MsgHup is a local-only message; RawNode::step rejects it with
    // Error::StepLocalMsg, which the wrapper maps to CatgaRaftError::Raft.
    let mut msg = Message::default();
    msg.set_msg_type(MessageType::MsgHup);

    let result = node.step(msg);
    assert!(matches!(result, Err(CatgaRaftError::Raft(_))));
}

#[test]
fn test_node_step_rejects_response_from_unknown_peer() {
    let node = new_single_node();

    // Node 99 is not part of this single-node cluster, so an append response
    // from it fails with Error::StepPeerNotFound.
    let mut msg = Message::default();
    msg.set_msg_type(MessageType::MsgAppendResponse);
    msg.set_from(99);
    msg.set_term(1);

    let result = node.step(msg);
    assert!(matches!(result, Err(CatgaRaftError::Raft(_))));
}

#[test]
fn test_node_step_heartbeat_adopts_new_leader_and_term() {
    let node = new_single_node();

    // A heartbeat from "node 2" at a higher term must demote us to follower
    // at that term with leader 2, and elicit a heartbeat response.
    let mut msg = Message::default();
    msg.set_msg_type(MessageType::MsgHeartbeat);
    msg.set_from(2);
    msg.set_term(7);

    node.step(msg)
        .expect("heartbeat from a higher term must be accepted");

    let rn_guard = node.raw_node();
    let rn = rn_guard.lock();
    assert_eq!(rn.raft.state, StateRole::Follower);
    assert_eq!(rn.raft.term, 7);
    assert_eq!(rn.raft.leader_id, 2);
    assert!(
        rn.raft
            .msgs
            .iter()
            .any(|m| m.msg_type == MessageType::MsgHeartbeatResponse && m.to == 2),
        "follower should queue a heartbeat response to the new leader"
    );
}

// ============================================================================
// Raw node handle
// ============================================================================

#[test]
fn test_node_raw_node_handles_share_state() {
    let config = single_node_config();
    let node = new_single_node();

    // raw_node() hands out clones of the same Arc-wrapped RawNode.
    let h1 = node.raw_node();
    let h2 = node.raw_node();
    assert!(Arc::ptr_eq(&h1, &h2));

    // State changes driven through the wrapper are visible through a
    // previously obtained handle.
    elect_leader(&node, 3 * config.election_tick);
    assert_eq!(h1.lock().raft.state, StateRole::Leader);
}
