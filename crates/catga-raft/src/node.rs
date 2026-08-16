use parking_lot::Mutex;
use raft::{Config, RawNode, Storage};
use slog::Logger;
use std::sync::Arc;

use crate::{CatgaRaftConfig, CatgaRaftError, CatgaRaftResult};

/// RaftNode wraps raft::RawNode with CatgaRaftConfig integration
pub struct RaftNode<S: Storage> {
    raw_node: Arc<Mutex<RawNode<S>>>,
    config: CatgaRaftConfig,
}

impl<S: Storage + 'static> RaftNode<S> {
    /// Create a new RaftNode with the given config and storage
    pub fn new(config: CatgaRaftConfig, storage: S, logger: &Logger) -> CatgaRaftResult<Self> {
        let cfg = Config {
            id: config.node_id,
            election_tick: config.election_tick,
            heartbeat_tick: config.heartbeat_tick,
            max_size_per_msg: config.max_size_per_msg,
            max_inflight_msgs: config.max_inflight_msgs,
            // Pre-Vote (raft thesis §9.6, `raft::Config::pre_vote`): before a
            // node increments its term it must first win a pre-election that
            // does not bump the term. Failure mode prevented: a node that was
            // partitioned away or stalled under CPU starvation keeps timing
            // out and calling elections on its own; without pre-vote every
            // rejoin forces the whole cluster through disruptive term
            // increases (leader step-downs, dropped proposals) even when the
            // node can never win. Observed as election storms at 50 nodes.
            pre_vote: true,
            // CheckQuorum (`raft::Config::check_quorum`): a leader steps down
            // if it cannot hear from a quorum within one election timeout.
            // Failure mode prevented: after a network partition (or when the
            // leader's outbound path degrades), a stale leader in the minority
            // partition would otherwise keep accepting proposals that can
            // never commit, and a pre-vote-only cluster could be wedged by it.
            // With both flags on, the pair also covers the removed-node case
            // (raft-rs step(): lower-term appends get an explicit higher-term
            // response instead of silent term bumps).
            check_quorum: true,
            ..Default::default()
        };

        let raw_node =
            RawNode::new(&cfg, storage, logger).map_err(|e| CatgaRaftError::Raft(e.to_string()))?;

        Ok(Self {
            raw_node: Arc::new(Mutex::new(raw_node)),
            config,
        })
    }

    /// Tick the raft node, advancing its internal logical clock
    pub fn tick(&self) {
        self.raw_node.lock().tick();
    }

    /// Propose a value to the raft cluster
    pub fn propose(&self, data: Vec<u8>) -> CatgaRaftResult<()> {
        let mut node = self.raw_node.lock();
        node.propose(vec![], data)
            .map_err(|e| CatgaRaftError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Step a raft message from a peer
    pub fn step(&self, msg: raft::prelude::Message) -> CatgaRaftResult<()> {
        let mut node = self.raw_node.lock();
        node.step(msg)
            .map_err(|e| CatgaRaftError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Get the underlying RawNode for advanced operations
    pub fn raw_node(&self) -> Arc<Mutex<RawNode<S>>> {
        Arc::clone(&self.raw_node)
    }

    /// Get the config used to initialize this node
    pub fn config(&self) -> &CatgaRaftConfig {
        &self.config
    }
}
