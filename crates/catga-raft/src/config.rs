use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CatgaRaftConfig {
    pub node_id: u64,
    pub cluster_id: u64,
    /// Number of owner-loop ticks a follower waits without leader contact
    /// before starting an election. raft-rs randomizes the actual timeout
    /// into `[election_tick, 2 * election_tick)`, so with the default 100ms
    /// tick this yields a 1.0-1.9s window.
    ///
    /// Sizing guidance: for clusters larger than ~10 nodes, raise this to
    /// 20-30 (e.g. via [`CatgaRaftConfig::for_cluster_size`]). A wider
    /// randomized window lowers the probability that several followers time
    /// out simultaneously, split the vote, and re-elect repeatedly
    /// ("election storms"), which were observed under CPU starvation at
    /// 50 nodes. Must stay greater than `heartbeat_tick`
    /// (raft::Config::validate).
    pub election_tick: usize,
    /// Number of owner-loop ticks between leader heartbeats. Keep this
    /// roughly 10x smaller than `election_tick` (raft-rs's own suggestion)
    /// so transient delays do not trigger needless elections.
    pub heartbeat_tick: usize,
    pub max_size_per_msg: u64,
    pub max_inflight_msgs: usize,
}

impl CatgaRaftConfig {
    /// Defaults sized for a cluster of `n` nodes.
    ///
    /// Returns [`CatgaRaftConfig::default()`] with `election_tick` scaled by
    /// cluster size (larger clusters get a wider randomized election window
    /// to reduce simultaneous-timeout vote-splitting, see the `election_tick`
    /// field docs):
    ///
    /// | cluster size | election_tick | randomized window @100ms tick |
    /// |--------------|---------------|-------------------------------|
    /// | <= 5         | 10 (default)  | 1.0s - 1.9s                   |
    /// | <= 20        | 20            | 2.0s - 3.9s                   |
    /// | <= 100       | 30            | 3.0s - 5.9s                   |
    /// | > 100        | 40            | 4.0s - 7.9s                   |
    ///
    /// All other fields keep their defaults; in particular `heartbeat_tick`
    /// stays at 3, so `election_tick > heartbeat_tick` holds for every size.
    /// The caller still sets `node_id`/`cluster_id`.
    ///
    /// This is an opt-in convenience constructor: nothing in the builder or
    /// runtime calls it automatically.
    pub fn for_cluster_size(n: u64) -> Self {
        let election_tick = match n {
            1..=5 => 10,
            6..=20 => 20,
            21..=100 => 30,
            _ => 40,
        };
        Self {
            election_tick,
            ..Default::default()
        }
    }
}

impl Default for CatgaRaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            cluster_id: 0,
            election_tick: 10,
            heartbeat_tick: 3,
            // Aligned with the gRPC wire limit: the tonic server in
            // `transport::server` decodes at most 8MB per message, so any
            // larger default would fail on the wire.
            max_size_per_msg: 8 * 1024 * 1024,
            max_inflight_msgs: 256,
        }
    }
}

/// Pipeline 配置
#[derive(Clone, Debug)]
pub struct PipelineConfig {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub max_inflight: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            flush_interval: Duration::from_millis(1),
            max_inflight: 1024,
        }
    }
}
