//! Serde-friendly cluster configuration with validated Raft timing.

use std::{collections::HashSet, error::Error, fmt, path::PathBuf, time::Duration};

use serde::Deserialize;

use crate::{RaftMember, RaftNode, RaftNodeError};

const DEFAULT_TICK_INTERVAL_MS: u64 = 10;
const DEFAULT_ELECTION_TIMEOUT_MS: u64 = 150;
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 50;

/// Validated tick-derived Raft timing used to build a node and runtime.
///
/// Obtain a timing through [`RaftClusterConfig::raft_timing`] or
/// [`RaftClusterConfig::open_node`]; direct construction is intentionally
/// not public so timing validation remains centralized.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RaftTiming {
    tick_interval: Duration,
    election_ticks: usize,
    heartbeat_ticks: usize,
}

impl RaftTiming {
    pub(crate) const fn default_node() -> Self {
        Self {
            tick_interval: Duration::from_millis(DEFAULT_TICK_INTERVAL_MS),
            election_ticks: 10,
            heartbeat_ticks: 1,
        }
    }

    /// Returns the Tokio interval used to advance the Raft logical clock.
    pub const fn tick_interval(self) -> Duration {
        self.tick_interval
    }

    /// Returns the validated Raft election timeout in logical ticks.
    pub const fn election_ticks(self) -> usize {
        self.election_ticks
    }

    /// Returns the validated Raft heartbeat interval in logical ticks.
    pub const fn heartbeat_ticks(self) -> usize {
        self.heartbeat_ticks
    }
}

/// One remote Raft member in a deserializable cluster configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftClusterMemberConfig {
    id: u64,
    endpoint: Box<str>,
}

/// Configuration errors caught before a Raft node is created.
#[derive(Debug)]
pub enum RaftClusterConfigError {
    /// A configured Raft member used the reserved zero identifier.
    ZeroMemberId,
    /// The configured local or remote endpoint was empty.
    EmptyEndpoint,
    /// A remote member duplicated the local node identifier.
    LocalMemberDuplicated(u64),
    /// More than one remote member used this identifier.
    DuplicateMemberId(u64),
    /// A timing duration or its derived tick count was invalid.
    InvalidTiming,
    /// The local cluster test helper received invalid dimensions.
    InvalidLocalCluster,
    /// Raft rejected the validated member or timing configuration.
    Node(RaftNodeError),
}

impl fmt::Display for RaftClusterConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMemberId => formatter.write_str("Raft member id zero is reserved"),
            Self::EmptyEndpoint => formatter.write_str("Raft member endpoints must not be empty"),
            Self::LocalMemberDuplicated(id) => {
                write!(formatter, "remote members must not repeat local node id {id}")
            }
            Self::DuplicateMemberId(id) => write!(formatter, "duplicate remote Raft member id {id}"),
            Self::InvalidTiming => formatter.write_str(
                "tick, heartbeat, and election timing must be non-zero with election after heartbeat",
            ),
            Self::InvalidLocalCluster => {
                formatter.write_str("a local cluster needs at least one node and a valid local id")
            }
            Self::Node(error) => error.fmt(formatter),
        }
    }
}

impl Error for RaftClusterConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Node(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RaftNodeError> for RaftClusterConfigError {
    fn from(error: RaftNodeError) -> Self {
        Self::Node(error)
    }
}

/// Serde-compatible equivalent of Catga's C# `RaftClusterConfiguration`.
///
/// `members` contains remote nodes only; `local_node_endpoint` and `node_id`
/// describe this process. Durations are integer milliseconds so JSON, TOML,
/// and environment-backed configuration use the same stable representation.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftClusterConfig {
    node_id: u64,
    local_node_endpoint: Box<str>,
    #[serde(default)]
    members: Vec<RaftClusterMemberConfig>,
    #[serde(default = "default_tick_interval_ms")]
    tick_interval_ms: u64,
    #[serde(default = "default_election_timeout_ms")]
    election_timeout_ms: u64,
    #[serde(default = "default_heartbeat_interval_ms")]
    heartbeat_interval_ms: u64,
    persistent_state_path: Option<PathBuf>,
}

impl RaftClusterConfig {
    /// Builds a deterministic local Raft configuration for development.
    ///
    /// `node_id` is a zero-based process index, matching Catga's C# helper;
    /// it is converted to Raft's non-zero numeric member identifier internally.
    pub fn local(
        node_id: u64,
        total_nodes: u64,
        base_port: u16,
    ) -> Result<Self, RaftClusterConfigError> {
        if total_nodes == 0 || node_id >= total_nodes {
            return Err(RaftClusterConfigError::InvalidLocalCluster);
        }
        let port = u64::from(base_port);
        let available_ports = u64::from(u16::MAX) + 1;
        if total_nodes > available_ports - port {
            return Err(RaftClusterConfigError::InvalidLocalCluster);
        }
        let raft_node_id = node_id
            .checked_add(1)
            .ok_or(RaftClusterConfigError::InvalidLocalCluster)?;
        let local_port = port
            .checked_add(node_id)
            .ok_or(RaftClusterConfigError::InvalidLocalCluster)?;
        let members = (0..total_nodes)
            .filter(|id| *id != node_id)
            .map(|id| {
                let member_id = id
                    .checked_add(1)
                    .ok_or(RaftClusterConfigError::InvalidLocalCluster)?;
                let member_port = port
                    .checked_add(id)
                    .ok_or(RaftClusterConfigError::InvalidLocalCluster)?;
                Ok(RaftClusterMemberConfig {
                    id: member_id,
                    endpoint: format!("http://localhost:{member_port}").into_boxed_str(),
                })
            })
            .collect::<Result<Vec<_>, RaftClusterConfigError>>()?;
        Ok(Self {
            node_id: raft_node_id,
            local_node_endpoint: format!("http://localhost:{local_port}").into(),
            members,
            tick_interval_ms: default_tick_interval_ms(),
            election_timeout_ms: default_election_timeout_ms(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            persistent_state_path: Some(format!("./raft-state-node{node_id}").into()),
        })
    }

    /// Returns the runtime tick interval after validating timing input.
    pub fn tick_interval(&self) -> Result<Duration, RaftClusterConfigError> {
        Ok(self.raft_timing()?.tick_interval())
    }

    /// Converts millisecond durations to the `raft-rs` tick configuration.
    pub fn raft_timing(&self) -> Result<RaftTiming, RaftClusterConfigError> {
        if self.tick_interval_ms == 0
            || self.heartbeat_interval_ms == 0
            || self.election_timeout_ms <= self.heartbeat_interval_ms
        {
            return Err(RaftClusterConfigError::InvalidTiming);
        }
        let heartbeat_ticks = ceil_ticks(self.heartbeat_interval_ms, self.tick_interval_ms)?;
        let election_ticks = ceil_ticks(self.election_timeout_ms, self.tick_interval_ms)?;
        if election_ticks <= heartbeat_ticks {
            return Err(RaftClusterConfigError::InvalidTiming);
        }
        Ok(RaftTiming {
            tick_interval: Duration::from_millis(self.tick_interval_ms),
            election_ticks,
            heartbeat_ticks,
        })
    }

    /// Produces the full fixed voter list, including this node.
    pub fn members(&self) -> Result<Vec<RaftMember>, RaftClusterConfigError> {
        if self.node_id == 0 {
            return Err(RaftClusterConfigError::ZeroMemberId);
        }
        if self.local_node_endpoint.is_empty() {
            return Err(RaftClusterConfigError::EmptyEndpoint);
        }
        let mut ids = HashSet::with_capacity(self.members.len());
        let mut members = Vec::with_capacity(self.members.len() + 1);
        members.push(RaftMember::new(
            self.node_id,
            self.local_node_endpoint.to_string(),
        ));
        for member in &self.members {
            if member.id == 0 {
                return Err(RaftClusterConfigError::ZeroMemberId);
            }
            if member.id == self.node_id {
                return Err(RaftClusterConfigError::LocalMemberDuplicated(member.id));
            }
            if member.endpoint.is_empty() {
                return Err(RaftClusterConfigError::EmptyEndpoint);
            }
            if !ids.insert(member.id) {
                return Err(RaftClusterConfigError::DuplicateMemberId(member.id));
            }
            members.push(RaftMember::new(member.id, member.endpoint.to_string()));
        }
        Ok(members)
    }

    /// Opens either an in-memory or `raft-engine` backed node from this config.
    pub fn open_node(&self) -> Result<RaftNode, RaftClusterConfigError> {
        let timing = self.raft_timing()?;
        let members = self.members()?;
        match &self.persistent_state_path {
            Some(path) => Ok(RaftNode::open_persistent_with_timing(
                self.node_id,
                self.local_node_endpoint.to_string(),
                members,
                path,
                timing,
            )?),
            None => Ok(RaftNode::new_with_timing(
                self.node_id,
                self.local_node_endpoint.to_string(),
                members,
                timing,
            )?),
        }
    }
}

fn ceil_ticks(duration_ms: u64, tick_ms: u64) -> Result<usize, RaftClusterConfigError> {
    let ticks = duration_ms / tick_ms + u64::from(!duration_ms.is_multiple_of(tick_ms));
    usize::try_from(ticks).map_err(|_| RaftClusterConfigError::InvalidTiming)
}

const fn default_tick_interval_ms() -> u64 {
    DEFAULT_TICK_INTERVAL_MS
}

const fn default_election_timeout_ms() -> u64 {
    DEFAULT_ELECTION_TIMEOUT_MS
}

const fn default_heartbeat_interval_ms() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_MS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterCoordinator, LeadershipSnapshot, LeadershipSubscription, cluster_health};
    use std::sync::Arc;

    // Mock ClusterCoordinator for testing cluster_health
    struct MockCoordinator {
        node_id: Box<str>,
        is_leader: bool,
        leader_endpoint: Option<Arc<str>>,
        member_endpoints: Arc<[Arc<str>]>,
    }

    impl MockCoordinator {
        fn new(
            node_id: &str,
            is_leader: bool,
            leader_endpoint: Option<&str>,
            member_count: usize,
        ) -> Self {
            let member_endpoints: Arc<[Arc<str>]> = (0..member_count)
                .map(|i| Arc::from(format!("http://localhost:{}", 9000 + i)))
                .collect();
            Self {
                node_id: node_id.into(),
                is_leader,
                leader_endpoint: leader_endpoint.map(Into::into),
                member_endpoints,
            }
        }
    }

    impl ClusterCoordinator for MockCoordinator {
        fn node_id(&self) -> &str {
            &self.node_id
        }
        fn is_leader(&self) -> bool {
            self.is_leader
        }
        fn leader_endpoint(&self) -> Option<Arc<str>> {
            self.leader_endpoint.clone()
        }
        fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot> {
            Arc::new(LeadershipSnapshot {
                epoch: 0,
                leader_node_id: self.leader_endpoint.clone(),
                leader_endpoint: self.leader_endpoint.clone(),
            })
        }
        fn subscribe_leadership(&self) -> LeadershipSubscription {
            todo!("not needed for tests")
        }
        fn member_endpoints(&self) -> Arc<[Arc<str>]> {
            Arc::clone(&self.member_endpoints)
        }
        async fn wait_for_leadership(&self, _timeout: Duration) -> bool {
            self.is_leader
        }
        async fn wait_for_leadership_change(&self, was_leader: bool) -> bool {
            if self.is_leader() != was_leader {
                self.is_leader()
            } else {
                false
            }
        }
    }

    // ===== RaftTiming tests =====

    #[test]
    fn raft_timing_default_node() {
        let timing = RaftTiming::default_node();
        assert_eq!(
            timing.tick_interval(),
            Duration::from_millis(DEFAULT_TICK_INTERVAL_MS)
        );
        assert_eq!(timing.election_ticks(), 10);
        assert_eq!(timing.heartbeat_ticks(), 1);
    }

    #[test]
    fn raft_timing_tick_interval() {
        let timing = RaftTiming::default_node();
        assert_eq!(timing.tick_interval(), Duration::from_millis(10));
    }

    // ===== RaftClusterConfigError tests =====

    #[test]
    fn config_error_display_zero_member_id() {
        let error = RaftClusterConfigError::ZeroMemberId;
        assert_eq!(format!("{}", error), "Raft member id zero is reserved");
    }

    #[test]
    fn config_error_display_empty_endpoint() {
        let error = RaftClusterConfigError::EmptyEndpoint;
        assert_eq!(
            format!("{}", error),
            "Raft member endpoints must not be empty"
        );
    }

    #[test]
    fn config_error_display_local_member_duplicated() {
        let error = RaftClusterConfigError::LocalMemberDuplicated(42);
        assert_eq!(
            format!("{}", error),
            "remote members must not repeat local node id 42"
        );
    }

    #[test]
    fn config_error_display_duplicate_member_id() {
        let error = RaftClusterConfigError::DuplicateMemberId(99);
        assert_eq!(format!("{}", error), "duplicate remote Raft member id 99");
    }

    #[test]
    fn config_error_display_invalid_timing() {
        let error = RaftClusterConfigError::InvalidTiming;
        assert!(format!("{}", error).contains("tick"));
        assert!(format!("{}", error).contains("heartbeat"));
        assert!(format!("{}", error).contains("election"));
    }

    #[test]
    fn config_error_display_invalid_local_cluster() {
        let error = RaftClusterConfigError::InvalidLocalCluster;
        assert!(format!("{}", error).contains("local cluster"));
    }

    #[test]
    fn config_error_source_returns_node_error() {
        // When error is Node variant, source() returns Some
        let node_error = RaftNodeError::EmptyMembers;
        let error = RaftClusterConfigError::Node(node_error);
        assert!(error.source().is_some());
    }

    #[test]
    fn config_error_source_returns_none_for_non_node() {
        let error = RaftClusterConfigError::ZeroMemberId;
        assert!(error.source().is_none());
    }

    // ===== RaftClusterConfig::local() tests =====

    #[test]
    fn local_cluster_valid_two_node() {
        let config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        assert_eq!(config.node_id, 1);
        assert_eq!(config.local_node_endpoint.as_ref(), "http://localhost:8000");
        assert_eq!(config.members.len(), 1);
        assert_eq!(config.members[0].id, 2);
        assert_eq!(config.members[0].endpoint.as_ref(), "http://localhost:8001");
    }

    #[test]
    fn local_cluster_valid_three_node_first() {
        let config = RaftClusterConfig::local(0, 3, 8000).expect("valid cluster config");
        assert_eq!(config.node_id, 1);
        assert_eq!(config.members.len(), 2);
        assert_eq!(config.members[0].id, 2);
        assert_eq!(config.members[1].id, 3);
    }

    #[test]
    fn local_cluster_valid_three_node_middle() {
        let config = RaftClusterConfig::local(1, 3, 8000).expect("valid cluster config");
        assert_eq!(config.node_id, 2);
        assert_eq!(config.local_node_endpoint.as_ref(), "http://localhost:8001");
        assert_eq!(config.members.len(), 2);
        // Should have members with ids 1 and 3
        let member_ids: Vec<u64> = config.members.iter().map(|m| m.id).collect();
        assert!(member_ids.contains(&1));
        assert!(member_ids.contains(&3));
    }

    #[test]
    fn local_cluster_valid_three_node_last() {
        let config = RaftClusterConfig::local(2, 3, 8000).expect("valid cluster config");
        assert_eq!(config.node_id, 3);
        assert_eq!(config.local_node_endpoint.as_ref(), "http://localhost:8002");
        assert_eq!(config.members.len(), 2);
    }

    #[test]
    fn local_cluster_error_zero_total_nodes() {
        let result = RaftClusterConfig::local(0, 0, 8000);
        assert!(matches!(
            result,
            Err(RaftClusterConfigError::InvalidLocalCluster)
        ));
    }

    #[test]
    fn local_cluster_error_node_id_equals_total() {
        let result = RaftClusterConfig::local(3, 3, 8000);
        assert!(matches!(
            result,
            Err(RaftClusterConfigError::InvalidLocalCluster)
        ));
    }

    #[test]
    fn local_cluster_error_node_id_exceeds_total() {
        let result = RaftClusterConfig::local(5, 3, 8000);
        assert!(matches!(
            result,
            Err(RaftClusterConfigError::InvalidLocalCluster)
        ));
    }

    #[test]
    fn local_cluster_error_port_overflow() {
        // When total_nodes would cause port to exceed u16::MAX
        let result = RaftClusterConfig::local(0, u64::from(u16::MAX) + 10, 8000);
        assert!(matches!(
            result,
            Err(RaftClusterConfigError::InvalidLocalCluster)
        ));
    }

    #[test]
    fn local_cluster_error_node_id_overflow() {
        // node_id + 1 overflow for u64
        let result = RaftClusterConfig::local(u64::MAX - 1, u64::MAX, 8000);
        assert!(matches!(
            result,
            Err(RaftClusterConfigError::InvalidLocalCluster)
        ));
    }

    #[test]
    fn local_cluster_persistent_state_path() {
        let config = RaftClusterConfig::local(1, 3, 8000).expect("valid cluster config");
        let path = config
            .persistent_state_path
            .as_ref()
            .expect("path should be set");
        let path_str = path.to_str().expect("valid utf8");
        assert_eq!(path_str, "./raft-state-node1");
    }

    #[test]
    fn local_cluster_default_timing() {
        let config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        assert_eq!(config.tick_interval_ms, DEFAULT_TICK_INTERVAL_MS);
        assert_eq!(config.election_timeout_ms, DEFAULT_ELECTION_TIMEOUT_MS);
        assert_eq!(config.heartbeat_interval_ms, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    // ===== RaftClusterConfig::members() tests =====

    #[test]
    fn members_returns_single_node() {
        let config = RaftClusterConfig::local(0, 1, 8000).expect("valid cluster config");
        let members = config.members().expect("valid cluster config");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id(), 1);
    }

    #[test]
    fn members_returns_multiple_nodes() {
        let config = RaftClusterConfig::local(0, 3, 8000).expect("valid cluster config");
        let members = config.members().expect("valid cluster config");
        assert_eq!(members.len(), 3);
    }

    #[test]
    fn members_error_zero_local_node_id() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.node_id = 0;
        let result = config.members();
        assert!(matches!(result, Err(RaftClusterConfigError::ZeroMemberId)));
    }

    #[test]
    fn members_error_empty_local_endpoint() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.local_node_endpoint = "".into();
        let result = config.members();
        assert!(matches!(result, Err(RaftClusterConfigError::EmptyEndpoint)));
    }

    #[test]
    fn members_error_remote_zero_member_id() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.members[0].id = 0;
        let result = config.members();
        assert!(matches!(result, Err(RaftClusterConfigError::ZeroMemberId)));
    }

    #[test]
    fn members_error_remote_duplicates_local_id() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.members[0].id = config.node_id; // Duplicate local node's id
        let result = config.members();
        assert!(matches!(
            result,
            Err(RaftClusterConfigError::LocalMemberDuplicated(_))
        ));
    }

    #[test]
    fn members_error_empty_remote_endpoint() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.members[0].endpoint = "".into();
        let result = config.members();
        assert!(matches!(result, Err(RaftClusterConfigError::EmptyEndpoint)));
    }

    #[test]
    fn members_error_duplicate_remote_member_id() {
        let mut config = RaftClusterConfig::local(0, 3, 8000).expect("valid cluster config");
        // Make both members have the same id
        config.members[0].id = 42;
        config.members[1].id = 42;
        let result = config.members();
        assert!(matches!(
            result,
            Err(RaftClusterConfigError::DuplicateMemberId(42))
        ));
    }

    // ===== RaftClusterConfig::raft_timing() tests =====

    #[test]
    fn raft_timing_valid_default_values() {
        let config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        let timing = config.raft_timing().expect("valid cluster config");
        assert_eq!(timing.tick_interval(), Duration::from_millis(10));
        assert!(timing.election_ticks() > timing.heartbeat_ticks());
    }

    #[test]
    fn raft_timing_error_zero_tick_interval() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.tick_interval_ms = 0;
        let result = config.raft_timing();
        assert!(matches!(result, Err(RaftClusterConfigError::InvalidTiming)));
    }

    #[test]
    fn raft_timing_error_zero_heartbeat_interval() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.heartbeat_interval_ms = 0;
        let result = config.raft_timing();
        assert!(matches!(result, Err(RaftClusterConfigError::InvalidTiming)));
    }

    #[test]
    fn raft_timing_error_election_leq_heartbeat() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.election_timeout_ms = config.heartbeat_interval_ms; // Equal
        let result = config.raft_timing();
        assert!(matches!(result, Err(RaftClusterConfigError::InvalidTiming)));
    }

    #[test]
    fn raft_timing_error_election_less_than_heartbeat() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        config.election_timeout_ms = config.heartbeat_interval_ms - 1;
        let result = config.raft_timing();
        assert!(matches!(result, Err(RaftClusterConfigError::InvalidTiming)));
    }

    #[test]
    fn raft_timing_ceil_rounds_up() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        // tick=10ms, heartbeat=15ms should round up to 2 ticks
        config.tick_interval_ms = 10;
        config.heartbeat_interval_ms = 15;
        config.election_timeout_ms = 25;
        let timing = config.raft_timing().expect("valid cluster config");
        assert_eq!(timing.heartbeat_ticks(), 2);
        assert_eq!(timing.election_ticks(), 3);
    }

    #[test]
    fn raft_timing_exact_division() {
        let mut config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        // tick=10ms, heartbeat=20ms should be exactly 2 ticks
        config.tick_interval_ms = 10;
        config.heartbeat_interval_ms = 20;
        config.election_timeout_ms = 30;
        let timing = config.raft_timing().expect("valid cluster config");
        assert_eq!(timing.heartbeat_ticks(), 2);
        assert_eq!(timing.election_ticks(), 3);
    }

    // ===== tick_interval() convenience method =====

    #[test]
    fn tick_interval_returns_duration() {
        let config = RaftClusterConfig::local(0, 2, 8000).expect("valid cluster config");
        let interval = config.tick_interval().expect("valid cluster config");
        assert_eq!(interval, Duration::from_millis(10));
    }

    // ===== ceil_ticks function tests =====

    #[test]
    fn ceil_ticks_exact_division() {
        // 100 / 10 = 10 exactly
        let result = ceil_ticks(100, 10).expect("valid cluster config");
        assert_eq!(result, 10);
    }

    #[test]
    fn ceil_ticks_rounds_up() {
        // 101 / 10 should round up to 11
        let result = ceil_ticks(101, 10).expect("valid cluster config");
        assert_eq!(result, 11);
    }

    #[test]
    fn ceil_ticks_small_remainder() {
        // 99 / 10 should round up to 10
        let result = ceil_ticks(99, 10).expect("valid cluster config");
        assert_eq!(result, 10);
    }

    #[test]
    fn ceil_ticks_single_tick() {
        // 1 / 10 should be 1 (1 tick minimum)
        let result = ceil_ticks(1, 10).expect("valid cluster config");
        assert_eq!(result, 1);
    }

    #[test]
    fn ceil_ticks_overflow_to_usize() {
        // Very large u64 value that would overflow if converted naively
        let result = ceil_ticks(u64::MAX, 1);
        assert!(result.is_ok());
        assert!(result.expect("should be ok") > 0);
    }

    // ===== Default timing constants =====

    #[test]
    fn default_constants() {
        assert_eq!(DEFAULT_TICK_INTERVAL_MS, 10);
        assert_eq!(DEFAULT_ELECTION_TIMEOUT_MS, 150);
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_MS, 50);
    }

    #[test]
    fn default_timing_const_functions() {
        assert_eq!(default_tick_interval_ms(), 10);
        assert_eq!(default_election_timeout_ms(), 150);
        assert_eq!(default_heartbeat_interval_ms(), 50);
    }

    // ===== ClusterHealth tests =====

    #[test]
    fn cluster_health_has_leader_when_endpoint_present() {
        let coordinator = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 3);
        let health = cluster_health(&coordinator);
        assert!(health.has_leader());
        assert!(health.is_leader());
        assert_eq!(health.cluster_size(), 3);
        assert_eq!(health.node_id(), "node1");
    }

    #[test]
    fn cluster_health_no_leader_when_endpoint_none() {
        let coordinator = MockCoordinator::new("node1", false, None, 3);
        let health = cluster_health(&coordinator);
        assert!(!health.has_leader());
        assert!(!health.is_leader());
    }

    #[test]
    fn cluster_health_is_leader_when_coordinator_is_leader() {
        let coordinator = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 2);
        let health = cluster_health(&coordinator);
        assert!(health.is_leader());
    }

    #[test]
    fn cluster_health_not_leader_when_coordinator_is_follower() {
        let coordinator = MockCoordinator::new("node2", false, Some("http://localhost:9000"), 2);
        let health = cluster_health(&coordinator);
        assert!(!health.is_leader());
    }

    #[test]
    fn cluster_health_leader_endpoint() {
        let coordinator = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 3);
        let health = cluster_health(&coordinator);
        assert_eq!(health.leader_endpoint(), Some("http://localhost:9000"));
    }

    #[test]
    fn cluster_health_leader_endpoint_none() {
        let coordinator = MockCoordinator::new("node1", false, None, 3);
        let health = cluster_health(&coordinator);
        assert_eq!(health.leader_endpoint(), None);
    }

    #[test]
    fn cluster_health_clone() {
        let coordinator = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 2);
        let health = cluster_health(&coordinator);
        let cloned = health.clone();
        assert_eq!(health.is_leader(), cloned.is_leader());
        assert_eq!(health.cluster_size(), cloned.cluster_size());
    }

    #[test]
    fn cluster_health_eq() {
        let coordinator1 = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 2);
        let coordinator2 = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 2);
        let health1 = cluster_health(&coordinator1);
        let health2 = cluster_health(&coordinator2);
        assert_eq!(health1, health2);
    }

    #[test]
    fn cluster_health_not_eq_different_leader() {
        let coordinator1 = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 2);
        let coordinator2 = MockCoordinator::new("node1", false, Some("http://localhost:9000"), 2);
        let health1 = cluster_health(&coordinator1);
        let health2 = cluster_health(&coordinator2);
        assert_ne!(health1, health2);
    }

    #[test]
    fn cluster_health_not_eq_different_size() {
        let coordinator1 = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 2);
        let coordinator2 = MockCoordinator::new("node1", true, Some("http://localhost:9000"), 3);
        let health1 = cluster_health(&coordinator1);
        let health2 = cluster_health(&coordinator2);
        assert_ne!(health1, health2);
    }
}
