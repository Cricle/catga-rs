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
