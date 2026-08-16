//! Coordinator implementation for CatgaRaft.

use std::sync::Arc;

use catga_core::ConsensusCoordinator;

/// CatgaRaftCoordinator provides cluster leadership and membership view.
pub struct CatgaRaftCoordinator {
    /// This node's stable identifier.
    node_id: Arc<str>,
    /// Whether this node currently holds leadership.
    is_leader: parking_lot::RwLock<bool>,
    /// The endpoint of the elected leader when known.
    leader_endpoint: parking_lot::RwLock<Option<Arc<str>>>,
    /// List of all member endpoints in the cluster.
    members: parking_lot::RwLock<Vec<Arc<str>>>,
}

impl CatgaRaftCoordinator {
    /// Creates a new coordinator for a node with the given ID.
    pub fn new(node_id: String) -> Self {
        Self {
            node_id: Arc::from(node_id.into_boxed_str()),
            is_leader: parking_lot::RwLock::new(false),
            leader_endpoint: parking_lot::RwLock::new(None),
            members: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Updates the leadership state.
    pub fn set_leader(&self, endpoint: Option<String>) {
        let is_leader = endpoint.is_some();
        let leader_ep = endpoint.map(|e| Arc::from(e.into_boxed_str()));
        *self.is_leader.write() = is_leader;
        *self.leader_endpoint.write() = leader_ep;
    }

    /// Updates the member list.
    pub fn set_members(&self, members: Vec<String>) {
        let members: Vec<Arc<str>> = members
            .into_iter()
            .map(|m| Arc::from(m.into_boxed_str()))
            .collect();
        *self.members.write() = members;
    }
}

impl ConsensusCoordinator for CatgaRaftCoordinator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_leader(&self) -> bool {
        *self.is_leader.read()
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        self.leader_endpoint.read().clone()
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        Arc::from(self.members.read().clone())
    }
}
