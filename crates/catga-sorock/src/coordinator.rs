//! Local cluster view of one sorock node.

use std::sync::{Arc, RwLock};

use catga_core::ConsensusCoordinator;

/// [`ConsensusCoordinator`] implementation for a sorock node.
///
/// # Leadership limitation
///
/// sorock 0.12 exposes **no public API to observe leadership**: the election
/// state and ballot are internal to the Raft process, and the gRPC request
/// type needed to poll node state is not exported from the crate. Therefore:
///
/// - [`Self::is_leader`] always returns `false`, and
/// - [`Self::leader_endpoint`] always returns `None`.
///
/// Both are conservative: they never claim leadership that does not exist.
/// Applications that need leader-only semantics must fence externally (for
/// example with a distributed lock or by checking proposal success).
///
/// # Member view
///
/// [`Self::member_endpoints`] returns the endpoints this node *locally knows*:
/// the configured seed members plus every endpoint added or removed through
/// the owning [`crate::SorockRuntime`]. It is not a live query against the
/// group and can lag behind membership changes issued on other nodes.
pub struct SorockCoordinator {
    node_id: String,
    members: RwLock<Arc<[Arc<str>]>>,
}

impl SorockCoordinator {
    /// Creates a coordinator view for `node_id`, seeded with `members`.
    pub fn new(
        node_id: impl Into<String>,
        members: impl IntoIterator<Item = impl Into<Arc<str>>>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            members: RwLock::new(members.into_iter().map(Into::into).collect()),
        }
    }

    fn read_members(&self) -> Arc<[Arc<str>]> {
        let members = self.members.read().unwrap_or_else(|e| e.into_inner());
        Arc::clone(&members)
    }

    pub(crate) fn add_endpoint(&self, endpoint: &str) {
        let mut members = self.members.write().unwrap_or_else(|e| e.into_inner());
        if members.iter().any(|m| m.as_ref() == endpoint) {
            return;
        }
        let mut next: Vec<Arc<str>> = members.to_vec();
        next.push(endpoint.into());
        *members = next.into();
    }

    pub(crate) fn remove_endpoint(&self, endpoint: &str) {
        let mut members = self.members.write().unwrap_or_else(|e| e.into_inner());
        let next: Vec<Arc<str>> = members
            .iter()
            .filter(|m| m.as_ref() != endpoint)
            .cloned()
            .collect();
        *members = next.into();
    }
}

impl ConsensusCoordinator for SorockCoordinator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_leader(&self) -> bool {
        // sorock 0.12 exposes no leadership query; see the type-level docs.
        false
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        // sorock 0.12 exposes no leadership query; see the type-level docs.
        None
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        self.read_members()
    }
}
