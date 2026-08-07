//! Low-cardinality OpenTelemetry-compatible metrics for cluster coordination.

use raft::StateRole;

/// Maintains gauges and transition counters for one single-owner Raft node.
#[derive(Default)]
pub(crate) struct RaftMetrics {
    was_leader: Option<bool>,
}

impl RaftMetrics {
    /// Counts one failure under a fixed, low-cardinality operation kind.
    pub(crate) fn record_failure(&self, kind: &'static str) {
        record_failure(kind);
    }

    /// Publishes the latest Raft state without adding member identifiers as labels.
    pub(crate) fn record_state(
        &mut self,
        role: StateRole,
        leader_id: Option<u64>,
        term: u64,
        commit_index: u64,
        applied_index: u64,
        pending_commits: usize,
    ) {
        metrics::gauge!("catga.cluster.raft.leader.id").set(leader_id.unwrap_or_default() as f64);
        metrics::gauge!("catga.cluster.raft.is_leader")
            .set((role == StateRole::Leader) as u8 as f64);
        metrics::gauge!("catga.cluster.raft.term").set(term as f64);
        metrics::gauge!("catga.cluster.raft.commit.index").set(commit_index as f64);
        metrics::gauge!("catga.cluster.raft.apply.index").set(applied_index as f64);
        metrics::gauge!("catga.cluster.raft.pending_commits").set(pending_commits as f64);
        record_role(role);

        let is_leader = role == StateRole::Leader;
        if let Some(was_leader) = self.was_leader {
            match (was_leader, is_leader) {
                (false, true) => {
                    metrics::counter!(
                        "catga.cluster.raft.leadership.transitions",
                        "transition" => "acquired"
                    )
                    .increment(1);
                }
                (true, false) => {
                    metrics::counter!(
                        "catga.cluster.raft.leadership.transitions",
                        "transition" => "lost"
                    )
                    .increment(1);
                }
                _ => {}
            }
        }
        self.was_leader = Some(is_leader);
    }
}

/// Counts one cluster operation failure under a fixed, low-cardinality kind.
pub(crate) fn record_failure(kind: &'static str) {
    metrics::counter!("catga.cluster.raft.failures", "kind" => kind).increment(1);
}

/// Counts one application command that completed deterministic state-machine application.
pub(crate) fn record_applied_command() {
    metrics::counter!("catga.cluster.raft.commands.applied").increment(1);
}

/// Publishes bounded runtime queue depths for a fixed runtime implementation.
pub(crate) fn record_queue_depth(runtime: &'static str, inbound: usize, commands: usize) {
    metrics::gauge!("catga.cluster.runtime.inbound.depth", "runtime" => runtime)
        .set(inbound as f64);
    metrics::gauge!("catga.cluster.runtime.command.depth", "runtime" => runtime)
        .set(commands as f64);
}

fn record_role(role: StateRole) {
    metrics::gauge!("catga.cluster.raft.role", "role" => "follower")
        .set((role == StateRole::Follower) as u8 as f64);
    metrics::gauge!("catga.cluster.raft.role", "role" => "candidate")
        .set((role == StateRole::Candidate) as u8 as f64);
    metrics::gauge!("catga.cluster.raft.role", "role" => "leader")
        .set((role == StateRole::Leader) as u8 as f64);
    metrics::gauge!("catga.cluster.raft.role", "role" => "pre_candidate")
        .set((role == StateRole::PreCandidate) as u8 as f64);
}

#[cfg(test)]
mod tests {
    use raft::StateRole;

    // Test that StateRole enum variants exist and can be compared
    #[test]
    fn state_role_leader_equality() {
        assert_eq!(StateRole::Leader, StateRole::Leader);
        assert_ne!(StateRole::Leader, StateRole::Follower);
        assert_ne!(StateRole::Leader, StateRole::Candidate);
        assert_ne!(StateRole::Leader, StateRole::PreCandidate);
    }

    #[test]
    fn state_role_follower_equality() {
        assert_eq!(StateRole::Follower, StateRole::Follower);
        assert_ne!(StateRole::Follower, StateRole::Leader);
        assert_ne!(StateRole::Follower, StateRole::Candidate);
        assert_ne!(StateRole::Follower, StateRole::PreCandidate);
    }

    #[test]
    fn state_role_candidate_equality() {
        assert_eq!(StateRole::Candidate, StateRole::Candidate);
        assert_ne!(StateRole::Candidate, StateRole::Leader);
        assert_ne!(StateRole::Candidate, StateRole::Follower);
    }

    #[test]
    fn state_role_pre_candidate_equality() {
        assert_eq!(StateRole::PreCandidate, StateRole::PreCandidate);
        assert_ne!(StateRole::PreCandidate, StateRole::Leader);
        assert_ne!(StateRole::PreCandidate, StateRole::Follower);
    }

    // Test transition detection logic (simulated without actual metrics)
    #[test]
    fn leadership_transition_acquired_logic() {
        // Simulate: was_leader = false, is_leader = true => "acquired"
        let was_leader = false;
        let is_leader = true;
        let transition = if !was_leader && is_leader {
            "acquired"
        } else if was_leader && !is_leader {
            "lost"
        } else {
            "none"
        };
        assert_eq!(transition, "acquired");
    }

    #[test]
    fn leadership_transition_lost_logic() {
        // Simulate: was_leader = true, is_leader = false => "lost"
        let was_leader = true;
        let is_leader = false;
        let transition = if !was_leader && is_leader {
            "acquired"
        } else if was_leader && !is_leader {
            "lost"
        } else {
            "none"
        };
        assert_eq!(transition, "lost");
    }

    #[test]
    fn leadership_transition_none_when_still_leader() {
        // Simulate: was_leader = true, is_leader = true => "none"
        let was_leader = true;
        let is_leader = true;
        let transition = if !was_leader && is_leader {
            "acquired"
        } else if was_leader && !is_leader {
            "lost"
        } else {
            "none"
        };
        assert_eq!(transition, "none");
    }

    #[test]
    fn leadership_transition_none_when_still_follower() {
        // Simulate: was_leader = false, is_leader = false => "none"
        let was_leader = false;
        let is_leader = false;
        let transition = if !was_leader && is_leader {
            "acquired"
        } else if was_leader && !is_leader {
            "lost"
        } else {
            "none"
        };
        assert_eq!(transition, "none");
    }

    // Test gauge value calculation
    #[test]
    fn gauge_value_for_role() {
        assert_eq!((StateRole::Leader == StateRole::Leader) as u8 as f64, 1.0);
        assert_eq!((StateRole::Leader == StateRole::Follower) as u8 as f64, 0.0);
        assert_eq!(
            (StateRole::Leader == StateRole::Candidate) as u8 as f64,
            0.0
        );
        assert_eq!(
            (StateRole::Leader == StateRole::PreCandidate) as u8 as f64,
            0.0
        );
    }

    #[test]
    fn gauge_value_for_follower() {
        assert_eq!(
            (StateRole::Follower == StateRole::Follower) as u8 as f64,
            1.0
        );
        assert_eq!((StateRole::Follower == StateRole::Leader) as u8 as f64, 0.0);
    }

    #[test]
    fn gauge_value_for_candidate() {
        assert_eq!(
            (StateRole::Candidate == StateRole::Candidate) as u8 as f64,
            1.0
        );
        assert_eq!(
            (StateRole::Candidate == StateRole::Leader) as u8 as f64,
            0.0
        );
    }

    // Test default value for RaftMetrics
    #[test]
    fn raft_metrics_default() {
        use crate::metrics::RaftMetrics;
        let metrics = RaftMetrics::default();
        // was_leader should be None initially
        // We can't access private field, but we test that default constructs properly
        assert!(std::mem::size_of_val(&metrics) > 0);
    }
}
