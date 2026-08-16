//! Membership-change (conf change) integration tests over the real gRPC
//! transport.
//!
//! Covers the joint-consensus wiring end to end:
//! - a single voter grows to two and shrinks back, with the member view and
//!   transport integration checked on both sides;
//! - a 3-node cluster adds a 4th voter, which proves it is wired in by
//!   learning the leader endpoint, and is then removed again;
//! - membership requests issued on a follower are rejected as not leader.
//!
//! These tests bind real ports (17xxx range, one base per test so parallel
//! tests in this binary never collide) and therefore keep generous timeouts.
//!
//! Note on finding the leader: `coordinator.is_leader()` reports "a leader
//! endpoint is known", which followers share, so the tests discover the
//! actual leader empirically — it is the only node that accepts a membership
//! change; every other node rejects immediately with NotLeader. Fresh
//! clusters can also flap through a leadership change before heartbeats
//! establish authority, so operations retry across nodes until they land.

use std::sync::Arc;
use std::time::{Duration, Instant};

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine, ErrorCode};
use catga_raft::{CatgaRaftRuntime, CatgaRaftRuntimeBuilder};
use parking_lot::Mutex;

/// State machine that records every applied entry.
#[derive(Clone)]
struct RecordingMachine {
    applied: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}

impl RecordingMachine {
    fn new() -> Self {
        Self {
            applied: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ConsensusStateMachine for RecordingMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.applied.lock().push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

type Runtime = CatgaRaftRuntime<RecordingMachine>;

/// Polls `condition` until it holds or the deadline passes.
async fn eventually<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn member_endpoints<R: ConsensusRuntime + ?Sized>(runtime: &R) -> Vec<String> {
    ConsensusRuntime::coordinator(runtime)
        .member_endpoints()
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// One membership operation to run against the current leader.
#[derive(Debug, Clone)]
enum MemberOp {
    Add(u64, String),
    Remove(u64),
}

impl MemberOp {
    async fn call(&self, runtime: &Runtime) -> CatgaResult<()> {
        match self {
            MemberOp::Add(id, endpoint) => runtime.add_member(*id, endpoint.clone()).await,
            MemberOp::Remove(id) => runtime.remove_member(*id).await,
        }
    }

    fn is_not_leader_rejection(result: &CatgaResult<()>) -> bool {
        matches!(result, Err(e) if e.code() == ErrorCode::Unavailable
            && e.message().contains("not leader"))
    }
}

/// Runs `op` against the group until it succeeds or `deadline` passes,
/// probing nodes round-robin: only the current leader accepts a membership
/// change, every other node rejects immediately with NotLeader, so cycling
/// through the group finds (and keeps following) the leader without any
/// external leadership oracle. Returns the index of the node that applied
/// the change.
async fn run_membership(runtimes: &[Runtime], op: MemberOp, deadline: Instant) -> usize {
    let mut next = 0usize;
    loop {
        let idx = next % runtimes.len();
        next += 1;
        // The per-attempt bound exceeds the owner loop's 10s pending-request
        // TTL, so an expired request surfaces as an error and is retried
        // instead of being swallowed by this timeout.
        match tokio::time::timeout(Duration::from_secs(15), op.call(&runtimes[idx])).await {
            Ok(Ok(())) => return idx,
            Ok(result) if MemberOp::is_not_leader_rejection(&result) => {
                // Immediate rejection: probe the next node right away.
                assert!(Instant::now() < deadline, "{op:?} found no accepting leader");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(other) => {
                assert!(
                    Instant::now() < deadline,
                    "{op:?} never succeeded; last error: {other:?}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => {
                assert!(Instant::now() < deadline, "{op:?} hung");
            }
        }
    }
}

/// (a) A single-voter node adds a second member: the request resolves Ok once
/// the conf entry is applied, the new peer appears in the member view, and
/// the new node demonstrably receives heartbeats (it learns a leader
/// endpoint). Removing the member again also resolves Ok and restores the
/// original member view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_add_and_remove_member() {
    const BASE_PORT: u16 = 17100;
    let node1_endpoint = format!("http://127.0.0.1:{BASE_PORT}");
    let node2_endpoint = format!("http://127.0.0.1:{}", BASE_PORT + 100);
    let deadline = Instant::now() + Duration::from_secs(60);

    // Node 1 starts as a one-voter cluster and wins its own election.
    let mut runtimes = Vec::new();
    runtimes.push(
        CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, 0, 1)
            .expect("builder")
            .start(RecordingMachine::new())
            .await
            .expect("start node 1"),
    );

    let leader_known = eventually(Duration::from_secs(15), || {
        ConsensusRuntime::coordinator(&runtimes[0]).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    // Node 2 boots knowing the two-node topology; it cannot campaign until
    // the group actually adds it, so it idles until then.
    runtimes.push(
        CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, 1, 2)
            .expect("builder")
            .start(RecordingMachine::new())
            .await
            .expect("start node 2"),
    );

    // Add node 2: resolves once the conf entry is committed and applied on
    // the proposing leader.
    run_membership(
        &runtimes,
        MemberOp::Add(2, node2_endpoint.clone()),
        deadline,
    )
    .await;

    // Every existing member mirrors the new peer into its member view when
    // it applies the conf entry.
    assert!(
        eventually(Duration::from_secs(10), || {
            member_endpoints(&runtimes[0]).contains(&node2_endpoint)
        })
        .await,
        "node 1 member view must contain the added endpoint, got {:?}",
        member_endpoints(&runtimes[0])
    );

    // Transport integration: the group now replicates to node 2, which must
    // eventually report a known leader endpoint from real heartbeats.
    let sees_leader = eventually(Duration::from_secs(30), || {
        match ConsensusRuntime::coordinator(&runtimes[1]).leader_endpoint() {
            Some(ep) => {
                let ep = ep.to_string();
                ep == node1_endpoint || ep == node2_endpoint
            }
            None => false,
        }
    })
    .await;
    assert!(
        sees_leader,
        "node 2 must learn a leader endpoint after being added, got {:?}",
        ConsensusRuntime::coordinator(&runtimes[1]).leader_endpoint()
    );

    // Remove node 2 again; commits under the two-voter config, so this also
    // proves node 2 participates in replication.
    run_membership(&runtimes, MemberOp::Remove(2), deadline).await;

    assert!(
        eventually(Duration::from_secs(10), || {
            member_endpoints(&runtimes[0]).is_empty()
        })
        .await,
        "node 1 member view must be empty after removing node 2, got {:?}",
        member_endpoints(&runtimes[0])
    );

    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join node");
    }
}

/// (b) A healthy 3-node cluster adds a 4th voter via the leader. The new
/// node, started outside the group but pointing at it, proves integration by
/// learning a known leader endpoint from replicated heartbeats; it is then
/// removed again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_adds_and_removes_fourth() {
    const BASE_PORT: u16 = 17300;
    let cluster_endpoints: Vec<String> = (0..3u16)
        .map(|i| format!("http://127.0.0.1:{}", BASE_PORT + i * 100))
        .collect();
    let node4_endpoint = format!("http://127.0.0.1:{}", BASE_PORT + 300);
    let deadline = Instant::now() + Duration::from_secs(60);

    let mut runtimes = Vec::new();
    for i in 0..3u64 {
        let runtime = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, i, 3)
            .expect("builder")
            .start(RecordingMachine::new())
            .await
            .expect("start cluster node");
        runtimes.push(runtime);
    }

    // Node 4 starts outside the group (its own bootstrap voters include it,
    // but the running cluster does not know it yet, so it cannot campaign).
    let rt4 = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, 3, 4)
        .expect("builder")
        .start(RecordingMachine::new())
        .await
        .expect("start node 4");

    // The leader adds node 4; the endpoint rides the conf-change context so
    // every existing member wires the transport peer when it applies.
    run_membership(
        &runtimes,
        MemberOp::Add(4, node4_endpoint.clone()),
        deadline,
    )
    .await;

    assert!(
        eventually(Duration::from_secs(10), || {
            runtimes
                .iter()
                .all(|rt| member_endpoints(rt).contains(&node4_endpoint))
        })
        .await,
        "every existing member must list node 4 after applying the conf entry, got {:?}",
        runtimes.iter().map(member_endpoints).collect::<Vec<_>>()
    );

    // Integration check: node 4 must eventually receive heartbeats and
    // report one of the cluster endpoints as the leader.
    let sees_leader = eventually(Duration::from_secs(30), || {
        match ConsensusRuntime::coordinator(&rt4).leader_endpoint() {
            Some(ep) => cluster_endpoints.contains(&ep.to_string()),
            None => false,
        }
    })
    .await;
    assert!(
        sees_leader,
        "node 4 must learn a cluster leader endpoint after being added, got {:?}",
        ConsensusRuntime::coordinator(&rt4).leader_endpoint()
    );

    // Remove node 4 again.
    run_membership(&runtimes, MemberOp::Remove(4), deadline).await;

    assert!(
        eventually(Duration::from_secs(10), || {
            runtimes
                .iter()
                .all(|rt| !member_endpoints(rt).contains(&node4_endpoint))
        })
        .await,
        "no member may still list node 4 after its removal, got {:?}",
        runtimes.iter().map(member_endpoints).collect::<Vec<_>>()
    );

    rt4.shutdown();
    Box::new(rt4).join().await.expect("join node 4");
    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join cluster node");
    }
}

/// (c) Membership changes are leader-only: a follower rejects `add_member`
/// with the NotLeader error (mapped to `Unavailable`) and its member view
/// stays untouched.
///
/// The actual leader is discovered empirically: at most one node accepts the
/// probe change (single leader per term), so probing nodes in order reaches a
/// rejecting follower within at most two attempts; if the probe hits the
/// leader first, the change is rolled back before probing on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_leader_add_member_is_rejected() {
    const BASE_PORT: u16 = 17700;
    let deadline = Instant::now() + Duration::from_secs(60);
    let probe_endpoint = "http://127.0.0.1:17999".to_string();

    let mut runtimes = Vec::new();
    for i in 0..3u64 {
        let runtime = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, i, 3)
            .expect("builder")
            .start(RecordingMachine::new())
            .await
            .expect("start cluster node");
        runtimes.push(runtime);
    }

    let mut follower_idx = None;
    for i in 0..runtimes.len() {
        let before = member_endpoints(&runtimes[i]);
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            runtimes[i].add_member(9, probe_endpoint.clone()),
        )
        .await
        .expect("add_member must not hang");

        if MemberOp::is_not_leader_rejection(&result) {
            assert_eq!(
                member_endpoints(&runtimes[i]),
                before,
                "rejected membership change must not alter the member view"
            );
            follower_idx = Some(i);
            break;
        }

        // Node i accepted: it is the leader and applied the probe add. Roll
        // it back, then keep probing at the remaining (non-leader) nodes.
        result.expect("probe add on the leader must succeed");
        run_membership(&runtimes, MemberOp::Remove(9), deadline).await;
    }
    assert!(
        follower_idx.is_some(),
        "some follower must reject the membership change with NotLeader"
    );

    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join cluster node");
    }
}
