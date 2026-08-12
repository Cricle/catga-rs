//! Multi-shard tests: two shards hosted by one node process share the gRPC
//! server and the redb database but stay fully independent — separate state
//! machines, separate membership, and independent progress when a node dies.

#[path = "common/harness.rs"]
mod harness;
#[path = "common/probe.rs"]
mod probe;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::sync::atomic::Ordering;

use catga_core::{ConsensusRuntime, ErrorCode};
use catga_sorock::{DEFAULT_SHARD_INDEX, SorockRuntime};
use harness::{loopback_config, wait_until};
use probe::MachineProbe;

/// The second shard attached next to the primary one.
const SHARD_B: u32 = 1;

/// One cluster node hosting two shards: the primary shard runtime and a
/// second shard attached through [`SorockRuntime::attach_shard`].
struct ShardNode {
    shard_a: SorockRuntime,
    shard_b: SorockRuntime,
}

impl ShardNode {
    async fn start(node_id: &str, probe_a: &MachineProbe, probe_b: &MachineProbe) -> Self {
        let shard_a = SorockRuntime::start(loopback_config(node_id), probe_a.machine())
            .await
            .expect("primary shard runtime must start");
        let shard_b = shard_a
            .attach_shard(SHARD_B, probe_b.machine())
            .await
            .expect("second shard must attach to the same node");
        Self { shard_a, shard_b }
    }

    fn uri(&self) -> String {
        self.shard_a.advertised_uri().to_owned()
    }

    /// Graceful stop: the first shard runtime only detaches its shard, the
    /// last one drains the shared server.
    async fn shutdown(self) {
        self.shard_a.shutdown();
        self.shard_a
            .join()
            .await
            .expect("detaching shard runtime must join");
        self.shard_b.shutdown();
        self.shard_b
            .join()
            .await
            .expect("last shard runtime must drain the node");
    }
}

/// Bootstraps one shard: the issuing node adds itself first (single-node
/// bootstrap elects it leader), then the remaining members one at a time.
async fn bootstrap_shard(runtime: &SorockRuntime, uris: &[String]) {
    for (i, uri) in uris.iter().enumerate() {
        runtime
            .add_member((i + 1) as u64, uri.clone())
            .await
            .expect("membership change must succeed");
    }
}

async fn propose_values(runtime: &SorockRuntime, values: impl IntoIterator<Item = u64>) {
    for value in values {
        runtime
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("proposal must succeed on a healthy shard");
    }
}

fn sums(probes: &[MachineProbe]) -> Vec<u64> {
    probes
        .iter()
        .map(|probe| probe.sum.load(Ordering::Acquire))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_stay_within_their_shard() {
    let probes_a: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let probes_b: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let mut nodes = Vec::new();
    for (i, (probe_a, probe_b)) in probes_a.iter().zip(&probes_b).enumerate() {
        nodes.push(ShardNode::start(&format!("node-{}", i + 1), probe_a, probe_b).await);
    }
    let uris: Vec<String> = nodes.iter().map(ShardNode::uri).collect();

    // Both shards form on all three nodes, each through its own runtime.
    bootstrap_shard(&nodes[0].shard_a, &uris).await;
    bootstrap_shard(&nodes[0].shard_b, &uris).await;

    // Writes on shard A replicate to every shard-A machine...
    propose_values(&nodes[0].shard_a, 1..=5).await;
    for (i, probe) in probes_a.iter().enumerate() {
        wait_until(
            &format!("node {} shard A to apply the batch", i + 1),
            || probe.sum.load(Ordering::Acquire) == 15,
        )
        .await;
    }
    // ...and never leak into shard B, whose machines saw no proposal at all.
    assert_eq!(
        sums(&probes_b),
        vec![0, 0, 0],
        "shard A writes must never appear in shard B"
    );

    // The same holds in the other direction.
    propose_values(&nodes[0].shard_b, 100..=102).await;
    for (i, probe) in probes_b.iter().enumerate() {
        wait_until(
            &format!("node {} shard B to apply its batch", i + 1),
            || probe.sum.load(Ordering::Acquire) == 303,
        )
        .await;
    }
    assert_eq!(
        sums(&probes_a),
        vec![15, 15, 15],
        "shard B writes must never appear in shard A"
    );

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_shards_survive_the_loss_of_one_node() {
    let probes_a: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let probes_b: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let mut nodes = Vec::new();
    for (i, (probe_a, probe_b)) in probes_a.iter().zip(&probes_b).enumerate() {
        nodes.push(ShardNode::start(&format!("node-{}", i + 1), probe_a, probe_b).await);
    }
    let uris: Vec<String> = nodes.iter().map(ShardNode::uri).collect();
    bootstrap_shard(&nodes[0].shard_a, &uris).await;
    bootstrap_shard(&nodes[0].shard_b, &uris).await;

    // Commit a batch on both shards while all three nodes are alive.
    propose_values(&nodes[0].shard_a, 1..=3).await;
    propose_values(&nodes[0].shard_b, 4..=6).await;
    for (i, (probe_a, probe_b)) in probes_a.iter().zip(&probes_b).enumerate() {
        wait_until(
            &format!("node {} shard A to apply the first batch", i + 1),
            || probe_a.sum.load(Ordering::Acquire) == 6,
        )
        .await;
        wait_until(
            &format!("node {} shard B to apply the first batch", i + 1),
            || probe_b.sum.load(Ordering::Acquire) == 15,
        )
        .await;
    }

    // Kill the third node (a follower on both shards: node 1 bootstrapped
    // both groups and leads them). Dropping both shard runtimes stops the
    // shared server; the survivors keep a two-of-three quorum per shard.
    let victim = nodes.pop().expect("the third node exists");
    drop(victim.shard_a);
    drop(victim.shard_b);

    // Both shards keep serving writes through the survivors — shard A from
    // the bootstrap node, shard B from the other survivor, exercising
    // per-shard forwarding after the kill.
    propose_values(&nodes[0].shard_a, [10, 20]).await;
    propose_values(&nodes[1].shard_b, [40, 50]).await;
    for (i, (probe_a, probe_b)) in probes_a.iter().zip(&probes_b).take(2).enumerate() {
        wait_until(
            &format!("node {} shard A to progress after the kill", i + 1),
            || probe_a.sum.load(Ordering::Acquire) == 36,
        )
        .await;
        wait_until(
            &format!("node {} shard B to progress after the kill", i + 1),
            || probe_b.sum.load(Ordering::Acquire) == 105,
        )
        .await;
    }

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_membership_changes_are_independent() {
    let probes_a: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let probes_b: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let mut nodes = Vec::new();
    for (i, (probe_a, probe_b)) in probes_a.iter().zip(&probes_b).enumerate() {
        nodes.push(ShardNode::start(&format!("node-{}", i + 1), probe_a, probe_b).await);
    }
    let uris: Vec<String> = nodes.iter().map(ShardNode::uri).collect();

    // Shard A forms on nodes 1 and 2 only; shard B forms on all three.
    // Node 3 hosts a shard-A process but is not a member of shard A.
    bootstrap_shard(&nodes[0].shard_a, &uris[..2]).await;
    bootstrap_shard(&nodes[0].shard_b, &uris).await;

    propose_values(&nodes[0].shard_a, 1..=4).await;
    for (i, probe) in probes_a.iter().take(2).enumerate() {
        wait_until(
            &format!("node {} shard A to apply the batch", i + 1),
            || probe.sum.load(Ordering::Acquire) == 10,
        )
        .await;
    }
    assert_eq!(
        probes_a[2].sum.load(Ordering::Acquire),
        0,
        "node 3 is not a shard A member and must receive nothing"
    );

    // Shard B reaches node 3 through its own, independent membership.
    propose_values(&nodes[0].shard_b, 5..=7).await;
    for (i, probe) in probes_b.iter().enumerate() {
        wait_until(
            &format!("node {} shard B to apply the batch", i + 1),
            || probe.sum.load(Ordering::Acquire) == 18,
        )
        .await;
    }
    assert_eq!(
        probes_a[2].sum.load(Ordering::Acquire),
        0,
        "shard B replication must not leak into shard A on node 3"
    );

    // Add node 3 to shard A only: it catches up on the entries it missed and
    // receives the new ones, while shard B membership is untouched.
    nodes[0]
        .shard_a
        .add_member(3, uris[2].clone())
        .await
        .expect("adding node 3 to shard A must succeed");
    propose_values(&nodes[0].shard_a, [8, 9]).await;
    for (i, probe) in probes_a.iter().enumerate() {
        wait_until(&format!("node {} shard A to converge at 27", i + 1), || {
            probe.sum.load(Ordering::Acquire) == 27
        })
        .await;
    }
    propose_values(&nodes[0].shard_b, [8]).await;
    for (i, probe) in probes_b.iter().enumerate() {
        wait_until(&format!("node {} shard B to converge at 26", i + 1), || {
            probe.sum.load(Ordering::Acquire) == 26
        })
        .await;
    }

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_shard_shares_the_node_and_validates_duplicates() {
    let probe_a = MachineProbe::default();
    let probe_b = MachineProbe::default();
    let shard_a = SorockRuntime::start(loopback_config("node-multi"), probe_a.machine())
        .await
        .expect("primary shard runtime must start");
    assert_eq!(shard_a.shard(), DEFAULT_SHARD_INDEX);

    let shard_b = shard_a
        .attach_shard(SHARD_B, probe_b.machine())
        .await
        .expect("second shard must attach");

    // Both runtimes are scoped to their shard but front the same node: same
    // server address, same advertised URI, same coordinator node id.
    assert_eq!(shard_b.shard(), SHARD_B);
    assert_eq!(
        shard_a.node().attached_shards(),
        vec![DEFAULT_SHARD_INDEX, SHARD_B]
    );
    assert_eq!(shard_a.node().local_addr(), shard_b.node().local_addr());
    assert_eq!(shard_a.advertised_uri(), shard_b.advertised_uri());
    assert_eq!(
        shard_a.coordinator().node_id(),
        shard_b.coordinator().node_id()
    );
    assert!(
        shard_b
            .coordinator()
            .member_endpoints()
            .iter()
            .any(|m| m.as_ref() == shard_a.advertised_uri()),
        "the attached shard's coordinator view starts with the node itself"
    );

    // Re-attaching an attached shard is rejected, whichever runtime issues it.
    for shard in [DEFAULT_SHARD_INDEX, SHARD_B] {
        let error = shard_a
            .attach_shard(shard, probe_a.machine())
            .await
            .err()
            .expect("attaching an attached shard must fail");
        assert_eq!(error.code(), ErrorCode::Validation);
        assert!(
            error.message().contains("already attached"),
            "unexpected message: {}",
            error.message()
        );
    }

    // Shutting down one of two shard runtimes only retires its shard: the
    // shared server keeps serving the other shard.
    assert!(shard_a.is_alive() && shard_b.is_alive());
    shard_a.shutdown();
    assert!(
        !shard_a.is_alive(),
        "a detached shard runtime reports not alive"
    );
    assert!(
        shard_b.is_alive(),
        "the surviving shard keeps the node alive"
    );
    let error = shard_a
        .propose(1_u64.to_le_bytes().to_vec())
        .await
        .expect_err("proposing on a detached shard must fail fast");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(
        error.message().contains("detached"),
        "unexpected message: {}",
        error.message()
    );
    shard_a
        .join()
        .await
        .expect("joining a shared node only detaches the shard");

    // The last shard runtime owns the node and drains it.
    shard_b.shutdown();
    shard_b
        .join()
        .await
        .expect("the last shard runtime must drain the node");
}
