//! Contract tests for [`SorockRuntime`]: coordinator seeding, id-registry
//! membership errors, retry/timeout behavior, proposal lifecycle before and
//! after shutdown, and member removal followed by a rejoin.

#[path = "common/harness.rs"]
mod harness;
#[path = "common/probe.rs"]
mod probe;
#[path = "common/propose.rs"]
mod propose;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::{net::Ipv4Addr, sync::atomic::Ordering, time::Duration};

use catga_core::{ConsensusRuntime, ErrorCode};
use catga_sorock::SorockRuntime;
use harness::{loopback_config, wait_until};
use probe::MachineProbe;
use propose::propose_eventually;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_reports_identity_and_seed_members() {
    let mut config = loopback_config("node-seed");
    config.members = vec![
        "http://seed-a:1001".to_owned(),
        "http://seed-b:1002".to_owned(),
    ];
    let probe = MachineProbe::default();

    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    let coordinator = runtime.coordinator();
    assert_eq!(coordinator.node_id(), "node-seed");
    assert!(
        !coordinator.is_leader(),
        "sorock 0.12 exposes no leadership query; is_leader is always false"
    );
    assert!(
        coordinator.leader_endpoint().is_none(),
        "sorock 0.12 exposes no leadership query; leader_endpoint is always None"
    );
    let members = coordinator.member_endpoints();
    assert_eq!(
        members.len(),
        3,
        "the view holds the two seeds plus the node itself: {members:?}"
    );
    for expected in ["http://seed-a:1001", "http://seed-b:1002"] {
        assert!(
            members.iter().any(|m| m.as_ref() == expected),
            "seed {expected} must be listed: {members:?}"
        );
    }
    assert!(
        members
            .iter()
            .any(|m| m.as_ref() == runtime.advertised_uri()),
        "the node itself must be listed: {members:?}"
    );

    assert!(runtime.is_alive());
    assert_eq!(
        runtime
            .applied_index()
            .await
            .expect("applied index readable"),
        0,
        "nothing applied before the first proposal"
    );
    assert_ne!(runtime.node().local_addr().port(), 0);

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_does_not_duplicate_a_seeded_self_endpoint() {
    // Reserve a port so the node's own advertised URI can be seeded upfront.
    let listener =
        std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("ephemeral bind must succeed");
    let addr = listener
        .local_addr()
        .expect("the bound address must be readable");
    drop(listener);

    let mut config = loopback_config("node-selfseed");
    config.bind_addr = addr;
    config.members = vec![format!("http://{addr}")];
    let probe = MachineProbe::default();

    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    let members = runtime.coordinator().member_endpoints();
    assert_eq!(
        members.len(),
        1,
        "seeding the node's own uri must not duplicate it: {members:?}"
    );
    assert_eq!(members[0].as_ref(), runtime.advertised_uri());

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propose_before_bootstrap_fails_transiently_and_runtime_recovers() {
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(loopback_config("node-lonely"), probe.machine())
        .await
        .expect("runtime must start");

    // sorock aborts writes while no leader is known (its handler panics);
    // the h2 abort surfaces to the client as `Unknown` or `Cancelled`
    // depending on stream timing, mapped to Transient / Cancelled.
    let error = runtime
        .propose(vec![1, 2, 3])
        .await
        .expect_err("a write on a leaderless node must fail");
    assert!(
        matches!(error.code(), ErrorCode::Transient | ErrorCode::Cancelled),
        "a leaderless write must surface as a retryable abort: {error:?}"
    );

    // The failure leaves the runtime consistent: bootstrap then works.
    assert!(runtime.is_alive());
    runtime
        .add_member(1, runtime.advertised_uri().to_owned())
        .await
        .expect("bootstrap must succeed");
    propose_eventually(&runtime, 7).await;
    wait_until("the recovered group to apply", || {
        probe.sum.load(Ordering::Acquire) == 7
    })
    .await;

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_change_without_leader_retries_then_times_out() {
    let mut config = loopback_config("node-retry");
    config.request_timeout = Duration::from_millis(600);
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    // The group was never bootstrapped: sorock aborts the forwarded kernel
    // request (`Unknown`, retryable), so the runtime retries until the
    // request budget expires.
    let started = std::time::Instant::now();
    let error = runtime
        .add_member(2, "http://127.0.0.1:9".to_owned())
        .await
        .expect_err("a membership change without a leader must time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "the full request budget must be spent on retries: {:?}",
        started.elapsed()
    );

    // A failed membership change must not register the id.
    let error = runtime
        .remove_member(2)
        .await
        .expect_err("a never-registered id must be rejected");
    assert_eq!(error.code(), ErrorCode::NotFound);

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_request_timeout_fails_fast() {
    let mut config = loopback_config("node-zero");
    config.request_timeout = Duration::ZERO;
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    let error = runtime
        .add_member(1, runtime.advertised_uri().to_owned())
        .await
        .expect_err("a zero budget rejects membership changes immediately");
    assert_eq!(error.code(), ErrorCode::Timeout);

    let error = runtime
        .propose(vec![0])
        .await
        .expect_err("a zero budget rejects proposals immediately");
    assert_eq!(error.code(), ErrorCode::Timeout);

    // The timed-out proposal raced a never-used connection attempt against
    // the server stop; tonic's graceful shutdown can wait on such a
    // connection indefinitely, so join falls back to its drain bound instead
    // of hanging (the bounded path is pinned by the lifecycle tests).
    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("bounded join must succeed despite the racing connection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_member_with_unknown_id_is_not_found() {
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(loopback_config("node-registry"), probe.machine())
        .await
        .expect("runtime must start");

    // The id registry is consulted before any RPC: this fails even on a
    // node that was never bootstrapped.
    let error = runtime
        .remove_member(77)
        .await
        .expect_err("an unknown id must be rejected");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert!(
        error.message().contains("77"),
        "the offending id must be reported: {}",
        error.message()
    );

    // The same holds once the group is bootstrapped.
    runtime
        .add_member(1, runtime.advertised_uri().to_owned())
        .await
        .expect("bootstrap must succeed");
    let error = runtime
        .remove_member(78)
        .await
        .expect_err("an id never added through this runtime must be rejected");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert!(error.message().contains("78"));

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_group_applies_proposals_and_snapshots() {
    let mut config = loopback_config("node-single");
    config.snapshot_interval = 1;
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    runtime
        .add_member(1, runtime.advertised_uri().to_owned())
        .await
        .expect("bootstrap must succeed");

    propose_eventually(&runtime, 1).await;
    for value in 2..=3_u64 {
        runtime
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("proposals must succeed once a leader is elected");
    }
    wait_until("the single node to apply the batch", || {
        probe.sum.load(Ordering::Acquire) == 6
    })
    .await;

    let applied_index = runtime
        .applied_index()
        .await
        .expect("applied index readable");
    assert!(
        applied_index >= 3,
        "all three proposals must be applied: {applied_index}"
    );
    assert_eq!(
        applied_index,
        probe.applied.load(Ordering::Acquire),
        "the runtime applied index must match the machine's record"
    );
    wait_until("every applied entry to be snapshotted", || {
        probe.snapshot_calls.load(Ordering::Acquire) >= 3
    })
    .await;

    // Bootstrapping registered the node's own endpoint exactly once.
    let members = runtime.coordinator().member_endpoints();
    assert_eq!(
        members
            .iter()
            .filter(|m| m.as_ref() == runtime.advertised_uri())
            .count(),
        1,
        "the self endpoint must not be duplicated: {members:?}"
    );

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_member_then_rejoin_restores_replication() {
    let probe1 = MachineProbe::default();
    let probe2 = MachineProbe::default();
    let runtime1 = SorockRuntime::start(loopback_config("node-one"), probe1.machine())
        .await
        .expect("first runtime must start");
    let runtime2 = SorockRuntime::start(loopback_config("node-two"), probe2.machine())
        .await
        .expect("second runtime must start");
    let uri1 = runtime1.advertised_uri().to_owned();
    let uri2 = runtime2.advertised_uri().to_owned();

    runtime1
        .add_member(1, uri1.clone())
        .await
        .expect("bootstrap must succeed");
    runtime1
        .add_member(2, uri2.clone())
        .await
        .expect("the second node must join");

    propose_eventually(&runtime1, 1).await;
    for value in 2..=3_u64 {
        runtime1
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("proposals must succeed on the two-node group");
    }
    wait_until("both nodes to apply the first batch", || {
        probe1.sum.load(Ordering::Acquire) == 6 && probe2.sum.load(Ordering::Acquire) == 6
    })
    .await;

    // Remove node 2: the id registry is cleaned and the coordinator view
    // drops the endpoint.
    runtime1
        .remove_member(2)
        .await
        .expect("removing a known member must succeed");
    let error = runtime1
        .remove_member(2)
        .await
        .expect_err("an already-removed id is no longer registered");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert!(
        !runtime1
            .coordinator()
            .member_endpoints()
            .iter()
            .any(|m| m.as_ref() == uri2.as_str()),
        "the removed endpoint must leave the coordinator view"
    );

    // The remaining single-member group keeps serving alone.
    runtime1
        .propose(10_u64.to_le_bytes().to_vec())
        .await
        .expect("the single-member group must keep serving");
    wait_until("node one to apply alone", || {
        probe1.sum.load(Ordering::Acquire) == 16
    })
    .await;

    // Rejoin: node 2 catches up on the entries it missed, then receives new
    // replication like any other member.
    runtime1
        .add_member(2, uri2.clone())
        .await
        .expect("rejoining with the same id must succeed");
    wait_until("the rejoined node to catch up", || {
        probe2.sum.load(Ordering::Acquire) == 16
    })
    .await;

    runtime1
        .propose(100_u64.to_le_bytes().to_vec())
        .await
        .expect("proposals must succeed after the rejoin");
    wait_until("both nodes to converge after the rejoin", || {
        probe1.sum.load(Ordering::Acquire) == 116 && probe2.sum.load(Ordering::Acquire) == 116
    })
    .await;

    runtime1.shutdown();
    runtime1
        .join()
        .await
        .expect("graceful shutdown must succeed");
    runtime2.shutdown();
    runtime2
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_change_without_quorum_times_out() {
    let mut config1 = loopback_config("node-quorum-1");
    config1.request_timeout = Duration::from_secs(4);
    let probe1 = MachineProbe::default();
    let probe2 = MachineProbe::default();
    let runtime1 = SorockRuntime::start(config1, probe1.machine())
        .await
        .expect("first runtime must start");
    let runtime2 = SorockRuntime::start(loopback_config("node-quorum-2"), probe2.machine())
        .await
        .expect("second runtime must start");
    let uri1 = runtime1.advertised_uri().to_owned();
    let uri2 = runtime2.advertised_uri().to_owned();

    runtime1
        .add_member(1, uri1)
        .await
        .expect("bootstrap must succeed");
    runtime1
        .add_member(2, uri2.clone())
        .await
        .expect("the second node must join");

    // Kill node 2 abruptly. sorock activates a new membership as soon as
    // the leader appends it, so adding a third member now requires votes
    // from two of {1, 2, 3}: with only node 1 alive the entry can never
    // commit and the leader waits until the client deadline resolves it.
    drop(runtime2);

    let started = std::time::Instant::now();
    let error = runtime1
        .add_member(3, "http://127.0.0.1:9".to_owned())
        .await
        .expect_err("a membership change without quorum must time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert!(
        started.elapsed() >= Duration::from_secs(3),
        "the full request budget must be spent: {:?}",
        started.elapsed()
    );

    // The failed addition must not register the id nor touch the
    // coordinator view: membership bookkeeping only happens on success.
    let error = runtime1
        .remove_member(3)
        .await
        .expect_err("a never-registered id must be rejected");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert!(
        !runtime1
            .coordinator()
            .member_endpoints()
            .iter()
            .any(|m| m.as_ref() == "http://127.0.0.1:9"),
        "the endpoint must stay out of the view after a failed addition"
    );

    runtime1.shutdown();
    runtime1
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propose_after_shutdown_fails_and_join_succeeds() {
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(loopback_config("node-halt"), probe.machine())
        .await
        .expect("runtime must start");
    runtime
        .add_member(1, runtime.advertised_uri().to_owned())
        .await
        .expect("bootstrap must succeed");
    propose_eventually(&runtime, 5).await;
    wait_until("the node to apply before shutdown", || {
        probe.sum.load(Ordering::Acquire) == 5
    })
    .await;

    runtime.shutdown();
    wait_until("the server task to stop", || !runtime.is_alive()).await;

    // The gRPC server is gone: the client observes the refused connection.
    let error = runtime
        .propose(vec![9])
        .await
        .expect_err("proposals after shutdown must fail");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(!runtime.is_alive());

    // The runtime stays consistent: joining the stopped node still works.
    runtime
        .join()
        .await
        .expect("join after shutdown must succeed");
}
