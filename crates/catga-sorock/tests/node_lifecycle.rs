//! Lifecycle tests for [`SorockNode`]: address resolution, accessors,
//! startup failure modes, graceful shutdown, and restart persistence of a
//! file-backed (redb) node.

#[path = "common/harness.rs"]
mod harness;
#[path = "common/probe.rs"]
mod probe;
#[path = "common/propose.rs"]
mod propose;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::atomic::Ordering,
    time::Duration,
};

use catga_core::{ConsensusRuntime, ConsensusStateMachine, ErrorCode};
use catga_sorock::{SorockApp, SorockNode, SorockNodeConfig, SorockRuntime, SorockStorage};
use harness::{POLL_INTERVAL, WAIT_TIMEOUT, loopback_config, wait_until};
use probe::MachineProbe;
use propose::propose_eventually;

/// Restarts a file-backed runtime, tolerating the brief window in which the
/// previous incarnation's redb file handle or listen port is still being
/// released by the OS.
async fn restart_file_backed<M>(
    config: &SorockNodeConfig,
    make_machine: impl Fn() -> M,
) -> SorockRuntime
where
    M: ConsensusStateMachine + 'static,
{
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match SorockRuntime::start(config.clone(), make_machine()).await {
            Ok(runtime) => return runtime,
            Err(error) => {
                assert!(
                    matches!(
                        error.code(),
                        ErrorCode::PersistenceFailed | ErrorCode::Unavailable
                    ) && std::time::Instant::now() < deadline,
                    "file-backed restart failed: {error}"
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_exposes_resolved_addresses_and_stops_gracefully() {
    let mut config = loopback_config("node-accessors");
    config.shard = 3;
    let probe = MachineProbe::default();

    let node = SorockNode::start(&config, SorockApp::new(probe.machine(), 0))
        .await
        .expect("node must start");

    assert_ne!(node.local_addr().port(), 0, "port 0 must be resolved");
    assert_eq!(node.local_addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(
        node.advertised_uri(),
        format!("http://{}", node.local_addr()),
        "without a public uri the node advertises its bound address"
    );
    assert_eq!(node.shard(), 3);
    assert!(node.is_running());
    // Accessor handles stay alive for the node's lifetime.
    let _ = node.raft_node();
    let _ = node.storage();

    node.request_shutdown();
    // A repeated shutdown request is harmless.
    node.request_shutdown();
    wait_until("server task to stop", || !node.is_running()).await;
    node.join().await.expect("graceful join must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_is_bounded_when_a_never_used_connection_races_shutdown() {
    let config = loopback_config("node-wedged-join");
    let probe = MachineProbe::default();
    let node = SorockNode::start(&config, SorockApp::new(probe.machine(), 0))
        .await
        .expect("node must start");

    // A TCP connection that never completes the HTTP/2 preface reproduces the
    // racing lazy client connection: tonic's graceful shutdown waits for such
    // a connection forever, so join must fall back to its drain bound instead
    // of hanging.
    let socket = tokio::net::TcpStream::connect(node.local_addr())
        .await
        .expect("raw tcp connect must succeed");
    // Let the server accept the wedged connection before the drain starts:
    // tonic's graceful shutdown only waits for connections it has already
    // accepted, so a shutdown landing ahead of the accept would skip the
    // drain-bound path this test pins. Loopback accepts complete within
    // milliseconds, even on instrumented coverage builds.
    tokio::time::sleep(Duration::from_millis(200)).await;
    node.request_shutdown();

    let started = std::time::Instant::now();
    node.join()
        .await
        .expect("bounded join must succeed despite the wedged connection");
    let elapsed = started.elapsed();
    drop(socket);

    assert!(
        elapsed >= Duration::from_secs(1),
        "the wedged connection must force the drain-bound path, not an instant return: {elapsed:?}"
    );
    assert!(
        elapsed < WAIT_TIMEOUT,
        "join must stay bounded under the racing connection: {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_with_unspecified_bind_ip_advertises_loopback() {
    let config = SorockNodeConfig::new(
        "node-wildcard",
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
    );
    let probe = MachineProbe::default();

    let node = SorockNode::start(&config, SorockApp::new(probe.machine(), 0))
        .await
        .expect("node must start");

    assert_eq!(node.local_addr().ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    let advertised = node.advertised_uri();
    assert!(
        advertised.starts_with("http://127.0.0.1:"),
        "an unspecified bind ip must be advertised as loopback: {advertised}"
    );
    assert!(
        advertised.ends_with(&node.local_addr().port().to_string()),
        "the advertised port must be the bound one: {advertised}"
    );

    node.request_shutdown();
    node.join().await.expect("graceful join must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_rejects_conflicting_bind_address() {
    let probe = MachineProbe::default();
    let first = SorockNode::start(
        &loopback_config("node-first"),
        SorockApp::new(probe.machine(), 0),
    )
    .await
    .expect("first node must start");

    let mut conflicting = loopback_config("node-second");
    conflicting.bind_addr = first.local_addr();
    let error = SorockNode::start(&conflicting, SorockApp::new(probe.machine(), 0))
        .await
        .err()
        .expect("a second bind on the same address must fail");

    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(
        error.message().contains("cannot bind"),
        "unexpected message: {}",
        error.message()
    );
    assert!(
        error.message().contains(&first.local_addr().to_string()),
        "the conflicting address must be reported: {}",
        error.message()
    );
    assert!(first.is_running(), "the first node is unaffected");

    first.request_shutdown();
    first.join().await.expect("graceful join must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_rejects_redb_file_in_missing_directory() {
    let missing_dir =
        std::env::temp_dir().join(format!("catga-sorock-node-{}", std::process::id()));
    assert!(
        !missing_dir.exists(),
        "test precondition: {missing_dir:?} must not exist"
    );

    let mut config = loopback_config("node-badpath");
    config.storage = SorockStorage::RedbFile(missing_dir.join("raft.redb"));
    let probe = MachineProbe::default();

    let error = SorockNode::start(&config, SorockApp::new(probe.machine(), 0))
        .await
        .err()
        .expect("an unwritable storage path must fail");

    assert_eq!(error.code(), ErrorCode::PersistenceFailed);
    assert!(
        error.message().starts_with("sorock redb storage failed: "),
        "unexpected message: {}",
        error.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_backed_runtime_recovers_applied_state_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir must be created");
    let db_path = dir.path().join("raft.redb");

    // sorock persists membership by advertised URI: a restarted node must
    // come back on the same address or it is no longer a member of its own
    // group. Reserve a fixed free port for both incarnations.
    let listener =
        std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("ephemeral bind must succeed");
    let addr = listener
        .local_addr()
        .expect("the bound address must be readable");
    drop(listener);

    let probe1 = MachineProbe::default();
    let mut config = loopback_config("node-restart");
    config.bind_addr = addr;
    config.storage = SorockStorage::RedbFile(db_path);

    // First incarnation: bootstrap a single-node group and commit a batch.
    let runtime = SorockRuntime::start(config.clone(), probe1.machine())
        .await
        .expect("file-backed node must start");
    runtime
        .add_member(1, runtime.advertised_uri().to_owned())
        .await
        .expect("bootstrap must succeed");
    propose_eventually(&runtime, 1).await;
    for value in 2..=4_u64 {
        runtime
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("proposals must succeed on the bootstrapped group");
    }
    wait_until("first incarnation to apply the batch", || {
        probe1.sum.load(Ordering::Acquire) == 10
    })
    .await;
    let applied_before = runtime
        .applied_index()
        .await
        .expect("applied index must be readable");
    assert!(
        applied_before >= 4,
        "all four proposals must be applied before shutdown: {applied_before}"
    );

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");

    // Second incarnation: same redb file, fresh state machine. The persisted
    // log is replayed and the persisted membership re-elects the node.
    let probe2 = MachineProbe::default();
    let runtime2 = restart_file_backed(&config, || probe2.machine()).await;

    // The first post-restart proposal waits out the re-election and, once
    // committed, advances the commit pointer over the recovered log entries.
    propose_eventually(&runtime2, 20).await;
    wait_until("restarted node to replay and apply", || {
        probe2.sum.load(Ordering::Acquire) == 30
    })
    .await;

    let applied_after = runtime2
        .applied_index()
        .await
        .expect("applied index must be readable after restart");
    assert!(
        applied_after >= applied_before,
        "the restarted node must cover the pre-shutdown index: {applied_after} < {applied_before}"
    );
    assert_eq!(probe1.sum.load(Ordering::Acquire), 10);

    runtime2.shutdown();
    runtime2
        .join()
        .await
        .expect("graceful shutdown must succeed");
}
