//! Failover tuning tests: the `propose` retry-loop semantics (classification,
//! attempt cap, deadline), the failover watchdog's failure tolerance, and a
//! three-node kill-leader failover timing guard for both the balanced
//! defaults and the watchdog profile.

#[path = "common/harness.rs"]
mod harness;
#[path = "common/probe.rs"]
mod probe;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use catga_core::{ConsensusRuntime, ErrorCode};
use catga_sorock::{SorockNodeConfig, SorockProposeRetry, SorockRuntime};
use harness::{loopback_config, wait_until};
use probe::MachineProbe;

/// Generous bound for the kill-leader failover tests. Both profiles are
/// expected to recover far below it locally (balanced ~4.2s, watchdog
/// ~2.1s — measured on loopback); the bound only guards against a
/// regression that breaks the failover outright, not against scheduling
/// noise on loaded CI hosts.
const FAILOVER_BOUND: Duration = Duration::from_secs(15);

/// A cluster node driven by its own dedicated tokio runtime on a companion
/// OS thread.
///
/// A sorock 0.12 node cannot be killed faithfully from the shared test
/// runtime: its per-peer heartbeat/replication thread futures hold the
/// `Voter`, through which they reach their own abort handles
/// (`Voter -> Peers -> peer_threads -> ThreadHandle`) — an Arc cycle that
/// keeps those threads alive even after `RaftNode::detach_process` drops the
/// process struct. A detach-and-dropped "dead" leader therefore keeps
/// heartbeating the survivors every 300ms, their phi-accrual failure
/// detectors never fire, and no election ever starts (verified by
/// instrumenting the detector: `last_ping` keeps refreshing after the kill).
///
/// Giving every node its own runtime makes the kill faithful:
/// `Runtime::shutdown_background` aborts *every* task the node ever spawned
/// — gRPC server, peer connections, process threads, and the leaked per-peer
/// threads — so heartbeats actually stop and the survivors elect a new
/// leader.
struct DedicatedNode {
    runtime: SorockRuntime,
    teardown: Option<std::sync::mpsc::Sender<()>>,
    driver: Option<std::thread::JoinHandle<()>>,
}

impl DedicatedNode {
    /// Starts a node on a fresh companion thread with its own multi-thread
    /// runtime. The returned handle is used from the test thread; every task
    /// the node spawns stays on the dedicated runtime.
    fn start(config: SorockNodeConfig, probe: &MachineProbe) -> Self {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (teardown_tx, teardown_rx) = std::sync::mpsc::channel::<()>();
        let machine = probe.machine();
        let driver = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("dedicated node runtime must build");
            let node = runtime
                .block_on(SorockRuntime::start(config, machine))
                .expect("sorock runtime must start");
            started_tx
                .send(node)
                .expect("the test thread must receive the node handle");
            // Park the driver thread until the test tears the node down (or
            // drops the sender); the runtime — and with it every spawned
            // task — lives until then.
            let _ = teardown_rx.recv();
            runtime.shutdown_background();
        });
        let runtime = started_rx
            .recv()
            .expect("the node driver must report startup");
        Self {
            runtime,
            teardown: Some(teardown_tx),
            driver: Some(driver),
        }
    }

    /// Graceful stop: drain the node through its handle, then tear the
    /// dedicated runtime down.
    async fn shutdown(self) {
        let Self {
            runtime,
            teardown,
            driver,
        } = self;
        runtime.shutdown();
        runtime
            .join()
            .await
            .expect("graceful shutdown must succeed");
        abort_driver(teardown, driver).await;
    }
}

/// Aborts every task of the node's dedicated runtime and joins the driver
/// thread.
async fn abort_driver(
    teardown: Option<std::sync::mpsc::Sender<()>>,
    driver: Option<std::thread::JoinHandle<()>>,
) {
    if let Some(teardown) = teardown {
        let _ = teardown.send(());
    }
    if let Some(driver) = driver {
        tokio::task::spawn_blocking(move || driver.join())
            .await
            .expect("the driver thread must not panic")
            .expect("the node driver thread must join");
    }
}

/// Faithful in-process kill of a node (see [`DedicatedNode`]): aborting the
/// dedicated runtime stops heartbeats instantly, so the survivors' failure
/// detectors fire and an election starts. Dropping the handle afterwards
/// closes the dead node's client channel; the next write forwarded there
/// fails fast, and the survivor's client observes a quick `Unavailable`.
async fn kill_node(node: DedicatedNode) {
    let DedicatedNode {
        runtime,
        teardown: signal,
        driver,
    } = node;
    abort_driver(signal, driver).await;
    drop(runtime);
}

/// Starts a three-node in-process cluster, bootstrapping the group on the
/// first node (which thereby becomes the leader). `configure` adjusts each
/// node's config after the harness defaults.
async fn three_node_cluster(
    prefix: &str,
    configure: impl Fn(&mut SorockNodeConfig),
) -> (Vec<DedicatedNode>, Vec<MachineProbe>) {
    let probes: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let mut nodes = Vec::new();
    for (i, probe) in probes.iter().enumerate() {
        let mut config = loopback_config(&format!("{prefix}-{}", i + 1));
        configure(&mut config);
        nodes.push(DedicatedNode::start(config, probe));
    }
    let uris: Vec<String> = nodes
        .iter()
        .map(|node| node.runtime.advertised_uri().to_owned())
        .collect();
    nodes[0]
        .runtime
        .add_member(1, uris[0].clone())
        .await
        .expect("self add must bootstrap the group");
    nodes[0]
        .runtime
        .add_member(2, uris[1].clone())
        .await
        .expect("second node must join");
    nodes[0]
        .runtime
        .add_member(3, uris[2].clone())
        .await
        .expect("third node must join");
    (nodes, probes)
}

async fn shutdown_all(nodes: Vec<DedicatedNode>) {
    for node in nodes {
        node.shutdown().await;
    }
}

/// Kills the bootstrap leader and returns the time until a write through the
/// first survivor succeeds again.
async fn measure_failover(nodes: &mut Vec<DedicatedNode>) -> Duration {
    let killed = nodes.remove(0);
    kill_node(killed).await;

    let started = Instant::now();
    loop {
        assert!(
            started.elapsed() < FAILOVER_BOUND,
            "a write must succeed within {FAILOVER_BOUND:?} of the leader kill"
        );
        match nodes[0]
            .runtime
            .propose(100_u64.to_le_bytes().to_vec())
            .await
        {
            Ok(()) => return started.elapsed(),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

#[test]
fn propose_retry_classifier_covers_documented_codes() {
    let retry = SorockProposeRetry::default();

    // The leader-transient classifications are retried.
    for code in [
        ErrorCode::Transient,
        ErrorCode::Unavailable,
        ErrorCode::Cancelled,
    ] {
        assert!(retry.retries(code), "{code:?} must be retried");
    }

    // Everything else stays single-shot.
    for code in [
        ErrorCode::Validation,
        ErrorCode::HandlerFailed,
        ErrorCode::HandlerNotFound,
        ErrorCode::PipelineFailed,
        ErrorCode::PersistenceFailed,
        ErrorCode::LockFailed,
        ErrorCode::TransportFailed,
        ErrorCode::SerializationFailed,
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::Unauthorized,
        ErrorCode::Forbidden,
        ErrorCode::Timeout,
        ErrorCode::FlowFailed,
        ErrorCode::FlowCancelled,
        ErrorCode::FlowTimeout,
        ErrorCode::FlowCompensating,
        ErrorCode::Unsupported,
        ErrorCode::Internal,
    ] {
        assert!(!retry.retries(code), "{code:?} must stay single-shot");
    }
}

/// The retry loop rides out transient leader-unknown failures: a proposal
/// issued before bootstrap must succeed once the group forms, without the
/// caller retrying.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propose_retries_transient_failures_until_group_bootstraps() {
    let mut config = loopback_config("retry-bootstrap");
    config.propose_retry = SorockProposeRetry {
        max_attempts: 100,
        backoff: Duration::from_millis(50),
    };
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    // The proposal starts on a leaderless node and keeps failing transiently;
    // the concurrent bootstrap after 300ms lets a later attempt succeed. A
    // single-shot propose would return the first failure long before.
    let propose = runtime.propose(7_u64.to_le_bytes().to_vec());
    let bootstrap = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        runtime
            .add_member(1, runtime.advertised_uri().to_owned())
            .await
            .expect("bootstrap must succeed");
    };
    let (result, ()) = tokio::join!(propose, bootstrap);
    result.expect("the retry loop must ride out the election");

    // Every attempt reused the same request id, which sorock deduplicates
    // on: the entry is applied exactly once.
    wait_until("the retried proposal to apply", || {
        probe.sum.load(Ordering::Acquire) == 7
    })
    .await;

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

/// `max_attempts = 1` preserves single-shot behavior: a transient failure is
/// returned immediately instead of consuming the request budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_attempt_config_disables_retries() {
    let mut config = loopback_config("retry-single");
    config.propose_retry = SorockProposeRetry {
        max_attempts: 1,
        backoff: Duration::from_millis(50),
    };
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    let started = Instant::now();
    let error = runtime
        .propose(vec![1])
        .await
        .expect_err("a leaderless write must fail");
    assert!(
        matches!(error.code(), ErrorCode::Transient | ErrorCode::Cancelled),
        "the first failure must surface unaltered: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "single-shot must return immediately, not after the 10s budget: {:?}",
        started.elapsed()
    );

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

/// The attempt cap stops the retry loop before the request deadline and
/// returns the last classified failure rather than a timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_loop_respects_attempt_cap() {
    let mut config = loopback_config("retry-cap");
    config.propose_retry = SorockProposeRetry {
        max_attempts: 3,
        backoff: Duration::from_millis(50),
    };
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    let started = Instant::now();
    let error = runtime
        .propose(vec![1])
        .await
        .expect_err("a leaderless write must fail");
    assert!(
        matches!(error.code(), ErrorCode::Transient | ErrorCode::Cancelled),
        "the last attempt's failure must surface: {error:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "two backoffs must elapse across three attempts: {:?}",
        started.elapsed()
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the attempt cap must cut the loop short of the 10s budget: {:?}",
        started.elapsed()
    );

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

/// The request deadline bounds the retry loop even with a huge attempt cap:
/// the loop reports `Timeout` once the budget is spent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_loop_respects_request_deadline() {
    let mut config = loopback_config("retry-deadline");
    config.request_timeout = Duration::from_millis(700);
    config.propose_retry = SorockProposeRetry {
        max_attempts: 10_000,
        backoff: Duration::from_millis(100),
    };
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    let started = Instant::now();
    let error = runtime
        .propose(vec![1])
        .await
        .expect_err("a leaderless write must fail");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert!(
        started.elapsed() >= Duration::from_millis(700),
        "the full budget must be spent: {:?}",
        started.elapsed()
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the deadline must bound the loop: {:?}",
        started.elapsed()
    );

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

/// The watchdog is best-effort: on a leaderless node every candidate fails
/// (the local node rejects `TimeoutNow` for an empty membership, the seeded
/// peer refuses the connection, the invalid URI is skipped) and the propose
/// error passes through unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watchdog_failures_are_swallowed_on_leaderless_node() {
    let mut config = loopback_config("watchdog-lonely");
    config.members = vec!["http://127.0.0.1:9".to_owned(), "not a uri".to_owned()];
    config.failover_watchdog = true;
    config.request_timeout = Duration::from_secs(5);
    config.propose_retry = SorockProposeRetry {
        max_attempts: 2,
        backoff: Duration::from_millis(50),
    };
    let probe = MachineProbe::default();
    let runtime = SorockRuntime::start(config, probe.machine())
        .await
        .expect("runtime must start");

    let started = Instant::now();
    let error = runtime
        .propose(vec![1])
        .await
        .expect_err("a leaderless write must fail");
    assert!(
        matches!(error.code(), ErrorCode::Transient | ErrorCode::Cancelled),
        "the write failure must pass through the watchdog: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "failing watchdog candidates must not stall the propose call: {:?}",
        started.elapsed()
    );

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}

/// Balanced profile (defaults, watchdog off): killing the leader must let a
/// write through a survivor succeed within the bound. The first attempt
/// hangs until the 2s client deadline (the follower's forwarding handler
/// dies mid-stream); the next call's retry loop then rides out sorock's
/// phi-gated election, landing the first successful write at ~4.2s locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn kill_leader_failover_within_bound_balanced() {
    let (mut nodes, probes) = three_node_cluster("failover-balanced", |config| {
        config.request_timeout = Duration::from_secs(2);
    })
    .await;

    // Warm up: three committed writes on the leader, applied everywhere.
    for value in 1..=3_u64 {
        nodes[0]
            .runtime
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("warmup proposals must succeed");
    }
    wait_until("warmup to apply everywhere", || {
        probes.iter().all(|p| p.sum.load(Ordering::Acquire) == 6)
    })
    .await;

    let failover = measure_failover(&mut nodes).await;
    eprintln!("balanced failover after leader kill: {failover:?}");

    wait_until("survivors to apply the post-failover write", || {
        probes[1].sum.load(Ordering::Acquire) == 106 && probes[2].sum.load(Ordering::Acquire) == 106
    })
    .await;

    shutdown_all(nodes).await;
}

/// Watchdog profile: when the hung first attempt hits the 2s client
/// deadline, the watchdog force-promotes the local survivor through
/// `TimeoutNow` (~2 election round trips instead of the failure-detector
/// window), so the immediately following `propose` call succeeds — ~2.1s
/// locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn kill_leader_failover_within_bound_with_watchdog() {
    let (mut nodes, probes) = three_node_cluster("failover-watchdog", |config| {
        config.request_timeout = Duration::from_secs(2);
        config.failover_watchdog = true;
    })
    .await;

    for value in 1..=3_u64 {
        nodes[0]
            .runtime
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("warmup proposals must succeed");
    }
    wait_until("warmup to apply everywhere", || {
        probes.iter().all(|p| p.sum.load(Ordering::Acquire) == 6)
    })
    .await;

    let failover = measure_failover(&mut nodes).await;
    eprintln!("watchdog failover after leader kill: {failover:?}");

    wait_until("survivors to apply the post-failover write", || {
        probes[1].sum.load(Ordering::Acquire) == 106 && probes[2].sum.load(Ordering::Acquire) == 106
    })
    .await;

    shutdown_all(nodes).await;
}
