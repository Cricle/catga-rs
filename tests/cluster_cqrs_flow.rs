//! Deep combination contracts: a replicated Raft cluster, the CQRS mediator, and the
//! `DslFlow` engine exercised as one system.
//!
//! 1. Flow steps are CQRS commands proposed onto the cluster leader; every voter
//!    converges to the flow's final state.
//! 2. A flow survives leader failover mid-flight: the old leader dies between steps,
//!    survivors re-elect, and the remaining steps complete through the new leader
//!    without losing any committed step.
//! 3. Command pipeline behaviors wrap the replicated command path: execution order is
//!    honored and a short-circuiting behavior's error surfaces through the flow's
//!    failure contract without the rejected write ever reaching the cluster.
//! 4. After a replicated write commits, an event fans out to every mediator handler
//!    exactly once per commit — even across a leader change — using the state
//!    machine's applied log index as the idempotency key.

#[path = "support/raft_kv_cluster.rs"]
mod raft_kv_cluster;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::ClusterCoordinator;
use catga_core::flow::DslFlow;
use catga_core::flow::dsl_lifecycle::{
    DslFlowLifecycleEvent, DslFlowLifecycleHooks, DslFlowLifecycleObserver,
};
use catga_core::{
    CatgaError, CatgaResult, Command, CommandBehavior, CommandHandler, CommandNext,
    CommandPipeline, ErrorCode, Event, EventHandler, Mediator, Message, Registry,
};
use futures::future::BoxFuture;

use raft_kv_cluster::{
    SharedNodes, boot_nodes, elect_first, encode_put, kill_leader, shutdown_all, wait_for_leader,
    wait_for_map,
};

/// Runs one combination scenario on a single-threaded Tokio runtime with real time,
/// matching the `catga-cluster` scale-contract pattern.
fn run_test(test: impl std::future::Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime builds");
    runtime.block_on(test);
}

// ---------------------------------------------------------------------------
// CQRS command proposed onto the cluster leader
// ---------------------------------------------------------------------------

/// One key/value write routed through CQRS onto the replicated state machine.
#[derive(Debug)]
struct PutKv {
    key: String,
    value: String,
}
impl Message for PutKv {}
impl Command for PutKv {
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Proposes the write on the current leader and waits until that leader applies it.
///
/// A leader only applies committed entries, so once this handler returns the write is
/// replicated to a majority and survives a leader failover.
struct ClusterPutHandler {
    nodes: SharedNodes,
}

#[async_trait]
impl CommandHandler<PutKv> for ClusterPutHandler {
    async fn handle(&self, command: PutKv) -> CatgaResult<()> {
        let nodes = self.nodes.read().await;
        let leader = nodes
            .iter()
            .find(|node| node.runtime.coordinator().is_leader())
            .ok_or_else(|| CatgaError::new(ErrorCode::Unavailable, "no cluster leader elected"))?;
        leader
            .runtime
            .propose(encode_put(&command.key, &command.value))
            .await
            .map_err(|error| {
                CatgaError::new(
                    ErrorCode::Unavailable,
                    format!("leader rejected the proposal: {error}"),
                )
            })?;
        let applied = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let committed = {
                    let state = leader.store.lock().expect("kv store mutex poisoned");
                    state.map.get(&command.key) == Some(&command.value)
                };
                if committed {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        applied.map_err(|_| {
            CatgaError::new(
                ErrorCode::Timeout,
                "leader never applied the proposed write",
            )
        })
    }
}

/// Builds a mediator whose `PutKv` commands replicate through the cluster.
fn put_mediator(nodes: &SharedNodes) -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry
        .register_command::<PutKv, _>(ClusterPutHandler {
            nodes: Arc::clone(nodes),
        })
        .expect("command handler registers");
    Arc::new(Mediator::new(registry))
}

/// Kills the leader after the first flow step commits, then waits until the survivors
/// re-elect a replacement.
async fn fail_over_after_first_step(
    nodes: &SharedNodes,
    failed_over: &AtomicBool,
    killed: &Mutex<Option<u64>>,
    index: usize,
) {
    if index == 0 && !failed_over.swap(true, Ordering::AcqRel) {
        let killed_id = kill_leader(nodes).await;
        *killed.lock().expect("killed leader slot") = Some(killed_id);
        wait_for_leader(nodes, Duration::from_secs(10)).await;
    }
}

/// Waits until every live replica converges to the expected map, then returns each
/// replica's applied log as `(index, key, value)` triples.
async fn converged_stores(
    nodes: &SharedNodes,
    expected: &BTreeMap<String, String>,
) -> Vec<Vec<(u64, String, String)>> {
    let stores: Vec<_> = nodes
        .read()
        .await
        .iter()
        .map(|node| Arc::clone(&node.store))
        .collect();
    for store in &stores {
        wait_for_map(store, expected).await;
    }
    stores
        .iter()
        .map(|store| {
            store
                .lock()
                .expect("kv store mutex poisoned")
                .log
                .iter()
                .map(|entry| (entry.index, entry.key.clone(), entry.value.clone()))
                .collect()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Scenario 1: flow steps are CQRS commands over the replicated cluster
// ---------------------------------------------------------------------------

fn replicated_put(
    mediator: Arc<Mediator>,
    key: &'static str,
    value: &'static str,
) -> impl for<'a> Fn(&'a mut Vec<String>) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync {
    move |state: &mut Vec<String>| -> BoxFuture<'_, CatgaResult<()>> {
        let mediator = Arc::clone(&mediator);
        Box::pin(async move {
            mediator
                .send_command(PutKv {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .await?;
            state.push(key.to_string());
            Ok(())
        })
    }
}

#[test]
fn flow_steps_are_cqrs_commands_replicated_across_the_cluster() {
    run_test(async {
        let nodes = boot_nodes(3).await;
        elect_first(&nodes).await;
        let mediator = put_mediator(&nodes);

        let flow = DslFlow::new()
            .action(replicated_put(Arc::clone(&mediator), "k1", "v1"))
            .action(replicated_put(Arc::clone(&mediator), "k2", "v2"))
            .action(replicated_put(Arc::clone(&mediator), "k3", "v3"));

        let mut completed: Vec<String> = Vec::new();
        flow.run(&mut completed).await.expect("flow completes");
        assert_eq!(completed, ["k1", "k2", "k3"], "steps ran in order");

        let expected = BTreeMap::from([
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
            ("k3".to_string(), "v3".to_string()),
        ]);
        let logs = converged_stores(&nodes, &expected).await;
        for log in &logs {
            let keys: Vec<&str> = log.iter().map(|entry| entry.1.as_str()).collect();
            assert_eq!(
                keys,
                ["k1", "k2", "k3"],
                "every voter applied the flow's writes in order"
            );
            let values: Vec<&str> = log.iter().map(|entry| entry.2.as_str()).collect();
            assert_eq!(values, ["v1", "v2", "v3"]);
            assert!(
                log.windows(2).all(|pair| pair[0].0 < pair[1].0),
                "applied log indexes increase monotonically"
            );
        }

        shutdown_all(&nodes).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 2: flow survives leader failover mid-flight
// ---------------------------------------------------------------------------

struct FailoverCtx {
    nodes: SharedNodes,
    mediator: Arc<Mediator>,
    completed: Vec<String>,
    failed_over: AtomicBool,
    killed: Mutex<Option<u64>>,
}

fn failover_put(
    key: &'static str,
    value: &'static str,
) -> impl for<'a> Fn(&'a mut FailoverCtx) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync {
    move |state: &mut FailoverCtx| -> BoxFuture<'_, CatgaResult<()>> {
        let mediator = Arc::clone(&state.mediator);
        Box::pin(async move {
            mediator
                .send_command(PutKv {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .await?;
            state.completed.push(key.to_string());
            Ok(())
        })
    }
}

#[test]
fn flow_survives_leader_failover_mid_flight() {
    run_test(async {
        let nodes = boot_nodes(3).await;
        elect_first(&nodes).await;
        let mediator = put_mediator(&nodes);

        let hooks = DslFlowLifecycleHooks::new().on_step_succeeded(
            |state: &FailoverCtx, index: usize| -> BoxFuture<'_, CatgaResult<()>> {
                Box::pin(async move {
                    fail_over_after_first_step(
                        &state.nodes,
                        &state.failed_over,
                        &state.killed,
                        index,
                    )
                    .await;
                    Ok(())
                })
            },
        );
        let flow = DslFlow::new()
            .with_lifecycle_hooks(hooks)
            .action(failover_put("before-1", "a"))
            .action(failover_put("after-1", "b"))
            .action(failover_put("after-2", "c"));

        let mut ctx = FailoverCtx {
            nodes: Arc::clone(&nodes),
            mediator,
            completed: Vec::new(),
            failed_over: AtomicBool::new(false),
            killed: Mutex::new(None),
        };
        flow.run(&mut ctx)
            .await
            .expect("the flow completes across the failover");

        assert_eq!(
            ctx.completed,
            ["before-1", "after-1", "after-2"],
            "no committed step was lost and later steps ran on the new leader"
        );
        let killed = ctx
            .killed
            .lock()
            .expect("killed leader slot")
            .expect("the failover hook ran after the first step");

        // The surviving pair re-elected someone else and converged on the full state.
        let expected = BTreeMap::from([
            ("before-1".to_string(), "a".to_string()),
            ("after-1".to_string(), "b".to_string()),
            ("after-2".to_string(), "c".to_string()),
        ]);
        let logs = converged_stores(&nodes, &expected).await;
        assert_eq!(logs.len(), 2, "two voters survive the failover");
        for log in &logs {
            let keys: Vec<&str> = log.iter().map(|entry| entry.1.as_str()).collect();
            assert_eq!(keys, ["before-1", "after-1", "after-2"]);
            let values: Vec<&str> = log.iter().map(|entry| entry.2.as_str()).collect();
            assert_eq!(values, ["a", "b", "c"]);
        }
        let leader_position = wait_for_leader(&nodes, Duration::from_secs(10)).await;
        let current_leader = nodes.read().await[leader_position].runtime.id();
        assert_ne!(
            current_leader, killed,
            "steps after the failover ran through the new leader"
        );

        shutdown_all(&nodes).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 3: behavior composition over the replicated command path
// ---------------------------------------------------------------------------

/// Records pipeline entry and exit for each command it wraps.
struct RecordingBehavior {
    name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl CommandBehavior<PutKv> for RecordingBehavior {
    async fn handle(&self, command: PutKv, next: CommandNext<PutKv>) -> CatgaResult<()> {
        self.log
            .lock()
            .expect("behavior log")
            .push(format!("{}:before:{}", self.name, command.key));
        let result = next.run(command).await;
        let outcome = match &result {
            Ok(()) => "ok".to_string(),
            Err(error) => format!("err:{:?}", error.code()),
        };
        self.log
            .lock()
            .expect("behavior log")
            .push(format!("{}:after:{outcome}", self.name));
        result
    }
}

/// Short-circuits one configured key before it can reach the cluster.
struct GateBehavior {
    rejected_key: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl CommandBehavior<PutKv> for GateBehavior {
    async fn handle(&self, command: PutKv, next: CommandNext<PutKv>) -> CatgaResult<()> {
        if command.key == self.rejected_key {
            self.log
                .lock()
                .expect("behavior log")
                .push(format!("gate:reject:{}", command.key));
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "gate rejected the key",
            ));
        }
        next.run(command).await
    }
}

/// Records the flow's lifecycle events as compact strings.
#[derive(Default)]
struct FlowEvents {
    events: Mutex<Vec<String>>,
}

impl FlowEvents {
    fn names(&self) -> Vec<String> {
        self.events.lock().expect("flow events").clone()
    }
}

impl DslFlowLifecycleObserver for FlowEvents {
    fn observe(&self, event: &DslFlowLifecycleEvent) {
        let name = match event {
            DslFlowLifecycleEvent::StepSucceeded { step_index } => {
                format!("step-ok:{step_index}")
            }
            DslFlowLifecycleEvent::StepFailed { step_index, error } => {
                format!("step-err:{step_index}:{:?}", error.code())
            }
            DslFlowLifecycleEvent::FlowSucceeded => "flow-ok".to_string(),
            DslFlowLifecycleEvent::FlowFailed { error } => {
                format!("flow-err:{:?}", error.code())
            }
        };
        self.events.lock().expect("flow events").push(name);
    }
}

fn gated_put(
    mediator: Arc<Mediator>,
    pipeline: Arc<CommandPipeline<PutKv>>,
    key: &'static str,
    value: &'static str,
) -> impl for<'a> Fn(&'a mut Vec<&'static str>) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync {
    move |state: &mut Vec<&'static str>| -> BoxFuture<'_, CatgaResult<()>> {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        Box::pin(async move {
            mediator
                .send_command_with(
                    PutKv {
                        key: key.to_string(),
                        value: value.to_string(),
                    },
                    pipeline.as_ref(),
                )
                .await?;
            state.push(key);
            Ok(())
        })
    }
}

#[test]
fn pipeline_behaviors_wrap_the_replicated_command_path() {
    run_test(async {
        let nodes = boot_nodes(3).await;
        elect_first(&nodes).await;
        let mediator = put_mediator(&nodes);

        let log = Arc::new(Mutex::new(Vec::new()));
        let pipeline = Arc::new(
            CommandPipeline::new()
                .with(RecordingBehavior {
                    name: "outer",
                    log: Arc::clone(&log),
                })
                .with(GateBehavior {
                    rejected_key: "blocked",
                    log: Arc::clone(&log),
                })
                .with(RecordingBehavior {
                    name: "inner",
                    log: Arc::clone(&log),
                }),
        );

        let events = Arc::new(FlowEvents::default());
        let flow = DslFlow::new()
            .with_lifecycle_observer(Arc::clone(&events))
            .action(gated_put(
                Arc::clone(&mediator),
                Arc::clone(&pipeline),
                "alpha",
                "1",
            ))
            .action(gated_put(
                Arc::clone(&mediator),
                Arc::clone(&pipeline),
                "blocked",
                "2",
            ))
            .action(gated_put(
                Arc::clone(&mediator),
                Arc::clone(&pipeline),
                "omega",
                "3",
            ));

        let mut completed: Vec<&'static str> = Vec::new();
        let error = flow
            .run(&mut completed)
            .await
            .expect_err("the gated step must fail the flow");
        assert_eq!(error.code(), ErrorCode::Validation);
        assert_eq!(error.message(), "gate rejected the key");
        assert_eq!(
            completed,
            ["alpha"],
            "the flow stops at the short-circuited step"
        );

        assert_eq!(
            *log.lock().expect("behavior log"),
            [
                "outer:before:alpha",
                "inner:before:alpha",
                "inner:after:ok",
                "outer:after:ok",
                "outer:before:blocked",
                "gate:reject:blocked",
                "outer:after:err:Validation",
            ],
            "behaviors nest in registration order and observe the short-circuit"
        );
        assert_eq!(
            events.names(),
            ["step-ok:0", "step-err:1:Validation", "flow-err:Validation"],
            "the flow records the behavior failure per its lifecycle contract"
        );

        // The short-circuited write never reached the cluster, and the flow stopped
        // before the third step, so only the first write replicates.
        let expected = BTreeMap::from([("alpha".to_string(), "1".to_string())]);
        let logs = converged_stores(&nodes, &expected).await;
        for log in &logs {
            let keys: Vec<&str> = log.iter().map(|entry| entry.1.as_str()).collect();
            assert_eq!(keys, ["alpha"], "only the admitted write was proposed");
            let values: Vec<&str> = log.iter().map(|entry| entry.2.as_str()).collect();
            assert_eq!(values, ["1"]);
        }

        shutdown_all(&nodes).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 4: event fan-out after commit, deduplicated by applied log index
// ---------------------------------------------------------------------------

/// Published once per committed write; the Raft log index is the idempotency key.
#[derive(Clone, Debug)]
struct KvCommitted {
    index: u64,
    key: String,
}
impl Message for KvCommitted {}
impl Event for KvCommitted {
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Per-handler record of observed commit events as `(log index, key)` pairs.
type SeenLog = Arc<Mutex<Vec<(u64, String)>>>;

/// Records the `(index, key)` pair of every observed commit event.
struct RecordingEventHandler {
    seen: SeenLog,
}

#[async_trait]
impl EventHandler<KvCommitted> for RecordingEventHandler {
    async fn handle(&self, event: KvCommitted) -> CatgaResult<()> {
        self.seen
            .lock()
            .expect("event log")
            .push((event.index, event.key));
        Ok(())
    }
}

struct FanoutCtx {
    nodes: SharedNodes,
    mediator: Arc<Mediator>,
    emitted: Mutex<BTreeSet<u64>>,
    failed_over: AtomicBool,
    killed: Mutex<Option<u64>>,
}

/// Publishes one event per committed write on the current leader, skipping log indexes
/// already emitted — so re-reading a prefix of the log after a leader change never
/// double-publishes an earlier commit.
async fn publish_committed(ctx: &FanoutCtx) -> CatgaResult<()> {
    let store = {
        let nodes = ctx.nodes.read().await;
        let leader = nodes
            .iter()
            .find(|node| node.runtime.coordinator().is_leader())
            .ok_or_else(|| CatgaError::new(ErrorCode::Unavailable, "no cluster leader elected"))?;
        Arc::clone(&leader.store)
    };
    let entries: Vec<(u64, String)> = {
        let state = store.lock().expect("kv store mutex poisoned");
        state
            .log
            .iter()
            .map(|entry| (entry.index, entry.key.clone()))
            .collect()
    };
    for (index, key) in entries {
        let fresh = ctx.emitted.lock().expect("emitted set").insert(index);
        if fresh {
            ctx.mediator.publish(KvCommitted { index, key }).await?;
        }
    }
    Ok(())
}

fn fanout_put(
    key: &'static str,
    value: &'static str,
) -> impl for<'a> Fn(&'a mut FanoutCtx) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync {
    move |state: &mut FanoutCtx| -> BoxFuture<'_, CatgaResult<()>> {
        Box::pin(async move {
            state
                .mediator
                .send_command(PutKv {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .await?;
            publish_committed(state).await
        })
    }
}

#[test]
fn committed_writes_fan_out_exactly_once_across_a_leader_change() {
    run_test(async {
        let nodes = boot_nodes(3).await;
        elect_first(&nodes).await;

        let seen: Vec<SeenLog> = (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut registry = Registry::new();
        registry
            .register_command::<PutKv, _>(ClusterPutHandler {
                nodes: Arc::clone(&nodes),
            })
            .expect("command handler registers");
        for handle in &seen {
            registry.register_event::<KvCommitted, _>(RecordingEventHandler {
                seen: Arc::clone(handle),
            });
        }
        let mediator = Arc::new(Mediator::new(registry));

        let hooks = DslFlowLifecycleHooks::new().on_step_succeeded(
            |state: &FanoutCtx, index: usize| -> BoxFuture<'_, CatgaResult<()>> {
                Box::pin(async move {
                    fail_over_after_first_step(
                        &state.nodes,
                        &state.failed_over,
                        &state.killed,
                        index,
                    )
                    .await;
                    Ok(())
                })
            },
        );
        let flow = DslFlow::new()
            .with_lifecycle_hooks(hooks)
            .action(fanout_put("x", "1"))
            .action(fanout_put("y", "2"));

        let mut ctx = FanoutCtx {
            nodes: Arc::clone(&nodes),
            mediator,
            emitted: Mutex::new(BTreeSet::new()),
            failed_over: AtomicBool::new(false),
            killed: Mutex::new(None),
        };
        flow.run(&mut ctx)
            .await
            .expect("the flow completes across the failover");

        // Two distinct commits were emitted in log order; the failover hook ran.
        let emitted: Vec<u64> = ctx
            .emitted
            .lock()
            .expect("emitted set")
            .iter()
            .copied()
            .collect();
        assert_eq!(emitted.len(), 2, "two distinct commits were emitted");
        assert!(
            ctx.killed.lock().expect("killed leader slot").is_some(),
            "the leader changed between the two commits"
        );

        // Every fan-out handler observed each commit exactly once, in log order, even
        // though the second drain re-read the first commit from the new leader's log.
        for handle in &seen {
            let mut observed = handle.lock().expect("event log").clone();
            observed.sort_unstable();
            let indexes: Vec<u64> = observed.iter().map(|entry| entry.0).collect();
            assert_eq!(
                indexes, emitted,
                "each handler observes every commit exactly once"
            );
            let keys: Vec<&str> = observed.iter().map(|entry| entry.1.as_str()).collect();
            assert_eq!(keys, ["x", "y"], "events carry the committed writes");
        }

        let expected = BTreeMap::from([
            ("x".to_string(), "1".to_string()),
            ("y".to_string(), "2".to_string()),
        ]);
        let logs = converged_stores(&nodes, &expected).await;
        assert_eq!(logs.len(), 2, "two voters survive the failover");

        shutdown_all(&nodes).await;
    });
}
