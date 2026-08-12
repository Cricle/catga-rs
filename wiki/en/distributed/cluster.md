# Cluster Mode

## Overview

`catga-cluster` provides Raft (raft-rs) consensus and cluster-coordination contracts: the application implements the state machine (`RaftStateMachine`) and the transport (`RaftTransport`); the runtime (`RaftStateMachineRuntime`) drives election, log replication, and command application serially on a single Tokio task. The crate does not create a network listener, choose durable storage for you, or provide distributed locks or sharding.

> Runnable reference implementation: [`examples/distributed-kv`](../../../examples/distributed-kv) (three-node Raft KV cluster with leader forwarding and snapshots). All types below have rustdoc; this page references them by name instead of copying full signatures.

```toml
[dependencies]
catga-cluster = "0.2"
catga-axum = "0.2"   # HTTP Raft transport / forwarding adapters
```

## Configuration

`RaftClusterConfig` is a serde configuration (camelCase): `nodeId`, `localNodeEndpoint`, `members[{id,endpoint}]` (remote members only), `tickIntervalMs` (10), `electionTimeoutMs` (150), `heartbeatIntervalMs` (50), `persistentStatePath` (in-memory when absent).

Helpers:

- `RaftClusterConfig::local(node_id, total_nodes, base_port)` — local development cluster; `node_id` is a **zero-based** process index (converted to a non-zero Raft member id internally), member endpoints are `http://localhost:{base_port + id}`, persistence dir `./raft-state-node{id}`.
- `.members()` → `Vec<RaftMember>` (full voter list including this node).
- `.raft_timing()` → validated `RaftTiming` (tick/election/heartbeat).
- `.open_node()` → persistent (raft-engine) or in-memory `RaftNode` according to `persistentStatePath`.

Without the config, construct manually: `RaftNode::new` / `new_with_timing` / `open_persistent(id, endpoint, members, dir)`, with members built via `RaftMember::new(id, endpoint)`.

## Bootstrap builder (recommended)

Since 0.2, `RaftHttpCluster::builder` from `catga-axum` is the **recommended** way to build a Raft HTTP cluster node. It collapses the generic wiring every HTTP-hosted Raft application used to rewrite by hand: opening the configured node, spawning the runtime with an `HttpRaftTransport` that carries a bounded request timeout and peer identity, mounting the inbound Raft route behind the peer-identity middleware and the static inbound policy, and exposing `/healthz` and `/status` probes. distributed-kv's node wiring shrank from 328 to 224 lines.

```rust
let cluster = RaftHttpCluster::builder(config)
    .state_machine(machine)
    .with_request_timeout(Duration::from_secs(2))  // optional, default DEFAULT_RAFT_HTTP_REQUEST_TIMEOUT = 2s
    .with_peer_naming(|id| format!("node-{id}"))   // optional, default "node-{id}"
    .with_client(reqwest_client)                   // optional, pass the mtls_reqwest_client product for mTLS
    .build()?;

let listener = TcpListener::bind("0.0.0.0:9100").await?;
cluster.serve(listener, app_routes).await?;        // serve-before-campaign; graceful SIGINT/SIGTERM exit
```

- **Startup ordering built in**: `serve()` binds and accepts HTTP connections first and only then starts the election, eliminating the "election stalls on peer connections" startup deadlock; if the campaign fails, the just-started server is aborted and an error returned.
- **Probes built in**: `/healthz` (`RAFT_HTTP_HEALTH_PATH`) returns 200 while the owner task runs and 503 once it stops (liveness); `/status` (`RAFT_HTTP_STATUS_PATH`) reports the node id, leadership view, leader endpoint, liveness, and latest applied index as JSON (readiness).
- **Surface**: `cluster.runtime()` (propose / `applied_index` / checkpoint), `cluster.coordinator()` (lock-free leadership view), `cluster.router()` (clone of the framework router — when serving it yourself, keep the "listener first, then `campaign()`" ordering), and `serve_until(listener, routes, shutdown)` for a custom shutdown signal.
- **Scope**: topology discovery is intentionally out of scope — the builder consumes an already-resolved `RaftClusterConfig` and never reads the environment. Your own routes, mediator pipelines, and leader duties (e.g. `ForwardToLeaderBehavior` + `HttpClusterForwarder`) compose into the routes passed to `serve` as before.

## Manual wiring (advanced)

Applications that need full lifecycle control can still wire everything by hand (this is exactly what the builder does internally):

```rust
let config = RaftClusterConfig::local(node, nodes, base_port)?;
let node = config.open_node()?;
let driver = RaftStateMachineDriver::new(node, machine)?;      // recovers snapshot + committed suffix
let runtime = RaftStateMachineRuntime::spawn(driver, transport, timing.tick_interval())?;

// 1. Bind and serve the HTTP listener first (raft_message_route injects runtime.inbox())
// 2. Then start the election
runtime.campaign().await?;
```

**Startup order matters**: the Raft owner task awaits transport sends, so bind and serve the HTTP listener (making the peers' `raft_message_route` reachable) before calling `campaign()`, otherwise election/heartbeat sends stall on peer connections.

Runtime methods: `campaign()`, `propose(Vec<u8>)`, `propose_and_wait(data, timeout)`, `checkpoint()`, `coordinator()`, `inbox()` (for the network receiver to inject `RaftMessage`), `shutdown()`, `join()`.

**`propose_and_wait(data, timeout)`** (push-based wait-for-applied): unlike fire-and-forget `propose`, it resolves once the entry is **committed and applied by the local state machine**, returning the applied index (at least the slot the proposal was assigned, so the entry's effects are observable when the call resolves) — no polling; it subscribes to the owner task's applied-index publication. Error surface: proposing without leadership fails fast with the same routine caller error as `propose` (including `raft::Error::ProposalDropped`); the owner task stopping mid-wait returns `RaftStateMachineRuntimeError::Stopped`; the deadline expiring first returns `RaftStateMachineRuntimeError::Timeout` (the proposal may still be committed and applied later). If leadership changes while the entry is in flight, its log slot can be overwritten by another entry and the resolved index reflects that slot rather than necessarily this proposal — callers needing exactly-once semantics must embed an application-level op-id in the payload and verify it against the state machine.

## State machine

Implement the `RaftStateMachine` trait:

- `apply(&mut self, entry: &RaftCommittedEntry)` — apply one committed entry (`entry.index` / `entry.data`).
- `snapshot()` / `restore()` — snapshot serialization and recovery.

`RaftStateMachineDriver::new(node, machine)` recovers the latest snapshot and replays the committed suffix at startup; all applies are serialized by the runtime, so the state machine needs no internal locking (a read model shared across threads still needs its own synchronization — see `SharedState` in distributed-kv).

## HTTP transport and authentication

The transport is abstracted by the `RaftTransport` trait: `async fn send(&self, RaftMessage) -> RaftTransportResult`. Error semantics:

- `RaftTransportError::retryable` — peer temporarily down (timeout/connect failure/5xx/429); the runtime survives and retries;
- `RaftTransportError::fatal` — unrecoverable error; the runtime stops.

`catga-axum` provides the HTTP adapter:

- Client `HttpRaftTransport::new(reqwest::Client, members)` POSTs protobuf frames to member endpoints (path `RAFT_MESSAGE_PATH`). Recommended chain: `.with_request_timeout(Duration)` (a hung peer must not stall the owner task) and `.with_peer_identity("node-1")` (carries the node identity header).
- Server `raft_message_route(runtime.inbox(), StaticRaftInboundPolicy::new(local_id, [(peer_id, "identity")]))` authorizes frames by authenticated peer identity plus the frame's from/to before enqueueing; pair it with `raft_peer_identity_middleware` (`middleware::from_fn(...)`), which copies the static `x-catga-peer` header (`RAFT_PEER_IDENTITY_HEADER`) into a `RaftPeerIdentity`, so the built-in client and server authenticate out of the box.

Runtime error semantics (since 0.2): **routine caller errors** (proposing without leadership, a full pending-commit queue, checkpointing before the first applied command) are returned to that caller and the owner task keeps running; **terminal failures** (storage errors, state-machine application rejection, fatal transport) stop the owner task. Health surface: `runtime.is_alive()` and `runtime.stop_reason()` (`RaftStopKind` + error detail); `stop_reason() == None` with `!is_alive()` means a graceful shutdown. `is_leader()` is only meaningful while `is_alive()`.

**Per-peer bounded dispatch (PeerDispatcher)**: between the owner task and the transport, the runtime keeps a bounded queue plus a serial dispatch worker per peer, so enqueueing never blocks the owner — the owner advances the Raft logical clock every tick, and awaiting a send inline would let one dead peer's hung send stall the whole clock (including election timeouts), turning a single-node outage into a tens-of-seconds failover; measured leader-stop → re-election dropped from 52s before this fix to about 1s. A full queue fails the send fast as retryable backpressure (the owner reports the peer unreachable so Raft backs off); a fatal worker failure surfaces on the next send and stops the runtime. Shutdown is equally thorough: the `shutdown()` cancellation token cancels even **in-flight blocked** worker sends, releasing workers promptly instead of letting them drag out the stop.

> **Security warning**: the static `x-catga-peer` header is a self-asserted identity — demo/trusted-network only. Production deployments use the mTLS support built into `catga-axum`, where identity comes from the **verified** client certificate, never from headers:
>
> - **Server**: build an acceptor with `MtlsAcceptor::from_pem_files(node_cert_chain, node_key, client_ca_root)` (rustls enforces WebPKI client verification), layer `middleware::from_fn(mtls_peer_identity_middleware)` onto the router holding `raft_message_route` so the verified certificate becomes the `RaftPeerIdentity` authorized by `StaticRaftInboundPolicy`, then `serve_mtls(listener, acceptor, app)`. Connections without a valid client certificate fail the TLS handshake and never reach the route.
> - **Client**: `mtls_reqwest_client(client_cert_chain, client_key, server_ca_root)` yields the `reqwest::Client` handed to `HttpRaftTransport` / `HttpClusterForwarder` unchanged — no transport changes needed.
> - **Tests/dev**: `DevCertificateAuthority::generate()` creates a throwaway CA; `issue_node_identity(trust_domain, node_name)` issues node certificates carrying a SPIFFE SAN URI.
>
> Identity mapping (`peer_identity_from_certificate`): the first SAN URI wins (e.g. `spiffe://cluster/node-2`), then the first DNS SAN, then the subject CN. There is no hot reload — rotating certificates means rebuilding the acceptor and restarting the listener process.

## Write routing (forward / leader-only)

Read leadership state through the `ClusterCoordinator` trait: `is_leader()`, `leader_endpoint()`, `subscribe_leadership()` (provided by `runtime.coordinator()`).

- **Forward to leader**: pipeline behavior `ForwardToLeaderBehavior::new(coordinator, forwarder)` — executes locally on the leader, otherwise forwards via a `ClusterForwarder`; the HTTP implementation is `HttpClusterForwarder::new(reqwest::Client)`. On the leader side mount `leader_forward_route::<M>(mediator)`, endpoint `/api/catga/forward/{Type}`.
- **Leader-only**: `LeaderOnlyBehavior::new(coordinator)` — rejects non-leaders with `Conflict` (HTTP 409); clients retry the leader themselves.
- **Cluster health snapshot**: `cluster_health(&coordinator)` returns a `ClusterHealth` (node id, whether this node is leader, the known leader endpoint, the member count) — a readiness snapshot without locking or polling.

> **Location change (paths unchanged)**: `LeaderOnlyBehavior` / `LeaderOnlyCommand` / `ClusterHealth` / `cluster_health` have been promoted to `catga-core`'s `cluster` module, now generic over the backend-agnostic `ConsensusCoordinator`; `catga-cluster` re-exports them verbatim with `pub use`, so existing `catga_cluster::{LeaderOnlyBehavior, ClusterHealth, ...}` import paths keep working.

## Leader-only tasks

`SingletonTaskRunner::new(coordinator).run(shutdown, task)` runs background work (e.g. periodic `checkpoint()`) only while this node holds leadership; the task is cancelled on leadership loss and can restart automatically.

> **Leadership is an observation, not a distributed lock**: a stale leader is possible under partitions. Fence externally visible leader-owned effects with the Raft term, an application version, or a storage lease, and make them idempotent across retries. `LeadershipSubscription` coalesces intermediate transitions for slow readers — consumers must compare the epoch and resynchronize rather than assume every election was delivered.

## Snapshots and restart

- `runtime.checkpoint()` persists a state-machine snapshot at the latest **successfully applied** command — so at least one applied command is required before a checkpoint has any progress to persist.
- After a restart, `RaftStateMachineDriver::new` loads the snapshot and replays the committed entries after it; no application intervention needed.

## Membership changes (dynamic since 0.2)

Since 0.2, voters can be added and removed online (raft-rs joint consensus):

- `RaftStateMachineRuntime::add_voter(id, endpoint)` / `remove_voter(id)`: call on the leader; change one member at a time and wait for commit. Routine caller errors (not leader, id 0, duplicate member) are returned to the caller and never stop the runtime.
- Membership persists with the log: after a restart, the **persisted member table wins** (bootstrap config is only used for a fresh directory); use `HttpRaftTransport::update_member/remove_member` to keep each node's peer address table in sync.
- Not supported yet (documented limits): learners, leadership transfer, multi-member changes in one step, automatic transport member-table sync (applications observe changes and call update/remove).

## Backend-agnostic consensus abstraction (catga-core)

Since 0.2, `catga-core` ships backend-agnostic consensus contracts (`consensus.rs`) so generic application code — such as a replicated key/value store — depends only on `catga-core` and never names a concrete backend crate:

- `ConsensusStateMachine`: `apply(index, data)` is fed committed entries in strictly increasing index order, exactly once per entry (implementations must be deterministic); plus `snapshot()` / `restore()`. Returning an error from `apply` is terminal for the owning runtime — the backend stops before acknowledging the failed entry so a durable node can recover and replay it on restart.
- `ConsensusCoordinator`: the node's cluster-readiness/leadership view (`node_id()` / `is_leader()` / `leader_endpoint()` / `member_endpoints()`), cheap lock-free snapshots that are only meaningful while the owning runtime is alive.
- `ConsensusRuntime`: a running consensus-group handle — `propose` / `propose_and_wait(data, timeout)` / `add_member(id, endpoint)` / `remove_member(id)` / `applied_index()` / `is_alive()` / `coordinator()` / `shutdown()` / `join()`. Membership changes are applied one at a time — wait until a change is observable before issuing the next; learners, leadership transfer, and multi-member changes in a single step are intentionally outside the contract. `propose_and_wait` resolves with the applied index once the entry is committed and applied by the local state machine (errors: the same routine error as `propose` without leadership; a stopped runtime → `ErrorCode::Unavailable`; deadline expiry → `ErrorCode::Timeout`, with the proposal possibly still applied later). **The contract ships a default implementation** — it polls `applied_index()` every 10ms once `propose` accepts the entry; backends with an applied-notification path override it with a push-based wait (`catga-cluster` does — see the push-based semantics above).

`catga-cluster` performs the bridging in `consensus_bridge.rs`: `CoreStateMachine::new(machine)` adapts an application `RaftStateMachine` to `ConsensusStateMachine` (wrapping the `(index, data)` pair into the `RaftCommittedEntry` the machine expects); `RaftClusterNode` implements `ConsensusCoordinator`; `RaftStateMachineRuntime` implements `ConsensusRuntime` (`add_member`/`remove_member` map to `add_voter`/`remove_voter`, and errors map onto the stable `CatgaError` categories: no leader / full pending-commit queue → retryable `Unavailable`, membership pre-validation rejection → `Conflict`).

> **`propose` is fire-and-forget**: `Ok(())` means the entry was **locally accepted** by the leader, **not** that it was committed or applied. Use the first-class `propose_and_wait(data, timeout)` when you need "return once the write took effect" semantics; otherwise observe durable progress through `applied_index()` — typically combined with an application-level op-id inside the payload (see "Consistency notes" below).

The contract currently has **two backend implementations**: raft-rs over HTTP (this page, `catga-cluster` + `catga-axum`) and sorock multi-Raft over gRPC ([`catga-sorock`](./sorock.md), tonic + redb, zero transport code but no leadership observability). See the [sorock page](./sorock.md#when-to-pick-sorock-vs-the-raft-rs-backend) for a selection matrix.

## Scale and operations guidance

The following findings come from `catga-cluster`'s scale contract tests (`tests/raft_scale.rs`, in-memory in-process clusters):

- **Verified scale**: 10- and 50-voter clusters — converge on one leader, replicate every committed write to all voters, re-elect after losing the leader, refuse writes below quorum, and heal a wiped node rejoining with its old id by replaying the entire log. At 50 voters, the local-accept latency of sequential single-client writes is about 2.4ms (quorum 26/50).
- **Below quorum is safe**: with fewer than quorum survivors, `check_quorum` makes a surviving leader step down and pre-vote blocks a new election — the cluster becomes safely unavailable with **zero commits** (every survivor's applied state stays frozen); no split-brain.
- **Exact-quorum livelock (important)**: with **exactly** quorum nodes alive (e.g. 26 of 50), every election needs a unanimous vote, and pre-vote grants are non-exclusive — concurrent candidates split the vote fatally every term, so the cluster can livelock indefinitely. **Recovery must restore election margin**: rejoin at least 2 nodes (back to quorum+1), or `remove_voter` first to shrink the member table to the surviving set before re-electing; rejoining just one node leaves the cluster stuck at exact quorum.
- **Prefer odd voter counts** (3/5/7): with an odd size a single-node failure still leaves margin, and the same fault tolerance costs one node less than the next even size.

## Kubernetes deployment

The framework runs on Kubernetes with no changes required (reference manifests: `examples/distributed-kv/k8s/kv.yaml` — headless Service, StatefulSet, PVC, PDB, topology spread):

- **Identity and discovery**: the StatefulSet ordinal is the Raft node identity (injected as `POD_NAME` via the downward API); peer addresses use stable headless-Service DNS (`<pod>.<svc>-headless`). distributed-kv shows the full derivation (`POD_NAME`/`KV_CLUSTER_NAME`/`KV_REPLICAS`, zero CLI flags).
- **Storage**: one PVC per pod mounted at `persistentStatePath`; a rescheduled pod recovers from its persisted membership and log.
- **Probes**: `/status` for readiness (API up); map liveness to the runtime health surface (`is_alive()`) — distributed-kv exposes `/healthz` (503 once the owner task stops, so zombie pods get restarted).
- **Scaling**: scale-in calls `remove_voter` first; scale-out adds pods then `add_voter`. PDB `maxUnavailable: 1` (3 nodes) preserves quorum.
- **Graceful shutdown**: container processes must handle SIGTERM (the Kubernetes termination signal); distributed-kv's `shutdown_signal()` shows SIGINT+SIGTERM handling.

## Consistency notes

- **Local reads are not linearizable**: there is no read-index or lease, so a follower reading its local applied state may serve stale data. Route strongly consistent reads to the leader, or run the read through consensus as a command.
- **`propose` semantics**: `propose()` returns when the **local Raft accepts the proposal**, not when it is committed/applied. To confirm a write took effect, embed an op-id in the payload and wait for the read model to observe it (reference: `KvService::put` + `SharedState::wait_applied` in distributed-kv).
- **Transport timeouts**: Raft frame requests must have a timeout (`HttpRaftTransport::with_request_timeout` or a custom client's `.timeout(...)`), otherwise a hung peer can stall the owner task.
