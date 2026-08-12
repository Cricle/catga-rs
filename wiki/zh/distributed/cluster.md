# 集群模式

## 概述

`catga-cluster` 提供基于 Raft(raft-rs)的共识与集群协调契约：应用自己实现状态机（`RaftStateMachine`）与传输（`RaftTransport`)，运行时（`RaftStateMachineRuntime`）在单个 Tokio 任务上串行驱动选举、日志复制与命令应用。本 crate 不内置网络监听、不替你选择存储，也不提供分布式锁或分片。

> 可运行参考实现：[`examples/distributed-kv`](../../examples/distributed-kv)（三节点 Raft KV 集群，含领导者转发、快照）。下文所有类型均有 rustdoc，本文只按名称引用，不复述完整签名。

```toml
[dependencies]
catga-cluster = "0.2"
catga-axum = "0.2"   # HTTP Raft 传输 / 转发适配器
```

## 配置

`RaftClusterConfig` 是 serde 配置（camelCase):`nodeId`、`localNodeEndpoint`、`members[{id,endpoint}]`（仅远端成员）、`tickIntervalMs`(10)、`electionTimeoutMs`(150)、`heartbeatIntervalMs`(50)、`persistentStatePath`（缺省为纯内存）。

便捷构造与派生方法：

- `RaftClusterConfig::local(node_id, total_nodes, base_port)` —— 本地开发集群；`node_id` 为**零基**进程序号（内部转为非零 Raft 成员 id)，成员端点生成 `http://localhost:{base_port + id}`，持久化目录 `./raft-state-node{id}`。
- `.members()` → `Vec<RaftMember>`（含本节点的完整投票者列表）。
- `.raft_timing()` → 校验过的 `RaftTiming`(tick/选举/心跳）。
- `.open_node()` → 按 `persistentStatePath` 打开持久化（raft-engine）或内存 `RaftNode`。

不用配置时也可手工构造：`RaftNode::new` / `new_with_timing` / `open_persistent(id, endpoint, members, dir)`，成员用 `RaftMember::new(id, endpoint)`。

## 引导构建器（推荐）

0.2 起，`catga-axum` 的 `RaftHttpCluster::builder` 是构建 Raft HTTP 集群节点的**推荐方式**。它把每个 HTTP 托管 Raft 应用都要手写一遍的通用接线一次性收敛：按配置打开节点、用带请求超时与对端身份的 `HttpRaftTransport` 启动运行时、把入箱 Raft 路由挂在对端身份中间件与静态入站策略之后，并暴露 `/healthz` 与 `/status` 探针。distributed-kv 的节点接线因此从 328 行降到 224 行。

```rust
let cluster = RaftHttpCluster::builder(config)
    .state_machine(machine)
    .with_request_timeout(Duration::from_secs(2))  // 可选，默认 DEFAULT_RAFT_HTTP_REQUEST_TIMEOUT = 2s
    .with_peer_naming(|id| format!("node-{id}"))   // 可选，默认 "node-{id}"
    .with_client(reqwest_client)                   // 可选，mTLS 部署传 mtls_reqwest_client 的产物
    .build()?;

let listener = TcpListener::bind("0.0.0.0:9100").await?;
cluster.serve(listener, app_routes).await?;        // 先监听、后选举；SIGINT/SIGTERM 优雅退出
```

- **启动顺序内建**:`serve()` 先绑定并开始接受 HTTP 连接，再发起选举（serve-before-campaign)，杜绝「选举卡在对端连接上」的启动死锁；选举失败会中止刚起的服务并返回错误。
- **探针内建**:`/healthz`(`RAFT_HTTP_HEALTH_PATH`）在 owner 任务存活时返回 200、停止后返回 503(liveness);`/status`(`RAFT_HTTP_STATUS_PATH`）返回节点 id、领导视图、leader 端点、存活状态与最新已应用 index 的 JSON(readiness)。
- **句面**:`cluster.runtime()`（提案 / `applied_index` / checkpoint)、`cluster.coordinator()`（无锁领导视图）、`cluster.router()`（框架路由克隆，自行 serve 时需保持「先监听后 `campaign()`」的顺序）、`serve_until(listener, routes, shutdown)`（自定义关停信号）。
- **边界**：拓扑发现不在构建器职责内——它消费一份已解析的 `RaftClusterConfig`，不读环境变量；应用自己的路由、mediator 管线与领导者职责（如 `ForwardToLeaderBehavior` + `HttpClusterForwarder`）照旧组合进 `serve` 的路由参数。

## 手动接线（高级）

需要完全掌控生命周期的应用仍可手工接线（这也是构建器内部做的事）:

```rust
let config = RaftClusterConfig::local(node, nodes, base_port)?;
let node = config.open_node()?;
let driver = RaftStateMachineDriver::new(node, machine)?;      // 恢复快照 + 已提交后缀
let runtime = RaftStateMachineRuntime::spawn(driver, transport, timing.tick_interval())?;

// 1. 先绑定并 serve HTTP 监听（raft_message_route 注入 runtime.inbox()）
// 2. 再发起选举
runtime.campaign().await?;
```

**启动顺序很重要**:Raft owner 任务会 await 传输发送，必须先绑定并开始服务 HTTP 监听（使对端的 `raft_message_route` 可达），再调用 `campaign()`，否则选举/心跳会卡在对端连接上。

运行时方法：`campaign()`、`propose(Vec<u8>)`、`propose_and_wait(data, timeout)`、`checkpoint()`、`coordinator()`、`inbox()`（供网络接收端注入 `RaftMessage`)、`shutdown()`、`join()`。

**`propose_and_wait(data, timeout)`**（推送式等待已应用）：与 fire-and-forget 的 `propose` 不同，它在条目被本地状态机**提交并应用**后才解析，返回已应用 index（不低于该提案分配的槽位，解析时其效果已在状态机可见）——不做轮询，直接订阅 owner 任务的已应用 index 发布。错误面：非领导者快速失败（常规调用错误，含 `raft::Error::ProposalDropped`，同 `propose`);owner 任务在等待中停止返回 `RaftStateMachineRuntimeError::Stopped`;`timeout` 先到返回 `RaftStateMachineRuntimeError::Timeout`（提案仍可能随后提交应用）。若等待期间领导权易主，该日志槽位可能被别的条目覆盖，解析到的 index 反映的是槽位而不一定是本次提案——需要 exactly-once 的调用方在负载内嵌应用级 op-id 并对状态机校验。

## 状态机

实现 `RaftStateMachine` trait:

- `apply(&mut self, entry: &RaftCommittedEntry)` —— 应用一条已提交日志（`entry.index` / `entry.data`)。
- `snapshot()` / `restore()` —— 快照序列化与恢复。

`RaftStateMachineDriver::new(node, machine)` 在启动时自动恢复最近快照并重放已提交后缀；所有应用调用都被运行时串行化，状态机内部无需加锁（跨线程共享读模型仍需自行同步，见 distributed-kv 的 `SharedState`)。

## HTTP 传输与认证

传输由 `RaftTransport` trait 抽象：`async fn send(&self, RaftMessage) -> RaftTransportResult`。错误语义：

- `RaftTransportError::retryable` —— 对端暂时不可用（超时/连接失败/5xx/429)，运行时存活并重试；
- `RaftTransportError::fatal` —— 不可恢复错误，运行时停止。

`catga-axum` 提供 HTTP 适配器：

- 客户端 `HttpRaftTransport::new(reqwest::Client, members)`，向成员端点 POST protobuf 帧（路径 `RAFT_MESSAGE_PATH`)。建议链式配置 `.with_request_timeout(Duration)`（防对端挂起拖死 owner 任务）与 `.with_peer_identity("node-1")`（携带节点身份头）。
- 服务端 `raft_message_route(runtime.inbox(), StaticRaftInboundPolicy::new(local_id, [(peer_id, "identity")]))`，按「经过认证的 peers 身份 + 帧的 from/to」鉴权后再入箱；配合 `raft_peer_identity_middleware`（`middleware::from_fn(...)`）把静态请求头 `x-catga-peer`(`RAFT_PEER_IDENTITY_HEADER`）复制为 `RaftPeerIdentity`，自带客户端/服务端即可开箱互认。

运行时错误语义（0.2 起）：**常规调用错误**（无领导权时 propose、pending-commit 队列满背压、首次应用前 checkpoint）只返回给调用方，owner 任务继续运行；**终态错误**（存储失败、状态机拒绝应用、fatal 传输）停止 owner 任务。健康面：`runtime.is_alive()` 与 `runtime.stop_reason()`（`RaftStopKind` + 错误详情）；`stop_reason()==None` 且 `!is_alive()` 表示优雅停机。`is_leader()` 仅在 `is_alive()` 时有意义。

**每对端有界派发（PeerDispatcher)**：运行时在 owner 任务与传输之间为每个对端维护一条有界队列 + 串行派发 worker，入队永不阻塞 owner——owner 任务每 tick 推进 Raft 逻辑时钟，若 inline await 发送，一个死对端的挂起发送会拖住整个时钟（含选举超时），把单节点故障放大成数十秒的故障转移；实测 leader 停止 → 重选从修复前的 52s 降到约 1s。队满时发送快速失败为 retryable 背压（owner 据此向 Raft 报告对端不可达以触发退避）;fatal worker 失败在下一次发送时上报，运行时停止。关停同样彻底：`shutdown()` 的取消令牌连**在途阻塞的** worker 发送也一并取消，worker 立即释放，不会拖着运行时拖延停机。

> **安全警告**：静态 `x-catga-peer` 头是自封身份，仅用于演示或受信网络。生产环境使用 `catga-axum` 内置的 mTLS 支持，身份取自**已验证**的客户端证书而非任何请求头：
>
> - **服务端**：`MtlsAcceptor::from_pem_files(节点证书链, 节点私钥, 客户端CA根)`（rustls 强制 WebPKI 校验客户端证书）；给挂有 `raft_message_route` 的路由叠加 `middleware::from_fn(mtls_peer_identity_middleware)`，把已验证证书转成 `RaftPeerIdentity` 供 `StaticRaftInboundPolicy` 鉴权；再 `serve_mtls(listener, acceptor, app)` 起服务。无有效客户端证书的连接在 TLS 握手即失败，不会到达路由。
> - **客户端**：`mtls_reqwest_client(客户端证书链, 客户端私钥, 服务端CA根)` 直接产出 `reqwest::Client`，照旧传给 `HttpRaftTransport` / `HttpClusterForwarder`，传输层零改动。
> - **测试/本地**：`DevCertificateAuthority::generate()` 生成一次性 CA,`issue_node_identity(trust_domain, node_name)` 签发带 SPIFFE SAN URI 的节点证书。
>
> 身份映射（`peer_identity_from_certificate`)：优先 SAN URI（如 `spiffe://cluster/node-2`)，其次 DNS SAN，最后 subject CN。暂不支持热加载：轮换证书需重建 acceptor 并重启监听进程。

## 写路由（转发 / 仅领导者）

读取领导状态用 `ClusterCoordinator` trait:`is_leader()`、`leader_endpoint()`、`subscribe_leadership()`（由 `runtime.coordinator()` 提供）。

- **转发到领导者**：管线行为 `ForwardToLeaderBehavior::new(coordinator, forwarder)` —— 本节点是领导者则本地执行，否则经 `ClusterForwarder` 转发；HTTP 实现为 `HttpClusterForwarder::new(reqwest::Client)`。领导者侧挂 `leader_forward_route::<M>(mediator)`，端点 `/api/catga/forward/{Type}`。
- **仅领导者**:`LeaderOnlyBehavior::new(coordinator)` —— 非领导者直接以 `Conflict`(HTTP 409）拒绝，由客户端自行重试领导者。
- **集群健康快照**:`cluster_health(&coordinator)` 返回 `ClusterHealth`（节点 id、是否领导者、已知领导者端点、成员数）——不加锁、不轮询的就绪快照。

> **所在地变更（路径不变）**:`LeaderOnlyBehavior` / `LeaderOnlyCommand` / `ClusterHealth` / `cluster_health` 已提升到 `catga-core` 的 `cluster` 模块，改由后端无关的 `ConsensusCoordinator` 泛型承载；`catga-cluster` 以 `pub use` 原样再导出，既有 `catga_cluster::{LeaderOnlyBehavior, ClusterHealth, ...}` 导入路径不变。

## 领导者专属任务

`SingletonTaskRunner::new(coordinator).run(shutdown, task)`：仅在本节点持有领导权期间运行后台任务（如定期 `checkpoint()`)，失去领导权即取消，可自动重启。

> **领导权是观测而非分布式锁**：网络分区下可能出现过期领导者。对外的领导者专属副作用必须用 Raft term、应用版本或存储租约做围栏（fencing)，并保证幂等；`LeadershipSubscription` 会合并慢读者的中间变迁，消费者应比较 epoch 并重新同步，而不是假设每次选举都被送达。

## 快照与重启

- `runtime.checkpoint()` 在最近一条**已成功应用**的命令处持久化状态机快照 —— 因此集群中至少要有一条已应用命令，否则没有可快照的进度。
- 重启后 `RaftStateMachineDriver::new` 自动加载快照并重放其后的已提交日志，应用层无需干预。

## 成员变更（0.2 起：动态成员）

0.2 起支持在线增删投票者（raft-rs 联合共识）:

- `RaftStateMachineRuntime::add_voter(id, endpoint)` / `remove_voter(id)`：在领导者上调用，每次只变更一个成员并等待提交；非常规错误（非领导者、id 为 0、重复成员）只返回给调用方，运行时不受影响。
- 成员关系随日志持久化：重启后**以持久化的成员表为准**（启动配置只用于全新目录的首次引导）;`HttpRaftTransport::update_member/remove_member` 用于同步各节点的对端地址表。
- 暂不支持（文档化限制）:learner、领导者转移、单步多成员变更、传输层成员表自动同步（需应用监听后调用 update/remove)。

## 后端无关共识抽象（catga-core）

0.2 起，`catga-core` 提供后端无关的共识契约（`consensus.rs`)，让通用应用代码（如复制型 KV）只依赖 `catga-core`，不点名任何具体后端 crate:

- `ConsensusStateMachine`:`apply(index, data)` 按严格递增 index、每条已提交日志恰好一次地喂给状态机（实现必须确定）；另有 `snapshot()` / `restore()`。`apply` 返回错误对所属运行时是终态的——后端在确认该条目前停止，以便持久化节点重启后重放。
- `ConsensusCoordinator`：节点的集群就绪/领导视图（`node_id()` / `is_leader()` / `leader_endpoint()` / `member_endpoints()`)，廉价无锁快照，仅在所属运行时存活时有意义。
- `ConsensusRuntime`：运行中共识组句柄——`propose` / `propose_and_wait(data, timeout)` / `add_member(id, endpoint)` / `remove_member(id)` / `applied_index()` / `is_alive()` / `coordinator()` / `shutdown()` / `join()`。成员变更每次一个、等待可观测后再发下一个；learner、领导者转移、单步多成员变更刻意不在契约内。`propose_and_wait` 在条目提交并被本地状态机应用后解析为已应用 index（错误：无领导权与 `propose` 相同的常规错误；运行时已停止 → `ErrorCode::Unavailable`;`timeout` 到期 → `ErrorCode::Timeout`，提案仍可能随后应用）。**契约带默认实现**——`propose` 接受后每 10ms 轮询一次 `applied_index()`；有应用内通知路径的后端会覆盖为推送式等待（`catga-cluster` 即覆盖，见上文的推送式语义）。

`catga-cluster` 在 `consensus_bridge.rs` 中完成桥接：`CoreStateMachine::new(machine)` 把应用 `RaftStateMachine` 适配为 `ConsensusStateMachine`（把 `(index, data)` 包成 `RaftCommittedEntry`);`RaftClusterNode` 实现 `ConsensusCoordinator`;`RaftStateMachineRuntime` 实现 `ConsensusRuntime`(`add_member`/`remove_member` 映射到 `add_voter`/`remove_voter`，错误映射到稳定的 `CatgaError` 分类：无领导者 / pending-commit 满 → `Unavailable` 可重试，成员预校验拒绝 → `Conflict`)。

> **`propose` 是 fire-and-forget**:`Ok(())` 只表示提案被领导者**本地接受**，不代表已提交或已应用。需要「写生效后再返回」的语义时用一等的 `propose_and_wait(data, timeout)`；或通过 `applied_index()` 观测持久进度——通常配合负载内的应用级 op-id（见下文「一致性注意点」)。

该契约目前有**两个后端实现**:raft-rs over HTTP（本页，`catga-cluster` + `catga-axum`）与 sorock 多 Raft over gRPC([`catga-sorock`](./sorock.md),tonic + redb，零传输代码但领导权不可观测）。选型对照见 [sorock 页](./sorock.md#何时选-sorock何时选-raft-rs-后端)。

## 规模与运维准则

以下结论来自 `catga-cluster` 的规模契约测试（`tests/raft_scale.rs`，纯内存进程内集群）:

- **已验证规模**:10 与 50 投票者集群——收敛单一领导者、每条已提交写复制到全部投票者、leader 丢失后重选、低于法定人数拒绝写入、被清空（wiped）的节点以原 id 重入后重放整个日志追赶。50 投票者下顺序单客户端写的本地接受延迟约 2.4ms(quorum 26/50)。
- **低于法定人数是安全的**：存活者不足 quorum 时，`check_quorum` 让存活 leader 退位、pre-vote 阻止新选举——集群变为安全不可用，**零提交**（各存活者的已应用状态冻结），不会脑裂。
- **精确 quorum 存活性死锁（重要）**：当**恰好** quorum 个节点存活（如 50 中活 26)，每次选举都需要全票通过，而 pre-vote 授予是非独占的——多个并发候选者每个 term 都会致命地分裂选票，集群可能持续 livelock。**恢复必须恢复选举余量**：至少重入 2 个节点（回到 quorum+1)，或先 `remove_voter` 把成员表缩到存活规模再重选；只重入 1 个节点会卡在精确 quorum 上。
- **建议奇数投票者**(3/5/7)：奇数规模下单节点故障仍保留余量，且同样的容错能力比相邻偶数规模少一个节点。

## Kubernetes 部署

框架无需任何修改即可跑在 K8s 上（参考清单：`examples/distributed-kv/k8s/kv.yaml`，含 headless Service、StatefulSet、PVC、PDB、拓扑分散）:

- **身份与发现**:StatefulSet 序号即 Raft 节点身份（downward API 注入 `POD_NAME`)，对端地址用 headless Service 的稳定 DNS(`<pod>.<svc>-headless`);distributed-kv 展示了完整派生逻辑（`POD_NAME`/`KV_CLUSTER_NAME`/`KV_REPLICAS`，零 CLI 参数）。
- **存储**：每 Pod 一个 PVC 挂在 `persistentStatePath`;Pod 重建后按持久化成员表与日志恢复。
- **探针**:`/status` 做 readiness(API 就绪）;liveness 应映射运行时健康面（`is_alive()`),distributed-kv 提供 `/healthz`(owner 任务死亡返回 503，僵尸 Pod 会被重启）。
- **扩缩容**：缩容先 `remove_voter` 再减副本；扩容加副本后 `add_voter`。PDB `maxUnavailable: 1`(3 节点）保法定人数。
- **优雅退出**：容器内进程必须处理 SIGTERM(K8s 终止信号）;distributed-kv 的 `shutdown_signal()` 展示了 SIGINT+SIGTERM 双监听。

## 一致性注意点

- **本地读非线性化**：没有 read-index 或租约，跟随者直接读本地已应用状态可能返回过期数据。强一致读应路由到领导者，或把读作为命令走一遍共识。
- **`propose` 的语义**:`propose()` 在**本地 Raft 接受提案**时返回，而不是提交/应用时。需要确认写入生效的模式：在负载中嵌入 op-id，然后等待读模型观测到该 op-id（参考实现：distributed-kv 的 `KvService::put` + `SharedState::wait_applied`)。
- **传输超时**：Raft 帧请求必须有超时（`HttpRaftTransport::with_request_timeout` 或自建 client 的 `.timeout(...)`)，否则 owner 任务会被对端挂起拖住。
