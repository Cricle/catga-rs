# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与语义化版本。

## [0.2.0] - 2026-08-11

本版本用真实三节点分布式集群应用（`examples/distributed-kv`）对框架做了实战验证，修复了两个数据路径级缺陷，并补齐运行时健康面、传输认证、动态成员与 mTLS。

### 破坏性变更（Breaking）

- `RaftNode::propose` 返回类型由 `raft::Result<()>` 改为 `Result<(), RaftNodeError>`，`try_propose` 已合并删除；背压以 `RaftNodeError::PendingCommitCapacity` 明确区分，不再被抹成 `StorageError::Unavailable`。
- `RaftStateMachineRuntimeError` 新增 `Node(RaftNodeError)` 变体。
- 删除 `RaftRuntimeError` 的有损 `Clone` 实现（它会伪造 "cloned error"/`Unavailable`，可能误导重试决策）；终态错误的调用方现在收到 `Stopped`，真实错误经 `join()`/`stop_reason()` 获取。
- `ForwardToLeaderBehavior` 的 `Behavior` 实现新增 `M: Clone` 约束（转发重试需要）。
- 持久化节点的成员表优先于启动配置：重开时使用持久化的 ConfState/成员表，`PersistedConfStateMismatch` 错误已移除；启动配置仅用于全新目录的首次引导。
- 运行时错误策略：常规调用错误（无领导权 propose、pending-commit 队列满、首次应用前 checkpoint、对已移除 voter 的过期响应）只返回给调用方，**不再停止 owner 任务**；终态错误（存储失败、状态机拒绝应用、fatal 传输）仍然停止任务。

### 修复（Fixed）

- **P0：checkpoint 后节点无法重启**。快照索引等于已提交索引时，`refill_committed` 把"游标后无已提交条目"误当硬错误并杀死运行时，只能删数据目录恢复。现在空页视为已追平，`acknowledge_recovered` 会推进提交游标。含确定性回归测试 `raft_restart.rs`。
- **P0：运行时死于常规操作且不可观测**。例如首次应用前调用 `checkpoint`（`NothingApplied`）会永久停止 Raft 循环，进程假活并持续报告过期 `is_leader:true`（脑裂视图）。新增 `is_alive()` / `stop_reason()`（`RaftStopKind`）与 `catga.cluster.runtime.stopped` 指标。
- 消息宏（`catga_request`/`catga_command`/`catga_event`）生成的 `{Name}TypeId` 现在继承输入类型的可见性；`pub`/`pub(crate)` 消息类型不再触发 E0446，可跨模块使用。
- 修复多个在 HEAD 即失败的既有测试：不可能满足的选举等待条件、双节点集群单运行时的选举、领导订阅初始快照语义、Windows 上 `SystemTime` 溢出的时间测试、flow-store SQLite 并发 CAS 测试的读改写竞态（现以 Barrier 保证确定性）、`assert!(true)` 空断言、e2e 文件缺失特性门控与 dev 依赖。
- `catga-redis` 的 `streams-rpc` feature 恢复可编译（移除已删除的 `ErrorCode::Connection` 并修复双重可变借用）。
- 修复 `pubsub_*` 契约测试的信封 QoS（Core NATS 仅 AtMostOnce）。
- 修复 `e2e-podman.sh`（PROJECT_ROOT 路径、podman-compose 容器名、动态端口发现与服务 URL 导出、移除不存在的 `--features e2e`）与 `e2e-scenarios.json` 的无效目标/空转过滤器。
- 删除一批以 `use super::*` 引用私有内部、根本无法编译且违反仓库测试规范的 tests/ 文件；外部契约覆盖由公开 API 测试与 e2e 套件保留。
- **重入 follower 复制停摆（节点假死）**。quorum 丢失、leader 退位后，跟随者上的提案被 raft-rs 转发给退位 leader，其 `step()` 返回的 `ProposalDropped` 被误判为终态错误杀死 owner 任务——进程假活、复制停摆。现将 `ProposalDropped` 归为常规调用错误（与 `StepPeerNotFound` 同级）；回归测试见 `raft_runtime_liveness.rs`。
- **派发 worker 关停泄漏**:`PeerDispatcher` worker 卡在挂起的对端发送时无视关停令牌；现在 `shutdown()` 的取消令牌连在途阻塞发送一并取消，worker 立即释放。
- `catga-redis` 的 `streams_rpc`:`request_to` 此前未纳入端到端预算，预算外发送可永久悬挂；现已纳入预算，杜绝悬挂。
- **宏展开修复 4 处**:memorypack-derive flags 的非法 `impl std::ops::Not for Self` 改为对 newtype 本身的实现、enum 显式判别式（每 variant）即通过门禁；`catga_main` 的 `catga_auto` 路径改为 `::catga_core::auto` 并补上参数列表括号；`catga_handler` 成功展开由永不编译的 `impl impl ...` 改为校验后原样重发 impl 块。每项均有先失败后通过的编译/行为测试。
- **`catga-nats` 的 `claim_due` 部分认领语义**：游标 CAS 重试耗尽时，此前已成功认领的项会随 `Err` 一并丢弃，调用方无法感知已成立的租约；改为已认领项作为部分批次随 `Ok` 返回（与 SQL `SKIP LOCKED` 竞争下的部分批次语义一致），仅在无任何已认领项时报错。含基于内置假 JetStream 的确定性契约测试。
- **sorock `join` 有界排空**:tonic 优雅关停需等所有连接关闭，一条从未完成 HTTP/2 预握手的半开连接（懒连接竞态取消后残留）会让 `SorockNode::join` 永久等待；改为 5s 有界排空后强制 abort 服务端任务并收割，`join` 仍然返回 `Ok`。

### 新增（Added）

- **动态成员**:`RaftStateMachineRuntime::add_voter(id, endpoint)` / `remove_voter(id)`(raft-rs 联合共识，单步单成员）；成员表随日志原子持久化；`HttpRaftTransport::update_member/remove_member` 支持对端地址热更新。暂不支持 learner/领导者转移/单步多成员（文档已注明）。
- **mTLS**:`catga-axum::tls` 模块 —— `serve_mtls`/`MtlsAcceptor::from_pem_files`（服务端，强制客户端证书）、`mtls_peer_identity_middleware`（从已验证证书的 SAN/CN 推导 `RaftPeerIdentity`)、`mtls_reqwest_client`（客户端）、`DevCertificateAuthority`（测试/演示证书）。
- **传输超时**:`HttpRaftTransport::with_request_timeout`，对端挂死转化为可重试背压，不再阻塞 owner 任务。
- **静态身份闭环**:`with_peer_identity` + `raft_peer_identity_middleware` + `RAFT_PEER_IDENTITY_HEADER`，自带客户端/服务端开箱互认（仅限受信网络/演示，生产用 mTLS)。
- **转发重试**:`ForwardToLeaderBehavior::with_retry(max_attempts, delay)`，仅对 `Conflict`/`Transient` 重试（故障转移窗口写入不再直接失败）。
- **进度查询**:`RaftStateMachineRuntime::applied_index()`。
- **示例**:`examples/distributed-kv` —— 三节点 Raft KV 集群（领导者转发、快照、故障转移），含 `--bench-writes` 基准与 Kubernetes 清单（`k8s/kv.yaml`，零参数 POD_NAME 推导、SIGTERM 优雅退出、`/healthz` 活性探针）。
- **文档**:wiki 集群页中英重写为真实 API；新增动态成员与 K8s 部署章节；`distributed-todo` 坏脚手架改为可用的 distributed-kv 部署。
- **catga-sorock（新 crate)**：基于 [sorock](https://crates.io/crates/sorock) 0.12(tonic gRPC + redb）的多点 Raft 共识后端，把单分片 sorock Raft 进程适配到 catga-core 共识契约：`SorockApp`（状态机适配 + 字节流快照协议）、`SorockNode`(gRPC 服务器 + redb 存储，一进程一节点）、`SorockRuntime`(`ConsensusRuntime` 实现，提案经本地客户端由 sorock 内部转发到 leader)、`SorockCoordinator`(`ConsensusCoordinator` 实现）。成员变更对「上一次变更仍在提交」的拒绝做透明重试直至 `request_timeout` 预算耗尽。已知限制（sorock 0.12)：领导权不可观测——`is_leader()` 保守返回 `false`、`leader_endpoint()` 返回 `None`；无 learner/联合共识；快照驻留内存，文件存储节点须保持 `snapshot_interval = 0`。详见 wiki 的 sorock 页。构建需要 `protoc`(CI 已固化 setup-protoc v3.20.3)。
- **后端无关共识抽象（catga-core)**:`ConsensusStateMachine` / `ConsensusRuntime` / `ConsensusCoordinator` 三契约，通用应用代码（如复制型 KV）只依赖 catga-core，不点名具体后端 crate;`propose` 为 fire-and-forget 语义，经 `applied_index()` 观测持久进度。`catga-cluster` 在 `consensus_bridge.rs` 完成桥接：`CoreStateMachine::new(machine)` 把应用 `RaftStateMachine` 适配为 `ConsensusStateMachine`,`RaftClusterNode` 实现 `ConsensusCoordinator`,`RaftStateMachineRuntime` 实现 `ConsensusRuntime`（错误映射到稳定的 `CatgaError` 分类）。
- **`RaftHttpCluster::builder`(catga-axum)**:Raft HTTP 集群节点的引导构建器——按配置打开节点、以带请求超时与对端身份的 `HttpRaftTransport` 启动运行时、把入站 Raft 路由挂在对端身份中间件与静态入站策略之后，内建 `/healthz` 与 `/status` 探针、serve-before-campaign 启动顺序与 SIGINT/SIGTERM 优雅退出。distributed-kv 节点接线由 328 行降到 224 行。
- **distributed-kv 双后端**:`--backend raft|sorock`（环境变量回退 `KV_BACKEND`，默认 `raft`)，应用代码只依赖 catga-core 共识 traits;sorock 分支的 gRPC 共识端口与 HTTP API 端口分离，提案由 sorock 内部转发 leader(无需转发管线）；新增 K8s 清单 `k8s/kv-sorock.yaml`。两后端本地三节点与 kind 集群均全场景实测通过。
- **规模契约测试**(`catga-cluster/tests/raft_scale.rs`，纯内存进程内集群）:10 与 50 投票者的选举收敛、全量复制、leader 丢失重选、低于 quorum 拒绝写入（零提交）、被清空节点以原 id 重入后全量重放追赶；实锤「精确 quorum 选举活锁」(pre-vote 授权非独占，26/50 存活时多候选每任期分票夭折）并给出恢复准则（重入 ≥2 节点回到 quorum+1，或先 `remove_voter` 缩编再重选），运维指引已写入 wiki 集群页。
- **故障注入场景矩阵**(`raft_partitions.rs` + `fault_transport`/`sequence_machine` 夹具）:3+2 网络分区、确定性丢包、慢 follower、故障中写入零丢失、带载成员变更。
- **`propose_and_wait` 一等助手**:`RaftStateMachineRuntime::propose_and_wait(data, timeout)` 推送式等待条目提交并被本地状态机应用后解析为已应用 index(owner 任务经 watch 频道发布 applied 序号，等待方零轮询）；非 leader 仍为 `ProposalDropped` 快速例行错误，运行时停止报 `Stopped`，超时报新增 `Timeout` 变体（提案仍可能稍后置入）。`ConsensusRuntime` 契约同步新增 `propose_and_wait`：默认实现按 10ms 节拍轮询 `applied_index`（后端零改动沿用），集群桥接覆写为推送路径并将 `Timeout` 映射为 `ErrorCode::Timeout`。注：老式 `RaftRuntime` 不跟踪 applied 序号，不挂接该语义。
- **sorock 多分片 API（每节点多 Raft 组）**:`SorockRuntime::attach_shard(shard, machine)`（底层 `SorockNode::attach_shard`）在同一进程内追加独立 Raft 组——每分片独立的日志/选票/成员表/选举/状态机；成员变更携带分片下标（按分片隔离的 voter 集，每分片各自引导）；存储按节点共享一个 redb、按分片分表（`log-{shard}`/`ballot-{shard}`)；分片 runtime 继承故障转移调优（`request_timeout`/`propose_retry`/`failover_watchdog`，看门狗只在本分片强制提升）；生命周期对共享节点引用计数，最后一个分片 runtime 关停才停 gRPC 服务器。
- **sorock 故障转移调优**:`request_timeout` 默认 10s→2s（给客户端可见尝试封顶）;`propose_retry`（默认 10 次 × 200ms 退避，仅重试 `Transient`/`Unavailable`/`Cancelled`，请求 id 去重保证重试安全）让一次 `propose` 撑过选主窗口；可选 `failover_watchdog`（默认关）在死 leader 模式下向存活成员发 `TimeoutNow` 强制提升。实测（2026-08-12,loopback 三节点，经 distributed-kv HTTP 写路径杀 leader)：平衡默认中位 ~3.0s、看门狗 crate 级测试 ~2.1s，取代调优前的 10–18s;phi 检测窗（300ms 硬编码心跳节拍）仍是下限。同期写路径优化消除 follower 提交可见性门控，写 p50 由 456ms 降到 ~9ms（与 raft 后端同量级）。

### 变更（Changed)

- **重复助手收敛进 catga-core**:`CatgaError::transient` 构造、`time` 模块的 unix-millis 助手（`now_unix_millis` 等）、统一退避（resilience)、`hash` 模块的 SHA-256 摘要助手（`sha256_digest`/`sha256_concat_digest`/`sha256_framed_digest`）与 NATS CAS 重试循环助手；nats/redis/robustmq/flow-store 改为复用，删除各自副本。
- **后端无关集群工具件提升**:`LeaderOnlyBehavior`/`LeaderOnlyCommand` 与 `ClusterHealth`/`cluster_health` 迁入 `catga-core` 新 `cluster` 模块，改由 `ConsensusCoordinator` 泛型承载，不再依赖具体后端；`catga-cluster` 以 `pub use` 再导出，既有导入路径不变。
- **示例瘦身**:distributed-kv 移除 CDC 接线（`cdc.rs`、`--nats-url`/`--subscribe` CLI、NATS 发布/订阅、`KvMachine` 事件通道，去掉 `catga-nats` 依赖）,`node.rs`(641 行）按职责拆为 `node/` 模块（`mod.rs` 入口与后端分发、`raft_backend.rs`、`sorock_backend.rs`、`api.rs`、`bench.rs`)，行为不变。
- **flow-store 方言去重**:sqlite/mssql 方言并入 server 宏模式，约 2.2k 行重复收敛。
- **CI 分级**:PR/main 推送只跑快速门禁（fmt/clippy/doc/单元+集成测试，无 Docker 依赖；Docker/外部服务 E2E 默认 `#[ignore]`);E2E 与覆盖率质量门（行/区域覆盖 ≥85%，冲刺目标 90%）只在 `v*` 标签或手动 `workflow_dispatch`(release.yml）跑。全部 CI job 补 `protoc` 支持；发布清单纳入 catga-sorock（可发布 crate 增至 10 个，位于 catga-core 之后）。
- **覆盖率浪潮**（行覆盖，llvm-cov 实测，services up, --include-ignored；覆盖率门禁 85%、冲刺目标 90%）:catga-memorypack-derive 98%、catga-core-macros 97.39%、catga-redis 95.22%、catga-robustmq 93.88%、catga-cluster 93.37%、catga-nats 92.63%、catga-axum 92.62%、catga-sorock 91.68%、catga-flow-store 90.76%(mssql/mysql/postgres 方言套件对实库跑通并计入）；catga-core 收尾中（约 80%，受 proc-macro 计量重叠所限）。

### 性能基线（Measured, loopback)

2026-08-12 复测（Windows 开发机 loopback、三节点、debug 构建，经 distributed-kv HTTP API 单连接顺序写 300 次，3 次取中位）:**raft 后端** ~114 writes/s、mean ≈ 8.7ms、p50 ≈ 8.6ms、p99 ≈ 22ms，杀 leader 故障转移（写恢复）中位 ~3.5s,follower 读回中位 ~170ms;**sorock 后端**（平衡默认）~106 writes/s、mean ≈ 9.4ms、p50 ≈ 9.0ms、p99 ≈ 13.9ms，杀 leader 故障转移中位 ~3.0s（看门狗档 crate 级测试 ~2.1s),follower 读回中位 ~230ms(300ms 心跳节拍下限）。节点 RSS ≈ 18MB;raft 后端进程内重选（leader 停止 → 选出新 leader,`raft_scale.rs` 语境）实测 ≈ 1s（每对端有界派发修复前为 52s);kind 与 k3s 真实集群部署均通过（选举、经 follower 写入、复制、删除 leader Pod 故障转移、PVC 持久化追赶）。

2026-08-12 **release 构建基准**（loopback 三节点、1000 顺序写）:**raft 后端** 125.9 writes/s、p50 5.98ms、p99 24.57ms;**sorock 后端** 118.4 writes/s、p50 8.22ms、p99 10.99ms、杀 leader 故障转移 ~2.1s。较 debug 基线快 10–12%,sorock p99 尾延迟显著改善。criterion 关键项:mediator 吞吐 ~85–90 万 ops/s、单步 flow 315ns、registry 查找 25.9ns、nats/redis 8KB 负载解码 ~17–24µs。

**KV 双后端 kind 实测**（三节点 kind 集群，全场景电池：选举/写入/复制/删除 leader Pod 故障转移/5 分钟浸泡）:raft 后端故障转移 0.45s、基准写 p50 5.36ms @112/s、浸泡 259 写 0 丢失；sorock 后端 loopback 写 p50 ≈ 8.8ms（进程内三节点探针，见 `catga-sorock/tests/write_perf.rs`)、故障转移经 0.2 调优后中位 ~3.0s（平衡默认，见上；调优前为 phi 检测窗主导的 10–18s)、浸泡 185 写 0 丢失。

## [0.1.x] - 此前

0.2.0 之前的状态，见 git 历史。
