# sorock 多 Raft 后端

## catga-cluster 合并

catga-cluster (raft-rs over HTTP) 已合并到 catga-sorock。
所有功能现在由 sorock 提供，不再需要单独的 Raft HTTP 传输。

## 概述

`catga-sorock` 是基于 [sorock](https://crates.io/crates/sorock) 0.12 的多点 Raft 共识后端：把一个 sorock 分片（shard）的 Raft 进程适配到 `catga-core` 的后端无关共识契约（`ConsensusStateMachine` / `ConsensusRuntime` / `ConsensusCoordinator`，见[集群模式](./distributed.md#后端无关共识抽象catga-core)）。sorock 自带 gRPC 传输（tonic）与 redb 存储，应用不需要写任何传输代码。

```toml
[dependencies]
catga-sorock = "0.2"
```

> 构建需要 `protoc`(sorock 的构建脚本编译 raft 服务的 protobuf 定义）。可运行参考：[`examples/distributed-kv`](../../examples/distributed-kv) 的 `--backend sorock` 分支。

## 类型地图

- `SorockNodeConfig` —— 单节点配置，`SorockNodeConfig::new(node_id, bind_addr)` 起步，公开字段直接改。
- `SorockNode` —— 一个运行中的节点：gRPC 服务器 + sorock `RaftNode` + redb 存储。
- `SorockRuntime` —— `ConsensusRuntime` 实现：提案、成员变更、进度、生命周期，经连到**本地**节点的 gRPC 客户端发出，sorock 内部转发到当前 leader（因此在 leader 与 follower 上行为一致）。
- `SorockCoordinator` —— `ConsensusCoordinator` 实现：本地成员视图与（受限的）领导视图，见下文「领导权不可观测」。
- `SorockApp` —— 把应用 `ConsensusStateMachine` 适配为 sorock 的 `RaftApp` trait（含字节流快照协议）；一般只经 `SorockRuntime::start` 间接构造。

## 配置

`SorockNodeConfig::new(node_id, bind_addr)` 的默认值：shard 0、内存存储、快照关闭、`request_timeout = 2s`(`DEFAULT_REQUEST_TIMEOUT`,0.2 从 10s 调低，见「故障转移行为与调优」)、`propose_retry = { max_attempts: 10, backoff: 200ms }`、`failover_watchdog = false`。字段：

- `node_id` —— 稳定节点标识，也用作提案请求 id 的前缀（sorock 按请求 id 去重）。
- `bind_addr` —— gRPC 监听地址；端口 `0` 让 OS 分配，生效地址经 `SorockNode::local_addr()` 读取。
- `public_uri` —— 向对端宣告的 URI（含 scheme，如 `http://raft-1.internal:7000`)；缺省为 `http://{绑定地址}`，未指定 IP 替换为 `127.0.0.1`。**这是 `add_member` 里使用的 server id**。
- `members` —— 启动时已知的成员端点；只播种 coordinator 的成员视图，真实成员关系由 `add_member` 建立。
- `shard` —— 主分片：本节点第一个 Raft 进程附着的分片；同组所有节点必须一致（默认 `DEFAULT_SHARD_INDEX = 0`)；更多分片经 `SorockRuntime::attach_shard` 追加，见「多分片模型」。
- `storage` —— `SorockStorage::InMemory`（默认，易失）或 `SorockStorage::RedbFile(path)`(redb 文件，缺失时创建）。
- `snapshot_interval` —— 每应用多少条用户日志做一次应用驱动快照；`0`（默认）完全关闭。**文件存储节点保持为 0**，原因见「快照模型」。
- `request_timeout` —— 本地客户端每次 gRPC 调用的截止，**也是一次 `propose` 调用（含其内部重试）的总预算**;sorock 客户端 API 没有内建超时，无 leader 时的写会无限等待，因此必须有一层预算。
- `propose_retry`(`SorockProposeRetry`)—— `propose` 对选主后通常自愈的失败的重试策略：恰好重试 `Transient` / `Unavailable` / `Cancelled` 三类（leader 瞬态），其余分类保持单发；同一提案携带相同请求 id,sorock 按 id 去重，重试不会重复应用。`max_attempts = 1` 即单发。退避会被钳制在剩余的 `request_timeout` 预算内，一次 `propose` 不会超出截止超过一个在途尝试。
- `failover_watchdog` —— 故障转移看门狗（默认关）:`propose` 尝试以死 leader 模式失败时，向存活成员发送 sorock 的 `TimeoutNow` RPC 强制提升一个存活者，把选主从 phi 检测窗口压缩到约两个往返。**注意**：触发无法区分「leader 死了」与「leader 慢/被分区」——对被分区但仍活着的 leader 开火会多推一个 term(sorock 的 pre-vote 把破坏限制在一次 term 跳变）；不能容忍这种扰动的客户端保持关闭。

## 引导顺序（重要）

sorock 命令式建组，没有静态成员表：

1. 所有节点用 `SorockRuntime::start(config, machine)` 启动（必须在 Tokio 运行时内调用，sorock 用 `tokio::spawn` 起 Raft 线程）。此阶段各节点只服务自己的分片，互不知晓。
2. 在**第一个节点自己的 runtime** 上，以**第一个节点自己的宣告 URI** 调用 `add_member`。空成员表上的自添加被 sorock 视为单节点引导，立即自选为 leader。
3. 其余节点逐一手动 `add_member(id, uri)`，串行发出。成员变更一次只提交一个：等上一个变更可观测（例如经 coordinator 的成员视图）再发下一个。

引导后任何节点都可发起成员变更，sorock 内部转发给当前 leader。

```rust
let runtime = SorockRuntime::start(config, machine).await?;
// 引导节点（node 1）上：
runtime.add_member(1, runtime.advertised_uri().to_owned()).await?; // 自添加 = 引导
runtime.add_member(2, "http://node-2:7000".into()).await?;
runtime.add_member(3, "http://node-3:7000".into()).await?;
```

## 成员变更重试语义

sorock 一次只接受一个成员变更：上一个变更（含引导配置）仍在提交时，新变更被拒绝，客户端观察到流被丢弃（`Cancelled`/`Unknown`，无 leader 时为 `Unavailable`)。`SorockRuntime` 对这三类拒绝按 100ms 间隔透明重试，直到 `request_timeout` 预算耗尽——成员变更是幂等的（添加已存在的 server 与移除不存在的 server 收敛到同一 voter 集），因此重试安全，调用方可以背靠背发变更。

`remove_member(id)` 需要成员端点，而 trait 只传数值 id:runtime 经本地注册表（本 runtime 此前的 `add_member` 调用）解析 id；移除从未经本 runtime 添加的 id 以 `ErrorCode::NotFound` 失败。

## 多分片模型

sorock 0.12 是真正的 multi-Raft 引擎：一个节点进程可以托管多个互相独立的 Raft 组（分片）。本 crate 通过 `SorockRuntime::attach_shard(shard, machine)`（以及更底层的 `SorockNode::attach_shard(shard, SorockApp)`）暴露该能力；只用 `SorockRuntime::start` 则保持单分片，行为与此前完全一致。

- **每个分片是一个独立的 Raft 组**：自己的日志、选票、成员表、选举与状态机。一个分片上的写永远不会在另一个分片可见，每个分片各自选举、各自故障转移。
- **成员关系按分片隔离**:`add_member` / `remove_member` 携带该 runtime 的分片下标（sorock 的 `AddServer`/`RemoveServer` RPC 带 `shard_index`)，同一进程的不同分片可以有不同的 voter 集。上文的引导顺序对每个分片独立适用：每个分片都要用该分片 runtime 上的自添加来引导。
- **存储按节点共享、按分片命名空间隔离**：每个分片的日志与选票放在节点单个 redb 数据库的专属表（`log-{shard}` / `ballot-{shard}`）里，`RedbFile` 节点用一个文件装下所有分片。
- **coordinator 视图按 runtime（即按分片）**：每个分片 runtime 只跟踪经它添加的成员。
- **故障转移调优按分片继承**:`attach_shard` 产出的 runtime 继承原 runtime 的 `request_timeout` / `propose_retry` / `failover_watchdog`；看门狗的 `TimeoutNow` RPC 携带分片下标，只在该分片内强制提升。
- **生命周期对共享节点引用计数**：关停、join 或 drop 一个分片 runtime 只摘取该分片的 Raft 进程；gRPC 服务器继续服务其余分片，直到最后一个分片 runtime 关停才停止。重复 attach 同一分片以 `ErrorCode::Validation` 失败；`SorockNode::detach_shard(shard)` 摘除分片（日志与选票留在存储里，之后可同下标 re-attach 恢复）,`attached_shards()` 列出当前分片。

```rust
let runtime = SorockRuntime::start(config, machine_a).await?;      // 主分片（config.shard）
let shard1 = runtime.attach_shard(1, machine_b).await?;            // 同进程第二个 Raft 组
// 每个分片各自引导：在自己的 runtime 上自添加，再逐一加对端
shard1.add_member(self_id, runtime.advertised_uri().to_owned()).await?;
```

## 快照模型

sorock 0.12 从不要求应用*主动*做快照；而是应用经 `get_latest_snapshot` 宣告最新快照，sorock 把它折进日志。`SorockApp` 因此每 `snapshot_interval` 条已应用日志把状态机快照进**内存**快照库，以 256KiB 分块流给 follower，并在 `install_snapshot` 时恢复状态机。日志索引 1 是 sorock 为每个新日志预置的隐式创世快照，不携带应用字节。

> **文件存储节点保持 `snapshot_interval = 0`**：快照驻留内存，重启后若日志已被压缩过内存快照的位置，节点无法恢复快照。distributed-kv 的 sorock 分支即保持默认关闭。

## 领导权不可观测（已知限制）

sorock 0.12 **没有公开的领导权查询 API**：选举状态与选票在 Raft 进程内部，轮询节点状态所需的 gRPC 请求类型未导出。因此：

- `SorockCoordinator::is_leader()` **始终返回 `false`**,`leader_endpoint()` **始终返回 `None`**——两者都是保守的，绝不虚报领导权；
- distributed-kv sorock 分支的 `/status` 相应返回 `"is_leader": false`、`"leader_endpoint": null`（节点 id、成员视图、存活状态与 `applied_index` 正常）;
- 需要「仅领导者」语义的应用必须**外部围栏**（分布式锁、存储租约或以提案成功与否判定），不能依赖 coordinator 的领导视图。

`member_endpoints()` 返回本节点*本地已知*的端点：配置种子成员 + 经本 runtime 增删的端点；不是对集群的实时查询，可能滞后于其他节点发起的成员变更。

其他 sorock 0.12 限制：无 learner、无联合共识——成员变更在单条日志里替换整个 voter 集，全新节点必须先经日志复制（或快照）追平才能投票。

## 写入语义

- sorock 的写 RPC 在条目于 leader 上**提交并应用**后才返回，因此这里的 `propose` 强于契约的最低 fire-and-forget 语义。
- 每次写携带单调递增、节点唯一的请求 id(`{node_id}-{seq}`),sorock 按之去重：同 id 的重试提案至多应用一次。
- `ConsensusStateMachine` 没有读路径：`process_read` 不支持，读请求返回空负载、不触碰状态机。需要线性一致读请在应用层自行实现。

## 故障转移行为与调优

leader 死亡时，sorock 自身的机制其实很快:phi 累积故障检测器（阈值 12.0，对 300ms 心跳）加选举节拍约 1.5–3s 选出新 leader——这是**依赖内建的下限**(sorock 0.12 的心跳节拍硬编码为 300ms，不可配置）。此前实测 10–18s 的大头是**客户端侧**:follower 会把写继续转发给已死的 leader(`ballot.voted_for`)，转发的写要等传输层放弃（sorock 内建 h2 pong 看门狗默认 20s）或客户端截止才解析。0.2 的调优把客户端侧收紧：

- `request_timeout` 默认 **10s → 2s**，给每次客户端可见的尝试封顶；
- `propose_retry` 在该预算内重试 leader 瞬态失败（默认 10 次 × 200ms 退避），一次 `propose` 调用撑过整个选主窗口而不是立刻失败；
- `failover_watchdog`（可选，默认关）在死 leader 模式下向存活成员发 `TimeoutNow` 强制提升，把选主压缩到约两个往返。

**实测**(2026-08-12,Windows 开发机 loopback，三节点，debug 构建；经 distributed-kv 的 HTTP 写路径杀 leader，以「写重新成功」计时，3 次取中位）:

| 配置 | 故障转移（杀 leader → 写恢复） | 说明 |
| --- | --- | --- |
| sorock 平衡默认 | **中位 ~3.0s**（三次：2.9 / 3.0 / 9.2s) | 首个杀后尝试通常挂到 2s 客户端截止（转发到死 leader 的流在超时才解析），随后重试撑过 phi 窗口落地 |
| sorock + 看门狗 | ~2.1s(crate 级测试） | 约一个请求预算加一次选举往返；distributed-kv 示例未暴露该开关，此数字来自 `catga-sorock/tests/failover_tuning.rs`(runtime 级 loopback，同文件平衡档 ~4.2s) |
| raft 后端对照 | 中位 ~3.5s（三次：3.1 / 3.5 / 6.2s) | 同一示例 raft 分支，经 follower HTTP 转发重试 |

> **持续写压下的选举活锁（实测观察）**：上述 sorock 4 次测量中有 1 次在杀 leader 后**超过 4 分钟**未能选主——两个同时启动的存活者选举节拍同相，各自 `try_promote` 全程持有 sorock 0.12 的 `vote_sequencer`（容量 1)，使对方的投票 RPC 在 `try_acquire` 处失败（服务端处理器以 "no permits available" panic)，pre-vote 反复互相饿死；随机化选举睡眠（0–900ms）最终会让周期错相恢复，但窗口可能很长。**运维建议**：故障转移窗口内客户端应带退避重试而不是密集重试；对选主延迟敏感的部署开启 `failover_watchdog`。

> **幽灵心跳（运维红线）**:sorock 0.12 的每对端心跳线程（在节点启动时的 Tokio runtime 上 `tokio::spawn` 的任务）经 Arc 环（Voter→Peers→peer_threads）存活——`detach_process`/drop Raft 进程**不会**停掉它们，已被「摘除」的节点会继续发心跳，从而阻塞选举（对端把死 leader 当活的）。**可靠的节点退役只有进程退出**（或连整个 Tokio runtime 一起拆，如测试里的 `shutdown_background`);`SorockRuntime::shutdown` + `join` 停 gRPC 服务器并摘取分片，但被摘分片泄漏的心跳任务在同一 Tokio runtime 内仍可能存活。运维上把 sorock 节点的上下线当作进程级事件；在存活进程里仅仅 drop runtime/节点必然留下幽灵心跳。

## 性能特性

- **写延迟**(2026-08-12 复测，同上 loopback 三节点、debug 构建，经 distributed-kv HTTP API 300 次顺序写）:**中位吞吐 ~106 writes/s,p50 ≈ 9.0ms,p99 ≈ 13.9ms**——与 raft 后端同量级（~114 writes/s,p50 ≈ 8.6ms)。crate 级探针（`catga-sorock/tests/write_perf.rs`,`--ignored` 手动运行）p50 ≈ 8.8ms。0.2 之前的 456ms 写延迟根因是 follower 提交可见性受心跳节拍门控，已在写路径优化中消除。
- **follower 读回滞后**：写经 leader 应答后，follower 要等领导者的提交 index 随心跳到达才应用——实测经 follower GET 读回中位 ~230ms(214–300ms);sorock 0.12 心跳节拍 300ms 硬编码（每 follower 心跳队列 300ms、多路复用器 300ms 排空、提交/应用线程 100ms 兜底轮询），这是依赖的下限，不是本适配器的。需要 read-your-writes 的应用把读钉在提案节点上。
- **吞吐定位**：适合正确性/简洁性优先、而非极致写延迟的场景。

## 何时选 sorock，何时选 raft-rs 后端

| 维度 | `catga-cluster`(raft-rs over HTTP) | `catga-sorock`(sorock 0.12 over gRPC) |
| --- | --- | --- |
| 传输/存储 | 应用自带（`RaftTransport` trait + 自选持久化） | 内建 tonic gRPC + redb，零传输代码 |
| 领导权观测 | 完整（`is_leader`/`leader_endpoint`/订阅） | **不可观测**（保守返回 false/None，需外部围栏） |
| 写路由 | 应用层转发管线（`ForwardToLeaderBehavior`） | sorock 内部转发，无管线需求 |
| 故障转移（2026-08-12 loopback 实测） | 中位 ~3.5s（写恢复） | 平衡默认中位 ~3.0s；看门狗 ~2.1s(crate 测试）；下限 = phi 检测窗（300ms 心跳节拍，硬编码） |
| 成员变更 | 联合共识（joint)，单步单成员 | 单日志替换 voter 集；无 learner/joint |
| 快照 | raft-engine 持久化 + 应用 checkpoint | 内存快照；文件存储节点须关闭 |
| 多分片 | 单 Raft 组 | multi-Raft（分片模型内建） |

经验法则：需要领导权路由/围栏、可观测的选举视图、或沿用 HTTP 生态时选 raft-rs 后端；想要内建存储+传输的最小接线、multi-Raft 分片，且能接受秒级故障转移与外部围栏时选 sorock。两者都实现同一组 catga-core 契约，应用代码（如 distributed-kv）可以在 `--backend` 一级切换。
