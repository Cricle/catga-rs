# 集群模式

## 概述

Catga Cluster 提供分布式协调和领导者选举。

## 配置

```toml
[dependencies]
catga-cluster = "0.1"
```

## 节点发现

```rust
use catga_cluster::{Cluster, NodeConfig};

let cluster = Cluster::new(NodeConfig {
    node_id: "node-1".into(),
    cluster_addr: "192.168.1.1:7946".parse()?,
    seed_nodes: vec![
        "192.168.1.2:7946".parse()?,
        "192.168.1.3:7946".parse()?,
    ],
});

cluster.start().await?;
```

## Raft 共识

```rust
use catga_cluster::{RaftNode, Command};

let raft = RaftNode::new(cluster.clone());

// 提交命令到 Raft 日志
raft.submit(Command::Set { key: "config".into(), value: data }).await?;

// 读取已提交的值
let value = raft.read("config").await?;
```

##领导者选举

```rust
use catga_cluster::Leader Election;

// 成为领导者
let election = cluster.start_election("service-name").await?;

if election.is_leader() {
    // 执行领导者任务
    run_leader_tasks().await?;
} else {
    // 跟随领导者
    follow_leader(election.leader()).await?;
}
```

## 分布式锁

```rust
use catga_cluster::DistributedLock;

let lock = DistributedLock::new(cluster.clone(), "resource-id");
let guard = lock.acquire(Duration::from_secs(10)).await?;

// 临界区操作
process_resource().await?;

// 释放锁
drop(guard);
```

## 分片

```rust
use catga_cluster::ShardManager;

let shards = ShardManager::new(cluster.clone(), num_shards: 16);

// 获取键的所属分片
let shard = shards.get_shard("user-123");

// 分片路由
shard.with_shard(|node| async move {
    node.send(command).await
}).await?;
```
