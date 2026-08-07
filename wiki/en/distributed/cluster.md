# Cluster Mode

## Overview

Catga Cluster provides distributed coordination and leader election.

## Configuration

```toml
[dependencies]
catga-cluster = "0.1"
```

## Node Discovery

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

## Raft Consensus

```rust
use catga_cluster::{RaftNode, Command};

let raft = RaftNode::new(cluster.clone());

// Submit command to Raft log
raft.submit(Command::Set { key: "config".into(), value: data }).await?;

// Read committed value
let value = raft.read("config").await?;
```

## Leader Election

```rust
use catga_cluster::Leader Election;

// Become a leader
let election = cluster.start_election("service-name").await?;

if election.is_leader() {
    // Execute leader tasks
    run_leader_tasks().await?;
} else {
    // Follow the leader
    follow_leader(election.leader()).await?;
}
```

## Distributed Lock

```rust
use catga_cluster::DistributedLock;

let lock = DistributedLock::new(cluster.clone(), "resource-id");
let guard = lock.acquire(Duration::from_secs(10)).await?;

// Critical section operations
process_resource().await?;

// Release lock
drop(guard);
```

## Sharding

```rust
use catga_cluster::ShardManager;

let shards = ShardManager::new(cluster.clone(), num_shards: 16);

// Get shard for a key
let shard = shards.get_shard("user-123");

// Shard routing
shard.with_shard(|node| async move {
    node.send(command).await
}).await?;
```
