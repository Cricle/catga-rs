# Leadership Snapshot Subscription Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a bounded, non-blocking leadership-state subscription from both in-memory and Raft cluster coordinators.

**Architecture:** A `tokio::sync::watch` channel retains only the newest immutable `LeadershipSnapshot`; its monotonic epoch exposes coalesced transitions without retaining an unbounded event history. Existing `ArcSwap` reads and `Notify` waiters remain unchanged so leadership checks and internal cancellation retain their current low-overhead fast path.

**Tech Stack:** Rust 2024, Tokio `watch`, `ArcSwap`, `cargo test`, `cargo clippy`.

---

### Task 1: Specify MemoryCluster subscription behavior

**Files:**
- Modify: `tests/cluster.rs`

- [x] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn leadership_subscription_coalesces_changes_without_blocking_other_receivers() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_a = cluster.node("node-a").expect("configured node-a");
    let node_b = cluster.node("node-b").expect("configured node-b");
    let mut first = node_a.subscribe_leadership();
    let mut second = node_b.subscribe_leadership();

    assert_eq!(first.borrow().epoch, 0);
    cluster.elect("node-b").expect("configured node-b");
    cluster.elect("node-a").expect("configured node-a");

    first.changed().await.expect("cluster remains alive");
    second.changed().await.expect("cluster remains alive");
    assert_eq!(first.borrow().epoch, 2);
    assert_eq!(first.borrow().leader_node_id.as_deref(), Some("node-a"));
    assert_eq!(second.borrow().leader_endpoint.as_deref(), Some("http://node-a"));
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `rtk cargo test -p catga-tests --test cluster leadership_subscription_coalesces_changes_without_blocking_other_receivers -- --exact`

Expected: FAIL because `ClusterCoordinator::subscribe_leadership` does not exist.

- [x] **Step 3: Commit the red test only if the repository policy permits intermediate commits**

```bash
rtk git add tests/cluster.rs
rtk git commit -m "test(cluster): specify leadership subscriptions"
```

### Task 2: Implement the bounded public snapshot API

**Files:**
- Modify: `crates/catga-cluster/src/lib.rs:1-210`
- Test: `tests/cluster.rs`

- [x] **Step 1: Add the public snapshot type and trait method**

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeadershipSnapshot {
    pub epoch: u64,
    pub leader_node_id: Option<Arc<str>>,
    pub leader_endpoint: Option<Arc<str>>,
}

fn subscribe_leadership(&self) -> tokio::sync::watch::Receiver<LeadershipSnapshot>;
```

Document that receivers observe latest state, may coalesce intermediate epochs, and never block state publication.

- [x] **Step 2: Add `watch::Sender<LeadershipSnapshot>` to `MemoryClusterInner`**

Initialize it with epoch `0`, current leader id, and its resolved endpoint. In `MemoryCluster::elect`, after the `ArcSwap` topology store, clone the previous snapshot, increment only on real leadership change, and call `send_replace`. Keep `Notify::notify_waiters()` for internal waiters.

- [x] **Step 3: Return `sender.subscribe()` from `MemoryClusterNode`**

```rust
fn subscribe_leadership(&self) -> watch::Receiver<LeadershipSnapshot> {
    self.inner.leadership.subscribe()
}
```

- [x] **Step 4: Run the focused test to verify it passes**

Run: `rtk cargo test -p catga-tests --test cluster leadership_subscription_coalesces_changes_without_blocking_other_receivers -- --exact`

Expected: PASS.

### Task 3: Publish Raft leadership snapshots

**Files:**
- Modify: `crates/catga-cluster/src/raft.rs:1-240,380-420,690-705`
- Modify: `tests/raft_cluster.rs`

- [x] **Step 1: Write the failing Raft test**

```rust
#[test]
fn raft_leadership_subscription_publishes_the_campaign_winner() {
    let mut node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    ).expect("valid single-node Raft configuration");
    let coordinator = node.coordinator();
    let receiver = coordinator.subscribe_leadership();

    assert_eq!(receiver.borrow().leader_node_id, None);
    node.campaign().expect("single node can campaign");

    assert_eq!(receiver.borrow().epoch, 1);
    assert_eq!(receiver.borrow().leader_node_id.as_deref(), Some("1"));
    assert_eq!(receiver.borrow().leader_endpoint.as_deref(), Some("http://node-1"));
}
```

- [x] **Step 2: Run the Raft test to verify it fails**

Run: `rtk cargo test -p catga-tests --test raft_cluster raft_leadership_subscription_publishes_the_campaign_winner -- --exact`

Expected: FAIL because Raft coordinator does not publish `LeadershipSnapshot` values.

- [x] **Step 3: Add the sender to `RaftCoordinatorInner` and initialize a no-leader snapshot**

Create an epoch-zero snapshot in the constructor. Resolve a known nonzero leader id using the configured `RaftMember` list only when leader state changes.

- [x] **Step 4: Publish after the Raft state transition**

In `publish_coordinator_state`, after atomically replacing `RaftCoordinatorState`, increment the existing snapshot epoch, construct the new immutable snapshot, call `send_replace`, then wake `Notify` waiters. Do not change proposal, replication, metrics, or `is_leader()` hot paths.

- [x] **Step 5: Run the focused Raft test to verify it passes**

Run: `rtk cargo test -p catga-tests --test raft_cluster raft_leadership_subscription_publishes_the_campaign_winner -- --exact`

Expected: PASS.

### Task 4: Regression verification and review

**Files:**
- Modify only files required by earlier tasks.

- [x] **Step 1: Format and run cluster regression suites**

Run:

```bash
rtk cargo fmt --check
rtk cargo test -p catga-tests --test cluster
rtk cargo test -p catga-tests --test raft_cluster
```

Expected: PASS with no formatting changes.

- [x] **Step 2: Run workspace quality gates**

Run:

```bash
rtk env CARGO_PROFILE_TEST_DEBUG=0 timeout --kill-after=5 180 cargo test --workspace --quiet
rtk cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Expected: PASS. Confirm no background cargo/test process remains before reporting success.

- [x] **Step 3: Review before committing**

Review the diff for: stable rustdoc, no production panics, no unbounded queues, no regression to the `ArcSwap`/`Notify` fast path, and no modification of `docs/superpowers/specs/2026-07-23-catga-core-design.md`.

- [ ] **Step 4: Commit and push the feature**

```bash
rtk git add crates/catga-cluster/src/lib.rs crates/catga-cluster/src/raft.rs tests/cluster.rs tests/raft_cluster.rs docs/superpowers/plans/2026-07-27-leadership-snapshot-subscription.md
rtk git commit -m "feat(cluster): publish leadership snapshots"
rtk git push
```
