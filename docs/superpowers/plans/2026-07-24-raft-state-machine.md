# Raft State Machine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide lock-free-at-the-boundary application-state replay and native Raft snapshots for `catga-cluster`.

**Architecture:** One `RaftStateMachineDriver<M>` exclusively owns a `RaftNode` and `M`, serializing application changes without a shared mutex. The storage adapter persists a protobuf `Snapshot` containing application bytes and compacts its covered Raft log atomically in its existing `raft-engine` batch.

**Tech Stack:** `raft-rs` 0.7 protobuf codec, `raft-engine` 0.4, `CatgaResult`, root integration tests.

---

### Task 1: Define the public state-machine contract and failing application test

**Files:**
- Modify: `tests/Cargo.toml`
- Create: `tests/raft_state_machine.rs`
- Modify: `crates/catga-cluster/src/lib.rs`

- [x] **Step 1: Add a test-owned counter state machine.**

```rust
#[derive(Default)]
struct Counter { value: u64 }
impl RaftStateMachine for Counter {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        self.value += u64::from_le_bytes(entry.data.as_slice().try_into().unwrap());
        Ok(())
    }
    fn snapshot(&self) -> CatgaResult<Vec<u8>> { Ok(self.value.to_le_bytes().to_vec()) }
    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        self.value = u64::from_le_bytes(bytes.try_into().unwrap()); Ok(())
    }
}
```

- [x] **Step 2: Verify the focused target compiles.**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p catga-tests --test raft_state_machine`

Expected: the focused test target compiles and exercises the public driver.

- [x] **Step 3: Add the minimal public trait and driver module.**

```rust
pub trait RaftStateMachine {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()>;
    fn snapshot(&self) -> CatgaResult<Vec<u8>>;
    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()>;
}
```

- [x] **Step 4: Verify the focused state-machine suite.**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p catga-tests --test raft_state_machine`

Expected: the driver applies a single-node proposal once, in log-index order.

### Task 2: Add native snapshot construction and durable recovery

**Files:**
- Modify: `crates/catga-cluster/src/storage.rs`
- Modify: `crates/catga-cluster/src/raft.rs`
- Modify: `crates/catga-cluster/src/state_machine.rs`
- Modify: `tests/raft_state_machine.rs`

- [x] **Step 1: Add recovery and premature-checkpoint assertions.**

```rust
assert_eq!(driver.checkpoint().unwrap_err().code(), ErrorCode::Validation);
driver.apply_committed().unwrap();
driver.checkpoint().unwrap();
assert_eq!(reopened.machine().value, 7);
```

- [x] **Step 2: Verify focused recovery coverage.**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p catga-tests --test raft_state_machine persistent_checkpoint_recovers_before_replay`

Expected: the focused test covers snapshot recovery and checkpoint ordering.

- [x] **Step 3: Persist a protobuf snapshot in one Raft-engine batch.**

```rust
fn create_snapshot(&self, index: u64, data: Vec<u8>) -> raft::Result<()>;
fn stored_snapshot(&self) -> raft::Result<Option<Snapshot>>;
```

Build metadata from the index term and current `ConfState`; reject indices above the durable commit index. Use a dedicated persistent checkpoint batch so snapshot data and `Command::Compact` share one sync write without decreasing a later commit index. The in-memory backend must reject a checkpoint before its log tip because `MemStorage::apply_snapshot` cannot retain a suffix safely.

- [x] **Step 4: Verify the state-machine suite.**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p catga-tests --test raft_state_machine`

Expected: checkpoint recovery restores the snapshot and only replays later entries; after a restart, another checkpoint followed by election and proposal remains valid because RawNode apply progress is acknowledged after replay.

### Task 3: Verify the complete cluster surface

**Files:**
- Modify: `tests/raft_state_machine.rs`

- [x] **Step 1: Add a test that verifies a command proposed after a checkpoint is applied once after restart.**

```rust
assert_eq!(reopened.apply_committed().unwrap(), 1);
assert_eq!(reopened.machine().value, 12);
```

- [x] **Step 2: Run the full verification suite.**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace && rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings && rtk proxy cargo fmt --all -- --check && rtk proxy git diff --check`

Expected: all commands exit zero.

- [x] **Step 3: Leave the implementation uncommitted as required for the shared dirty worktree.**

```bash
rtk proxy git add crates/catga-cluster tests/raft_state_machine.rs tests/Cargo.toml docs/superpowers
rtk proxy git commit -m "feat: add raft application state machine snapshots"
```
