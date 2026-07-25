# Raft State Machine and Snapshot Design

## Goal

Close the gap between committed `raft-rs` log entries and durable application
state without adding locks to the Raft hot path.

## Decision

`RaftNode` remains the sole owner of `RawNode`. `RaftStateMachineDriver<M>`
owns both one `RaftNode` and one mutable `M: RaftStateMachine`; it applies
committed entries sequentially, so application state needs neither a mutex nor
an actor hop. The trait accepts opaque command bytes and produces opaque
snapshot bytes. Applications may use `catga-codec-postcard` for their model,
but the cluster crate has no serialization dependency.

## Persistence and recovery

The driver restores the most recent Raft snapshot before replaying committed
entries after its index. A checkpoint first obtains immutable application
snapshot bytes, then stores them in the native Raft protobuf snapshot and
compacts the covered log entries in the same synchronous `raft-engine` write.
If snapshot encoding fails, no Raft data is changed. A failed persistence write
does not mark the checkpoint as installed.

After recovery replay succeeds, the driver advances `RawNode`'s apply progress
to the same index before accepting further Raft work. This prevents a later
checkpoint from compacting entries that `raft-rs` still considers unapplied.

The production `raft-engine` backend preserves a later log suffix while it
compacts the snapshot prefix. `raft-rs`'s `MemStorage` cannot safely install a
snapshot while retaining that suffix, so the in-memory backend accepts only a
checkpoint at its durable log tip. This keeps the test backend correct without
duplicating a Raft log implementation.

## API boundary

`RaftStateMachine` has `apply`, `snapshot`, and `restore`; each returns
`CatgaResult`. `RaftStateMachineDriver` exposes the existing Raft operations,
`apply_committed`, `checkpoint`, and a read-only `machine` accessor. The
driver rejects checkpoints beyond its successfully applied index.

## Testing

Root integration tests prove ordered application, rejection of premature
checkpoints, persistent snapshot recovery without replaying compacted entries,
and post-restart command application. No tests live in source crates.
