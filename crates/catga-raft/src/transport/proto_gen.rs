//! Generated protobuf/gRPC bindings for the raft wire transport.
//!
//! The code is produced by `tonic-build` from `proto/raft.proto` at build
//! time (see `build.rs`); it must never be edited by hand.

/// Bindings for the `catga.raft` package.
///
/// Contains the `RaftMessage`, `RaftMessageBatch` and `StepReply` message
/// structs plus the `raft_server` and `raft_client` modules implementing the
/// `catga.raft.Raft` service. Message payloads are opaque bytes holding a
/// `raft::prelude::Message` encoded with the `protobuf` crate.
pub mod pb {
    tonic::include_proto!("catga.raft");
}
