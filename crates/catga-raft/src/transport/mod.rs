//! Transport layer module for Raft cluster communication.
//!
//! This module provides a high-performance gRPC-based transport layer with:
//! - Connection pooling per peer
//! - Backpressure control
//! - Circuit breaker for fault isolation
//! - Message batching and aggregation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      RaftTransport                          │
//! │  - Manages peer lifecycle                                   │
//! │  - Broadcast / send_many operations                         │
//! └─────────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      PeerClient                             │
//! │  - Backpressure (semaphore)                                 │
//! │  - Circuit breaker                                          │
//! │  - Batch sender                                             │
//! └─────────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    ConnectionPool                           │
//! │  - Multiple channels per peer (4 default)                   │
//! │  - Round-robin load balancing                               │
//! └─────────────────────────────────────────────────────────────┘
//! ```

pub mod backpressure;
pub mod batch;
pub mod breaker;
pub mod codec;
pub mod connection;
pub mod grpc;
pub mod proto_gen;
pub mod server;
pub mod trait_;

pub use backpressure::BackpressureController;
pub use batch::BatchSender;
pub use breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState};
pub use codec::{BincodeCodec, ProstCodec, RaftCodec};
pub use connection::ConnectionPool;
pub use grpc::{GrpcTransport, RaftTransport};
pub use proto_gen::pb;
pub use server::{RaftGrpcService, serve, serve_with_bound_addr};
pub use trait_::Transport;
