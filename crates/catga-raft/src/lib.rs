pub mod prelude;
pub mod error;
pub mod config;
pub mod apply;
pub mod builder;
pub mod coordinator;
pub mod node;
mod owner;
pub mod pipeline;
pub mod runtime;
pub mod transport;
pub mod storage;

// Transport layer re-exports
pub use transport::{
    backpressure::BackpressureController, batch::BatchSender, breaker::CircuitBreaker,
    breaker::CircuitBreakerConfig, breaker::CircuitBreakerState, codec::BincodeCodec,
    codec::ProstCodec, codec::RaftCodec, connection::ConnectionPool, grpc::GrpcTransport,
    RaftTransport,
};

pub use apply::{ApplySender, ApplyThread};
pub use config::{CatgaRaftConfig, PipelineConfig};
pub use error::{CatgaRaftError, CatgaRaftResult};
pub use pipeline::PipelineManager;
pub use coordinator::CatgaRaftCoordinator;
pub use runtime::CatgaRaftRuntime;
pub use builder::CatgaRaftRuntimeBuilder;

// Storage layer re-exports
pub use storage::{CatgaStorage, EngineStorage};
