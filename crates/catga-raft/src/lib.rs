pub mod apply;
pub mod builder;
pub mod config;
pub mod coordinator;
pub mod error;
pub mod node;
mod owner;
pub mod pipeline;
pub mod prelude;
pub mod runtime;
pub mod storage;
pub mod transport;

// Transport layer re-exports
pub use transport::{
    RaftTransport, backpressure::BackpressureController, batch::BatchSender,
    breaker::CircuitBreaker, breaker::CircuitBreakerConfig, breaker::CircuitBreakerState,
    codec::BincodeCodec, codec::ProstCodec, codec::RaftCodec, connection::ConnectionPool,
    grpc::GrpcTransport,
};

pub use apply::{ApplySender, ApplyThread};
pub use builder::CatgaRaftRuntimeBuilder;
pub use config::{CatgaRaftConfig, PipelineConfig};
pub use coordinator::CatgaRaftCoordinator;
pub use error::{CatgaRaftError, CatgaRaftResult};
pub use pipeline::PipelineManager;
pub use runtime::CatgaRaftRuntime;

// Storage layer re-exports
pub use storage::{CatgaStorage, EngineStorage};
