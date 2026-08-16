#[derive(thiserror::Error, Debug, Clone)]
pub enum CatgaRaftError {
    #[error("raft error: {0}")]
    Raft(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("apply error: {0}")]
    Apply(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("circuit breaker is open")]
    CircuitBreakerOpen,
    #[error("backpressure: peer overloaded")]
    Backpressure,
    #[error("not leader")]
    NotLeader,
    #[error("timeout")]
    Timeout,
    #[error("node not found: {0}")]
    NodeNotFound(u64),
}

pub type CatgaRaftResult<T> = Result<T, CatgaRaftError>;

impl From<CatgaRaftError> for catga_core::CatgaError {
    fn from(error: CatgaRaftError) -> Self {
        let code = match &error {
            CatgaRaftError::Raft(_) | CatgaRaftError::Storage(_) | CatgaRaftError::Apply(_) => {
                catga_core::ErrorCode::Internal
            }
            CatgaRaftError::Transport(_) => catga_core::ErrorCode::TransportFailed,
            CatgaRaftError::Codec(_) => catga_core::ErrorCode::SerializationFailed,
            // NotLeader maps to Unavailable rather than Conflict: the request did not clash
            // with persisted state; this node simply cannot serve it and the caller should
            // retry against the current leader.
            CatgaRaftError::CircuitBreakerOpen
            | CatgaRaftError::Backpressure
            | CatgaRaftError::NotLeader => catga_core::ErrorCode::Unavailable,
            CatgaRaftError::Timeout => catga_core::ErrorCode::Timeout,
            CatgaRaftError::NodeNotFound(_) => catga_core::ErrorCode::NotFound,
        };
        let message = error.to_string();
        catga_core::CatgaError::with_source(code, message, error)
    }
}
