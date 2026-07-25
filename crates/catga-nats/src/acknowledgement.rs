use async_trait::async_trait;
use catga_core::{Acknowledger, CatgaError, CatgaResult, ErrorCode, OperationGuard};

pub(crate) struct NatsAcknowledger {
    pub(crate) message: async_nats::jetstream::Message,
    pub(crate) _operation: OperationGuard,
}

#[async_trait]
impl Acknowledger for NatsAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.message
            .ack()
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.message
            .ack_with(async_nats::jetstream::AckKind::Nak(None))
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))
    }
}
