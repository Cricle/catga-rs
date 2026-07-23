use async_trait::async_trait;
use catga_core::{Acknowledger, CatgaError, CatgaResult, ErrorCode};

pub(crate) struct NatsAcknowledger(pub(crate) async_nats::jetstream::Message);

#[async_trait]
impl Acknowledger for NatsAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.0
            .ack()
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))
    }
}
