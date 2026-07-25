use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{Acknowledger, CatgaError, CatgaResult, ErrorCode, OperationGuard};
use redis::{AsyncCommands, aio::ConnectionManager};

use crate::transport::{InFlight, map_error};

pub(crate) struct RedisAcknowledger {
    pub(crate) connection: ConnectionManager,
    pub(crate) stream: Box<str>,
    pub(crate) group: Box<str>,
    pub(crate) entry_id: Box<str>,
    pub(crate) in_flight: Arc<InFlight>,
    pub(crate) _operation: OperationGuard,
}

#[async_trait]
impl Acknowledger for RedisAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        let acknowledged: usize = connection
            .xack(
                self.stream.as_ref(),
                self.group.as_ref(),
                &[self.entry_id.as_ref()],
            )
            .await
            .map_err(map_error)?;
        self.in_flight
            .release(self.stream.as_ref(), self.entry_id.as_ref());
        if acknowledged != 1 {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "Redis did not acknowledge the stream entry",
            ));
        }
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.in_flight
            .release(self.stream.as_ref(), self.entry_id.as_ref());
        Ok(())
    }
}

impl Drop for RedisAcknowledger {
    fn drop(&mut self) {
        self.in_flight
            .release(self.stream.as_ref(), self.entry_id.as_ref());
    }
}
