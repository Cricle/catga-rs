use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{Acknowledger, CatgaError, CatgaResult, ErrorCode, OperationGuard};
use redis::{Script, aio::ConnectionManager};

use crate::transport::{InFlight, map_error};

const ACK_IF_OWNER: &str = r#"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pending ~= 1 or pending[1][2] ~= ARGV[2] then return 0 end
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
"#;

/// Acknowledger that removes a stream entry from Redis only if it is still assigned to this consumer.
///
/// This prevents acknowledging entries that were claimed by another consumer during redelivery.
/// The Lua script `ACK_IF_OWNER` performs an atomic check-and-ack: it first verifies the entry
/// is still assigned to this consumer via `XPENDING`, then executes `XACK` only if the check passes.
///
/// ## Drop semantics
///
/// Dropping a `RedisAcknowledger` without calling `acknowledge` or `nack` releases the entry from
/// the in-flight tracking but does NOT call `XACK`. This means the entry remains pending in Redis
/// and will be redelivered to the same or another consumer. Applications should always call
/// `acknowledge` when processing succeeds, or `nack` when the entry should be redelivered.
///
/// ## Thread safety
///
/// The contained `ConnectionManager` is cloned for each acknowledgement operation, sharing the
/// underlying connection pool across concurrent acknowledgements.
pub(crate) struct RedisAcknowledger {
    pub(crate) connection: ConnectionManager,
    pub(crate) stream: Box<str>,
    pub(crate) group: Box<str>,
    pub(crate) consumer: Box<str>,
    pub(crate) entry_id: Box<str>,
    pub(crate) in_flight: Arc<InFlight>,
    pub(crate) _operation: OperationGuard,
}

#[async_trait]
impl Acknowledger for RedisAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        let acknowledged: i64 = Script::new(ACK_IF_OWNER)
            .key(self.stream.as_ref())
            .arg(self.group.as_ref())
            .arg(self.consumer.as_ref())
            .arg(self.entry_id.as_ref())
            .invoke_async(&mut connection)
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
