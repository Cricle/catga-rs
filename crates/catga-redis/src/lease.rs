//! Redis Lua-backed distributed leases.

use std::time::Duration;

use async_trait::async_trait;
use catga_core::{CatgaResult, LeaseStore};
use redis::{
    Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

use crate::transport::map_error;

const ACQUIRE: &str = "local current=redis.call('GET',KEYS[1]); if not current then return redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2],'NX') and 1 or 0 end; if current==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end; return 0";
const RENEW: &str = "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end; return 0";
const RELEASE: &str =
    "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end; return 0";

/// A Redis-backed lease store using server-side conditional mutations.
pub struct RedisLeases {
    connection: ConnectionManager,
    prefix: Box<str>,
}

impl RedisLeases {
    /// Connects to Redis and prefixes every lease resource key.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new().set_response_timeout(None),
            )
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
        })
    }
    async fn execute(
        &self,
        source: &str,
        resource: &str,
        owner: &str,
        ttl: Option<Duration>,
    ) -> CatgaResult<bool> {
        let mut connection = self.connection.clone();
        let script = Script::new(source);
        let key = format!("{}:{resource}", self.prefix);
        let mut invocation = script.key(&key);
        invocation.arg(owner);
        if let Some(ttl) = ttl {
            invocation.arg(ttl_millis(ttl));
        }
        let result: i32 = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(result != 0)
    }
}

#[async_trait]
impl LeaseStore for RedisLeases {
    async fn try_acquire(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        self.execute(ACQUIRE, resource, owner, Some(ttl)).await
    }
    async fn renew(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        self.execute(RENEW, resource, owner, Some(ttl)).await
    }
    async fn release(&self, resource: &str, owner: &str) -> CatgaResult<bool> {
        self.execute(RELEASE, resource, owner, None).await
    }
}

fn ttl_millis(ttl: Duration) -> u64 {
    u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1)
}
