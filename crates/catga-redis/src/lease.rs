//! Redis Lua-backed distributed leases.

use std::time::Duration;

use async_trait::async_trait;
use catga_core::{CatgaResult, LeaseStore, telemetry};
use redis::{Script, aio::ConnectionManager};

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
            .get_connection_manager_with_config(crate::config::command_connection_manager_config())
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
        telemetry::record_persistence_claim("redis", "lease", "try_acquire", async {
            self.execute(ACQUIRE, resource, owner, Some(ttl)).await
        })
        .await
    }
    async fn renew(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        telemetry::record_persistence("redis", "lease", "renew", async {
            self.execute(RENEW, resource, owner, Some(ttl)).await
        })
        .await
    }
    async fn release(&self, resource: &str, owner: &str) -> CatgaResult<bool> {
        telemetry::record_persistence("redis", "lease", "release", async {
            self.execute(RELEASE, resource, owner, None).await
        })
        .await
    }
}

fn ttl_millis(ttl: Duration) -> u64 {
    u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_millis_exact() {
        assert_eq!(ttl_millis(Duration::from_millis(1000)), 1000);
        assert_eq!(ttl_millis(Duration::from_secs(5)), 5000);
    }

    #[test]
    fn ttl_millis_zero_returns_one() {
        assert_eq!(ttl_millis(Duration::ZERO), 1);
    }

    #[test]
    fn ttl_millis_sub_millis_rounds_up() {
        // Duration with sub-millisecond should round up
        assert_eq!(ttl_millis(Duration::from_nanos(500)), 1);
        assert_eq!(ttl_millis(Duration::from_micros(1)), 1);
    }

    #[test]
    fn ttl_millis_large_value() {
        let large = Duration::from_secs(u64::MAX / 1000);
        // Should not panic and should return at least 1
        assert!(ttl_millis(large) >= 1);
    }

    #[test]
    fn lua_scripts_are_valid_strings() {
        // Just verify the scripts are non-empty
        assert!(!ACQUIRE.is_empty());
        assert!(!RENEW.is_empty());
        assert!(!RELEASE.is_empty());
    }

    #[test]
    fn lua_scripts_contain_expected_commands() {
        // Verify ACQUIRE contains SET NX (new acquire)
        assert!(ACQUIRE.contains("SET"));
        assert!(ACQUIRE.contains("NX"));

        // Verify RENEW contains PEXPIRE
        assert!(RENEW.contains("PEXPIRE"));

        // Verify RELEASE contains DEL
        assert!(RELEASE.contains("DEL"));
    }
}
