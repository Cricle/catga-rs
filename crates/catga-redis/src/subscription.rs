//! Redis-backed persistent subscriptions and owner leases.

use crate::transport::map_error;
use async_trait::async_trait;
use catga_core::{CatgaResult, PersistentSubscription, SubscriptionCheckpoint, SubscriptionStore};
use redis::{AsyncCommands, Script, aio::ConnectionManager};

const RELEASE: &str =
    r#"if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0"#;

/// Redis durable subscription definitions, stream checkpoints, and owner leases.
pub struct RedisSubscriptions {
    connection: ConnectionManager,
    prefix: Box<str>,
}
impl RedisSubscriptions {
    /// Connects and namespaces subscription data beneath `prefix`.
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
    fn definition(&self, n: &str) -> String {
        format!("{}:definition:{n}", self.prefix)
    }
    fn index(&self) -> String {
        format!("{}:index", self.prefix)
    }
    fn checkpoints(&self, n: &str) -> String {
        format!("{}:checkpoints:{n}", self.prefix)
    }
    fn lease(&self, n: &str) -> String {
        format!("{}:lease:{n}", self.prefix)
    }
}
#[async_trait]
impl SubscriptionStore for RedisSubscriptions {
    async fn save(&self, s: PersistentSubscription) -> CatgaResult<()> {
        let types = s
            .event_types()
            .iter()
            .map(|t| t.as_ref())
            .collect::<Vec<_>>()
            .join("\u{1f}");
        let mut c = self.connection.clone();
        let _: usize = c
            .hset(self.definition(s.name()), "pattern", s.stream_pattern())
            .await
            .map_err(map_error)?;
        let _: usize = c
            .hset(self.definition(s.name()), "types", types)
            .await
            .map_err(map_error)?;
        let _: usize = c.sadd(self.index(), s.name()).await.map_err(map_error)?;
        Ok(())
    }
    async fn load(&self, n: &str) -> CatgaResult<Option<PersistentSubscription>> {
        let mut c = self.connection.clone();
        let pattern: Option<String> = c
            .hget(self.definition(n), "pattern")
            .await
            .map_err(map_error)?;
        let Some(pattern) = pattern else {
            return Ok(None);
        };
        let types: Option<String> = c
            .hget(self.definition(n), "types")
            .await
            .map_err(map_error)?;
        let sub = PersistentSubscription::new(n, pattern).with_event_types(
            types
                .unwrap_or_default()
                .split('\u{1f}')
                .filter(|s| !s.is_empty()),
        );
        Ok(Some(sub))
    }
    async fn delete(&self, n: &str) -> CatgaResult<()> {
        let mut c = self.connection.clone();
        let _: usize = c.del(self.definition(n)).await.map_err(map_error)?;
        let _: usize = c.del(self.checkpoints(n)).await.map_err(map_error)?;
        let _: usize = c.del(self.lease(n)).await.map_err(map_error)?;
        let _: usize = c.srem(self.index(), n).await.map_err(map_error)?;
        Ok(())
    }
    async fn list(&self) -> CatgaResult<Vec<PersistentSubscription>> {
        let mut c = self.connection.clone();
        let mut names: Vec<String> = c.smembers(self.index()).await.map_err(map_error)?;
        names.sort_unstable();
        let mut subs = Vec::with_capacity(names.len());
        for n in names {
            if let Some(s) = self.load(&n).await? {
                subs.push(s)
            }
        }
        Ok(subs)
    }
    async fn save_checkpoint(&self, cpt: SubscriptionCheckpoint) -> CatgaResult<()> {
        let mut c = self.connection.clone();
        let _: usize = c
            .hset(
                self.checkpoints(cpt.subscription_name()),
                cpt.stream_id(),
                cpt.version(),
            )
            .await
            .map_err(map_error)?;
        Ok(())
    }
    async fn load_checkpoint(
        &self,
        n: &str,
        s: &str,
    ) -> CatgaResult<Option<SubscriptionCheckpoint>> {
        let mut c = self.connection.clone();
        let v: Option<i64> = c.hget(self.checkpoints(n), s).await.map_err(map_error)?;
        Ok(v.map(|v| SubscriptionCheckpoint::new(n, s, v)))
    }
    async fn try_acquire(&self, n: &str, o: &str) -> CatgaResult<bool> {
        let mut c = self.connection.clone();
        c.set_nx(self.lease(n), o).await.map_err(map_error)
    }
    async fn release(&self, n: &str, o: &str) -> CatgaResult<()> {
        let mut c = self.connection.clone();
        Script::new(RELEASE)
            .key(self.lease(n))
            .arg(o)
            .invoke_async::<i64>(&mut c)
            .await
            .map(|_| ())
            .map_err(map_error)
    }
}
