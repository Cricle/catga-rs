//! Redis state-machine snapshots with exact-value Lua CAS.

use std::marker::PhantomData;

use async_trait::async_trait;
use catga_codec_postcard::PostcardSnapshotCodec;
use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};
use catga_flow::{
    StateMachineSnapshot, StateMachineStore, decode_state_machine_snapshot,
    encode_state_machine_snapshot,
};
use redis::{
    AsyncCommands, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

use crate::transport::map_error;

const MAX_CAS_RETRIES: usize = 8;
const COMPARE_AND_SET: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2])
    return 1
end
return 0
"#;

/// Redis-backed state-machine store using per-instance binary compare-and-set.
pub struct RedisStateMachines<S, C = PostcardSnapshotCodec<S>> {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: C,
    state: PhantomData<fn() -> S>,
}

impl<S> RedisStateMachines<S>
where
    S: Send + Sync + 'static,
    PostcardSnapshotCodec<S>: SnapshotCodec<S>,
{
    /// Connects with compact Postcard state encoding.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        Self::with_codec(server, prefix, PostcardSnapshotCodec::default()).await
    }
}

impl<S, C> RedisStateMachines<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    /// Connects with an explicit state codec.
    pub async fn with_codec(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
        codec: C,
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
            codec,
            state: PhantomData,
        })
    }

    fn key(&self, instance_id: &str) -> String {
        format!("{}:{instance_id}", self.prefix)
    }

    async fn load_raw(&self, key: &str) -> CatgaResult<Option<Vec<u8>>> {
        let mut connection = self.connection.clone();
        connection.get(key).await.map_err(map_error)
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Vec<u8>,
        next: Vec<u8>,
    ) -> CatgaResult<bool> {
        let mut connection = self.connection.clone();
        let updated = Script::new(COMPARE_AND_SET)
            .key(key)
            .arg(expected)
            .arg(next)
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(updated == 1)
    }
}

#[async_trait]
impl<S, C> StateMachineStore<S> for RedisStateMachines<S, C>
where
    S: Clone + Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    async fn create(&self, snapshot: StateMachineSnapshot<S>) -> CatgaResult<bool> {
        let key = self.key(snapshot.instance_id());
        let mut connection = self.connection.clone();
        connection
            .set_nx(key, encode_state_machine_snapshot(&snapshot, &self.codec)?)
            .await
            .map_err(map_error)
    }

    async fn get(&self, instance_id: &str) -> CatgaResult<Option<StateMachineSnapshot<S>>> {
        self.load_raw(&self.key(instance_id))
            .await?
            .map(|value| decode_state_machine_snapshot(instance_id, &value, &self.codec))
            .transpose()
    }

    async fn update(
        &self,
        expected_version: i64,
        next: StateMachineSnapshot<S>,
    ) -> CatgaResult<bool> {
        if next.version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        let key = self.key(next.instance_id());
        let next_raw = encode_state_machine_snapshot(&next, &self.codec)?;
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current_raw) = self.load_raw(&key).await? else {
                return Ok(false);
            };
            let current =
                decode_state_machine_snapshot(next.instance_id(), &current_raw, &self.codec)?;
            if current.version() != expected_version {
                return Ok(false);
            }
            if self
                .compare_and_set(&key, current_raw, next_raw.clone())
                .await?
            {
                return Ok(true);
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "Redis state-machine compare-and-set did not stabilize",
        ))
    }
}
