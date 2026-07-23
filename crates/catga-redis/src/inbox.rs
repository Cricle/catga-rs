//! Redis Lua-CAS inbox processing records.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, InboxStore, ProcessingState};
use redis::{
    AsyncCommands, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

use crate::transport::map_error;

const CLAIMED: u8 = 1;
const COMPLETED_EMPTY: u8 = 2;
const COMPLETED_RESULT: u8 = 3;
const FAILED: u8 = 4;

const CLAIM: &str = r#"
local value = redis.call('GET', KEYS[1])
if value == false or string.byte(value, 1) == 4 then
    redis.call('SET', KEYS[1], string.char(1))
    return 1
end
return 0
"#;

const TRANSITION: &str = r#"
local value = redis.call('GET', KEYS[1])
if value == false then return -1 end
if string.byte(value, 1) ~= 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
return 1
"#;

/// Redis-backed inbox with atomic per-message processing transitions.
pub struct RedisInbox {
    connection: ConnectionManager,
    prefix: Box<str>,
}

impl RedisInbox {
    /// Connects and namespaces message records beneath `prefix`.
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

    fn key(&self, message_id: u64) -> String {
        format!("{}:{message_id}", self.prefix)
    }

    async fn transition(&self, message_id: u64, value: Vec<u8>) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        match Script::new(TRANSITION)
            .key(self.key(message_id))
            .arg(value)
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(map_error)?
        {
            1 => Ok(()),
            -1 => Err(CatgaError::new(
                ErrorCode::NotFound,
                "inbox message is not claimed",
            )),
            _ => Err(CatgaError::new(
                ErrorCode::Conflict,
                "inbox message is not currently claimed",
            )),
        }
    }
}

#[async_trait]
impl InboxStore for RedisInbox {
    async fn try_claim(&self, message_id: u64) -> CatgaResult<bool> {
        let mut connection = self.connection.clone();
        Script::new(CLAIM)
            .key(self.key(message_id))
            .invoke_async::<i64>(&mut connection)
            .await
            .map(|result| result == 1)
            .map_err(map_error)
    }

    async fn complete(&self, message_id: u64, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        let mut value = Vec::with_capacity(
            result
                .as_ref()
                .map_or(1, |value| value.len().saturating_add(1)),
        );
        value.push(if result.is_some() {
            COMPLETED_RESULT
        } else {
            COMPLETED_EMPTY
        });
        if let Some(result) = result {
            value.extend_from_slice(&result);
        }
        self.transition(message_id, value).await
    }

    async fn fail(&self, message_id: u64) -> CatgaResult<()> {
        self.transition(message_id, vec![FAILED]).await
    }

    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>> {
        let mut connection = self.connection.clone();
        let value: Option<Vec<u8>> = connection
            .get(self.key(message_id))
            .await
            .map_err(map_error)?;
        value.map(|value| state(&value)).transpose()
    }

    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        let mut connection = self.connection.clone();
        let value: Option<Vec<u8>> = connection
            .get(self.key(message_id))
            .await
            .map_err(map_error)?;
        Ok(value.and_then(|value| {
            (value.first() == Some(&COMPLETED_RESULT)).then(|| Arc::from(&value[1..]))
        }))
    }
}

fn state(value: &[u8]) -> CatgaResult<ProcessingState> {
    match value.first() {
        Some(&CLAIMED) => Ok(ProcessingState::Claimed),
        Some(&COMPLETED_EMPTY | &COMPLETED_RESULT) => Ok(ProcessingState::Completed),
        Some(&FAILED) => Ok(ProcessingState::Failed),
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            "Redis inbox record is malformed",
        )),
    }
}
