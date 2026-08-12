//! Redis-backed durable projection checkpoints.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, ProjectionCheckpoint, ProjectionCheckpointStore,
};
use redis::{AsyncCommands, aio::ConnectionManager};

/// Redis hash-backed checkpoints, partitioned by projection name.
pub struct RedisProjectionCheckpoints {
    connection: ConnectionManager,
    prefix: Box<str>,
}

impl RedisProjectionCheckpoints {
    /// Connects and namespaces checkpoint hashes beneath `prefix`.
    ///
    /// Checkpoints are partitioned by projection name, so one projection's progress never
    /// contends with another's keyspace.
    ///
    /// ```no_run
    /// use catga_redis::RedisProjectionCheckpoints;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let checkpoints = RedisProjectionCheckpoints::connect("redis://127.0.0.1/", "app.projections").await?;
    /// # drop(checkpoints);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(CatgaError::transient)?;
        let connection = client
            .get_connection_manager_with_config(crate::config::command_connection_manager_config())
            .await
            .map_err(CatgaError::transient)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
        })
    }

    fn key(&self, projection_name: &str) -> String {
        format!("{}:{projection_name}", self.prefix)
    }
}

#[async_trait]
impl ProjectionCheckpointStore for RedisProjectionCheckpoints {
    async fn save(&self, checkpoint: ProjectionCheckpoint) -> CatgaResult<()> {
        let value = format!(
            "{}\t{}",
            checkpoint.version(),
            unix_millis(checkpoint.updated_at())
        );
        let mut connection = self.connection.clone();
        connection
            .hset(
                self.key(checkpoint.projection_name()),
                checkpoint.stream_id(),
                value,
            )
            .await
            .map_err(CatgaError::transient)
    }

    async fn load(
        &self,
        projection_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<ProjectionCheckpoint>> {
        let mut connection = self.connection.clone();
        let value: Option<String> = connection
            .hget(self.key(projection_name), stream_id)
            .await
            .map_err(CatgaError::transient)?;
        value
            .map(|value| decode(projection_name, stream_id, &value))
            .transpose()
    }

    async fn delete(&self, projection_name: &str, stream_id: &str) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        connection
            .hdel(self.key(projection_name), stream_id)
            .await
            .map_err(CatgaError::transient)
    }

    async fn delete_all(&self, projection_name: &str) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        connection
            .del(self.key(projection_name))
            .await
            .map_err(CatgaError::transient)
    }
}

fn decode(
    projection_name: &str,
    stream_id: &str,
    value: &str,
) -> CatgaResult<ProjectionCheckpoint> {
    let (version, timestamp) = value.split_once('\t').ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "Redis projection checkpoint is malformed",
        )
    })?;
    let version = version.parse().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "Redis projection checkpoint version is malformed",
        )
    })?;
    let timestamp = timestamp.parse().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "Redis projection checkpoint timestamp is malformed",
        )
    })?;
    Ok(ProjectionCheckpoint::from_persisted(
        projection_name,
        stream_id,
        version,
        UNIX_EPOCH + Duration::from_millis(timestamp),
    ))
}

fn unix_millis(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}
