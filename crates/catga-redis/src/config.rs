use std::time::Duration;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use redis::aio::ConnectionManagerConfig;

/// Largest number of one-entry pending scans made before a receive waits for Redis again.
pub const MAX_REDIS_PENDING_RECLAIM_SCANS: usize = 64;

const DEFAULT_REDIS_PENDING_RECLAIM_SCANS: usize = 16;

/// Default maximum time ordinary Redis commands wait for a server response.
pub const DEFAULT_REDIS_COMMAND_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);

/// Response-timeout policy for ordinary Redis persistence commands.
///
/// This policy applies to command connections such as stores, schedulers, and stream
/// publishing. It deliberately does not apply to the separate blocking connection used by
/// Redis Streams `XREAD`, whose response timeout remains unbounded while it long-polls.
///
/// ```
/// use std::time::Duration;
/// use catga_redis::RedisCommandOptions;
///
/// let options = RedisCommandOptions::new(Duration::from_millis(250))?;
/// assert_eq!(options.response_timeout(), Duration::from_millis(250));
/// # Ok::<(), catga_core::CatgaError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisCommandOptions {
    response_timeout: Duration,
}

impl RedisCommandOptions {
    /// Creates an ordinary-command timeout policy.
    ///
    /// `response_timeout` must be nonzero. A command that does not receive a Redis response in
    /// this interval returns a transient Catga error instead of waiting indefinitely.
    pub fn new(response_timeout: Duration) -> CatgaResult<Self> {
        if response_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Redis command response timeout must be greater than zero",
            ));
        }
        Ok(Self { response_timeout })
    }

    /// Returns the maximum time an ordinary Redis command waits for its response.
    pub fn response_timeout(&self) -> Duration {
        self.response_timeout
    }

    pub(crate) fn connection_manager_config(&self) -> ConnectionManagerConfig {
        ConnectionManagerConfig::new().set_response_timeout(Some(self.response_timeout))
    }
}

impl Default for RedisCommandOptions {
    fn default() -> Self {
        Self {
            response_timeout: DEFAULT_REDIS_COMMAND_RESPONSE_TIMEOUT,
        }
    }
}

pub(crate) fn command_connection_manager_config() -> ConnectionManagerConfig {
    RedisCommandOptions::default().connection_manager_config()
}

/// Bounded Redis Streams recovery policy for deliveries abandoned by another consumer.
///
/// Every scan examines at most one pending entry and claims at most one eligible entry. The
/// transport carries Redis's cursor between receive attempts, so a group with many non-idle
/// entries is traversed incrementally without copying its pending list into process memory.
///
/// ```
/// use std::time::Duration;
/// use catga_redis::RedisPendingReclaimOptions;
///
/// let options = RedisPendingReclaimOptions::new(Duration::from_secs(5), 4)?;
/// assert_eq!(options.max_scans(), 4);
/// # Ok::<(), catga_core::CatgaError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisPendingReclaimOptions {
    minimum_idle: Duration,
    minimum_idle_millis: u64,
    max_scans: usize,
}

impl RedisPendingReclaimOptions {
    /// Creates a reclaim policy after validating Redis's millisecond command boundary.
    pub fn new(minimum_idle: Duration, max_scans: usize) -> CatgaResult<Self> {
        let minimum_idle_millis = u64::try_from(minimum_idle.as_millis()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "Redis pending reclaim idle duration exceeds Redis millisecond precision",
            )
        })?;
        if minimum_idle_millis == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Redis pending reclaim idle duration must be at least one millisecond",
            ));
        }
        if !(1..=MAX_REDIS_PENDING_RECLAIM_SCANS).contains(&max_scans) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!(
                    "Redis pending reclaim scan limit must be between 1 and {MAX_REDIS_PENDING_RECLAIM_SCANS}",
                ),
            ));
        }
        Ok(Self {
            minimum_idle,
            minimum_idle_millis,
            max_scans,
        })
    }

    /// Returns the minimum idle duration required before ownership can move to this consumer.
    pub fn minimum_idle(&self) -> Duration {
        self.minimum_idle
    }

    /// Returns the maximum number of one-entry scans performed before waiting for Redis again.
    pub fn max_scans(&self) -> usize {
        self.max_scans
    }

    pub(crate) fn minimum_idle_millis(&self) -> u64 {
        self.minimum_idle_millis
    }
}

impl Default for RedisPendingReclaimOptions {
    fn default() -> Self {
        Self {
            minimum_idle: Duration::from_secs(30),
            minimum_idle_millis: 30_000,
            max_scans: DEFAULT_REDIS_PENDING_RECLAIM_SCANS,
        }
    }
}

/// Redis Streams resources used by one Catga transport instance.
///
/// ```
/// use catga_redis::RedisConfig;
///
/// let config = RedisConfig {
///     server: "redis://127.0.0.1/".into(),
///     stream: "orders".into(),
///     group: "workers".into(),
///     consumer: "worker-a".into(),
/// };
/// assert_eq!(&*config.group, "workers");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisConfig {
    /// Redis server URL.
    pub server: Box<str>,
    /// Redis stream key used to publish envelopes.
    pub stream: Box<str>,
    /// Redis consumer group used to coordinate deliveries.
    pub group: Box<str>,
    /// Consumer name used for this transport instance.
    pub consumer: Box<str>,
}

/// Redis Pub/Sub resources used by one ephemeral broadcast transport instance.
///
/// Unlike [`RedisConfig`], this configuration is intentionally not backed by a Redis Stream:
/// messages published while no subscriber is connected are not retained or redelivered.
///
/// ```
/// use catga_redis::RedisPubSubConfig;
///
/// let config = RedisPubSubConfig {
///     server: "redis://127.0.0.1/".into(),
///     channel: "orders.notifications".into(),
/// };
/// assert_eq!(&*config.channel, "orders.notifications");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisPubSubConfig {
    /// Redis server URL.
    pub server: Box<str>,
    /// Redis channel used for both publication and subscription.
    pub channel: Box<str>,
}
