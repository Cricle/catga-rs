use std::time::Duration;

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// Largest number of one-entry pending scans made before a receive waits for Redis again.
pub const MAX_REDIS_PENDING_RECLAIM_SCANS: usize = 64;

const DEFAULT_REDIS_PENDING_RECLAIM_SCANS: usize = 16;

/// Bounded Redis Streams recovery policy for deliveries abandoned by another consumer.
///
/// Every scan examines at most one pending entry and claims at most one eligible entry. The
/// transport carries Redis's cursor between receive attempts, so a group with many non-idle
/// entries is traversed incrementally without copying its pending list into process memory.
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisPubSubConfig {
    /// Redis server URL.
    pub server: Box<str>,
    /// Redis channel used for both publication and subscription.
    pub channel: Box<str>,
}
