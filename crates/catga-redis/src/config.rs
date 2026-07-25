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
