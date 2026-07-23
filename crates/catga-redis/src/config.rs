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
