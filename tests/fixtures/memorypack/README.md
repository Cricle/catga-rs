# MemoryPack v1 fixtures

These binary fixtures are the compatibility oracle for the pure-Rust
MemoryPack codec. They are immutable input data: Rust tests verify their
manifest SHA-256 values before decoding them and never require a .NET runtime.

`v1/manifest.json` records the type, sample, formatter, byte length and
SHA-256 of every payload. Each supported reference type has null, empty,
non-empty, and Unicode samples. `DeadLetterMessage` is a non-nullable value
type in upstream Catga, so it has empty, non-empty, and Unicode samples only.

The generic `StoredSnapshot<TState>` formatter is deliberately not covered
here: its nested state payload is application-owned and requires a separately
declared MemoryPack-compatible state schema. RabbitMQ/AMQP and HTTP health
routes are outside this fixture set.
