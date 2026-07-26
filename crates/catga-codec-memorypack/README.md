# Catga MemoryPack codec

This crate embeds the Rust source of
[`Deathemonic/MemoryPack-rs`](https://github.com/Deathemonic/MemoryPack-rs), crates.io release
`memorypack` 1.2.2, revision `82737dd71b526b15a55d3af9e62666a30ea287be`.

The upstream MIT license remains in [LICENSE](LICENSE). Catga-specific changes are limited to
bounded untrusted-frame decoding, exact-frame validation, and the Catga payload adapter. Use
[`MemoryPackCodec`](src/codec.rs) at transport boundaries; it rejects oversized frames, allocation
bombs, invalid booleans, excessive nesting, and trailing bytes.

The upstream `circular` and `version_tolerant` derive modes are intentionally unavailable because
their wire layouts cannot currently enforce Catga's nesting and length budgets. Use regular
`MemoryPackable` records for transport payloads.
