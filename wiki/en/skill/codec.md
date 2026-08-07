# Codec, Compression, and Message Signing

## 1. Codec Contracts (`catga-core`)

| Contract | Purpose |
| --- | --- |
| `PayloadEncoder<T>` / `PayloadDecoder<T>` | Format-agnostic typed payload codec |
| `EnvelopeCodec` | Transport envelope frame codec (NATS/Redis default MemoryPack) |
| `SnapshotCodec` | Aggregate/state machine snapshot codec |
| `CachedResultCodec` | Idempotency/cache result codec |

To use a custom format: implement the corresponding contract and inject via `*_with_codec` constructors (e.g., `NatsTransport::connect_with_codec`, store's `connect_*_with_codec`).

## 2. MemoryPack (`catga-codec-memorypack`, default)

Catga's vendored bounded MemoryPack implementation (corresponding to crates.io `memorypack` 1.2.2), default codec for envelope/snapshot/RPC:

```rust,ignore
use catga_codec_memorypack::MemoryPackCodec;

let codec = MemoryPackCodec::default();
let frame = codec.encode_value(&42_u64)?;
let value: u64 = codec.decode_value(&frame)?;
```

- **Decode safety**: `MemoryPackDecodeLimits` is applied before decoding untrusted frames; each frame must be fully consumed.
- Application model derive (feature `derive`): `#[derive(MemoryPackable)]` (re-exported from `catga_memorypack_derive`, no second memorypack dependency needed).
- Convenient aliases: `MemoryPackTransport<T> = TypedTransport<T, MemoryPackCodec>`, `MemoryPackDelivery<M>`, `MemoryPackProcessOutcome`.
- Ready-made components: `MemoryPackRequestClient` / `MemoryPackRequestClientFactory` (RPC), `MemoryPackScheduledOutbox` (scheduled outbox, see [reliability.md](reliability.md)), `MemoryPackSnapshotCodec`.

## 3. bincode (`catga-codec-bincode`)

Standalone `bincode-next` payload codec (`Encode`/`Decode` re-exported from crate):

```rust,ignore
use catga_codec_bincode::BincodeCodec;
use catga_core::{PayloadDecoder, PayloadEncoder};

let codec = BincodeCodec;
let frame = <BincodeCodec as PayloadEncoder<u64>>::encode_payload(&codec, &42)?;
let value = <BincodeCodec as PayloadDecoder<u64>>::decode_payload(&codec, &frame)?;
```

Frame limit `MAX_BINCODE_FRAME_BYTES`. Decoupled from envelope codec — choosing it does not bind Core's serialization format.

## 4. Compression (`catga-core`)

```rust,ignore
use catga_core::{CompressionAlgorithm, compress, decompress_limited, is_compressed};

let packed = compress(&payload, CompressionAlgorithm::Brotli)?;   // None/Gzip/Brotli/Deflate
if is_compressed(&packed) {
    let plain = decompress_limited(&packed, MAX_LIMIT)?;          // Decompression bomb protection
}
```

- `decompress(data)` uses default limit `DEFAULT_MAX_DECOMPRESSED_BYTES = 64 MiB`; untrusted data **must** use `decompress_limited` with an application-level limit.
- `compress_into` / `compress_to_slice` reuse buffers; `CompressionStats` available for metrics collection.

## 5. Message Signing (`catga-core`)

```rust,ignore
use catga_core::{HmacMessageSigner, MessageSigner};

let signer = HmacMessageSigner::new(b"shared-secret")?;    // HMAC-SHA256, empty key -> Validation
let signature = signer.sign(payload);                      // Base64 signature
signer.verify(payload, &signature);                        // Constant-time comparison
```

Use cases: Raft/cluster frame and cross-boundary message integrity authentication. Store keys in a key manager; during rotation, accept both old and new keys at the application boundary.

## Selection Guide

| Scenario | Choice |
| --- | --- |
| Default (envelope/snapshot/RPC) | MemoryPack (bounded, fast, with decode limits) |
| Existing bincode ecosystem | `BincodeCodec` + `*_with_codec` injection |
| Large payload in production | Compress first (Brotli/Gzip), decompress with `decompress_limited` at receiver |
| Cross untrusted boundaries | Payload + `HmacMessageSigner` signing |
