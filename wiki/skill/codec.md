# 编解码、压缩与消息签名

## 1. Codec 契约（`catga-core`）

| 契约 | 用途 |
| --- | --- |
| `PayloadEncoder<T>` / `PayloadDecoder<T>` | 格式无关的 typed 载荷编解码 |
| `EnvelopeCodec` | transport envelope 帧编解码（NATS/Redis 默认 MemoryPack） |
| `SnapshotCodec` | 聚合/状态机快照编解码 |
| `CachedResultCodec` | 幂等/缓存结果的编解码 |

接入自定义格式：实现对应契约并通过 `*_with_codec` 构造函数注入（如 `NatsTransport::connect_with_codec`、存储的 `connect_*_with_codec`）。

## 2. MemoryPack（`catga-codec-memorypack`，默认）

Catga vendored 的有界 MemoryPack 实现（对应 crates.io `memorypack` 1.2.2），envelope/快照/RPC 的默认编解码：

```rust,ignore
use catga_codec_memorypack::MemoryPackCodec;

let codec = MemoryPackCodec::default();
let frame = codec.encode_value(&42_u64)?;
let value: u64 = codec.decode_value(&frame)?;
```

- **解码安全**：`MemoryPackDecodeLimits` 在解码不可信帧前应用；每帧必须被完整消费。
- 应用模型 derive（feature `derive`）：`#[derive(MemoryPackable)]`（`catga_memorypack_derive` 重导出，无需第二个 memorypack 依赖）。
- 便捷别名：`MemoryPackTransport<T> = TypedTransport<T, MemoryPackCodec>`、`MemoryPackDelivery<M>`、`MemoryPackProcessOutcome`。
- 现成构件：`MemoryPackRequestClient` / `MemoryPackRequestClientFactory`（RPC）、`MemoryPackScheduledOutbox`（定时 outbox，见 [reliability.md](reliability.md)）、`MemoryPackSnapshotCodec`。

## 3. bincode（`catga-codec-bincode`）

独立的 `bincode-next` 载荷编解码（`Encode`/`Decode` 已从 crate 重导出）：

```rust,ignore
use catga_codec_bincode::BincodeCodec;
use catga_core::{PayloadDecoder, PayloadEncoder};

let codec = BincodeCodec;
let frame = <BincodeCodec as PayloadEncoder<u64>>::encode_payload(&codec, &42)?;
let value = <BincodeCodec as PayloadDecoder<u64>>::decode_payload(&codec, &frame)?;
```

帧上限 `MAX_BINCODE_FRAME_BYTES`。与 envelope codec 解耦——选它不绑定 Core 的序列化格式。

## 4. 压缩（`catga-core`）

```rust,ignore
use catga_core::{CompressionAlgorithm, compress, decompress_limited, is_compressed};

let packed = compress(&payload, CompressionAlgorithm::Brotli)?;   // None/Gzip/Brotli/Deflate
if is_compressed(&packed) {
    let plain = decompress_limited(&packed, MAX_LIMIT)?;          // 解压炸弹防护
}
```

- `decompress(data)` 使用默认上限 `DEFAULT_MAX_DECOMPRESSED_BYTES = 64 MiB`；不可信数据**必须**用 `decompress_limited` 并给应用级上限。
- `compress_into` / `compress_to_slice` 复用缓冲区；`CompressionStats` 供指标采集。

## 5. 消息签名（`catga-core`）

```rust,ignore
use catga_core::{HmacMessageSigner, MessageSigner};

let signer = HmacMessageSigner::new(b"shared-secret")?;    // HMAC-SHA256，空 key → Validation
let signature = signer.sign(payload);                      // Base64 签名
signer.verify(payload, &signature);                        // 常数时间比较
```

用途：Raft/集群帧与跨边界消息的完整性认证。密钥放密钥管理器；轮换期在应用边界同时接受新旧两个 key。

## 选择建议

| 场景 | 选择 |
| --- | --- |
| 默认（envelope/快照/RPC） | MemoryPack（有界、快速、带解码限额） |
| 自有 bincode 生态 | `BincodeCodec` + `*_with_codec` 注入 |
| 大 payload 上线 | 先压缩（Brotli/Gzip），接收端 `decompress_limited` |
| 跨不信任边界 | 载荷 + `HmacMessageSigner` 签名 |
