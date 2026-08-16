fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Zero-copy payloads: `payload`/`payloads` are opaque raft-protobuf
        // bytes, so surface them as `bytes::Bytes` instead of `Vec<u8>` and
        // drop the per-message copies on both the send and receive paths.
        .bytes(["payload", "payloads"])
        .compile_protos(&["proto/raft.proto"], &["proto"])?;
    Ok(())
}
