//! Build script: generates the gRPC KV service code from proto/kv.proto.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/kv.proto"], &["proto"])?;
    Ok(())
}
