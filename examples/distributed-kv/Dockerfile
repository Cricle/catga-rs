# Builds the distributed-kv example binary from the workspace root.
# Build context must be the workspace root:
#   podman build -f examples/distributed-kv/Dockerfile -t localhost/catga-distributed-kv:local .
#
# Networks without direct crates.io access can redirect the registry:
#   podman build --build-arg CARGO_REGISTRY_SPARSE_MIRROR=sparse+https://rsproxy.cn/index/ ...
FROM rust:1.96-bookworm AS build

ARG CARGO_REGISTRY_SPARSE_MIRROR=""
RUN if [ -n "$CARGO_REGISTRY_SPARSE_MIRROR" ]; then \
      mkdir -p "$CARGO_HOME" \
      && printf '[source.crates-io]\nreplace-with = "mirror"\n[source.mirror]\nregistry = "%s"\n' \
         "$CARGO_REGISTRY_SPARSE_MIRROR" > "$CARGO_HOME/config.toml"; \
    fi

# catga-sorock builds protobuf at compile time: sorock's gRPC build script
# needs protoc on PATH, and raft-proto's protobuf-build rejects protoc >= 4
# (and the 2x line). bookworm ships protoc 3.21.x, which satisfies both.
# libprotobuf-dev carries the well-known type imports (google/protobuf/*.proto)
# that sorock.proto references; protobuf-compiler alone ships only the binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p distributed-kv --bin distributed-kv

FROM debian:bookworm-slim

# The slim base ships no CA certificates; reqwest(rustls) needs them for client
# construction even on plain HTTP. Copy the bundle from the build stage (offline-safe).
COPY --from=build /etc/ssl/certs /etc/ssl/certs
COPY --from=build /workspace/target/release/distributed-kv /usr/local/bin/distributed-kv
ENTRYPOINT ["/usr/local/bin/distributed-kv"]
