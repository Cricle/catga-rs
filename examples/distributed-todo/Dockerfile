# Builds the distributed-kv example binary from the workspace root.
# Build context must be the workspace root (see compose.yaml).
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

WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p distributed-kv --bin distributed-kv

FROM debian:bookworm-slim

# The slim base ships no CA certificates; reqwest(rustls) needs them for client
# construction even on plain HTTP. Copy the bundle from the build stage (offline-safe).
COPY --from=build /etc/ssl/certs /etc/ssl/certs
COPY --from=build /workspace/target/release/distributed-kv /usr/local/bin/distributed-kv
ENTRYPOINT ["/usr/local/bin/distributed-kv"]
