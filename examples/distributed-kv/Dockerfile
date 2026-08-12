# Builds the distributed-kv example binary from the workspace root.
# Build context must be the workspace root:
#   docker build -f examples/distributed-kv/Dockerfile -t catga-distributed-kv:local .
#
# Networks without direct crates.io access can redirect the registry:
#   docker build --build-arg CARGO_REGISTRY_SPARSE_MIRROR=sparse+https://rsproxy.cn/index/ ...
FROM rust:1.96-bookworm AS build

ARG CARGO_REGISTRY_SPARSE_MIRROR=""
RUN if [ -n "$CARGO_REGISTRY_SPARSE_MIRROR" ]; then \
      mkdir -p "$CARGO_HOME" \
      && printf '[source.crates-io]\nreplace-with = "mirror"\n[source.mirror]\nregistry = "%s"\n' \
         "$CARGO_REGISTRY_SPARSE_MIRROR" > "$CARGO_HOME/config.toml"; \
    fi

# Install MUSL target and cross-compiler
RUN rustup target add x86_64-unknown-linux-musl \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       musl-tools \
       protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .

# Static build with MUSL
RUN cargo build --locked --release \
    --target x86_64-unknown-linux-musl \
    -p distributed-kv --bin distributed-kv

FROM scratch

# Copy the static binary (no SSL certs needed - using rustls)
COPY --from=build /workspace/target/x86_64-unknown-linux-musl/release/distributed-kv /distributed-kv

ENTRYPOINT ["/distributed-kv"]
