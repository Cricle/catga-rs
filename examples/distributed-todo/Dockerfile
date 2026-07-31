FROM rust:1.96-bookworm AS build

WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p catga-examples \
    --bin distributed_todo_api \
    --bin distributed_todo_worker

FROM debian:bookworm-slim

COPY --from=build /workspace/target/release/distributed_todo_api /usr/local/bin/distributed_todo_api
COPY --from=build /workspace/target/release/distributed_todo_worker /usr/local/bin/distributed_todo_worker
