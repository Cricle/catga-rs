# Catga Auto 示例替换计划

## 目标

用 `catga-auto` 取代主要 CQRS/HTTP 示例中重复的
`Registry + Mediator + MediatorHandle::bind` 启动样板，同时保持 Transport、Flow、Cluster、存储与任务生命周期由应用显式持有。

## 范围

替换：

- `examples/src/bin/mediator.rs`
- `examples/src/bin/axum_checkout.rs`
- `examples/src/order_service/service.rs`
- `examples/src/order_service/in_memory.rs`

保留底层 API，不做 facade 替换：

- `examples/src/bin/flow.rs`
- `examples/src/bin/memory_transport.rs`
- `examples/src/bin/typed_mediator.rs`
- distributed Todo 的 NATS publisher、event store、projection runner、`CompetingConsumer`。

distributed Todo 的 API 和 worker 当前没有 mediator handler；仅为获得 shutdown token 而引入 `AutoApp` 没有价值，也会掩盖资源所有权。因此保持其 `CancellationToken`、NATS 与 consumer 生命周期显式，除非先为 `catga-auto` 增加真正的 runner 组合 API。

## 设计

1. 在 `catga-auto` 新增：

```rust
pub fn mediator_arc(&self) -> Arc<Mediator>
```

它只克隆 builder 已创建的 `Arc<Mediator>`，不创建任务、不新增分配层，也不绑定 Axum。

2. `mediator.rs` 使用：

```rust
let app = AutoApp::builder()
    .register_request::<Double, _>(request_handler(|value: Double| async move {
        Ok(value.0 * 2)
    }))?
    .build()?;
let result = app.mediator().send(Double(21)).await?;
```

3. `axum_checkout.rs` 将 `build_mediator()` 替换为 `build_app()`；保留 `request_handler_with` 及业务函数，使用 `app.mediator_arc()` 作为现有 `MediatorState` 的 Axum state。Correlation、trace、JSON 解析和错误映射不变。

4. `OrderService::in_memory()` 通过 `AutoApp::builder()` 注册 request、command、event handler。`OrderRuntime` 从应用获取的 `MediatorHandle` 初始化，避免手写 `MediatorHandle::new()` 与后续 `bind()`。服务继续保存 mediator arc 供现有 Axum extractor 使用，内存 event store、outbox、transport、Flow、cluster 都不移动到 facade 内。

## TDD 步骤

1. 在 `crates/catga-auto/tests/builder.rs` 先添加 `mediator_arc` 测试：通过 arc 克隆发送 `Ping`；运行失败后实现 accessor。
2. 运行 `cargo test -p catga-auto --test builder` 与严格 clippy。
3. 替换 `mediator.rs`，运行 `cargo run -p catga-examples --bin mediator`，输出仍为 `21 doubled is 42`。
4. 替换 `axum_checkout.rs`，运行该 binary 的 `cargo check` 和现有 Axum tests。
5. 替换 order service，先添加/调整一个构造后 request dispatch 测试，运行 `cargo test -p catga-examples --test order_service`。
6. 最终运行：

```bash
cargo fmt --all -- --check
cargo clippy -p catga-examples --all-targets --all-features -- -D warnings
cargo test -p catga-examples --all-features
git diff --check
```

完整 distributed Todo Docker E2E 继续由 CI 执行。
