# 测试、文档与性能基准化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 catga-rs 建立完整测试覆盖、文档体系和性能基准，确保性能 >10M ops/s

**Architecture:** 按模块逐个加强测试+文档+性能。catga-core 为基础模块，catga-service、catga-flow、传输层依次展开。

**Tech Stack:** Rust, cargo test, cargo doc, criterion (性能基准), tarpaulin (覆盖率)

## Global Constraints

| 约束项 | 值 |
|--------|-----|
| Mediator 性能目标 | > 10M ops/s (单线程) |
| TypedMediator 性能目标 | > 5M ops/s (单线程) |
| 测试覆盖率门槛 | ≥ 80% (按行) |
| 文档标准 | 所有公共 API 有 rustdoc + doctest |
| CI 检查 | 单元测试阻塞 PR，性能测试可选 |
| 文档语言 | 双语 zh/en |

---

## Phase 1: catga-core

### Task 1: 创建 Mediator 性能基准测试

**Files:**
- Create: `crates/catga-core/tests/mediator_performance.rs`
- Test: `crates/catga-core/tests/mediator_performance.rs`

**Interfaces:**
- Produces: `mediator_throughput_benchmark` - 测量 Mediator::send 吞吐量

- [ ] **Step 1: 创建性能测试文件**

```rust
//! Mediator throughput performance benchmark.
//!
//! Run: `cargo test -p catga-core --test mediator_performance -- --ignored --nocapture`

use std::time::Instant;
use catga_core::{CatgaResult, Mediator, Message, Registry, Request, catga_handlers, request_handler};

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[tokio::test]
#[ignore = "manual performance benchmark"]
async fn mediator_throughput_benchmark() -> CatgaResult<()> {
    const COUNT: usize = 10_000_000;
    let mediator = Mediator::new(catga_handlers! {
        request Ping => request_handler(|msg: Ping| async move { Ok(msg.0) })
    })?;

    let start = Instant::now();
    for _ in 0..COUNT {
        // 同步发送，无 await
        let _ = mediator.try_send(Ping(1));
    }
    let elapsed = start.elapsed();

    let ops_per_sec = COUNT as f64 / elapsed.as_secs_f64();
    println!("mediator_throughput: {} ops/s", ops_per_sec as u64);
    assert!(ops_per_sec > 10_000_000.0, "Mediator should achieve >10M ops/s");
    Ok(())
}
```

- [ ] **Step 2: 运行测试验证性能**

Run: `cargo test -p catga-core --test mediator_performance -- --ignored --nocapture`
Expected: 输出 > 10M ops/s

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/tests/mediator_performance.rs
git commit -m "perf: add mediator throughput benchmark (>10M ops/s target)"
```

---

### Task 2: 创建 TypedMediator 性能基准测试

**Files:**
- Create: `crates/catga-core/tests/typed_mediator_performance.rs`
- Test: `crates/catga-core/tests/typed_mediator_performance.rs`

- [ ] **Step 1: 创建 TypedMediator 性能测试**

```rust
//! TypedMediator throughput performance benchmark.

use std::time::Instant;
use catga_core::{CatgaResult, Mediator, Message, Registry, Request, catga_handlers, request_handler};

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[tokio::test]
#[ignore = "manual performance benchmark"]
async fn typed_mediator_throughput_benchmark() -> CatgaResult<()> {
    const COUNT: usize = 5_000_000;
    let mediator = Mediator::new(catga_handlers! {
        request Ping => request_handler(|msg: Ping| async move { Ok(msg.0) })
    })?;

    let start = Instant::now();
    for _ in 0..COUNT {
        let _ = mediator.try_send(Ping(1));
    }
    let elapsed = start.elapsed();

    let ops_per_sec = COUNT as f64 / elapsed.as_secs_f64();
    println!("typed_mediator_throughput: {} ops/s", ops_per_sec as u64);
    assert!(ops_per_sec > 5_000_000.0, "TypedMediator should achieve >5M ops/s");
    Ok(())
}
```

- [ ] **Step 2: 运行测试验证**

Run: `cargo test -p catga-core --test typed_mediator_performance -- --ignored --nocapture`

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/tests/typed_mediator_performance.rs
git commit -m "perf: add TypedMediator throughput benchmark"
```

---

### Task 3: 补充 catga-core 单元测试覆盖率

**Files:**
- Modify: `crates/catga-core/src/registry.rs` (检查缺少测试的函数)
- Modify: `crates/catga-core/src/handler.rs` (补充 Handler trait 测试)
- Create: `crates/catga-core/tests/registry_comprehensive.rs`

**Interfaces:**
- Consumes: Registry, Handler traits
- Produces: 补充 registry.rs 的测试覆盖

- [ ] **Step 1: 检查现有测试覆盖率**

Run: `cargo coverage --manifest-path crates/catga-core/Cargo.toml 2>/dev/null || echo "coverage tool not available"`

- [ ] **Step 2: 补充 Registry 测试**

在 `crates/catga-core/tests/registry_memory.rs` 补充测试：
```rust
#[tokio::test]
async fn registry_rejects_duplicate_request_handler() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(Double)?;
    let result = registry.register_request::<Ping, _>(Double);
    assert!(matches!(result, Err(e) if e.code() == ErrorCode::Conflict));
    Ok(())
}

#[tokio::test]
async fn registry_rejects_duplicate_command_handler() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_command::<Add, _>(AddTo(Arc::new(AtomicUsize::new(0))))?;
    // 尝试重复注册同一命令
    let result = registry.register_command::<Add, _>(AddTo(Arc::new(AtomicUsize::new(0))));
    assert!(matches!(result, Err(e) if e.code() == ErrorCode::Conflict));
    Ok(())
}
```

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/tests/registry_memory.rs
git commit -m "test: add comprehensive registry conflict detection tests"
```

---

### Task 4: 补充 catga-core rustdoc 和 doctest

**Files:**
- Modify: `crates/catga-core/src/registry.rs`
- Modify: `crates/catga-core/src/handler.rs`
- Modify: `crates/catga-core/src/mediator.rs`

- [ ] **Step 1: 检查缺失文档的公共 API**

Run: `cargo doc --workspace --no-deps 2>&1 | grep "warning: missing documentation"`

- [ ] **Step 2: 补充 Registry 文档**

在 `Registry` impl 块添加文档：
```rust
/// Registers a request handler for the specified message type.
///
/// # Errors
/// Returns [`ErrorCode::Conflict`] if a handler for this message type
/// is already registered.
pub fn register_request<M: Message + Request, H: Handler<M>>(&mut self, handler: H) -> CatgaResult<&mut Self> {
    // ...
}
```

- [ ] **Step 3: 验证文档编译**

Run: `cargo doc -p catga-core --no-deps`

- [ ] **Step 4: 提交**

```bash
git add crates/catga-core/src/registry.rs crates/catga-core/src/handler.rs
git commit -m "docs: add missing rustdoc for Registry and Handler"
```

---

## Phase 2: catga-service 宏

### Task 5: 创建 #[catga_service] 宏综合测试

**Files:**
- Create: `crates/catga-core/tests/catga_service_macro.rs`
- Test: `crates/catga-core/tests/catga_service_macro.rs`

**Interfaces:**
- Consumes: #[catga_service] 宏, CatgaResult
- Produces: registry() 函数, 测试覆盖

- [ ] **Step 1: 创建宏测试文件**

```rust
//! Comprehensive tests for #[catga_service] macro

use catga_core::{auto::AutoApp, CatgaResult};

#[catga_core::catga_request(response = u64)]
struct Double(u64);

#[derive(catga_core::catga_command)]
struct Log(String);

#[derive(catga_core::catga_event)]
struct OrderCreated { order_id: u64 }

struct TestService;

#[catga_core::catga_service]
impl TestService {
    // Request handler: CatgaResult<T> where T != ()
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }

    // Command handler: CatgaResult<()>
    async fn log(&self, msg: Log) -> CatgaResult<()> {
        println!("[TestService] {}", msg.0);
        Ok(())
    }

    // Event handler: publishes event
    async fn on_order_created(&self, event: OrderCreated) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn catga_service_generates_registry_function() -> CatgaResult<()> {
    let registry = TestService::registry()?;
    let app = AutoApp::from_registry(registry)?;

    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);

    app.mediator().send_command(Log("test".to_string())).await?;
    Ok(())
}

#[tokio::test]
async fn catga_service_detects_request_vs_command() -> CatgaResult<()> {
    // Double 返回 CatgaResult<u64> -> Request
    // Log 返回 CatgaResult<()> -> Command
    let registry = TestService::registry()?;
    let app = AutoApp::from_registry(registry)?;

    // Request 有响应
    let response: u64 = app.mediator().send(Double(5)).await?;
    assert_eq!(response, 10);

    // Command 无响应
    app.mediator().send_command(Log("hello".to_string())).await?;
    Ok(())
}
```

- [ ] **Step 2: 运行测试**

Run: `cargo test -p catga-core --test catga_service_macro`

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/tests/catga_service_macro.rs
git commit -m "test: add comprehensive #[catga_service] macro tests"
```

---

### Task 6: 补充 #[catga_service] 宏文档

**Files:**
- Modify: `crates/catga-core/src/macros/proc-macros/src/lib.rs`
- Modify: `crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs`

- [ ] **Step 1: 添加宏文档**

在 `lib.rs` 的 `catga_service` proc_macro_attribute 添加文档：

```rust
/// Scans an impl block for async methods and generates handler registrations.
///
/// # Overview
/// This macro automatically detects handler types based on method signatures:
/// - `async fn method(&self, msg: M) -> CatgaResult<T>` where `T != ()` → Request handler
/// - `async fn method(&self, msg: M) -> CatgaResult<()>` → Command handler
/// - `async fn method(&self, event: E) -> CatgaResult<()>` → Event handler
///
/// # Usage
/// ```
/// use catga_core::{CatgaResult, auto::AutoApp};
///
/// #[catga_core::catga_request(response = u64)]
/// struct Double(u64);
///
/// struct Calculator;
///
/// #[catga_core::catga_service]
/// impl Calculator {
///     async fn double(&self, msg: Double) -> CatgaResult<u64> {
///         Ok(msg.0 * 2)
///     }
/// }
///
/// # async fn example() -> CatgaResult<()> {
/// let app = AutoApp::from_registry(Calculator::registry()?)?;
/// let result = app.mediator().send(Double(21)).await?;
/// assert_eq!(result, 42);
/// # Ok(())
/// # }
/// ```
```

- [ ] **Step 2: 验证文档**

Run: `cargo doc -p catga-core --no-deps`

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/src/macros/proc-macros/src/lib.rs
git commit -m "docs: add comprehensive documentation for #[catga_service] macro"
```

---

## Phase 3: catga-flow

### Task 7: 创建 Flow 性能基准测试

**Files:**
- Modify: `tests/flow_performance.rs`
- Create: `crates/catga-core/tests/flow_performance.rs`

**Interfaces:**
- Consumes: Flow, DslFlow
- Produces: flow_execution_throughput, dsl_flow_execution_throughput

- [ ] **Step 1: 检查现有 Flow 性能测试**

Read: `tests/flow_performance.rs`

- [ ] **Step 2: 补充单步骤 Flow 性能测试**

在 `crates/catga-core/tests/flow_performance.rs` 添加：

```rust
//! catga-core Flow performance benchmarks

#[tokio::test]
#[ignore = "manual performance benchmark"]
async fn single_step_flow_throughput() -> CatgaResult<()> {
    const COUNT: usize = 1_000_000;

    let flow = Flow::new("single-step")
        .step(|| async { Ok(()) }, || async { Ok(()) });

    let start = Instant::now();
    for _ in 0..COUNT {
        flow.clone().run().await?;
    }
    let elapsed = start.elapsed();

    let ops_per_sec = COUNT as f64 / elapsed.as_secs_f64();
    println!("single_step_flow_throughput: {} ops/s", ops_per_sec as u64);
    // 空操作 Flow 应该 > 1M ops/s
    assert!(ops_per_sec > 1_000_000.0);
    Ok(())
}
```

- [ ] **Step 3: 运行测试**

Run: `cargo test -p catga-core --test flow_performance -- --ignored --nocapture`

- [ ] **Step 4: 提交**

```bash
git add crates/catga-core/tests/flow_performance.rs
git commit -m "perf: add single-step Flow throughput benchmark"
```

---

### Task 8: 补充 Flow 单元测试和文档

**Files:**
- Create: `crates/catga-core/tests/flow_comprehensive.rs`
- Modify: `crates/catga-core/src/flow/mod.rs`

- [ ] **Step 1: 创建 Flow 综合测试**

```rust
//! Comprehensive tests for Flow and DslFlow

use catga_core::flow::{Flow, DslFlow, dsl_action, FlowResult};

#[tokio::test]
async fn flow_runs_all_steps_in_sequence() -> CatgaResult<()> {
    let flow = Flow::new("test")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) });

    let result = flow.run().await;
    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 2);
    Ok(())
}

#[tokio::test]
async fn flow_runs_compensation_on_failure() -> CatgaResult<()> {
    let compensation_run = Arc::new(AtomicBool::new(false));
    let comp = Arc::clone(&compensation_run);

    let flow = Flow::new("compensation-test")
        .step(
            || async { Err(CatgaError::new(ErrorCode::Internal, "simulated failure")) },
            move || async {
                comp.store(true, Ordering::SeqCst);
                Ok(())
            },
        );

    let result = flow.run().await;
    assert!(result.is_failure());
    assert!(compensation_run.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn dsl_flow_updates_state() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }));

    let mut state = 0_u32;
    flow.run(&mut state).await?;
    assert_eq!(state, 2);
    Ok(())
}
```

- [ ] **Step 2: 补充 Flow rustdoc**

在 `crates/catga-core/src/flow/mod.rs` 添加文档：

```rust
/// Creates a new named flow for step-by-step execution with compensation.
///
/// # Example
/// ```
/// use catga_core::flow::Flow;
///
/// # async fn example() -> catga_core::CatgaResult<()> {
/// let flow = Flow::new("checkout")
///     .step(
///         || async { /* reserve inventory */ Ok(()) },
///         || async { /* release inventory */ Ok(()) },
///     )
///     .step(
///         || async { /* charge payment */ Ok(()) },
///         || async { /* refund payment */ Ok(()) },
///     );
///
/// let result = flow.run().await;
/// assert!(result.is_success());
/// # Ok(())
/// # }
/// ```
pub fn new(name: impl Into<String>) -> Flow { ... }
```

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/tests/flow_comprehensive.rs crates/catga-core/src/flow/mod.rs
git commit -m "test+docs: add comprehensive Flow tests and documentation"
```

---

## Phase 4: 传输层

### Task 9: 创建 NATS 传输性能基准测试

**Files:**
- Modify: `tests/nats_performance.rs`
- Create: `crates/catga-nats/tests/performance.rs`

- [ ] **Step 1: 检查现有 NATS 性能测试**

Read: `tests/nats_performance.rs`

- [ ] **Step 2: 创建模块级 NATS 测试**

在 `crates/catga-nats/tests/` 创建性能测试

- [ ] **Step 3: 提交**

```bash
git add crates/catga-nats/tests/
git commit -m "perf: add NATS transport performance benchmarks"
```

---

### Task 10: 创建 Redis 传输性能基准测试

**Files:**
- Create: `crates/catga-redis/tests/performance.rs`

- [ ] **Step 1: 创建 Redis 性能测试**

```rust
//! Redis transport performance benchmarks

#[tokio::test]
#[ignore = "manual performance benchmark"]
async fn redis_publish_subscribe_throughput() -> CatgaResult<()> {
    // 测试 publish/subscribe 吞吐量
}
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-redis/tests/performance.rs
git commit -m "perf: add Redis transport performance benchmarks"
```

---

### Task 11: 创建 catga-robustmq 性能基准测试

**Files:**
- Create: `crates/catga-robustmq/tests/performance.rs`

- [ ] **Step 1: 创建 robustmq 性能测试**

```rust
//! RobustMQ performance benchmarks

#[tokio::test]
#[ignore = "manual performance benchmark"]
async fn priority_queue_throughput() -> CatgaResult<()> {
    // 测试优先级队列吞吐量
}
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-robustmq/tests/performance.rs
git commit -m "perf: add RobustMQ performance benchmarks"
```

---

## Phase 5: CI 配置

### Task 12: 配置覆盖率门槛

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `codecov.yml`

- [ ] **Step 1: 检查现有 CI 配置**

Read: `.github/workflows/ci.yml`

- [ ] **Step 2: 添加覆盖率检查**

```yaml
coverage:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Run tests with coverage
      run: |
        # 使用 tarpaulin 或 cargo-llvm-cov
        cargo llvm-cov --workspace --lcov --output-path lcov.info
    - name: Upload coverage
      uses: codecov/codecov-action@v4
      with:
        files: lcov.info
        fail_ci_if_error: true
        threshold: 80%
```

- [ ] **Step 3: 提交**

```bash
git add .github/workflows/ci.yml codecov.yml
git commit -m "ci: add coverage threshold (80%) check"
```

---

### Task 13: 更新 performance.sh 脚本

**Files:**
- Modify: `scripts/performance.sh`

- [ ] **Step 1: 添加新基准测试**

在脚本中添加对新增性能测试的调用：
```bash
# core benchmarks
cargo test -p catga-core --test mediator_performance --release -- --ignored --nocapture
cargo test -p catga-core --test typed_mediator_performance --release -- --ignored --nocapture
cargo test -p catga-core --test flow_performance --release -- --ignored --nocapture
```

- [ ] **Step 2: 提交**

```bash
git add scripts/performance.sh
git commit -m "perf: update performance.sh with new benchmark targets"
```

---

## 验收检查清单

- [ ] `cargo test --workspace` 全部通过
- [ ] `cargo doc --workspace --no-deps` 生成完整文档
- [ ] `cargo test --doc --workspace` 所有 doctest 通过
- [ ] Mediator 性能 > 10M ops/s
- [ ] TypedMediator 性能 > 5M ops/s
- [ ] 代码覆盖率 ≥ 80%
- [ ] CI 配置正确，单元测试阻塞 PR
- [ ] 文档提供双语版本 (zh/en)
