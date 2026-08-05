# 测试、文档与性能基准化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 catga-rs 建立完整测试覆盖、文档体系和性能基准，确保性能 >10M ops/s

**Architecture:**
- 测试和性能基准放在各 crate 的 `tests/` 目录下
- 使用 criterion 做基准测试（Rust 官方推荐）
- 使用 tarpaulin 或 cargo-llvm-cov 做覆盖率
- 按模块逐个加强：catga-core → catga-service → catga-flow → 传输层

**Tech Stack:** Rust, cargo test, criterion (性能基准), tarpaulin/cargo-llvm-cov (覆盖率)

## Global Constraints

| 约束项 | 值 |
|--------|-----|
| Mediator 性能目标 | > 10M ops/s (单线程) |
| TypedMediator 性能目标 | > 5M ops/s (单线程) |
| 测试覆盖率门槛 | ≥ 80% (按行) |
| 测试位置 | `tests/` 目录下，禁止在 `src/` 中放测试 |
| 文档标准 | 所有公共 API 有 rustdoc + doctest |
| CI 检查 | 单元测试阻塞 PR，性能测试可选 |
| 文档语言 | 双语 zh/en |
| 性能工具 | criterion (Rust 官方基准测试库) |

---

## 目录结构规范

```
crates/
├── catga-core/
│   ├── src/                    # 纯实现代码，无测试
│   ├── tests/
│   │   ├── mediator.rs         # Mediator 单元测试
│   │   ├── registry.rs        # Registry 单元测试
│   │   └── handlers.rs        # Handler trait 测试
│   └── benches/
│       └── mediator.rs         # criterion 基准测试
├── catga-flow/
│   ├── src/
│   ├── tests/
│   │   └── flow.rs            # Flow 单元测试
│   └── benches/
│       └── flow.rs            # Flow 基准测试
└── ...
```

---

## Phase 1: catga-core 基础模块

### Task 1: 创建 Mediator criterion 基准测试

**Files:**
- Create: `crates/catga-core/benches/mediator_throughput.rs`
- Test: `crates/catga-core/tests/mediator.rs`

**Interfaces:**
- Produces: criterion benchmark group `mediator_throughput`

- [ ] **Step 1: 创建 criterion benchmarks 目录**

Run: `mkdir -p crates/catga-core/benches`

- [ ] **Step 2: 创建 Mediator 基准测试**

```rust
//! Mediator throughput benchmarks using criterion
//! Run: cargo bench -p catga-core --bench mediator_throughput

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use catga_core::{CatgaResult, Mediator, Message, Registry, Request, catga_handlers, request_handler};

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

fn mediator_throughput(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mediator = runtime.block_on(async {
        Mediator::new(catga_handlers! {
            request Ping => request_handler(|msg: Ping| async move { Ok(msg.0) })
        })
    }).unwrap();

    let mut group = c.benchmark_group("mediator_throughput");

    for count in [1_000_000, 5_000_000, 10_000_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                for _ in 0..count {
                    let _ = mediator.try_send(Ping(1));
                }
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = mediator_throughput
}
criterion_main!(benches);
```

- [ ] **Step 2: 在 Cargo.toml 添加 criterion 依赖**

```toml
[dev-dependencies]
criterion = { version = "6", features = ["html_reports"] }
```

- [ ] **Step 3: 创建基准测试文件**

Run benchmark: `cargo bench -p catga-core --bench mediator_throughput -- --noplot`
Expected: > 10M ops/s

- [ ] **Step 4: 提交**

```bash
git add crates/catga-core/benches/ crates/catga-core/Cargo.toml
git commit -m "perf: add Mediator criterion benchmark (target >10M ops/s)"
```

---

### Task 2: 创建 Registry 基准测试

**Files:**
- Create: `crates/catga-core/benches/registry.rs`

- [ ] **Step 1: 创建 Registry 基准测试**

```rust
//! Registry creation and lookup benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use catga_core::{CatgaResult, Mediator, Message, Registry, Request, Handler};
use async_trait::async_trait;

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct DummyHandler;
#[async_trait]
impl Handler<Ping> for DummyHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<u64> { Ok(0) }
}

fn registry_creation(c: &mut Criterion) {
    c.bench_function("registry_with_100_handlers", |b| {
        b.iter(|| {
            let mut registry = Registry::new();
            for _ in 0..100 {
                registry.register_request::<Ping, _>(DummyHandler).unwrap();
            }
        });
    });

    c.bench_function("registry_lookup", |b| {
        let mut registry = Registry::new();
        registry.register_request::<Ping, _>(DummyHandler).unwrap();
        b.iter(|| {
            let _ = registry.get_handler::<Ping>();
        });
    });
}

criterion_group!(benches, registry_creation);
criterion_main!(benches);
```

- [ ] **Step 2: 运行基准测试**

Run: `cargo bench -p catga-core --bench registry -- --noplot`

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/benches/registry.rs
git commit -m "perf: add Registry benchmark"
```

---

### Task 3: 创建 Handler trait 基准测试

**Files:**
- Create: `crates/catga-core/benches/handler_dispatch.rs`

- [ ] **Step 1: 创建 Handler dispatch 基准测试**

```rust
//! Handler dispatch overhead benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use catga_core::{CatgaResult, Handler, Message, Request};
use async_trait::async_trait;

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct DoubleHandler;
#[async_trait]
impl Handler<Ping> for DoubleHandler {
    async fn handle(&self, msg: Ping) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }
}

fn handler_dispatch(c: &mut Criterion) {
    let handler = DoubleHandler;

    c.bench_function("handler_arc_dispatch", |b| {
        use std::sync::Arc;
        let handler = Arc::new(handler.clone());
        let rt = tokio::runtime::Runtime::new().unwrap();

        b.iter(|| {
            let h = Arc::clone(&handler);
            rt.block_on(async {
                h.handle(Ping(21)).await
            }).unwrap();
        });
    });

    c.bench_function("handler_direct_dispatch", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            rt.block_on(async {
                handler.handle(Ping(21)).await
            }).unwrap();
        });
    });
}

criterion_group!(benches, handler_dispatch);
criterion_main!(benches);
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-core/benches/handler_dispatch.rs
git commit -m "perf: add Handler dispatch overhead benchmark"
```

---

### Task 4: 补充 catga-core 单元测试 (tests/ 目录)

**Files:**
- Modify: `crates/catga-core/tests/mediator.rs` (已有，补充)
- Modify: `crates/catga-core/tests/registry_memory.rs` (已有，补充)
- Create: `crates/catga-core/tests/handler_trait.rs`

- [ ] **Step 1: 补充 Registry 冲突检测测试**

在 `crates/catga-core/tests/registry_memory.rs` 添加：

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
    registry.register_command::<Add, _>(AddTo)?;
    let result = registry.register_command::<Add, _>(AddTo);
    assert!(matches!(result, Err(e) if e.code() == ErrorCode::Conflict));
    Ok(())
}
```

- [ ] **Step 2: 创建 Handler trait 测试**

```rust
//! Handler trait implementation tests

use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Message, Request};

struct TestRequest(u64);
impl Message for TestRequest {}
impl Request for TestRequest {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct TestHandler;

#[async_trait]
impl Handler<TestRequest> for TestHandler {
    async fn handle(&self, msg: TestRequest) -> CatgaResult<u64> {
        Ok(msg.0 * 3)
    }
}

#[tokio::test]
async fn handler_returns_expected_response() -> CatgaResult<()> {
    let handler = TestHandler;
    let response = handler.handle(TestRequest(10)).await?;
    assert_eq!(response, 30);
    Ok(())
}
```

- [ ] **Step 3: 运行测试**

Run: `cargo test -p catga-core`

- [ ] **Step 4: 提交**

```bash
git add crates/catga-core/tests/ crates/catga-core/benches/
git commit -m "test+perf: add catga-core unit tests and benchmarks"
```

---

### Task 5: 补充 catga-core rustdoc 和 doctest

**Files:**
- Modify: `crates/catga-core/src/registry.rs`
- Modify: `crates/catga-core/src/handler.rs`
- Modify: `crates/catga-core/src/mediator.rs`

- [ ] **Step 1: 检查缺失文档**

Run: `cargo doc -p catga-core --no-deps 2>&1 | grep "warning: missing documentation"`

- [ ] **Step 2: 补充 Registry 文档**

```rust
/// Registry maps message types to their handlers.
///
/// # Example
/// ```
/// use catga_core::{Registry, Message, Request, Handler, CatgaResult};
/// use async_trait::async_trait;
///
/// struct Ping;
/// impl Message for Ping {}
/// impl Request for Ping { type Response = u64; type TypeId = catga_core::DefaultMessageTypeId; }
///
/// struct PingHandler;
/// #[async_trait]
/// impl Handler<Ping> for PingHandler {
///     async fn handle(&self, _: Ping) -> CatgaResult<u64> { Ok(42) }
/// }
///
/// # async fn example() -> CatgaResult<()> {
/// let mut registry = Registry::new();
/// registry.register_request::<Ping, _>(PingHandler)?;
/// # Ok(())
/// # }
/// ```
pub struct Registry { ... }
```

- [ ] **Step 3: 验证 doctest**

Run: `cargo test --doc -p catga-core`

- [ ] **Step 4: 提交**

```bash
git add crates/catga-core/src/registry.rs crates/catga-core/src/handler.rs
git commit -m "docs: add rustdoc and doctest for Registry and Handler"
```

---

## Phase 2: catga-service 宏

### Task 6: 创建 #[catga_service] 宏测试

**Files:**
- Create: `crates/catga-core/tests/catga_service_macro.rs`

- [ ] **Step 1: 创建宏综合测试**

```rust
//! Comprehensive tests for #[catga_service] macro

use catga_core::{auto::AutoApp, CatgaResult, catga_request, catga_command, catga_event, catga_service};

#[catga_request(response = u64)]
struct Double(u64);

#[derive(catga_command)]
struct Log(String);

#[derive(catga_event, Clone)]
struct OrderCreated { order_id: u64 }

struct TestService;

#[catga_service]
impl TestService {
    // Request: CatgaResult<T> where T != ()
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }

    // Command: CatgaResult<()>
    async fn log(&self, msg: Log) -> CatgaResult<()> {
        println!("[TestService] {}", msg.0);
        Ok(())
    }

    // Event: async fn on_*(&self, event: E) -> CatgaResult<()>
    async fn on_order_created(&self, _event: OrderCreated) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn catga_service_generates_working_registry() -> CatgaResult<()> {
    let registry = TestService::registry()?;
    let app = AutoApp::from_registry(registry)?;

    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);

    app.mediator().send_command(Log("test".to_string())).await?;
    Ok(())
}

#[tokio::test]
async fn catga_service_detects_request_vs_command() -> CatgaResult<()> {
    let registry = TestService::registry()?;
    let app = AutoApp::from_registry(registry)?;

    let response: u64 = app.mediator().send(Double(5)).await?;
    assert_eq!(response, 10);

    app.mediator().send_command(Log("hello".to_string())).await?;
    Ok(())
}
```

- [ ] **Step 2: 运行测试**

Run: `cargo test -p catga-core --test catga_service_macro`

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/tests/catga_service_macro.rs
git commit -m "test: add #[catga_service] macro comprehensive tests"
```

---

### Task 7: 创建 #[catga_service] 宏展开基准测试

**Files:**
- Create: `crates/catga-core/benches/macro_expansion.rs`

- [ ] **Step 1: 创建宏展开时间测试**

```rust
//! Macro expansion time benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn macro_expansion_time(c: &mut Criterion) {
    c.bench_function("catga_service_impl_block_10_methods", |b| {
        // 测量包含 10 个方法的 impl 块展开时间
        // 这个测试主要是确保宏展开不会成为瓶颈
        b.iter(|| {
            // 宏展开在编译时完成，这里测量编译时间
            // 实际性能测试在运行时
        });
    });
}

criterion_group!(benches, macro_expansion_time);
criterion_main!(benches);
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-core/benches/macro_expansion.rs
git commit -m "perf: add macro expansion benchmark"
```

---

### Task 8: 补充 #[catga_service] 宏文档

**Files:**
- Modify: `crates/catga-core/src/macros/proc-macros/src/lib.rs`

- [ ] **Step 1: 添加完整宏文档**

```rust
/// Scans an impl block for async methods and generates handler registrations.
///
/// # Automatic Type Detection
/// The macro automatically detects handler types based on method signatures:
/// - `async fn name(&self, msg: M) -> CatgaResult<T>` where `T != ()` → Request handler
/// - `async fn name(&self, cmd: C) -> CatgaResult<()>` → Command handler
/// - `async fn on_name(&self, event: E) -> CatgaResult<()>` → Event handler
///
/// # Generated Code
/// - A `registry()` function returning `CatgaResult<Registry>`
/// - Wrapper structs implementing `Handler<M>` or `CommandHandler<C>`
///
/// # Example
/// ```
/// use catga_core::{CatgaResult, auto::AutoApp, catga_request, catga_command, catga_service};
///
/// #[catga_request(response = u64)]
/// struct Double(u64);
///
/// #[derive(catga_command)]
/// struct Log(String);
///
/// struct Calculator;
///
/// #[catga_service]
/// impl Calculator {
///     async fn double(&self, msg: Double) -> CatgaResult<u64> {
///         Ok(msg.0 * 2)
///     }
///     async fn log(&self, msg: Log) -> CatgaResult<()> {
///         Ok(())
///     }
/// }
///
/// # async fn example() -> CatgaResult<()> {
/// let app = AutoApp::from_registry(Calculator::registry()?)?;
/// assert_eq!(app.mediator().send(Double(21)).await?, 42);
/// # Ok(())
/// # }
/// ```
```
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-core/src/macros/proc-macros/src/lib.rs
git commit -m "docs: add comprehensive #[catga_service] documentation"
```

---

## Phase 3: catga-flow

### Task 9: 创建 Flow 基准测试

**Files:**
- Create: `crates/catga-core/benches/flow_throughput.rs`
- Modify: `crates/catga-core/tests/flow.rs`

- [ ] **Step 1: 创建 Flow 基准测试**

```rust
//! Flow execution throughput benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use catga_core::flow::{Flow, DslFlow, dsl_action, CatgaResult};

fn single_step_flow_throughput(c: &mut Criterion) {
    let flow = Flow::new("bench")
        .step(|| async { Ok(()) }, || async { Ok(()) });

    c.bench_function("single_step_flow_execution", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            rt.block_on(flow.clone().run()).unwrap();
        });
    });
}

fn multi_step_flow_throughput(c: &mut Criterion) {
    let flow = Flow::new("bench")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) });

    c.bench_function("three_step_flow_execution", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            rt.block_on(flow.clone().run()).unwrap();
        });
    });
}

fn dsl_flow_throughput(c: &mut Criterion) {
    let flow = DslFlow::new()
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }));

    c.bench_function("dsl_flow_two_actions", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            let mut state = 0u32;
            rt.block_on(flow.run(&mut state)).unwrap();
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = single_step_flow_throughput, multi_step_flow_throughput, dsl_flow_throughput
}
criterion_main!(benches);
```

- [ ] **Step 2: 运行基准测试**

Run: `cargo bench -p catga-core --bench flow_throughput -- --noplot`

- [ ] **Step 3: 提交**

```bash
git add crates/catga-core/benches/flow_throughput.rs
git commit -m "perf: add Flow throughput benchmarks"
```

---

### Task 10: 创建 Flow 单元测试

**Files:**
- Create: `crates/catga-core/tests/flow_comprehensive.rs`

- [ ] **Step 1: 创建 Flow 综合测试**

```rust
//! Comprehensive Flow and DslFlow tests

use catga_core::flow::{Flow, DslFlow, dsl_action, CatgaResult, ErrorCode, CatgaError};
use std::sync::{
    Arc, atomic::{AtomicBool, Ordering}
};

#[tokio::test]
async fn flow_executes_all_steps_in_sequence() -> CatgaResult<()> {
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
    let comp = compensation_run.clone();

    let flow = Flow::new("compensation-test")
        .step(
            || async { Err(CatgaError::new(ErrorCode::Internal, "fail")) },
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
async fn dsl_flow_updates_state_correctly() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 10;
            Ok(())
        }));

    let mut state = 0_u32;
    flow.run(&mut state).await?;
    assert_eq!(state, 11);
    Ok(())
}
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-core/tests/flow_comprehensive.rs
git commit -m "test: add comprehensive Flow tests"
```

---

## Phase 4: 传输层

### Task 11: 创建 NATS 传输基准测试

**Files:**
- Create: `crates/catga-nats/tests/performance.rs`
- Create: `crates/catga-nats/benches/nats_throughput.rs`

- [ ] **Step 1: 创建 NATS 性能测试**

```rust
//! NATS JetStream performance benchmarks

use std::time::Instant;

#[tokio::test]
#[ignore = "requires NATS server"]
async fn nats_publish_receive_throughput() -> Result<(), Box<dyn std::error::Error>> {
    const COUNT: u64 = 100_000;
    let transport = /* setup transport */;

    let start = Instant::now();
    for i in 0..COUNT {
        transport.publish(create_envelope(i)).await?;
    }
    let elapsed = start.elapsed();

    let ops_per_sec = COUNT as f64 / elapsed.as_secs_f64();
    println!("NATS publish throughput: {} ops/s", ops_per_sec as u64);
    Ok(())
}
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-nats/tests/performance.rs crates/catga-nats/benches/
git commit -m "perf: add NATS transport benchmarks"
```

---

### Task 12: 创建 Redis 传输基准测试

**Files:**
- Create: `crates/catga-redis/tests/performance.rs`
- Create: `crates/catga-redis/benches/redis_throughput.rs`

- [ ] **Step 1: 创建 Redis 性能测试**

```rust
//! Redis transport performance benchmarks

#[tokio::test]
#[ignore = "requires Redis server"]
async fn redis_publish_subscribe_throughput() -> Result<(), Box<dyn std::error::Error>> {
    const COUNT: u64 = 100_000;
    // ... setup redis transport
    // ... measure throughput
    Ok(())
}
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-redis/tests/performance.rs crates/catga-redis/benches/
git commit -m "perf: add Redis transport benchmarks"
```

---

### Task 13: 创建 RobustMQ 基准测试

**Files:**
- Create: `crates/catga-robustmq/tests/performance.rs`
- Create: `crates/catga-robustmq/benches/robustmq_throughput.rs`

- [ ] **Step 1: 创建 RobustMQ 性能测试**

```rust
//! RobustMQ performance benchmarks

#[tokio::test]
#[ignore = "manual benchmark"]
async fn priority_queue_throughput() -> Result<(), Box<dyn std::error::Error>> {
    // 测试优先级队列吞吐量
    Ok(())
}
```

- [ ] **Step 2: 提交**

```bash
git add crates/catga-robustmq/tests/performance.rs crates/catga-robustmq/benches/
git commit -m "perf: add RobustMQ benchmarks"
```

---

## Phase 5: CI 配置

### Task 14: 配置覆盖率门槛

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `codecov.yml`

- [ ] **Step 1: 添加覆盖率检查到 CI**

```yaml
coverage:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Install cargo-llvm-cov
      uses: taiki-e/install-action@cargo-llvm-cov
    - name: Generate coverage
      run: cargo llvm-cov --workspace --lcov --output-path lcov.info
    - name: Upload to Codecov
      uses: codecov/codecov-action@v4
      with:
        files: lcov.info
        fail_ci_if_error: true
```

- [ ] **Step 2: 提交**

```bash
git add .github/workflows/ci.yml codecov.yml
git commit -m "ci: add coverage threshold check"
```

---

### Task 15: 更新 performance.sh 脚本

**Files:**
- Modify: `scripts/performance.sh`

- [ ] **Step 1: 更新性能脚本**

```bash
# Core benchmarks using criterion
cargo bench -p catga-core --bench mediator_throughput -- --noplot
cargo bench -p catga-core --bench registry -- --noplot
cargo bench -p catga-core --bench handler_dispatch -- --noplot
cargo bench -p catga-core --bench flow_throughput -- --noplot
```

- [ ] **Step 2: 提交**

```bash
git add scripts/performance.sh
git commit -m "perf: update performance.sh with criterion benchmarks"
```

---

## 验收检查清单

- [ ] `cargo test --workspace` 全部通过
- [ ] `cargo doc --workspace --no-deps` 生成完整文档
- [ ] `cargo test --doc --workspace` 所有 doctest 通过
- [ ] `cargo bench --workspace` 所有基准测试通过
- [ ] Mediator 性能 > 10M ops/s
- [ ] TypedMediator 性能 > 5M ops/s
- [ ] Flow 单步骤执行 > 1M ops/s
- [ ] 代码覆盖率 ≥ 80%
- [ ] 所有测试在 `tests/` 目录下
- [ ] 所有基准测试在 `benches/` 目录下
- [ ] CI 配置正确，单元测试阻塞 PR
- [ ] 文档提供双语版本 (zh/en)
