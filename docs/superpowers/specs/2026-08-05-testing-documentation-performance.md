# 完整测试、文档与性能基准化设计

**日期**: 2026-08-05
**状态**: 设计中

## 目标

为 catga-rs 项目建立完整的测试覆盖、文档体系和性能基准，确保：
- 代码质量稳定可靠
- 文档完整且可运行
- 性能不退化且达目标

## 范围

按依赖顺序逐个加强以下模块：

1. **catga-core** — 核心接口：Mediator、Registry、Handler traits、TypedMediator
2. **catga-service** — #[catga_service] 过程宏
3. **catga-flow** — Flow DSL 与补偿机制
4. **传输层** — catga-nats、catga-redis、catga-robustmq

## 决策

| 维度 | 决策 |
|------|------|
| 测试深度 | 所有公共 API 都有测试（包括边界条件和错误处理） |
| 文档标准 | 每个公共函数/方法都有 rustdoc + 可运行的 doctest |
| 性能目标 | Mediator > 10M ops/s |
| 性能策略 | 相对基准（防止退化）+ 绝对目标 |
| CI 集成 | 单元测试和覆盖率阻塞 PR，性能测试可选 |
| 基础设施 | 扩展现有 `tests/` 和 `scripts/performance.sh` |
| 文档语言 | 双语 (zh/cn)，支持未来扩展更多语言 |

## 模块详细计划

### 1. catga-core

**文件结构**：
```
crates/catga-core/src/
├── lib.rs
├── mediator.rs        # Mediator
├── registry.rs       # Registry
├── handler.rs        # Handler/CommandHandler traits
├── typed_mediator.rs # TypedMediator
├── auto/
│   ├── mod.rs
│   ├── mediator.rs   # AutoMediator
│   └── app.rs        # AutoApp
└── flow/             # Flow 模块
```

**测试覆盖要求**：
- [ ] `mediator.rs`: send, try_send, batch_send, 错误传播
- [ ] `registry.rs`: register_request, register_command, register_event, get_handler
- [ ] `handler.rs`: Handler trait 实现，CommandHandler trait 实现
- [ ] `typed_mediator.rs`: TypedMediator::new, typed dispatch
- [ ] `auto/`: AutoApp::from_registry, AutoMediator dispatch

**文档要求**：
- [ ] 每个 public type/trait/function 都有 rustdoc
- [ ] 关键 API 有 doctest（如 Mediator::send）

**性能基准**：
- [ ] `mediator_throughput`: > 10M ops/s (单线程)
- [ ] `typed_mediator_throughput`: > 5M ops/s (单线程)
- [ ] `registry_creation`: < 1ms (100 个 handler)

### 2. catga-service (#[catga_service] 宏)

**文件结构**：
```
crates/catga-core/src/macros/proc-macros/src/
├── lib.rs
├── impl_handlers.rs  # #[catga_service] 实现
└── derive_request.rs # #[catga_request] 实现
```

**测试覆盖要求**：
- [ ] 请求方法识别（CatgaResult<T> where T != ()）
- [ ] 命令方法识别（CatgaResult<()>）
- [ ] 事件方法识别（CatgaResult<Event>）
- [ ] 多方法 impl 块
- [ ] 错误消息类型检测

**文档要求**：
- [ ] 宏的使用文档
- [ ] doctest 演示完整用法

**性能基准**：
- [ ] `macro_expansion_time`: < 100ms (合理规模 impl 块)

### 3. catga-flow

**文件结构**：
```
crates/catga-core/src/flow/
├── mod.rs
├── flow.rs           # Flow
├── dsl_flow.rs       # DslFlow
└── step.rs           # Step
```

**测试覆盖要求**：
- [ ] Flow::new, step, run
- [ ] DslFlow::new, action, run
- [ ] 补偿机制（成功/失败路径）
- [ ] DSL 语法错误检测

**文档要求**：
- [ ] Flow 链式调用示例
- [ ] 补偿回调示例
- [ ] DslFlow 状态管理示例

**性能基准**：
- [ ] `flow_execution`: < 1us per step (空操作)
- [ ] `dsl_flow_execution`: < 500ns per action (空操作)

### 4. 传输层

**catga-nats**:
- [ ] publish, receive, ack 循环
- [ ] 连接错误处理
- [ ] 流和消费者管理

**catga-redis**:
- [ ] publish/subscribe
- [ ] 队列操作
- [ ] 死信队列

**catga-robustmq**:
- [ ] 优先级队列
- [ ] 邮件箱配置

**性能基准**：
- [ ] `nats_roundtrip`: 测量 pub/sub/ack 延迟
- [ ] `redis_roundtrip`: 测量 publish/subscribe 延迟

## 文档结构

```
docs/
├── README.md
├── zh/                    # 中文文档
│   ├── getting-started.md
│   ├── architecture.md
│   └── ...
└── en/                    # 英文文档
    ├── getting-started.md
    ├── architecture.md
    └── ...
```

## CI 配置

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run tests
        run: cargo test --workspace
      - name: Check coverage
        run: cargo coverage --fail-under-lines 80

  doc:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Check documentation
        run: cargo doc --workspace --no-deps

  # 性能测试可选，不阻塞 PR
  performance:
    runs-on: ubuntu-latest
    if: github.event_name == 'workflow_dispatch'
    steps:
      - uses: actions/checkout@v4
      - name: Run benchmarks
        run: scripts/performance.sh
```

## 性能基准脚本

扩展现有 `scripts/performance.sh`：

```bash
# 新增测试
- "core_mediator_performance"      # >10M ops/s
- "core_typed_mediator_performance" # >5M ops/s
- "core_flow_performance"          # <1us per step
- "core_macro_expansion_performance" # <100ms
```

## 实施顺序

1. **Phase 1**: catga-core 测试 + 文档 + 性能基准
2. **Phase 2**: catga-service 宏测试 + 文档
3. **Phase 3**: catga-flow 测试 + 文档 + 性能基准
4. **Phase 4**: 传输层测试 + 文档 + 性能基准
5. **Phase 5**: CI 配置 + 覆盖率门槛

## 验收标准

- [ ] `cargo test --workspace` 全部通过
- [ ] `cargo doc --workspace --no-deps` 生成完整文档
- [ ] `cargo test --doc --workspace` 所有 doctest 通过
- [ ] 性能基准测试达标（Mediator > 10M ops/s）
- [ ] 代码覆盖率 ≥ 80%（按行）
- [ ] 文档同时提供 zh/ 和 en/ 版本

## 已知问题

- `async-nats` 依赖编译失败，需要解决版本兼容问题
- 部分老 worktree 目录可能导致干扰
