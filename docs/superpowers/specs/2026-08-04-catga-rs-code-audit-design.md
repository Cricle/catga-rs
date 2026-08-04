# Catga-rs 代码审计与重构设计

**日期**: 2026-08-04
**状态**: 草稿

## 1. 目标

对 catga-rs 代码库进行全面审计和结构优化，涵盖：
- 架构重组（使用文件夹而非新增 crate）
- 代码质量（死代码删除、重复代码整合、文档完善）
- 性能与内存优化
- 安全审计（跳过 DoS 保护）

## 2. 架构重组

### 2.1 问题分析

| 文件 | 行数 | 问题 |
|------|------|------|
| `crates/catga-core/src/flow/dsl.rs` | 1924 | 单文件过大 |
| `crates/catga-core/src/lib.rs` | 499 | 暴露 360+ 公开项 |
| `crates/catga-core/src/mediator.rs` | 638 | 较大但可接受 |

### 2.2 重组方案

**目录结构变更**（不改变 crate 边界）:

```
crates/catga-core/src/
├── lib.rs                          # 精简至 ~200 行，保留核心导出
├── flow/
│   ├── mod.rs                      # 导出 flow 模块
│   ├── dsl.rs                      # 保持 DSL 主文件
│   ├── dsl_step.rs                 # 新: Step/CompensatingStep trait 定义
│   ├── runtime.rs                  # FlowRunner
│   └── dsl_checkpoint.rs           # Checkpoint DSL
├── validation/                     # 新: 整合 validation 模块
│   ├── mod.rs                      # 导出
│   ├── endpoint.rs                 # EndpointValidation 及 helpers
│   └── behavior.rs                 # ValidationBehavior + Validator trait
├── behaviors/                      # 已有，保持结构
│   └── ...
└── memory/                         # 已有，保持结构
    └── ...
```

**拆分阈值**: 超过 600 行的文件应拆分

### 2.3 lib.rs 精简策略

- 将 360+ 公开项按功能分组导出到子模块
- 使用 `pub use` 重新导出，保持向后兼容
- 文档字符串引导用户到具体模块

## 3. 代码质量

### 3.1 重复代码整合

**问题**: `validation.rs` 与 `behaviors/validation.rs` 有相似模式

| 文件 | 用途 |
|------|------|
| `validation.rs` | HTTP endpoint 输入验证（同步） |
| `behaviors/validation.rs` | 行为管道验证（异步） |

**整合方案**: 保留两个实现（用途不同），但共享错误消息格式化逻辑：

```rust
// 新建 validation/shared.rs
pub fn format_validation_errors(errors: &[Box<str>], prefix: &str) -> CatgaError {
    // 统一的错误格式化
}
```

### 3.2 死代码检查

- ✅ 已确认无 `TODO`/`FIXME` 注释
- ✅ Clippy 无警告
- 需手动审查：`#[allow(dead_code)]` 标注的代码

### 3.3 文档完善

- 大文件缺少模块级文档（`dsl.rs`, `runtime.rs`）
- 公共 API 已有良好文档，保持

## 4. 性能与内存

### 4.1 DashMap 使用分析

| 文件 | 用途 | 建议 |
|------|------|------|
| `memory/transport.rs` | 消息队列 | 保留（DashMap 适合） |
| `memory/event_store.rs` | 事件存储 | 考虑 RwLock<HashMap> |
| 10+ 其他文件 | 各种并发场景 | 评估后决定 |

**原则**: DashMap 适用于写多用场景，读多用场景考虑 `RwLock<HashMap>`

### 4.2 无界 Channel

检查点:
- `FlowRunner` 使用有界 channel ✅
- `MemoryTransport` 使用有界 channel ✅
- 其他异步处理需审查

### 4.3 内存分配

- `EndpointValidation::into_result()`: 使用 `String::with_capacity(capacity)` 避免多次分配 ✅
- 考虑: 大批量事件处理时使用对象池

## 5. 安全审计

### 5.1 输入验证

| 组件 | 状态 |
|------|------|
| `validate_required` | ✅ 检查 None 和空白字符串 |
| `validate_positive` | ✅ 检查零和负值 |
| `SnowflakeLayout::validate` | ✅ 位分配验证 |
| `RaftInboundPolicy` | ✅ 认证检查 |

### 5.2 错误信息

- ✅ 无内部路径泄露
- ✅ `MAX_ERROR_DETAILS_BYTES` 限制详情大小
- ✅ 错误信息使用 `Box<str>` 避免堆碎片

### 5.3 Panic 安全

- ✅ `CatgaError::new` 处理 panic（lock poisoning）
- ✅ `std::sync::Mutex` lock poisoning 转为 `CatgaError`

## 6. 实施计划

### Phase 1: 架构重组
1. 创建 `flow/` 子目录结构
2. 创建 `validation/` 子目录结构
3. 移动 `dsl_step.rs` 和 `dsl_checkpoint.rs`
4. 更新 `mod.rs` 导出
5. 精简 `lib.rs`

### Phase 2: 代码质量
1. 创建 `validation/shared.rs` 共享格式化
2. 审查 `#[allow(dead_code)]` 标注
3. 添加模块级文档

### Phase 3: 性能优化
1. 评估 DashMap 使用场景
2. 添加文档说明并发数据结构选择

### Phase 4: 安全确认
1. 确认现有安全措施充分
2. 文档化安全边界

## 7. 风险与限制

- **向后兼容**: 重构不能破坏现有公开 API
- **测试覆盖**: 现有测试需全部通过
- **不涉及**: DoS 保护（用户明确跳过）
