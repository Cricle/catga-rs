# Catga 开发环境配置

## 概述

本仓库 (catga-rs) 是一个 Rust 事件驱动分布式系统框架，包含 CQRS、事件溯源、工作流、Raft 共识等核心功能。

## 已配置的环境

### 系统要求

| 组件 | 要求 | 当前状态 |
|------|------|----------|
| Rust | 1.96+ | ✅ 1.97.1 |
| Cargo | 对应 Rust 版本 | ✅ 1.97.1 |
| protobuf-compiler | 最新稳定版 | ✅ 29.2 |

### 已安装工具

1. **Rust 工具链**
   - rustc 1.97.1
   - cargo 1.97.1
   - rustup 已配置

2. **Protocol Buffers 编译器**
   - 版本: 29.2
   - 安装位置: `/c/tools/protobuf`
   - 已添加到 PATH (需重新加载 shell)

### 项目结构

```
catga-rs/
├── crates/
│   ├── catga-core/      # 核心框架
│   ├── catga-axum/      # Axum HTTP 集成
│   ├── catga-cluster/    # Raft 集群支持
│   ├── catga-nats/      # NATS JetStream 传输
│   ├── catga-redis/     # Redis 队列
│   ├── catga-robustmq/  # RobustMQ 传输
│   ├── catga-sorock/     # Sorock Raft 后端
│   └── catga-flow-store/ # Flow 状态持久化
├── examples/             # 示例代码
│   └── distributed-kv/   # 分布式 KV 示例
└── tests/               # 集成测试
```

## 快速开始

### 1. 配置 PATH (仅首次需要)

```bash
export PATH="/c/tools/protobuf/bin:$PATH"
```

永久配置 (添加到 `~/.bashrc`):
```bash
echo 'export PATH="/c/tools/protobuf/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### 2. 构建项目

```bash
cargo build --workspace --all-features
```

### 3. 运行测试

```bash
# 运行所有测试
cargo test --workspace --all-features

# 运行单个 crate 测试
cargo test -p catga-core --all-features
```

### 4. 运行示例

```bash
# 快速开始示例
cargo run --example simple_handler

# 分布式 KV 示例 (需要 Raft)
cargo run -p distributed-kv
```

## 开发工作流

根据 AGENT.md，本项目使用 TDD 开发模式：

```bash
# 提交前质量门禁
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-features
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

## 依赖说明

### 可选 Features

| Feature | 说明 |
|---------|------|
| `flow` | 工作流和补偿事务支持 |
| `uuid` | UUID 支持 |
| `chrono` | 日期时间支持 |
| `hashbrown` | 高性能 HashMap/HashSet |
| `ahash` | AHashMap/AHashSet |
| `rust_decimal` | 高精度小数 |
| `half` | 半精度浮点数 |
| `num-bigint` | 大整数支持 |
| `glam` | 游戏数学库 |
| `num-complex` | 复数支持 |

## 常见问题

### Q: 编译时找不到 `protoc`?

确保 protobuf 路径已添加到 PATH:
```bash
export PATH="/c/tools/protobuf/bin:$PATH"
```

### Q: 测试失败，提示缺少 chrono 等依赖?

使用 `--all-features` 运行测试:
```bash
cargo test --workspace --all-features
```

## IDE 配置

项目已包含 `.vscode/settings.json`，推荐使用 VS Code + rust-analyzer 插件。
