# CI 分级与发布

## CI 分级（0.2 起）

仓库的流水线按反馈速度分两层：

**PR / main 推送（`.github/workflows/ci.yml`，无 Docker)**:

1. `quality` 作业：`cargo fmt --all -- --check`、逐 crate `cargo clippy --all-targets --all-features -- -D warnings`、doc 测试（`cargo test --doc`)、`RUSTDOCFLAGS='-D warnings' cargo doc`、`cargo check --workspace --all-targets`。
2. `tests` 作业：`cargo test --workspace --all-features`（单元 + 集成）+ 全部 doc 测试。

Docker / 外部服务依赖的 E2E 测试默认 `#[ignore]`，在 PR CI 中自动跳过，不拖慢门禁。

**Release 质量门（`.github/workflows/release.yml`，仅 `v*` 标签或手动 `workflow_dispatch`)**:

- 完整 E2E + `cargo-tarpaulin` 覆盖率门禁：**行覆盖 ≥ 85%**（分支覆盖已收集但未单独设阈值；proc-macro 源码已排除；另有 E2E 通过率 ≥ 95% 的要求）;
- Docker E2E 性能套件，诊断产物上传到 workflow artifacts 与草稿 release;
- 全部通过后才进入发布作业。

本地复现 PR 门禁：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
```

## 发布与打包

工作区版本统一为 **0.2.0**；可发布 crate 共 10 个，release 工作流按依赖顺序逐个 `cargo publish`（发布前校验标签版本与各 manifest 一致，已存在的版本跳过，crates.io 429 限流自动重试）:

```
catga-memorypack-derive → catga-core-macros → catga-core → catga-sorock
→ catga-redis / catga-nats / catga-robustmq / catga-flow-store
→ catga-cluster → catga-axum
```

> crates.io 目前只有早年的 0.0.1 占位版本；0.2.0 随 `v0.2.0` 标签推送（或手动触发 release 工作流）正式发布。触发发布前请确认所有 manifest 的 `version` 已与目标标签一致——版本不一致会在发布作业前置校验处直接失败。
