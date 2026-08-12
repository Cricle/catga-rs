# CI Tiers and Release

## CI tiers (since 0.2)

The pipelines are tiered by feedback speed:

**PR / main pushes (`.github/workflows/ci.yml`, no Docker)**:

1. `quality` job: `cargo fmt --all -- --check`, per-crate `cargo clippy --all-targets --all-features -- -D warnings`, doc tests (`cargo test --doc`), `RUSTDOCFLAGS='-D warnings' cargo doc`, `cargo check --workspace --all-targets`.
2. `tests` job: `cargo test --workspace --all-features` (unit + integration) plus all doc tests.

Docker/external-service E2E tests are `#[ignore]`d by default and skip themselves in PR CI, keeping the gate fast.

**Release quality gate (`.github/workflows/release.yml`, only on `v*` tags or manual `workflow_dispatch`)**:

- Full E2E plus a `cargo-tarpaulin` coverage gate: **line coverage ≥ 85%** (branch coverage is collected but not gated separately; proc-macro source is excluded; with an additional ≥ 95% E2E pass-rate requirement);
- Docker E2E performance suite, with diagnostics uploaded to the workflow artifacts and the draft release;
- the publish job only runs after all of the above pass.

Reproduce the PR gate locally:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
```

## Release and packaging

The workspace version is uniformly **0.2.0**. There are 10 publishable crates, and the release workflow runs `cargo publish` for each in dependency order (it first verifies the tag version matches every manifest, skips versions already on crates.io, and retries automatically on crates.io 429 rate limits):

```
catga-memorypack-derive → catga-core-macros → catga-core → catga-sorock
→ catga-redis / catga-nats / catga-robustmq / catga-flow-store
→ catga-cluster → catga-axum
```

> crates.io currently only carries the early 0.0.1 placeholder versions; 0.2.0 is published by pushing the `v0.2.0` tag (or triggering the release workflow manually). Before triggering a release, make sure every manifest `version` already matches the target tag — a mismatch fails the publish job's pre-validation.
