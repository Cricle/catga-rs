# Critical Runtime E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Flow DSL, CQRS, durable recovery, and Raft critical scenarios explicit CI-gated integration coverage.

**Architecture:** Keep behavior tests in their owning integration-test packages. Extend the E2E scenario catalog and CI commands so the same public-contract tests run as required release gates. Preserve existing real-service SQL coverage and repair any compilation error before the combined verification.

**Tech Stack:** Rust 2024, Tokio, cargo test, GitHub Actions, Docker service containers.

---

### Task 1: Repair and verify SQL E2E test compilation

**Files:**
- Modify: `crates/catga-flow-store/tests/mssql.rs`
- Modify: `crates/catga-flow-store/tests/sql_backend_contracts.rs`

- [x] Scope any temporary SQL connection borrow before returning its owner.
- [x] Run `rtk cargo test -p catga-flow-store --all-features --test mssql --no-run` and require exit code 0.
- [x] Run `rtk cargo test -p catga-flow-store --all-features --test sql_backend_contracts --no-run` and require exit code 0.

### Task 2: Register critical runtime contracts as E2E scenarios

**Files:**
- Modify: `testing/e2e-scenarios.json`
- Modify: `.github/workflows/ci.yml`

- [x] Add Flow DSL progress, Flow recovery, mediator, and Raft runtime entries as `critical` core scenarios.
- [x] Make CI call the repository-owned Docker E2E and coverage scripts so every declared matrix entry runs in both gates without duplicated workflow command lists.
- [x] Run `rtk bash scripts/e2e.sh --profile full --validate-only` and require exit code 0.

### Task 3: Strengthen the recovery boundary where it is genuinely untested

**Files:**
- Modify: `tests/raft_runtime.rs` only if the public runtime lacks a composed retry/recovery assertion.

- [x] Add a focused test that proves a retryable peer delivery failure does not terminate the Raft owner task and a subsequent command remains accepted.
- [x] Run `rtk cargo test -p catga-tests --test raft_runtime` and require exit code 0.

### Task 4: Combined quality verification and delivery

**Files:**
- Modify: only files from Tasks 1-3.

- [x] Run `rtk cargo fmt --all -- --check`, `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`, selected critical tests, and `rtk cargo test --doc --workspace --all-features` as disk permits.
- [x] Run `rtk git diff --check`; ensure `a.md` is not staged.
- [ ] Commit the changes and push `main`; CI remains the full Docker E2E and coverage authority.
