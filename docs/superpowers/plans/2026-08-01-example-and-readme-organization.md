# Example And README Organization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Organize Catga's runnable examples into a clear learning path and split the root README into a concise entry point plus focused example and performance references.

**Architecture:** Keep `catga-examples` as one crate, but replace automatic `src/bin` discovery with explicit `[[bin]]` entries that preserve all existing public binary names. Move only source paths and documentation; retain the current shared `order_service` module, distributed Todo Docker layout, runtime behavior, and existing commands.

**Tech Stack:** Rust 2024, Cargo binary targets, Markdown, existing `catga-examples` tests, Rustfmt, Clippy.

---

## File Map

| Path | Responsibility |
| --- | --- |
| `examples/Cargo.toml` | Explicit stable binary names and their grouped source paths. |
| `examples/src/quickstart/` | Small, infrastructure-free mediator, typed mediator, flow, and memory transport programs. |
| `examples/src/runtime/` | Bus routing and OpenTelemetry composition programs. |
| `examples/src/web/` | Axum checkout, local checkout, and complete order-service HTTP entry points. |
| `examples/src/distributed/` | Distributed Todo API/worker entry points and shared Todo domain module. |
| `examples/src/lib.rs` | Re-exports grouped shared modules without leaking file layout to callers. |
| `tests/examples.rs` | Static contract for every public binary source path and documentation guide. |
| `README.md` | Concise repository entry point and navigation. |
| `docs/examples.md` | Ordered example learning guide. |
| `docs/performance.md` | Full current benchmark report and reproduction notes. |

### Task 1: Make grouped binary paths an explicit contract

**Files:**
- Modify: `examples/Cargo.toml`
- Modify: `tests/examples.rs`

- [ ] **Step 1: Write the failing path and Cargo-target contract**

  Replace the `EXAMPLES` array in `tests/examples.rs` with the eleven grouped
  source paths and load `examples/Cargo.toml` through `include_str!`.

  ```rust
  const EXAMPLE_TARGETS: [(&str, &str); 11] = [
      ("mediator", "examples/src/quickstart/mediator.rs"),
      ("typed_mediator", "examples/src/quickstart/typed_mediator.rs"),
      ("memory_transport", "examples/src/quickstart/memory_transport.rs"),
      ("flow", "examples/src/quickstart/flow.rs"),
      ("bus_cqrs", "examples/src/runtime/bus_cqrs.rs"),
      ("otel_bus", "examples/src/runtime/otel_bus.rs"),
      ("axum_checkout", "examples/src/web/axum_checkout.rs"),
      ("checkout", "examples/src/web/checkout.rs"),
      ("order_service", "examples/src/web/order_service.rs"),
      ("distributed_todo_api", "examples/src/distributed/todo_api.rs"),
      ("distributed_todo_worker", "examples/src/distributed/todo_worker.rs"),
  ];
  ```

  The test must assert both `workspace.join(path).is_file()` and that the
  manifest has `name = "<binary>"` and `path = "<path relative to examples>"`.

- [ ] **Step 2: Verify the contract fails before the move**

  Run: `rtk cargo test -p catga-tests --test examples public_examples_are_present`

  Expected: FAIL because the grouped paths and explicit `[[bin]]` entries do
  not yet exist.

- [ ] **Step 3: Define all stable binary targets**

  Append these eleven entries to `examples/Cargo.toml` after the dependency
  sections, using the exact existing binary names:

  ```toml
  [[bin]]
  name = "mediator"
  path = "src/quickstart/mediator.rs"

  [[bin]]
  name = "typed_mediator"
  path = "src/quickstart/typed_mediator.rs"

  [[bin]]
  name = "memory_transport"
  path = "src/quickstart/memory_transport.rs"

  [[bin]]
  name = "flow"
  path = "src/quickstart/flow.rs"

  [[bin]]
  name = "bus_cqrs"
  path = "src/runtime/bus_cqrs.rs"

  [[bin]]
  name = "otel_bus"
  path = "src/runtime/otel_bus.rs"

  [[bin]]
  name = "axum_checkout"
  path = "src/web/axum_checkout.rs"

  [[bin]]
  name = "checkout"
  path = "src/web/checkout.rs"

  [[bin]]
  name = "order_service"
  path = "src/web/order_service.rs"

  [[bin]]
  name = "distributed_todo_api"
  path = "src/distributed/todo_api.rs"

  [[bin]]
  name = "distributed_todo_worker"
  path = "src/distributed/todo_worker.rs"
  ```

- [ ] **Step 4: Move sources without changing public target names**

  Run:

  ```bash
  rtk mkdir -p examples/src/quickstart examples/src/runtime examples/src/web examples/src/distributed
  rtk git mv examples/src/bin/mediator.rs examples/src/bin/typed_mediator.rs examples/src/bin/memory_transport.rs examples/src/bin/flow.rs examples/src/quickstart/
  rtk git mv examples/src/bin/bus_cqrs.rs examples/src/bin/otel_bus.rs examples/src/runtime/
  rtk git mv examples/src/bin/axum_checkout.rs examples/src/bin/checkout.rs examples/src/bin/order_service.rs examples/src/web/
  rtk git mv examples/src/bin/distributed_todo_api.rs examples/src/distributed/todo_api.rs
  rtk git mv examples/src/bin/distributed_todo_worker.rs examples/src/distributed/todo_worker.rs
  rtk git mv examples/src/distributed_todo.rs examples/src/distributed/todo.rs
  ```

  Change `examples/src/lib.rs` from `pub mod distributed_todo;` to an inline
  compatibility module that loads `distributed/todo.rs`:

  ```rust
  #[path = "distributed/todo.rs"]
  pub mod distributed_todo;
  ```

  Keep `pub mod order_service;` unchanged so downstream test imports remain
  stable. Do not rename `examples/distributed-todo`, Docker binary names, or
  Compose commands.

- [ ] **Step 5: Verify the new paths and all binary targets**

  Run:

  ```bash
  rtk cargo test -p catga-tests --test examples public_examples_are_present
  rtk cargo check -p catga-examples --bins
  ```

  Expected: the path contract passes and Cargo checks all eleven explicitly
  declared binaries.

- [ ] **Step 6: Commit the source organization**

  ```bash
  rtk git add examples/Cargo.toml examples/src tests/examples.rs
  rtk git commit -m "refactor: organize runnable examples by use case"
  ```

### Task 2: Build the example learning guide

**Files:**
- Create: `docs/examples.md`
- Modify: `tests/examples.rs`

- [ ] **Step 1: Extend the documentation contract**

  Add constants for `README.md`, `docs/examples.md`, and `docs/performance.md`
  in `tests/examples.rs`. Add a test named
  `documentation_links_to_grouped_examples_and_performance_report` that checks:

  ```rust
  for link in ["docs/examples.md", "docs/performance.md"] {
      assert!(README.contains(link), "README must link to {link}");
  }
  for source in [
      "examples/src/quickstart/mediator.rs",
      "examples/src/runtime/bus_cqrs.rs",
      "examples/src/web/order_service.rs",
      "examples/distributed-todo/compose.yaml",
  ] {
      assert!(EXAMPLE_GUIDE.contains(source), "guide must link to {source}");
  }
  ```

- [ ] **Step 2: Verify the guide contract fails**

  Run: `rtk cargo test -p catga-tests --test examples documentation_links_to_grouped_examples_and_performance_report`

  Expected: FAIL because the guide and README links do not exist.

- [ ] **Step 3: Write `docs/examples.md` as an ordered path**

  Create these sections, each with purpose, source link, direct command, and
  next step:

  ```markdown
  # Catga Examples
  ## 1. Local building blocks
  ## 2. Runtime composition
  ## 3. HTTP applications
  ## 4. Distributed Todo
  ## Choosing production consumption APIs
  ```

  Use these exact commands in their matching sections:

  ```bash
  cargo run -p catga-examples --bin mediator
  cargo run -p catga-examples --bin typed_mediator
  cargo run -p catga-examples --bin memory_transport
  cargo run -p catga-examples --bin flow
  cargo run -p catga-examples --bin bus_cqrs
  RUST_LOG=catga=info cargo run -p catga-examples --bin otel_bus
  cargo run -p catga-examples --bin axum_checkout
  cargo run -p catga-examples --bin checkout
  cargo run -p catga-examples --bin order_service
  docker compose --file examples/distributed-todo/compose.yaml up --build
  ```

  Explain that `process_next` is for local composition/tests and that durable
  production consumers use `CompetingConsumer`; point to the transport and
  reliability reference in `skill/transport.md` and `skill/reliability.md`.
  State that the distributed Todo sample owns NATS, consumer, projection, and
  shutdown lifecycle explicitly and is verified by
  `examples/distributed-todo/verify.sh`.

- [ ] **Step 4: Verify the guide contract passes**

  Run: `rtk cargo test -p catga-tests --test examples documentation_links_to_grouped_examples_and_performance_report`

  Expected: PASS.

- [ ] **Step 5: Commit the guide and contract**

  ```bash
  rtk git add docs/examples.md tests/examples.rs
  rtk git commit -m "docs: add ordered Catga example guide"
  ```

### Task 3: Extract the full performance report

**Files:**
- Create: `docs/performance.md`
- Modify: `README.md`

- [ ] **Step 1: Create the focused performance document**

  Move the entire existing README content from `## Performance snapshot`
  through the paragraph immediately before `## Quick start` into
  `docs/performance.md`. Prefix it with:

  ```markdown
  # Performance

  Catga publishes reproducible release-mode measurements as workflow and
  release artifacts. These figures are observations from the recorded runner,
  not hardware-independent guarantees.
  ```

  Preserve all metric tables, benchmark scope, CI workflow link, database
  durability explanation, PowerShell reproduction commands, and the
  `scripts/performance.sh --profile full` command. Do not alter benchmark
  values during this documentation-only move.

- [ ] **Step 2: Replace the README report with one stable link**

  Replace the extracted section with:

  ```markdown
  ## Performance

  The current release-mode benchmark table, measurement scope, database
  durability analysis, and reproduction commands live in
  [Performance](docs/performance.md). Release artifacts contain the complete
  machine-readable JSON reports.
  ```

- [ ] **Step 3: Check the document keeps the required report content**

  Extend `documentation_links_to_grouped_examples_and_performance_report` to
  assert `docs/performance.md` contains `SQLite`, `MySQL`, `PostgreSQL`,
  `SQL Server`, `Redis`, `p50`, `p95`, `p99`, and
  `scripts/performance.sh --profile full`.

- [ ] **Step 4: Run the documentation contract**

  Run: `rtk cargo test -p catga-tests --test examples`

  Expected: all example and documentation contracts pass.

- [ ] **Step 5: Commit the report extraction**

  ```bash
  rtk git add README.md docs/performance.md tests/examples.rs
  rtk git commit -m "docs: move benchmark detail out of README"
  ```

### Task 4: Rewrite README as the concise entry point

**Files:**
- Modify: `README.md`
- Modify: `tests/examples.rs`

- [ ] **Step 1: Define the README navigation assertions**

  Extend the documentation test to assert the README includes each heading:

  ```rust
  for heading in [
      "# Catga: Rust Event-Driven Distributed Runtime",
      "## Start here",
      "## Run an example",
      "## Production boundaries",
      "## Documentation",
      "## Verification",
  ] {
      assert!(README.contains(heading), "README is missing {heading}");
  }
  ```

- [ ] **Step 2: Verify the navigation assertion fails**

  Run: `rtk cargo test -p catga-tests --test examples documentation_links_to_grouped_examples_and_performance_report`

  Expected: FAIL because the current README does not use the concise heading
  structure.

- [ ] **Step 3: Rewrite `README.md` without duplicating reference material**

  Keep the title and the opening two paragraphs about pure-Rust event-driven
  composition. Then use exactly these sections:

  ```markdown
  ## Start here
  ## Install
  ## Run an example
  ## Production boundaries
  ## Performance
  ## Documentation
  ## Verification
  ## License
  ```

  `Start here` contains a five-row table linking distributed Todo,
  quickstart, runtime Bus, HTTP order service, and `catga-auto`. `Run an
  example` links to `docs/examples.md`, gives the `mediator` command, and
  shows the `distributed-todo` Compose command. `Production boundaries` keeps
  the concise at-least-once/idempotency, explicit lifecycle, smallest feature
  set, and external ownership guidance. `Documentation` links to
  `docs/examples.md`, `docs/performance.md`, and the existing `skill/` guides.
  Retain the repository license link.

- [ ] **Step 4: Verify README navigation and links**

  Run: `rtk cargo test -p catga-tests --test examples`

  Expected: PASS.

- [ ] **Step 5: Commit the README rewrite**

  ```bash
  rtk git add README.md tests/examples.rs
  rtk git commit -m "docs: streamline Catga README navigation"
  ```

### Task 5: Remove obsolete planning material and verify the migration

**Files:**
- Delete: `sample.md`
- Modify: `skill/SKILL.md`

- [ ] **Step 1: Update the local development guide path**

  Replace both references to `examples/src/bin/` in `skill/SKILL.md` with
  `docs/examples.md`, preserving every existing `cargo run --bin` command.

- [ ] **Step 2: Remove the obsolete root plan**

  Run: `rtk git rm sample.md`

  The deleted file is the superseded Catga Auto migration plan. Do not delete
  the committed design specification or this implementation plan.

- [ ] **Step 3: Check no stale source-directory reference remains**

  Run:

  ```bash
  rtk rg -n 'examples/src/bin|sample\.md' README.md skill examples tests Cargo.toml .github
  ```

  Expected: no matches. Fix any discovered internal link before continuing.

- [ ] **Step 4: Run focused and workspace-facing verification**

  Run:

  ```bash
  rtk cargo fmt --all -- --check
  rtk cargo check -p catga-examples --bins
  rtk cargo test -p catga-examples --all-features
  rtk cargo test -p catga-tests --test examples
  rtk cargo clippy -p catga-examples --all-targets --all-features -- -D warnings
  rtk git diff --check
  ```

  Expected: every command exits zero. Do not run the Docker E2E suite locally;
  it remains a manual/release workflow under the configured CI policy.

- [ ] **Step 5: Commit cleanup and final verification**

  ```bash
  rtk git add README.md docs examples skill tests
  rtk git add -u sample.md
  rtk git commit -m "docs: organize Catga example entry points"
  ```

## Plan Self-Review

- Spec coverage: Tasks 1-5 cover stable binary names, grouped paths, Docker
  compatibility, example navigation, complete performance content, README
  reduction, stale-plan removal, and focused validation.
- No placeholders: commands, paths, target names, expected outcomes, and
  document contents are concrete.
- Type consistency: all binary names match the current Dockerfile, Compose
  file, README commands, and Cargo target names; the public library module
  remains `catga_examples::distributed_todo`.
