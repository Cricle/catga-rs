# Documentation, examples, and release design

## Goal

Replace the repository's historical documentation tree with concise Rust-first
documentation, runnable API contracts, examples, and automated CI/release
workflows.

## Documentation surface

The final repository has no `docs/` directory. `README.md` becomes the single
repository entry point: install, a minimal mediator example, Flow usage,
feature-gated FlowStore selection, verification commands, and the project
boundaries (no RabbitMQ, hot reload, or HTTP health route).

Each public crate root supplies focused Rustdoc that explains its role,
feature flags, and a minimal composition pattern. Key public APIs include
runnable doctests with assertions. Doctests validate public contracts only;
their dependencies must be local and deterministic. Examples requiring Redis,
NATS, SQL, or credentials use `no_run` and point to the matching integration
test under `tests/`.

All non-doctest tests remain outside production `src` in the workspace or
crate `tests/` directories. The CI Rustdoc gate runs doctests as part of the
normal test suite and rejects warnings.

## Examples

The workspace adds small examples that compile without hidden setup:

- `mediator_basics` registers and dispatches one typed request;
- `flow_basics` builds and runs an in-memory durable Flow;
- `flow_store_features` documents compile-time selection of SQLite, MySQL,
  PostgreSQL, SQL Server, and Redis adapters without connecting by default;
- `transport_features` shows explicit Memory, Redis, and NATS composition,
  marking external-service variants as `no_run`.

Examples use `CatgaResult`, bounded constructors, and explicit composition.
They do not create workers or read credentials implicitly.

## CI and release

`ci.yml` runs format, workspace all-feature Clippy with warnings denied,
workspace tests including doctests, and Rustdoc with warnings denied on pull
requests and pushes to `main`. It uses the stable Rust toolchain and Cargo
caching.

`release.yml` runs only for tags matching `v*`. It first performs the same
quality checks, extracts the semantic version from the tag, verifies matching
crate package versions, and publishes in dependency order. The workflow maps
the GitHub secret `CRATES_KEY` to Cargo's `CARGO_REGISTRY_TOKEN`; it fails
before publication if the secret is absent. Each package is inspected before
publishing and an already-published matching version is skipped safely. The
workflow never creates a tag and does not run for ordinary branch pushes.

## Verification

The change is accepted only when `docs/` is absent, README links are valid,
all examples compile, doctests with assertions pass, no production `src`
contains test attributes, and CI workflow YAML parses. Workspace formatting,
Clippy, tests, and Rustdoc gates must pass before the release workflow is
considered ready.
