# Strict CI and root examples design

The final repository keeps runnable examples in root `examples/`, using
existing `catga_handlers!` registration rather than verbose generic calls.

CI has quality, coverage, E2E, and release layers. Quality runs formatting,
Clippy, Rustdoc, doctests, root examples, and documentation tests. Coverage
uses `cargo-llvm-cov` and fails below 100 percent for measurable lines and
branches. E2E starts NATS JetStream, Redis, MySQL, PostgreSQL, and SQL Server
containers, tests each service, and tests cross-backend combinations such as
MySQL storage plus Redis transport and PostgreSQL storage plus NATS transport.

Performance tests remain manual and ignored. Release runs only for `v*` tags,
uses `CRATES_KEY` as `CARGO_REGISTRY_TOKEN`, and never creates tags.
