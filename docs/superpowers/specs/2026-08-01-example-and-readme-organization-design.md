# Example And README Organization Design

## Goal

Make Catga's runnable examples easy to discover without changing their public
binary names, and make the repository README a concise entry point rather than
a complete reference manual and benchmark archive.

## Example Layout

Move the single-process example binaries out of the flat `examples/src/bin`
directory into source groups. `examples/Cargo.toml` will declare each binary
explicitly, preserving existing commands such as `cargo run -p catga-examples
--bin mediator`.

```text
examples/src/
  quickstart/
    mediator.rs
    typed_mediator.rs
    memory_transport.rs
    flow.rs
  runtime/
    bus_cqrs.rs
    otel_bus.rs
  web/
    axum_checkout.rs
    checkout.rs
    order_service.rs
    order_service/
  distributed/
    todo.rs
    todo_api.rs
    todo_worker.rs
examples/distributed-todo/
  compose.yaml
  Dockerfile
  verify.sh
```

The `distributed-todo` Compose directory remains a standalone multi-process
reference application. Its shared domain/read-model module moves alongside its
two process binaries. No application behavior, environment variable, image
path, or binary name changes.

## Documentation Layout

`README.md` will contain only:

- Catga's runtime positioning and explicit-ownership model.
- Installation and dependency guidance.
- A short choose-a-starting-point table.
- The shortest local and distributed run commands.
- Links to focused example, performance, production, and verification guidance.

`docs/examples.md` will be the ordered learning path. Every example will state
its purpose, dependencies, command, source link, and its natural next step.
It will distinguish local composition/testing APIs from production services and
call out that the durable distributed Todo sample is the production topology.

`docs/performance.md` will contain the complete current benchmark snapshot,
measurement scope, database durability explanation, tuning guidance, and
reproduction commands. The README will link to it instead of duplicating the
large report.

## Repository Cleanup

Delete the root `sample.md`. It is an implementation plan from an earlier
change and is neither a runnable sample nor user documentation.

## Compatibility And Validation

- Preserve all current Cargo binary names and `cargo run` commands.
- Update internal README/source links after moves.
- Keep Docker Compose paths stable for the distributed Todo verification.
- Add a static test covering each explicit binary mapping and README guide
  links, so a future move cannot silently break the learning path.
- Run example compilation, focused example tests, format, Clippy, and link/path
  checks after the migration. The complete Docker E2E suite remains release or
  manual-only under the current CI policy.
