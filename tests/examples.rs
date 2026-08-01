//! Presence contracts for the public executable examples.

use std::path::Path;

const EXAMPLES: [(&str, &str); 11] = [
    ("mediator", "src/quickstart/mediator.rs"),
    ("typed_mediator", "src/quickstart/typed_mediator.rs"),
    ("memory_transport", "src/quickstart/memory_transport.rs"),
    ("flow", "src/quickstart/flow.rs"),
    ("bus_cqrs", "src/runtime/bus_cqrs.rs"),
    ("otel_bus", "src/runtime/otel_bus.rs"),
    ("axum_checkout", "src/web/axum_checkout.rs"),
    ("checkout", "src/web/checkout.rs"),
    ("order_service", "src/web/order_service.rs"),
    ("distributed_todo_api", "src/distributed/todo_api.rs"),
    ("distributed_todo_worker", "src/distributed/todo_worker.rs"),
];

#[test]
fn public_examples_are_present() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or_else(|| {
            panic!("the integration-test crate must live directly below the workspace root")
        });

    let manifest = std::fs::read_to_string(workspace.join("examples/Cargo.toml"))
        .expect("the examples Cargo manifest must be readable");
    let mut missing = Vec::new();

    for (name, path) in EXAMPLES {
        let source = format!("examples/{path}");
        if !workspace.join(&source).is_file() {
            missing.push(format!("missing required public example: {source}"));
        }

        let target = format!("[[bin]]\nname = \"{name}\"\npath = \"{path}\"");
        if !manifest.contains(&target) {
            missing.push(format!("missing explicit Cargo binary target: {target}"));
        }
    }

    assert!(missing.is_empty(), "{}", missing.join("\n"));
}
