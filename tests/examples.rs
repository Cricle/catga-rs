//! Presence contracts for the public executable examples.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::Command,
};

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

    let expected_targets = EXAMPLES
        .iter()
        .map(|(name, path)| (name.to_string(), workspace.join("examples").join(path)))
        .collect();
    assert_eq!(
        example_binary_targets(workspace),
        expected_targets,
        "Cargo metadata must expose exactly the approved public example binaries"
    );

    let bin_directory = workspace.join("examples/src/bin");
    let bin_sources = bin_directory
        .is_dir()
        .then(|| {
            std::fs::read_dir(&bin_directory)
                .expect("the examples src/bin directory must be readable")
                .map(|entry| {
                    entry
                        .expect("the examples src/bin directory must be readable")
                        .path()
                })
                .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    assert!(
        bin_sources.is_empty(),
        "examples/src/bin must not contain auto-discovered binaries: {bin_sources:?}"
    );
}

fn example_binary_targets(workspace: &Path) -> BTreeSet<(String, PathBuf)> {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(workspace)
        .output()
        .expect("Cargo metadata must run");
    assert!(
        output.status.success(),
        "Cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("Cargo metadata must be valid JSON");
    let package = metadata["packages"]
        .as_array()
        .expect("Cargo metadata must list packages")
        .iter()
        .find(|package| package["name"].as_str() == Some("catga-examples"))
        .expect("Cargo metadata must include catga-examples");

    package["targets"]
        .as_array()
        .expect("catga-examples metadata must list targets")
        .iter()
        .filter(|target| {
            target["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some("bin")))
        })
        .map(|target| {
            (
                target["name"]
                    .as_str()
                    .expect("binary target metadata must include a name")
                    .to_string(),
                PathBuf::from(
                    target["src_path"]
                        .as_str()
                        .expect("binary target metadata must include a source path"),
                ),
            )
        })
        .collect()
}
