//! Compile contracts for the public examples.

use std::{path::Path, process::Command};

const EXAMPLES: [&str; 4] = [
    "crates/catga-core/examples/mediator_basics.rs",
    "crates/catga-flow/examples/flow_basics.rs",
    "crates/catga-flow-store/examples/flow_store_features.rs",
    "crates/catga-memory/examples/transport_basics.rs",
];

#[test]
fn public_examples_exist_and_compile_with_every_feature() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or_else(|| {
            panic!("the integration-test crate must live directly below the workspace root")
        });

    for example in EXAMPLES {
        assert!(
            workspace.join(example).is_file(),
            "missing required public example: {example}"
        );
    }

    let status = Command::new("cargo")
        .args(["check", "--examples", "--workspace", "--all-features"])
        .current_dir(workspace)
        .status()
        .unwrap_or_else(|error| panic!("failed to run cargo check for examples: {error}"));
    assert!(
        status.success(),
        "public examples must compile with every feature"
    );
}
