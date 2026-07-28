//! Presence contracts for the public executable examples.

use std::path::Path;

const EXAMPLES: [&str; 5] = [
    "examples/src/bin/axum_checkout.rs",
    "examples/src/bin/mediator.rs",
    "examples/src/bin/flow.rs",
    "examples/src/bin/memory_transport.rs",
    "examples/src/bin/checkout.rs",
];

#[test]
fn public_examples_are_present() {
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
}
