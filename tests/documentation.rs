//! Documentation-surface contract tests.

const README: &str = include_str!("../README.md");

#[test]
fn readme_exposes_the_required_user_onboarding_sections() {
    for heading in ["## Quick start", "## Flow", "## FlowStore", "## Features"] {
        assert!(README.contains(heading), "README is missing {heading}");
    }

    assert!(README.contains("catga_handlers!"));
    assert!(!README.contains("registry.register_request::<Double, _>(DoubleHandler)?"));
    assert!(
        README.contains("NATS JetStream tests start and remove an isolated Testcontainers server")
    );
    assert!(README.contains("marked `#[ignore]` locally."));
    assert!(
        README.contains("test-only mailbox-creation control-plane harness"),
        "README must distinguish mq9 protocol testing from a real RobustMQ broker"
    );
}
