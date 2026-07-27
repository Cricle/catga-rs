//! Documentation-surface contract tests.

const README: &str = include_str!("../README.md");

#[test]
fn readme_exposes_the_required_user_onboarding_sections() {
    for heading in ["## Quick start", "## Flow", "## FlowStore", "## Features"] {
        assert!(README.contains(heading), "README is missing {heading}");
    }
}
