//! Documentation-surface contract tests.

const README: &str = include_str!("../README.md");

#[test]
fn readme_exposes_the_required_user_onboarding_sections() {
    // Check for required Chinese headings
    for heading in ["## 安装", "## 快速开始", "## 核心功能", "## 示例"] {
        assert!(README.contains(heading), "README is missing {heading}");
    }
}
