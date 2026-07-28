//! Static contracts for release-only and manually dispatched performance automation.

const PERFORMANCE_RUNNER: &str = include_str!("../scripts/performance.sh");
const RELEASE_WORKFLOW: &str = include_str!("../.github/workflows/release.yml");
const MANUAL_WORKFLOW: &str = include_str!("../.github/workflows/performance.yml");

#[test]
fn performance_runner_executes_every_manual_benchmark_class() {
    for benchmark in [
        "critical_path_performance",
        "mediator_performance",
        "flow_performance",
        "nats_performance",
        "e2e_performance",
    ] {
        assert!(
            PERFORMANCE_RUNNER.contains(benchmark),
            "performance runner must execute {benchmark}"
        );
    }
    assert!(
        PERFORMANCE_RUNNER.contains("cargo test --release -p catga-tests"),
        "performance runner must measure optimized release binaries"
    );
    assert!(PERFORMANCE_RUNNER.contains("--ignored --nocapture"));
}

#[test]
fn complete_performance_suite_is_manual_or_release_only() {
    assert!(RELEASE_WORKFLOW.contains("scripts/performance.sh --profile full"));
    assert!(MANUAL_WORKFLOW.contains("workflow_dispatch:"));
    assert!(MANUAL_WORKFLOW.contains("scripts/performance.sh --profile"));
}
