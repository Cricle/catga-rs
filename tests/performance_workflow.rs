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
        "memory_performance",
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
fn performance_runner_publishes_a_total_table_with_memory_metrics() {
    assert!(
        PERFORMANCE_RUNNER.contains("memory-performance.json"),
        "memory measurements must be retained in their own machine-readable artifact"
    );
    assert!(
        PERFORMANCE_RUNNER.contains("summary.md"),
        "performance runner must publish a Markdown total table"
    );
    for report in [
        "memory-performance.json",
        "in-process-performance.json",
        "nats-performance.json",
        "performance.json",
    ] {
        assert!(
            PERFORMANCE_RUNNER.contains(report),
            "total table must include the {report} benchmark source"
        );
    }
    assert!(
        PERFORMANCE_RUNNER.contains("p50_ns")
            && PERFORMANCE_RUNNER.contains("p95_ns")
            && PERFORMANCE_RUNNER.contains("p99_ns"),
        "total table must expose latency percentiles"
    );
    assert!(
        PERFORMANCE_RUNNER.contains("rss_before_bytes")
            && PERFORMANCE_RUNNER.contains("rss_after_bytes")
            && PERFORMANCE_RUNNER.contains("rss_peak_bytes"),
        "total table must label Linux RSS measurements explicitly"
    );
}

#[test]
fn complete_performance_suite_is_manual_or_release_only() {
    assert!(RELEASE_WORKFLOW.contains("scripts/performance.sh --profile full"));
    assert!(MANUAL_WORKFLOW.contains("workflow_dispatch:"));
    assert!(MANUAL_WORKFLOW.contains("scripts/performance.sh --profile"));
}
