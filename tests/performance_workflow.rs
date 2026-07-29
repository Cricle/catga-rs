//! Static contracts for release-only and manually dispatched performance automation.

const PERFORMANCE_RUNNER: &str = include_str!("../scripts/performance.sh");
const RELEASE_WORKFLOW: &str = include_str!("../.github/workflows/release.yml");
const MANUAL_WORKFLOW: &str = include_str!("../.github/workflows/performance.yml");
const MEMORY_BENCHMARK: &str = include_str!("../crates/catga-memory/tests/memory_performance.rs");

#[test]
fn performance_runner_executes_every_manual_benchmark_class() {
    for benchmark in [
        "critical_path_performance",
        "mediator_performance",
        "flow_performance",
        "memory_performance",
        "nats_performance",
        "e2e_performance",
        "storage_performance",
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
    assert!(
        PERFORMANCE_RUNNER.contains("  ][],\n  ("),
        "the Markdown header must be emitted as lines rather than a JSON array"
    );
    for report in [
        "memory-performance.json",
        "critical-performance.json",
        "mediator-performance.json",
        "flow-performance.json",
        "nats-performance.json",
        "performance.json",
        "storage-performance.json",
    ] {
        assert!(
            PERFORMANCE_RUNNER.contains(report),
            "total table must include the {report} benchmark source"
        );
    }
    assert!(
        !PERFORMANCE_RUNNER.contains("extract_throughput"),
        "the total table must consume structured benchmark reports rather than parse logs"
    );
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
    for backend in ["SQLite", "MySQL", "PostgreSQL", "SQL Server", "Redis"] {
        assert!(
            PERFORMANCE_RUNNER.contains(backend),
            "total table must identify the {backend} storage benchmark"
        );
    }
}

#[test]
fn complete_performance_suite_is_manual_or_release_only() {
    assert!(RELEASE_WORKFLOW.contains("scripts/performance.sh --profile full"));
    assert!(MANUAL_WORKFLOW.contains("workflow_dispatch:"));
    assert!(MANUAL_WORKFLOW.contains("scripts/performance.sh --profile"));
}

#[test]
fn memory_benchmark_uses_the_complete_report_schema() {
    for field in [
        "source: \"in-process memory\"",
        "payload_bytes",
        "latency_scope",
    ] {
        assert!(
            MEMORY_BENCHMARK.contains(field),
            "memory benchmark must emit the complete report field {field}"
        );
    }
}
