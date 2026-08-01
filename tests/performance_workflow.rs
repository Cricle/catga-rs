//! Static contracts for release-only and manually dispatched performance automation.

const PERFORMANCE_RUNNER: &str = include_str!("../scripts/performance.sh");
const COVERAGE_RUNNER: &str = include_str!("../scripts/coverage.sh");
const E2E_RUNNER: &str = include_str!("../scripts/e2e.sh");
const CI_WORKFLOW: &str = include_str!("../.github/workflows/ci.yml");
const RELEASE_WORKFLOW: &str = include_str!("../.github/workflows/release.yml");
const MANUAL_WORKFLOW: &str = include_str!("../.github/workflows/performance.yml");
const MEMORY_BENCHMARK: &str = include_str!("../crates/catga-memory/tests/memory_performance.rs");
const SQLITE_FLOW_STORE: &str = include_str!("../crates/catga-flow-store/src/sqlite.rs");
const MYSQL_FLOW_STORE: &str = include_str!("../crates/catga-flow-store/src/mysql.rs");
const POSTGRES_FLOW_STORE: &str = include_str!("../crates/catga-flow-store/src/postgres.rs");
const MSSQL_FLOW_STORE: &str = include_str!("../crates/catga-flow-store/src/mssql.rs");
const STORAGE_BENCHMARK: &str = include_str!("storage_performance.rs");
const MEMORYPACK_CODEC_MANIFEST: &str = include_str!("../crates/catga-codec-memorypack/Cargo.toml");
const MEMORYPACK_DERIVE_MANIFEST: &str =
    include_str!("../crates/catga-codec-memorypack/memorypack-derive/Cargo.toml");

#[test]
fn performance_runner_executes_every_manual_benchmark_class() {
    for benchmark in [
        "critical_path_performance",
        "mediator_performance",
        "mediator_pure_throughput",
        "typed_mediator_bench",
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
    // performance.sh may be checked out with CRLF line endings on Windows; compare on a
    // normalized view so the assertion is platform-independent.
    let runner = PERFORMANCE_RUNNER.replace("\r\n", "\n");
    assert!(
        runner.contains("  ][],\n  ("),
        "the Markdown header must be emitted as lines rather than a JSON array"
    );
    for report in [
        "memory-performance.json",
        "critical-performance.json",
        "mediator-performance.json",
        "mediator-pure-performance.json",
        "typed-mediator-performance.json",
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
fn release_publishes_and_summarizes_the_complete_performance_report() {
    for asset in ["summary.md", "summary.txt", "*.json"] {
        assert!(
            RELEASE_WORKFLOW.contains(asset),
            "the release workflow must upload the complete performance asset set including {asset}"
        );
    }
    for release_body_update in ["--notes-file", "catga-performance-summary"] {
        assert!(
            RELEASE_WORKFLOW.contains(release_body_update),
            "the release workflow must publish the performance table in the release body"
        );
    }
}

#[test]
fn release_uses_catga_owned_memorypack_derive_and_safe_registry_checks() {
    assert!(
        MEMORYPACK_DERIVE_MANIFEST.contains("name = \"catga-memorypack-derive\""),
        "the vendored derive macro must use a Catga-owned registry name"
    );
    assert!(
        MEMORYPACK_CODEC_MANIFEST.contains("catga-memorypack-derive"),
        "the packaged codec must depend on the Catga-owned derive macro"
    );
    assert!(
        RELEASE_WORKFLOW.contains("catga-memorypack-derive"),
        "the derive macro must publish before the codec that depends on it"
    );
    for registry_safety_check in ["--user-agent", "http_status", "404"] {
        assert!(
            RELEASE_WORKFLOW.contains(registry_safety_check),
            "release registry checks must handle {registry_safety_check} explicitly"
        );
    }
    assert!(
        RELEASE_WORKFLOW.contains("version=\"${package_id##*@}\""),
        "release registry checks must extract the pure SemVer suffix from every Cargo package ID"
    );
    assert!(
        RELEASE_WORKFLOW.contains("cargo check --workspace --no-default-features"),
        "release publishing must verify public APIs without optional backend features"
    );
    for rate_limit_handling in [
        "publish_with_rate_limit_retry",
        "status 429 Too Many Requests",
        "sleep 300",
    ] {
        assert!(
            RELEASE_WORKFLOW.contains(rate_limit_handling),
            "release publishing must handle crates.io rate limits with {rate_limit_handling}"
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
fn coverage_gate_requires_eighty_percent_without_relaxing_e2e() {
    for source in [COVERAGE_RUNNER, CI_WORKFLOW] {
        assert!(
            source.contains("required_line_coverage=80")
                || source.contains("--required-line-coverage 80"),
            "line coverage must require 80 percent"
        );
        assert!(
            source.contains("required_region_coverage=80")
                || source.contains("--required-region-coverage 80"),
            "region coverage must require 80 percent"
        );
        assert!(
            source.contains("95"),
            "the independent Docker E2E pass-rate gate must remain strict"
        );
    }
}

#[test]
fn ci_runs_strict_workspace_and_e2e_gates_once() {
    assert!(
        COVERAGE_RUNNER.contains("llvm-cov test --workspace --all-features --no-report"),
        "the coverage job must execute the complete workspace test suite"
    );
    assert!(
        COVERAGE_RUNNER.contains("--profile \"$profile\" --coverage"),
        "the coverage job must execute the complete Docker E2E scenario matrix with instrumentation"
    );
    assert!(
        !CI_WORKFLOW.contains("\n  e2e:\n"),
        "CI must not execute the full Docker E2E matrix a second time"
    );
    assert_eq!(
        CI_WORKFLOW.matches("--e2e-jobs 2").count(),
        1,
        "CI must use bounded parallelism for the instrumented Docker E2E matrix"
    );
    assert!(
        !CI_WORKFLOW.contains("      - run: cargo test --workspace --all-features"),
        "CI must not execute the workspace test suite outside the coverage gate"
    );
    for artifact in [
        "name: e2e-results",
        "target/coverage/e2e-results.json",
        "target/e2e-logs",
    ] {
        assert!(
            CI_WORKFLOW.contains(artifact),
            "the consolidated coverage job must retain the {artifact} artifact"
        );
    }
}

#[test]
fn e2e_runner_groups_equivalent_test_invocations_without_dropping_scenarios() {
    assert!(
        E2E_RUNNER.contains("group_by([.package, .target, .testArguments])"),
        "E2E execution must first compare package, target, and test-argument invocations"
    );
    assert!(
        E2E_RUNNER.contains("any(.[]; .testFilter == null)"),
        "a complete target invocation may cover filtered scenarios, but filtered-only groups must stay isolated"
    );
    assert!(
        E2E_RUNNER.contains(".scenarios[]"),
        "the grouped runner must still record one result for every declared scenario"
    );
    assert!(
        E2E_RUNNER.contains("executionGroup"),
        "the grouped runner must disclose when scenarios share one target execution"
    );
    assert!(
        E2E_RUNNER.contains("filter=$(jq -r '.testFilter // empty' <<<\"$group\")"),
        "a grouped invocation must retain its test filter when building the Cargo command"
    );
}

#[test]
fn e2e_runner_refills_a_completed_parallel_slot() {
    assert!(
        E2E_RUNNER.contains("wait -n -p"),
        "parallel E2E execution must start the next group as soon as one group completes"
    );
}

#[test]
fn storage_benchmark_uses_fresh_services_after_the_functional_e2e_gate() {
    assert!(
        PERFORMANCE_RUNNER.contains("compose_project=catga-performance"),
        "storage benchmarks must use a dedicated Compose project and volume set"
    );
    assert!(
        PERFORMANCE_RUNNER.contains("start_benchmark_services"),
        "the performance runner must start fresh services after functional E2E completes"
    );
    assert!(
        !PERFORMANCE_RUNNER.contains("--profile \"$profile\" --keep-services"),
        "functional E2E must clean up before the benchmark services start"
    );
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

#[test]
fn sql_flow_updates_use_one_business_version_compare_and_swap() {
    for (backend, source) in [
        ("sqlite", SQLITE_FLOW_STORE),
        ("mysql", MYSQL_FLOW_STORE),
        ("postgres", POSTGRES_FLOW_STORE),
        ("mssql", MSSQL_FLOW_STORE),
    ] {
        let update = source
            .split("pub(crate) async fn update")
            .nth(1)
            .and_then(|body| body.split("async fn").next())
            .expect("FlowStore update implementation must exist");
        assert!(
            !update.contains("load(pool"),
            "{backend} updates must avoid the extra pre-read round trip"
        );
        assert!(
            update.contains("expected_version"),
            "{backend} updates must retain the public business-version CAS fence"
        );
    }
}

#[test]
fn storage_benchmark_reports_pool_concurrency_capacity() {
    assert!(
        STORAGE_BENCHMARK.contains("CONCURRENCY_LEVELS"),
        "storage performance reports must measure bounded pool concurrency"
    );
    assert!(
        STORAGE_BENCHMARK.contains("bounded_concurrency"),
        "storage performance reports must identify their concurrency level"
    );
    assert!(
        STORAGE_BENCHMARK.contains("16"),
        "storage performance reports must measure the server-pool saturation point"
    );
    assert!(
        include_str!("../crates/catga-flow-store/src/flow_store.rs")
            .contains("SqlFlowStoreOptions"),
        "server FlowStore constructors must expose user-configurable pool options"
    );
}

#[test]
fn storage_benchmark_reports_lifecycle_phases_and_database_counter_deltas() {
    for field in ["phase_latencies", "database_metric_deltas"] {
        assert!(
            include_str!("support/performance_report.rs").contains(field),
            "the structured performance report must retain {field}"
        );
    }
    for helper in [
        "capture_mysql_metrics",
        "capture_postgres_metrics",
        "capture_mssql_metrics",
        "database_metric_deltas",
    ] {
        assert!(
            STORAGE_BENCHMARK.contains(helper),
            "storage benchmark must collect {helper}"
        );
    }
    assert!(
        STORAGE_BENCHMARK.contains("pg_stat_wal"),
        "PostgreSQL WAL bytes must be collected from pg_stat_wal rather than pg_stat_database"
    );
    for phase in ["create", "get", "update"] {
        assert!(
            STORAGE_BENCHMARK.contains(phase),
            "storage benchmark must report {phase} latency separately"
        );
    }
    for field in ["phase_latencies", "database_metric_deltas"] {
        assert!(
            PERFORMANCE_RUNNER.contains(field),
            "the published performance summary must render {field}"
        );
    }
}
