//! Manual caller-facing local Flow and DSL execution throughput benchmarks.
//!
//! Run only when measuring performance:
//! `cargo test --manifest-path tests/Cargo.toml --test flow_performance -- --ignored --nocapture`
//!
//! The timed intervals exclude flow construction, state allocation, and warm-up calls. Each
//! benchmark keeps correctness assertions active, reports observed throughput for a fixed
//! workload, and deliberately has no host-dependent timing threshold.

use std::time::Instant;

use catga_core::CatgaResult;
use catga_flow::{DslFlow, Flow, dsl_action};

#[path = "support/performance_report.rs"]
mod performance_report;

const FLOW_COUNT: usize = 4_096;
const STEPS_PER_FLOW: u32 = 3;

/// Measures complete caller-facing local [`Flow::run`] execution throughput.
#[tokio::test]
#[ignore = "manual performance benchmark; run with --ignored --nocapture"]
async fn local_flow_execution_throughput_benchmark() -> CatgaResult<()> {
    let flows = (0..FLOW_COUNT)
        .map(|_| local_flow_fixture())
        .collect::<Vec<_>>();

    assert_successful_flow(local_flow_fixture().run().await);

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(FLOW_COUNT);
    for flow in flows {
        let operation_started = Instant::now();
        assert_successful_flow(flow.run().await);
        latencies.push(operation_started.elapsed());
    }
    let elapsed = started.elapsed();
    let executions_per_second = (FLOW_COUNT as f64) / elapsed.as_secs_f64();

    println!(
        "local_flow_execution_throughput: flows={FLOW_COUNT}, steps_per_flow={STEPS_PER_FLOW}, total={elapsed:?}, flows_per_second={executions_per_second:.2}"
    );
    write_report("local_flow_execution", elapsed, latencies, rss_before_bytes)?;
    Ok(())
}

/// Measures complete caller-facing [`DslFlow::run`] execution throughput.
#[tokio::test]
#[ignore = "manual performance benchmark; run with --ignored --nocapture"]
async fn local_dsl_flow_execution_throughput_benchmark() -> CatgaResult<()> {
    let flow = dsl_flow_fixture();
    let mut states = vec![0_u32; FLOW_COUNT];
    let mut warm_up_state = 0_u32;

    flow.run(&mut warm_up_state).await?;
    assert_eq!(warm_up_state, STEPS_PER_FLOW);

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(FLOW_COUNT);
    for state in &mut states {
        let operation_started = Instant::now();
        flow.run(state).await?;
        assert_eq!(*state, STEPS_PER_FLOW);
        latencies.push(operation_started.elapsed());
    }
    let elapsed = started.elapsed();
    let executions_per_second = (FLOW_COUNT as f64) / elapsed.as_secs_f64();

    println!(
        "local_dsl_flow_execution_throughput: flows={FLOW_COUNT}, steps_per_flow={STEPS_PER_FLOW}, total={elapsed:?}, flows_per_second={executions_per_second:.2}"
    );
    write_report(
        "local_dsl_flow_execution",
        elapsed,
        latencies,
        rss_before_bytes,
    )?;
    Ok(())
}

fn write_report(
    name: &'static str,
    elapsed: std::time::Duration,
    latencies: Vec<std::time::Duration>,
    rss_before_bytes: Option<u64>,
) -> CatgaResult<()> {
    let report = performance_report::PerformanceReport {
        schema_version: 1,
        source: "in-process flow",
        environment: performance_report::environment(),
        results: vec![performance_report::measured(
            name,
            None,
            elapsed,
            latencies,
            "flow execution",
            rss_before_bytes,
        )],
        database_metric_deltas: Vec::new(),
    };
    performance_report::write_report_if_configured(&report)
        .map_err(|error| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, error))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    Ok(())
}

/// Builds the fixed three-step local flow outside the timed benchmark interval.
fn local_flow_fixture() -> Flow {
    Flow::new("local-flow-performance")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
}

/// Asserts the complete successful outcome expected from every local flow execution.
fn assert_successful_flow(result: catga_flow::FlowResult) {
    assert!(result.is_success());
    assert_eq!(result.completed_steps(), STEPS_PER_FLOW);
}

/// Builds the fixed three-step local DSL flow outside the timed benchmark interval.
fn dsl_flow_fixture() -> DslFlow<u32> {
    DslFlow::new()
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
}
