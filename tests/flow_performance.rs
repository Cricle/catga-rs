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

    let started = Instant::now();
    for flow in flows {
        assert_successful_flow(flow.run().await);
    }
    let elapsed = started.elapsed();
    let executions_per_second = (FLOW_COUNT as f64) / elapsed.as_secs_f64();

    println!(
        "local_flow_execution_throughput: flows={FLOW_COUNT}, steps_per_flow={STEPS_PER_FLOW}, total={elapsed:?}, flows_per_second={executions_per_second:.2}"
    );
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

    let started = Instant::now();
    for state in &mut states {
        flow.run(state).await?;
        assert_eq!(*state, STEPS_PER_FLOW);
    }
    let elapsed = started.elapsed();
    let executions_per_second = (FLOW_COUNT as f64) / elapsed.as_secs_f64();

    println!(
        "local_dsl_flow_execution_throughput: flows={FLOW_COUNT}, steps_per_flow={STEPS_PER_FLOW}, total={elapsed:?}, flows_per_second={executions_per_second:.2}"
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
