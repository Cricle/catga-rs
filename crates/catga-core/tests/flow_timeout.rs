//! Tests for timeout module

use std::time::{Duration, SystemTime};

use catga_core::flow::{FlowState, WaitCondition, WaitPolicy};
use catga_core::flow::timeout::{
    flow_timeout_deadline_unix_ms, FlowTimeoutOptions, TimedOutFlowPoll, TimedOutFlowReceipt,
    MAX_FLOW_TIMEOUT_BATCH_SIZE, MAX_FLOW_TIMEOUT_SCAN_LIMIT,
};
use catga_core::{ErrorCode, CatgaResult};

#[test]
fn timeout_polls_options_and_receipts_validate_all_bounds() -> CatgaResult<()> {
    let now = SystemTime::UNIX_EPOCH;
    let poll = TimedOutFlowPoll::new(now, 2, 4)?;
    assert_eq!(poll.now(), now);
    assert_eq!(poll.limit(), 2);
    assert_eq!(poll.scan_limit(), 4);
    for (limit, scan_limit) in [
        (0, 1),
        (1, 0),
        (2, 1),
        (MAX_FLOW_TIMEOUT_BATCH_SIZE + 1, 2),
        (1, MAX_FLOW_TIMEOUT_SCAN_LIMIT + 1),
    ] {
        assert_eq!(
            TimedOutFlowPoll::new(now, limit, scan_limit)
                .expect_err("invalid timeout poll bounds are rejected")
                .code(),
            ErrorCode::Validation
        );
    }

    let options = FlowTimeoutOptions::new(Duration::from_secs(1), 2, 4)?;
    assert_eq!(options.batch_size, 2);
    assert!(FlowTimeoutOptions::new(Duration::ZERO, 1, 1).is_err());
    assert!(FlowTimeoutOptions::new(Duration::from_secs(1), 0, 1).is_err());

    let receipt = TimedOutFlowReceipt::new("flow", [1_u8, 2]);
    assert_eq!(receipt.flow_id(), "flow");
    assert_eq!(receipt.token(), [1, 2]);
    Ok(())
}

#[test]
fn timeout_deadlines_require_a_suspended_wait_and_round_up_fractional_millis() -> CatgaResult<()> {
    use catga_core::flow::suspension::FlowContinuation;

    let state = FlowState::new("flow", "checkout", [], "worker");
    assert_eq!(
        flow_timeout_deadline_unix_ms(&FlowContinuation::new(state.clone(), "step"))?,
        None
    );
    let suspended = state.suspended();
    assert_eq!(
        flow_timeout_deadline_unix_ms(&FlowContinuation::new(suspended.clone(), "step"))?,
        None
    );
    let wait = WaitCondition::new(
        "wait",
        WaitPolicy::All,
        1,
        SystemTime::UNIX_EPOCH + Duration::from_nanos(500),
        Duration::from_nanos(500),
    );
    let continuation = FlowContinuation::waiting(suspended, "step", wait);
    assert_eq!(
        flow_timeout_deadline_unix_ms(&continuation)?,
        Some(1)
    );
    Ok(())
}
