//! Free helpers for durable flow wait evaluation and staleness checks.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, ErrorCode};

use crate::{WaitCondition, WaitPolicy};

pub(crate) const MAX_CHILD_LAUNCH_CAS_RETRIES: usize = 8;

pub(crate) enum WaitEvaluation {
    Pending,
    Ready,
    Failed(CatgaError),
}

pub(crate) fn evaluate_wait(wait: &WaitCondition, now: SystemTime) -> WaitEvaluation {
    if wait.is_expired_at(now) {
        return WaitEvaluation::Failed(CatgaError::new(
            ErrorCode::Timeout,
            "flow wait condition timed out",
        ));
    }
    match wait.policy() {
        WaitPolicy::All => {
            if let Some(error) = wait
                .results()
                .iter()
                .find_map(|result| result.error().cloned())
            {
                return WaitEvaluation::Failed(error);
            }
            if wait.completed_count() >= wait.expected_count() {
                WaitEvaluation::Ready
            } else {
                WaitEvaluation::Pending
            }
        }
        WaitPolicy::Any => {
            if wait.results().iter().any(|result| result.is_success()) {
                return WaitEvaluation::Ready;
            }
            if wait.completed_count() >= wait.expected_count() {
                let error = wait
                    .results()
                    .last()
                    .and_then(|result| result.error().cloned())
                    .unwrap_or_else(|| {
                        CatgaError::new(ErrorCode::Transient, "all flow wait children failed")
                    });
                WaitEvaluation::Failed(error)
            } else {
                WaitEvaluation::Pending
            }
        }
    }
}

pub(crate) fn is_stale(heartbeat: SystemTime, now: SystemTime, stale_after: Duration) -> bool {
    now.duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}
