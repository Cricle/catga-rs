//! Shared bounded due-receipt contract for durable continuation stores.

use std::time::{Duration, SystemTime};

use catga_core::CatgaResult;
use catga_core::flow::{
    FlowContinuation, FlowState, TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};

const FUTURE_FLOW_COUNT: usize = 64;
const RECEIPT_POLL_COUNT: usize = 3;
const RECEIPT_SCAN_LIMIT: usize = 3;

pub async fn run_timeout_store_contract<S>(
    store: &S,
    prefix: &str,
    stream_history: bool,
) -> CatgaResult<()>
where
    S: TimedOutFlowStore + ?Sized,
{
    let poll_budget = if stream_history {
        FUTURE_FLOW_COUNT.div_ceil(RECEIPT_SCAN_LIMIT) + RECEIPT_POLL_COUNT
    } else {
        RECEIPT_POLL_COUNT
    };
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
    for index in 0..FUTURE_FLOW_COUNT {
        let id = format!("{prefix}/future/{index}");
        assert!(
            store
                .create(FlowContinuation::waiting(
                    FlowState::new(id.as_str(), "timeout-contract", [], "node-a").suspended(),
                    "finish",
                    WaitCondition::new(
                        format!("{id}/wait"),
                        WaitPolicy::All,
                        1,
                        now,
                        Duration::from_secs(60),
                    ),
                ))
                .await?
        );
    }

    let ids = (0..5)
        .map(|index| format!("{prefix}/{index}"))
        .collect::<Vec<_>>();
    for id in &ids {
        assert!(
            store
                .create(FlowContinuation::waiting(
                    FlowState::new(id.as_str(), "timeout-contract", [], "node-a").suspended(),
                    "finish",
                    WaitCondition::new(
                        format!("{id}/wait"),
                        WaitPolicy::All,
                        1,
                        now - Duration::from_secs(2),
                        Duration::from_secs(1),
                    ),
                ))
                .await?
        );
    }

    let mut found = Vec::new();
    for _ in 0..poll_budget {
        let receipts = store
            .poll_timed_out(&TimedOutFlowPoll::new(now, 2, RECEIPT_SCAN_LIMIT)?)
            .await?;
        assert!(receipts.len() <= 2);
        for receipt in receipts {
            found.push(receipt.flow_id().to_owned());
            store.ack_timed_out(&receipt).await?;
        }
        if found.len() == ids.len() {
            break;
        }
    }
    found.sort();
    found.dedup();
    let mut expected = ids;
    expected.sort();
    assert_eq!(found, expected);

    let released_id = format!("{prefix}/released");
    assert!(
        store
            .create(FlowContinuation::waiting(
                FlowState::new(released_id.as_str(), "timeout-contract", [], "node-a").suspended(),
                "finish",
                WaitCondition::new(
                    format!("{released_id}/wait"),
                    WaitPolicy::All,
                    1,
                    now - Duration::from_secs(2),
                    Duration::from_secs(1),
                ),
            ))
            .await?
    );
    let receipt = store
        .poll_timed_out(&TimedOutFlowPoll::new(now, 1, 1)?)
        .await?
        .pop()
        .expect("released due receipt");
    assert_eq!(receipt.flow_id(), released_id);
    store.release_timed_out(&receipt).await?;
    let redelivered = store
        .poll_timed_out(&TimedOutFlowPoll::new(now, 1, 1)?)
        .await?;
    assert_eq!(redelivered[0].flow_id(), released_id);
    store.ack_timed_out(&redelivered[0]).await?;

    let stale_id = format!("{prefix}/stale");
    let stale = FlowContinuation::waiting(
        FlowState::new(stale_id.as_str(), "timeout-contract", [], "node-a").suspended(),
        "finish",
        WaitCondition::new(
            format!("{stale_id}/wait"),
            WaitPolicy::All,
            1,
            now - Duration::from_secs(2),
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(stale.clone()).await?);
    let ready = stale
        .clone()
        .ready()
        .with_state(stale.state().clone().running().next_version()?);
    assert!(store.update(0, ready).await?);
    assert!(
        store
            .poll_timed_out(&TimedOutFlowPoll::new(now, 2, 3)?)
            .await?
            .is_empty()
    );
    Ok(())
}
