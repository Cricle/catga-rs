//! Contract tests for typed aggregate test scenarios.

use catga_core::{Aggregate, CatgaResult, Envelope, MessageMetadata};
use catga_core::testing::AggregateScenario;

#[derive(Clone)]
struct Balance {
    id: Box<str>,
    version: i64,
    total: u64,
    pending: Vec<Envelope>,
}

impl Aggregate for Balance {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            version: -1,
            total: 0,
            pending: Vec::new(),
        }
    }

    fn stream_id(id: &str) -> Box<str> {
        format!("balance:{id}").into()
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn apply(&mut self, event: &Envelope) -> CatgaResult<()> {
        self.total += u64::from(event.payload()[0]);
        self.version += 1;
        Ok(())
    }

    fn pending_events(&self) -> &[Envelope] {
        &self.pending
    }

    fn clear_pending_events(&mut self) {
        self.pending.clear();
    }
}

fn credited(id: u64, amount: u8) -> Envelope {
    Envelope::new(
        id,
        "balance.credited",
        vec![amount],
        MessageMetadata::new(id, None),
    )
}

#[tokio::test]
async fn aggregate_scenario_replays_seeded_envelopes_and_asserts_history() {
    let seeded = vec![credited(1, 4), credited(2, 5)];
    let scenario = AggregateScenario::<Balance>::new("account-42")
        .expect("non-empty aggregate ids are valid test setup");

    let replay = scenario.replay(&seeded).await.expect("history replays");

    assert_eq!(replay.aggregate().total, 9);
    replay
        .assert_version(1)
        .expect("replay retains the persisted stream version");
    replay
        .assert_events(&seeded)
        .expect("replay exposes the immutable persisted envelopes");
}

#[test]
fn aggregate_scenario_rejects_an_empty_aggregate_id() {
    let error = AggregateScenario::<Balance>::new("")
        .err()
        .expect("an aggregate scenario needs a stable aggregate id");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
}
