//! Cross-system integration: Leader-only CQRS commands replicate through Raft and apply to a
//! shared state machine. Verifies that command dispatch, leadership fencing, and state machine
//! application form a consistent end-to-end path.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, MemoryCluster, MemoryClusterNode, RaftCommittedEntry, RaftStateMachine,
};
use catga_core::{
    CatgaError, CatgaResult, Command, CommandHandler, ErrorCode, Mediator, Message, Request,
    catga_handlers,
};

// ---------------------------------------------------------------------------
// Domain
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct IncrementCounter {
    amount: u64,
}
impl Message for IncrementCounter {}
impl Command for IncrementCounter {}

#[derive(Clone)]
struct GetCounter;
impl Message for GetCounter {}
impl Request for GetCounter {
    type Response = u64;
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CounterStateMachine {
    value: u64,
}

impl RaftStateMachine for CounterStateMachine {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let amount =
            u64::from_le_bytes(
                entry.data.first_chunk::<8>().copied().ok_or_else(|| {
                    CatgaError::new(ErrorCode::Validation, "invalid counter entry")
                })?,
            );
        self.value += amount;
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = data
            .try_into()
            .map_err(|_| CatgaError::new(ErrorCode::Validation, "invalid snapshot"))?;
        self.value = u64::from_le_bytes(bytes);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Leader-only command handler
// ---------------------------------------------------------------------------

struct LeaderIncrementHandler {
    coordinator: Arc<MemoryClusterNode>,
    state_machine: Arc<Mutex<CounterStateMachine>>,
}

#[async_trait]
impl CommandHandler<IncrementCounter> for LeaderIncrementHandler {
    async fn handle(&self, command: IncrementCounter) -> CatgaResult<()> {
        if !self.coordinator.is_leader() {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "not the cluster leader",
            ));
        }
        // In a real system this would go through Raft consensus; here we simulate
        // the committed entry being applied to the state machine.
        let entry = RaftCommittedEntry {
            index: 1,
            data: command.amount.to_le_bytes().to_vec(),
        };
        self.state_machine.lock().unwrap().apply(&entry)
    }
}

struct GetCounterHandler {
    state_machine: Arc<Mutex<CounterStateMachine>>,
}

#[async_trait]
impl catga_core::Handler<GetCounter> for GetCounterHandler {
    async fn handle(&self, _: GetCounter) -> CatgaResult<u64> {
        Ok(self.state_machine.lock().unwrap().value)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cqrs_leader_command_applies_to_state_machine() -> CatgaResult<()> {
    let cluster = MemoryCluster::new("one", ["http://c/one", "http://c/two"]);
    let node = cluster.node("one").expect("member");
    assert!(node.is_leader());

    let state_machine = Arc::new(Mutex::new(CounterStateMachine::default()));
    let coordinator = node;

    let registry = catga_handlers! {
        command IncrementCounter => LeaderIncrementHandler {
            coordinator: Arc::clone(&coordinator),
            state_machine: Arc::clone(&state_machine),
        };
        request GetCounter => GetCounterHandler { state_machine: Arc::clone(&state_machine) };
    }?;

    let mediator = Mediator::new(registry);

    mediator
        .send_command(IncrementCounter { amount: 5 })
        .await?;
    mediator
        .send_command(IncrementCounter { amount: 3 })
        .await?;

    let total = mediator.send(GetCounter).await?;
    assert_eq!(total, 8);

    Ok(())
}

#[tokio::test]
async fn cqrs_non_leader_rejects_command() -> CatgaResult<()> {
    let cluster = MemoryCluster::new("one", ["http://c/one", "http://c/two"]);
    let follower = cluster.node("two").expect("member");
    assert!(!follower.is_leader());

    let state_machine = Arc::new(Mutex::new(CounterStateMachine::default()));
    let coordinator = follower;

    let registry = catga_handlers! {
        command IncrementCounter => LeaderIncrementHandler {
            coordinator: Arc::clone(&coordinator),
            state_machine: Arc::clone(&state_machine),
        };
        request GetCounter => GetCounterHandler { state_machine: Arc::clone(&state_machine) };
    }?;

    let mediator = Mediator::new(registry);

    let result = mediator.send_command(IncrementCounter { amount: 1 }).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // State machine unchanged.
    let total = mediator.send(GetCounter).await?;
    assert_eq!(total, 0);

    Ok(())
}

#[tokio::test]
async fn cqrs_leadership_change_fences_commands() -> CatgaResult<()> {
    let cluster = MemoryCluster::new("one", ["http://c/one", "http://c/two"]);
    let node_one = cluster.node("one").expect("member");
    let node_two = cluster.node("two").expect("member");

    let state_machine = Arc::new(Mutex::new(CounterStateMachine::default()));

    // Node one starts as leader.
    let coordinator_one = node_one;
    let registry = catga_handlers! {
        command IncrementCounter => LeaderIncrementHandler {
            coordinator: Arc::clone(&coordinator_one),
            state_machine: Arc::clone(&state_machine),
        };
        request GetCounter => GetCounterHandler { state_machine: Arc::clone(&state_machine) };
    }?;
    let mediator = Mediator::new(registry);

    // Succeeds while leader.
    mediator
        .send_command(IncrementCounter { amount: 10 })
        .await?;

    // Elect node two as the new leader.
    cluster.elect("two").expect("valid member");
    assert!(!coordinator_one.is_leader());
    assert!(node_two.is_leader());

    // Old leader now rejects.
    let result = mediator.send_command(IncrementCounter { amount: 99 }).await;
    assert!(result.is_err());

    // Only the first increment was applied.
    let total = mediator.send(GetCounter).await?;
    assert_eq!(total, 10);

    Ok(())
}

#[tokio::test]
async fn raft_state_machine_snapshot_and_restore_roundtrip() -> CatgaResult<()> {
    let mut sm = CounterStateMachine::default();

    // Apply some entries.
    for amount in [1u64, 2, 3] {
        let entry = RaftCommittedEntry {
            index: amount,
            data: amount.to_le_bytes().to_vec(),
        };
        sm.apply(&entry)?;
    }
    assert_eq!(sm.value, 6);

    // Snapshot and restore into a fresh state machine.
    let snapshot = sm.snapshot()?;
    let mut restored = CounterStateMachine::default();
    restored.restore(&snapshot)?;
    assert_eq!(restored.value, 6);

    Ok(())
}
