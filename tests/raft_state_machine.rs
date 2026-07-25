use catga_cluster::{
    RaftCommittedEntry, RaftMember, RaftMessage, RaftNode, RaftStateMachine, RaftStateMachineDriver,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

#[derive(Default)]
struct Counter {
    applications: usize,
    value: u64,
}

struct FailsOnceCounter {
    failed: bool,
    value: u64,
}

impl RaftStateMachine for FailsOnceCounter {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        if !self.failed {
            self.failed = true;
            return Err(CatgaError::new(ErrorCode::Transient, "retry application"));
        }
        let bytes: [u8; 8] = entry.data.as_slice().try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "counter commands must contain eight bytes",
            )
        })?;
        self.value += u64::from_le_bytes(bytes);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "counter snapshots must contain eight bytes",
            )
        })?;
        self.value = u64::from_le_bytes(bytes);
        Ok(())
    }
}

impl RaftStateMachine for Counter {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let bytes: [u8; 8] = entry.data.as_slice().try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "counter commands must contain eight bytes",
            )
        })?;
        self.applications += 1;
        self.value += u64::from_le_bytes(bytes);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "counter snapshots must contain eight bytes",
            )
        })?;
        self.value = u64::from_le_bytes(bytes);
        Ok(())
    }
}

fn member() -> RaftMember {
    RaftMember::new(1, "http://node-1")
}

fn members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
        RaftMember::new(3, "http://node-3"),
    ]
}

fn relay(drivers: &mut [RaftStateMachineDriver<Counter>]) {
    for _ in 0..100 {
        let messages: Vec<RaftMessage> = drivers
            .iter_mut()
            .flat_map(RaftStateMachineDriver::drain_messages)
            .collect();
        if messages.is_empty() {
            return;
        }
        for message in messages {
            drivers
                .iter_mut()
                .find(|driver| driver.id() == message.to)
                .expect("Raft must only address configured peers")
                .step(message)
                .unwrap();
        }
    }
    panic!("Raft messages did not quiesce");
}

#[test]
fn state_machine_driver_applies_committed_commands_in_log_order() {
    let node = RaftNode::new(1, "http://node-1", vec![member()]).unwrap();
    let mut driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();

    driver.campaign().unwrap();
    driver.propose(3_u64.to_le_bytes()).unwrap();
    driver.propose(4_u64.to_le_bytes()).unwrap();

    assert_eq!(driver.apply_committed().unwrap(), 2);
    assert_eq!(driver.machine().applications, 2);
    assert_eq!(driver.machine().value, 7);
}

#[test]
fn state_machine_drivers_apply_the_same_replicated_command_once_per_node() {
    let cluster_members = members();
    let mut drivers = cluster_members
        .iter()
        .map(|member| {
            RaftStateMachineDriver::new(
                RaftNode::new(member.id(), member.endpoint(), cluster_members.clone()).unwrap(),
                Counter::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    drivers[0].campaign().unwrap();
    relay(&mut drivers);
    drivers[0].propose(11_u64.to_le_bytes()).unwrap();
    relay(&mut drivers);

    for driver in &mut drivers {
        assert_eq!(driver.apply_committed().unwrap(), 1);
        assert_eq!(driver.machine().applications, 1);
        assert_eq!(driver.machine().value, 11);
    }
}

#[test]
fn in_memory_checkpoint_at_the_log_tip_allows_later_proposals() {
    let node = RaftNode::new(1, "http://node-1", vec![member()]).unwrap();
    let mut driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();

    driver.campaign().unwrap();
    driver.propose(7_u64.to_le_bytes()).unwrap();
    assert_eq!(driver.apply_committed().unwrap(), 1);
    driver.checkpoint().unwrap();

    driver.propose(5_u64.to_le_bytes()).unwrap();
    assert_eq!(driver.apply_committed().unwrap(), 1);
    assert_eq!(driver.machine().value, 12);
}

#[test]
fn in_memory_checkpoint_rejects_an_unapplied_log_tail_without_losing_it() {
    let node = RaftNode::new(1, "http://node-1", vec![member()]).unwrap();
    let mut driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();

    driver.campaign().unwrap();
    driver.propose(7_u64.to_le_bytes()).unwrap();
    assert_eq!(driver.apply_committed().unwrap(), 1);
    driver.propose(5_u64.to_le_bytes()).unwrap();

    assert!(driver.checkpoint().is_err());
    assert_eq!(driver.apply_committed().unwrap(), 1);
    assert_eq!(driver.machine().value, 12);
    driver.checkpoint().unwrap();
}

#[test]
fn state_machine_keeps_a_failed_application_for_explicit_retry() {
    let node = RaftNode::new(1, "http://node-1", vec![member()]).unwrap();
    let mut driver = RaftStateMachineDriver::new(
        node,
        FailsOnceCounter {
            failed: false,
            value: 0,
        },
    )
    .unwrap();

    driver.campaign().unwrap();
    driver.propose(9_u64.to_le_bytes()).unwrap();
    assert!(driver.apply_committed().is_err());
    assert_eq!(driver.applied_index(), 0);
    assert_eq!(driver.machine().value, 0);

    assert_eq!(driver.apply_committed().unwrap(), 1);
    assert_eq!(driver.machine().value, 9);
}

#[test]
fn persistent_checkpoint_recovers_before_replaying_later_commands() {
    let directory = tempfile::tempdir().unwrap();
    let members = vec![member()];

    {
        let node = RaftNode::open_persistent(1, "http://node-1", members.clone(), directory.path())
            .unwrap();
        let mut driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();
        driver.campaign().unwrap();
        driver.propose(7_u64.to_le_bytes()).unwrap();
        assert!(driver.checkpoint().is_err());
        assert_eq!(driver.apply_committed().unwrap(), 1);
        driver.checkpoint().unwrap();

        driver.propose(5_u64.to_le_bytes()).unwrap();
        assert_eq!(driver.apply_committed().unwrap(), 1);
        assert_eq!(driver.machine().value, 12);
    }

    let node = RaftNode::open_persistent(1, "http://node-1", members, directory.path()).unwrap();
    let mut driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();

    assert_eq!(driver.machine().applications, 1);
    assert_eq!(driver.machine().value, 12);

    driver.checkpoint().unwrap();
    driver.campaign().unwrap();
    driver.propose(1_u64.to_le_bytes()).unwrap();
    assert_eq!(driver.apply_committed().unwrap(), 1);
    assert_eq!(driver.machine().value, 13);
}

#[test]
fn persistent_recovery_replays_a_committed_suffix_larger_than_one_page() {
    let directory = tempfile::tempdir().unwrap();
    let members = vec![member()];
    const COMMANDS: u64 = 257;

    {
        let node = RaftNode::open_persistent(1, "http://node-1", members.clone(), directory.path())
            .unwrap();
        let mut driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();
        driver.campaign().unwrap();
        for _ in 0..COMMANDS {
            driver.propose(1_u64.to_le_bytes()).unwrap();
        }
        assert_eq!(driver.apply_committed().unwrap(), COMMANDS as usize);
    }

    let node = RaftNode::open_persistent(1, "http://node-1", members, directory.path()).unwrap();
    let driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();

    assert_eq!(driver.machine().applications, COMMANDS as usize);
    assert_eq!(driver.machine().value, COMMANDS);
}
