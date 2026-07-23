use catga_cluster::{
    RaftCommittedEntry, RaftMember, RaftNode, RaftStateMachine, RaftStateMachineDriver,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

#[derive(Default)]
struct Counter {
    applications: usize,
    value: u64,
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
    let driver = RaftStateMachineDriver::new(node, Counter::default()).unwrap();

    assert_eq!(driver.machine().applications, 1);
    assert_eq!(driver.machine().value, 12);
}
