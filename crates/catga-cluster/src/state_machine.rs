//! Deterministic application of committed Raft commands and durable snapshots.

use std::{collections::VecDeque, error::Error, fmt, sync::Arc};

use catga_core::{CatgaError, CatgaResult};

use crate::{RaftApplicationSnapshot, RaftClusterNode, RaftCommittedEntry, RaftMessage, RaftNode};

/// Applies deterministic Raft commands and converts its state to durable bytes.
///
/// Implementations are owned by one [`RaftStateMachineDriver`] and therefore do
/// not require a mutex. `apply` must be deterministic and idempotent with
/// respect to its entry index when it performs externally visible work.
pub trait RaftStateMachine {
    /// Applies one newly committed application command.
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()>;
    /// Encodes the complete state after the last successfully applied command.
    fn snapshot(&self) -> CatgaResult<Vec<u8>>;
    /// Replaces state from a Raft snapshot before subsequent log replay.
    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()>;
}

/// A failure while applying or recovering an application state machine.
#[derive(Debug)]
pub enum RaftStateMachineError {
    /// The application state machine rejected an operation.
    Application(CatgaError),
    /// Raft storage could not read or write protocol state.
    Raft(raft::Error),
    /// A checkpoint was requested before any command had been applied.
    NothingApplied,
}

impl fmt::Display for RaftStateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Application(error) => {
                write!(formatter, "application state machine: {}", error.message())
            }
            Self::Raft(error) => error.fmt(formatter),
            Self::NothingApplied => {
                formatter.write_str("cannot checkpoint before applying a Raft command")
            }
        }
    }
}

impl Error for RaftStateMachineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Application(_) | Self::NothingApplied => None,
            Self::Raft(error) => Some(error),
        }
    }
}

impl From<CatgaError> for RaftStateMachineError {
    fn from(error: CatgaError) -> Self {
        Self::Application(error)
    }
}

impl From<raft::Error> for RaftStateMachineError {
    fn from(error: raft::Error) -> Self {
        Self::Raft(error)
    }
}

/// Owns one Raft node and one mutable application state machine.
///
/// The driver is intentionally not `Sync`: calling it from one owner task or
/// thread avoids lock contention while preserving exactly ordered application.
pub struct RaftStateMachineDriver<M> {
    node: RaftNode,
    machine: M,
    applied_index: u64,
    pending_entries: VecDeque<RaftCommittedEntry>,
    pending_snapshots: VecDeque<RaftApplicationSnapshot>,
}

impl<M> RaftStateMachineDriver<M>
where
    M: RaftStateMachine,
{
    /// Recovers the persisted snapshot and committed suffix before returning.
    pub fn new(mut node: RaftNode, machine: M) -> Result<Self, RaftStateMachineError> {
        node.defer_application_acknowledgement();
        let mut driver = Self {
            node,
            machine,
            applied_index: 0,
            pending_entries: VecDeque::new(),
            pending_snapshots: VecDeque::new(),
        };
        if let Some(snapshot) = driver.node.application_snapshot()? {
            driver.restore_snapshot(snapshot)?;
        }
        driver
            .pending_entries
            .extend(driver.node.persisted_committed_entries()?);
        driver.apply_available()?;
        Ok(driver)
    }

    /// Returns the locally owned application state.
    pub fn machine(&self) -> &M {
        &self.machine
    }

    /// Returns the greatest log index that this driver has successfully applied.
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    /// Returns the lock-free coordinator associated with the owned Raft node.
    pub fn coordinator(&self) -> Arc<RaftClusterNode> {
        self.node.coordinator()
    }

    /// Starts a Raft election.
    pub fn campaign(&mut self) -> raft::Result<()> {
        self.node.campaign()
    }

    /// Advances the Raft clock.
    pub fn tick(&mut self) -> raft::Result<()> {
        self.node.tick()
    }

    /// Delivers one wire-level Raft message.
    pub fn step(&mut self, message: RaftMessage) -> raft::Result<()> {
        self.node.step(message)
    }

    /// Proposes one application command on the current leader.
    pub fn propose(&mut self, data: impl Into<Vec<u8>>) -> raft::Result<()> {
        self.node.propose(data)
    }

    /// Takes Raft protocol messages for transport delivery.
    pub fn drain_messages(&mut self) -> Vec<RaftMessage> {
        self.node.drain_messages()
    }

    /// Applies every currently committed command in ascending log-index order.
    ///
    /// An application failure leaves that entry pending for an explicit retry.
    pub fn apply_committed(&mut self) -> Result<usize, RaftStateMachineError> {
        self.pending_snapshots
            .extend(self.node.drain_installed_snapshots());
        self.pending_entries.extend(self.node.drain_committed());
        self.apply_available()
    }

    /// Snapshots the state at the last successfully applied command and compacts
    /// the covered Raft log in one durable Raft-engine write.
    pub fn checkpoint(&mut self) -> Result<(), RaftStateMachineError> {
        if self.applied_index == 0 {
            return Err(RaftStateMachineError::NothingApplied);
        }
        let data = self.machine.snapshot()?;
        self.node.checkpoint(self.applied_index, data)?;
        Ok(())
    }

    fn apply_available(&mut self) -> Result<usize, RaftStateMachineError> {
        self.apply_snapshots()?;
        let mut applied = 0;
        while let Some(entry) = self.pending_entries.front() {
            if entry.index <= self.applied_index {
                self.pending_entries.pop_front();
                continue;
            }
            self.machine.apply(entry)?;
            self.applied_index = entry.index;
            self.pending_entries.pop_front();
            applied += 1;
        }
        if self.pending_entries.is_empty() {
            self.node.acknowledge_all_committed()?;
        }
        Ok(applied)
    }

    fn apply_snapshots(&mut self) -> Result<(), RaftStateMachineError> {
        while let Some(snapshot) = self.pending_snapshots.front() {
            if snapshot.index > self.applied_index {
                self.machine.restore(&snapshot.data)?;
                self.applied_index = snapshot.index;
                self.pending_entries
                    .retain(|entry| entry.index > self.applied_index);
            }
            self.pending_snapshots.pop_front();
        }
        Ok(())
    }

    fn restore_snapshot(
        &mut self,
        snapshot: RaftApplicationSnapshot,
    ) -> Result<(), RaftStateMachineError> {
        self.machine.restore(&snapshot.data)?;
        self.applied_index = snapshot.index;
        Ok(())
    }
}
