//! Deterministic application of committed Raft commands and durable snapshots.

use std::{collections::VecDeque, error::Error, fmt, sync::Arc};

use catga_core::{CatgaError, CatgaResult};

use crate::{
    RaftApplicationSnapshot, RaftClusterNode, RaftCommittedEntry, RaftMessage, RaftNode,
    RaftNodeError,
    metrics::{record_applied_command, record_failure},
};

const RECOVERY_PAGE_ENTRIES: usize = 128;

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
    /// The owned Raft node could not page committed application commands.
    Node(RaftNodeError),
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
            Self::Node(error) => error.fmt(formatter),
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
            Self::Node(error) => Some(error),
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

impl From<RaftNodeError> for RaftStateMachineError {
    fn from(error: RaftNodeError) -> Self {
        Self::Node(error)
    }
}

/// Owns one Raft node and one mutable application state machine.
///
/// The driver is intentionally not `Sync`: calling it from one owner task or
/// thread avoids lock contention while preserving exactly ordered application.
///
/// A single-node in-memory cluster applies a proposal synchronously, which makes
/// the driver convenient for deterministic state-machine tests:
///
/// ```
/// use catga_cluster::{
///     RaftCommittedEntry, RaftMember, RaftNode, RaftStateMachine, RaftStateMachineDriver,
/// };
///
/// /// Counts applied entries; a real machine would decode `entry.data` here.
/// #[derive(Default)]
/// struct Counter {
///     applied: u64,
/// }
///
/// impl RaftStateMachine for Counter {
///     fn apply(&mut self, entry: &RaftCommittedEntry) -> catga_core::CatgaResult<()> {
///         self.applied += 1;
///         Ok(())
///     }
///     fn snapshot(&self) -> catga_core::CatgaResult<Vec<u8>> {
///         Ok(self.applied.to_le_bytes().to_vec())
///     }
///     fn restore(&mut self, bytes: &[u8]) -> catga_core::CatgaResult<()> {
///         let raw: [u8; 8] = bytes.try_into().unwrap_or_default();
///         self.applied = u64::from_le_bytes(raw);
///         Ok(())
///     }
/// }
///
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let members = vec![RaftMember::new(1, "http://node-1")];
/// let node = RaftNode::new(1, "http://node-1", members)?;
/// let mut driver = RaftStateMachineDriver::new(node, Counter::default())?;
/// driver.campaign()?;
/// driver.propose(b"increment".to_vec())?;
/// // The election's empty no-op entry occupies log index 1 but is never
/// // delivered to the machine, so one apply advances the index to 2.
/// assert_eq!(driver.apply_committed()?, 1);
/// assert_eq!(driver.applied_index(), 2);
/// assert_eq!(driver.machine().applied, 1);
/// # Ok(())
/// # }
/// # run().expect("driver example");
/// ```
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
        driver.recover_committed_entries()?;
        driver.node.acknowledge_recovered(driver.applied_index)?;
        Ok(driver)
    }

    /// Returns the locally owned application state.
    pub fn machine(&self) -> &M {
        &self.machine
    }

    /// Returns the numeric identifier of the owned Raft node.
    pub fn id(&self) -> u64 {
        self.node.id()
    }

    /// Returns the greatest log index that this driver has successfully applied.
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    /// Returns the greatest log index present in the owned Raft log.
    ///
    /// Immediately after a successful [`Self::propose`] on the leader this is
    /// the index the new entry was assigned.
    pub(crate) fn last_log_index(&self) -> u64 {
        self.node.last_log_index()
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

    /// Reports one temporarily unreachable remote peer to the owned Raft node.
    ///
    /// This is used by the owner runtime after a retryable transport failure. It updates native
    /// Raft replication state without retaining an unbounded application retry queue.
    pub fn report_unreachable(&mut self, peer_id: u64) -> raft::Result<()> {
        self.node.report_unreachable(peer_id)
    }

    /// Proposes one application command on the current leader.
    ///
    /// Returns [`RaftNodeError::PendingCommitCapacity`] when the bounded
    /// pending-commit queue is full, so callers can apply backpressure.
    pub fn propose(&mut self, data: impl Into<Vec<u8>>) -> Result<(), RaftNodeError> {
        self.node.propose(data)
    }

    /// Proposes adding one voter; see [`crate::RaftStateMachineRuntime::add_voter`].
    pub(crate) fn add_voter(&mut self, id: u64, endpoint: Arc<str>) -> Result<(), RaftNodeError> {
        self.node.propose_add_voter(id, endpoint)
    }

    /// Proposes removing one voter; see [`crate::RaftStateMachineRuntime::remove_voter`].
    pub(crate) fn remove_voter(&mut self, id: u64) -> Result<(), RaftNodeError> {
        self.node.propose_remove_voter(id)
    }

    /// Takes Raft protocol messages for transport delivery.
    pub fn drain_messages(&mut self) -> Vec<RaftMessage> {
        self.node.drain_messages()
    }

    /// Applies every currently committed command in ascending log-index order.
    ///
    /// An application failure leaves that entry pending for an explicit retry.
    ///
    /// Raft protocol entries — such as the empty no-op entry a new leader commits after an
    /// election — occupy log indexes and advance [`Self::applied_index`] without being
    /// delivered to the machine. The returned count covers only entries actually passed to
    /// [`RaftStateMachine::apply`].
    pub fn apply_committed(&mut self) -> Result<usize, RaftStateMachineError> {
        self.pending_snapshots
            .extend(self.node.drain_installed_snapshots());
        let mut applied = 0;
        self.apply_snapshots()?;
        loop {
            if !self.pending_entries.is_empty() {
                applied += self.apply_available()?;
                continue;
            }
            let entries = self.node.try_drain_committed()?;
            if entries.is_empty() {
                self.node.acknowledge_applied_through(self.applied_index)?;
                return Ok(applied);
            }
            self.pending_entries.extend(entries);
        }
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
            if let Err(error) = self.machine.apply(entry) {
                record_failure("apply");
                return Err(error.into());
            }
            self.applied_index = entry.index;
            self.pending_entries.pop_front();
            applied += 1;
            record_applied_command();
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

    /// Replays the durable committed suffix in fixed-size pages.
    ///
    /// Each page is applied before the next is read, so recovery memory is
    /// bounded by `RECOVERY_PAGE_ENTRIES` plus one application command.
    fn recover_committed_entries(&mut self) -> Result<(), RaftStateMachineError> {
        let mut start_index = self
            .applied_index
            .checked_add(1)
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?;
        loop {
            let page = self
                .node
                .persisted_committed_page(start_index, RECOVERY_PAGE_ENTRIES)?;
            for entry in page.entries {
                if entry.index > self.applied_index {
                    self.machine.apply(&entry)?;
                    self.applied_index = entry.index;
                }
            }
            let Some(next_index) = page.next_index else {
                return Ok(());
            };
            start_index = next_index;
        }
    }
}
