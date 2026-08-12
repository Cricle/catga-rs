//! Async single-owner runtime for a deterministic Raft state machine.

use std::{error::Error, fmt, sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;

use crate::{
    RaftClusterNode, RaftMessage, RaftStateMachine, RaftStateMachineDriver, RaftStateMachineError,
    RaftStopKind, RaftStopReason, RaftTransport, RaftTransportError,
    metrics::{record_failure, record_queue_depth, record_runtime_stop},
};

const COMMAND_BUFFER: usize = 64;
const INBOUND_BUFFER: usize = 256;

/// Errors returned while operating or joining a [`RaftStateMachineRuntime`].
#[derive(Debug)]
pub enum RaftStateMachineRuntimeError {
    /// The configured logical Raft clock interval was zero.
    InvalidTickInterval,
    /// The owner task stopped before it could complete a request, including a
    /// request interrupted by [`RaftStateMachineRuntime::shutdown`].
    Stopped,
    /// A [`RaftStateMachineRuntime::propose_and_wait`] deadline expired before
    /// the proposed entry was committed and applied. The proposal may still be
    /// applied later; the runtime stays alive.
    Timeout,
    /// `raft-rs` rejected an operation or an inbound protocol message.
    Raft(raft::Error),
    /// The owned Raft node rejected an operation, including a full bounded
    /// pending-commit queue.
    Node(crate::RaftNodeError),
    /// The state machine or its Raft storage failed while applying an entry.
    StateMachine(RaftStateMachineError),
    /// The configured transport failed while sending an outbound protocol message.
    Transport(RaftTransportError),
    /// The owner task panicked or was aborted.
    Task(tokio::task::JoinError),
}

impl fmt::Display for RaftStateMachineRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTickInterval => {
                formatter.write_str("Raft state-machine runtime tick interval must be non-zero")
            }
            Self::Stopped => formatter.write_str("Raft state-machine runtime stopped"),
            Self::Timeout => formatter.write_str(
                "Raft state-machine proposal was not committed and applied before the deadline",
            ),
            Self::Raft(error) => error.fmt(formatter),
            Self::Node(error) => error.fmt(formatter),
            Self::StateMachine(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::Task(error) => error.fmt(formatter),
        }
    }
}

impl Error for RaftStateMachineRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Raft(error) => Some(error),
            Self::Node(error) => Some(error),
            Self::StateMachine(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::InvalidTickInterval | Self::Stopped | Self::Timeout => None,
        }
    }
}

/// Drives one [`RaftStateMachineDriver`] on a single Tokio task.
///
/// All Raft mutations and state-machine applications are serialized by this
/// task, so application state does not need a mutex. An application failure
/// stops the runtime before its entry is acknowledged; a durable node can
/// recover and replay that entry through [`RaftStateMachineDriver::new`].
pub struct RaftStateMachineRuntime {
    id: u64,
    coordinator: Arc<RaftClusterNode>,
    inbox: mpsc::Sender<RaftMessage>,
    commands: mpsc::Sender<Command>,
    applied: watch::Receiver<u64>,
    shutdown: CancellationToken,
    terminal: Arc<ArcSwapOption<RaftStopReason>>,
    task: JoinHandle<Result<(), RaftStateMachineRuntimeError>>,
}

impl RaftStateMachineRuntime {
    /// Starts an owned state-machine runtime with bounded inbound queues.
    ///
    /// Routine caller errors — proposing without leadership, a full pending-commit
    /// queue, a checkpoint before the first applied command, or a membership change
    /// rejected by pre-validation — are returned to that caller and the owner task
    /// keeps running. Terminal failures (durable storage, application rejection,
    /// fatal transport) stop the owner task before the failed entry is acknowledged
    /// and are recorded for [`Self::stop_reason`]; a durable node recovers and
    /// replays that entry through [`RaftStateMachineDriver::new`].
    ///
    /// A single-node runtime with a transport that accepts every message applies
    /// proposals without any network:
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use catga_cluster::{
    ///     ClusterCoordinator, RaftCommittedEntry, RaftMember, RaftMessage, RaftNode,
    ///     RaftStateMachine, RaftStateMachineDriver, RaftStateMachineRuntime, RaftTransport,
    ///     RaftTransportResult,
    /// };
    ///
    /// /// Single-node deployments never leave the process, so dropping frames is fine here.
    /// struct NullTransport;
    /// #[async_trait::async_trait]
    /// impl RaftTransport for NullTransport {
    ///     async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
    ///         Ok(())
    ///     }
    /// }
    ///
    /// #[derive(Default)]
    /// struct Counter {
    ///     applied: u64,
    /// }
    /// impl RaftStateMachine for Counter {
    ///     fn apply(&mut self, _entry: &RaftCommittedEntry) -> catga_core::CatgaResult<()> {
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
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let members = vec![RaftMember::new(1, "http://node-1")];
    /// let node = RaftNode::new(1, "http://node-1", members)?;
    /// let driver = RaftStateMachineDriver::new(node, Counter::default())?;
    /// let runtime =
    ///     RaftStateMachineRuntime::spawn(driver, Arc::new(NullTransport), Duration::from_millis(10))?;
    /// runtime.campaign().await?;
    /// runtime.propose(b"set a=1".to_vec()).await?;
    /// // The election's initial no-op entry plus the proposal have both been applied.
    /// assert_eq!(runtime.applied_index().await?, 2);
    /// assert!(runtime.coordinator().is_leader());
    /// runtime.shutdown();
    /// runtime.join().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn<M, T>(
        driver: RaftStateMachineDriver<M>,
        transport: Arc<T>,
        tick_interval: Duration,
    ) -> Result<Self, RaftStateMachineRuntimeError>
    where
        M: RaftStateMachine + Send + 'static,
        T: RaftTransport + 'static,
    {
        if tick_interval.is_zero() {
            return Err(RaftStateMachineRuntimeError::InvalidTickInterval);
        }
        let id = driver.id();
        let coordinator = driver.coordinator();
        let (inbox, inbound) = mpsc::channel(INBOUND_BUFFER);
        let (commands, requests) = mpsc::channel(COMMAND_BUFFER);
        record_queue_depth("state_machine", 0, 0);
        let shutdown = CancellationToken::new();
        let runtime_shutdown = shutdown.clone();
        let transport: Arc<dyn RaftTransport> = transport;
        let transport: Arc<dyn RaftTransport> = Arc::new(crate::dispatch::PeerDispatcher::new(
            transport,
            runtime_shutdown.clone(),
        ));
        let terminal: Arc<ArcSwapOption<RaftStopReason>> = Arc::new(ArcSwapOption::empty());
        let task_terminal = Arc::clone(&terminal);
        let (applied_updates, applied) = watch::channel(driver.applied_index());
        let task = tokio::spawn(async move {
            let result = run(
                driver,
                transport,
                tick_interval,
                inbound,
                requests,
                runtime_shutdown,
                applied_updates,
            )
            .await;
            if let Err(error) = &result {
                let reason = RaftStopReason::new(stop_kind_of(error), error.to_string());
                record_runtime_stop(reason.kind());
                task_terminal.store(Some(Arc::new(reason)));
            }
            result
        });
        Ok(Self {
            id,
            coordinator,
            inbox,
            commands,
            applied,
            shutdown,
            terminal,
            task,
        })
    }

    /// Returns whether the owner task is still running.
    ///
    /// A `false` value with [`Self::stop_reason`] of `None` indicates a graceful
    /// shutdown; `Some` indicates a terminal failure. Leadership reads from
    /// [`Self::coordinator`] are only meaningful while the runtime is alive.
    pub fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    /// Returns the recorded terminal failure once the owner task has stopped.
    pub fn stop_reason(&self) -> Option<RaftStopReason> {
        self.terminal.load_full().map(|reason| (*reason).clone())
    }

    /// Returns this runtime's Raft member identifier.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the lock-free leadership view for this node.
    pub fn coordinator(&self) -> Arc<RaftClusterNode> {
        Arc::clone(&self.coordinator)
    }

    /// Returns a bounded sender used by the network receiver to inject Raft messages.
    pub fn inbox(&self) -> mpsc::Sender<RaftMessage> {
        self.inbox.clone()
    }

    /// Starts an election immediately.
    pub async fn campaign(&self) -> Result<(), RaftStateMachineRuntimeError> {
        self.request(Command::Campaign).await
    }

    /// Proposes one application command on the locally elected leader.
    pub async fn propose(
        &self,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), RaftStateMachineRuntimeError> {
        let data = data.into();
        self.request(move |reply| Command::Propose(data, reply))
            .await
    }

    /// Proposes one application command and resolves with the applied index
    /// once the entry is committed and applied by the local state machine.
    ///
    /// Unlike fire-and-forget [`Self::propose`], this waits — without polling —
    /// on the owner task's applied-index publication: the returned index is at
    /// least the index the proposed entry was assigned, so the entry's effects
    /// are observable in the state machine when the call resolves.
    ///
    /// # Errors
    ///
    /// The same routine caller errors as [`Self::propose`] apply, including a
    /// fast [`raft::Error::ProposalDropped`] when this node is not the leader.
    /// [`RaftStateMachineRuntimeError::Stopped`] is returned when the owner
    /// task stops while waiting, and [`RaftStateMachineRuntimeError::Timeout`]
    /// when `timeout` expires first — the proposal may still be committed and
    /// applied later. If leadership changes while the entry is in flight, its
    /// log slot can be overwritten by another entry; the resolved index then
    /// reflects that slot rather than necessarily this proposal. Callers
    /// needing exactly-once semantics must embed an application-level
    /// operation id in `data`.
    pub async fn propose_and_wait(
        &self,
        data: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<u64, RaftStateMachineRuntimeError> {
        let data = data.into();
        // Subscribe before proposing so the applied-index publication that
        // covers this entry cannot be missed.
        let mut applied = self.applied.clone();
        let assigned = self
            .request_value(move |reply| Command::ProposeAndWait(data, reply))
            .await?;
        let notified = async {
            loop {
                let current = *applied.borrow_and_update();
                if current >= assigned {
                    return Ok(current);
                }
                applied
                    .changed()
                    .await
                    .map_err(|_| RaftStateMachineRuntimeError::Stopped)?;
            }
        };
        match tokio::time::timeout(timeout, notified).await {
            Ok(result) => result,
            Err(_) => Err(RaftStateMachineRuntimeError::Timeout),
        }
    }

    /// Proposes adding one voter to the cluster through the locally elected
    /// leader.
    ///
    /// `endpoint` is the new member's externally reachable address; it travels
    /// inside the committed conf-change entry, so every member's coordinator
    /// ([`crate::ClusterCoordinator::member_endpoints`]) reflects the new set
    /// once the change applies. The applied membership is written to durable
    /// storage in the same atomic batch as the Raft conf state, so a restarted
    /// persistent node keeps the new voter set even when its static bootstrap
    /// configuration still lists the old members.
    ///
    /// Membership changes are applied one at a time: wait until a change is
    /// observable through the coordinator before issuing the next one, because
    /// `raft-rs` silently blanks a conf change proposed while another is
    /// pending. Learners, leadership transfer, and multi-member changes in a
    /// single step are intentionally not supported.
    ///
    /// A node joining the cluster must be started with the full intended
    /// member list and becomes a voter once the committed change reaches it.
    ///
    /// # Errors
    ///
    /// Returns a routine caller error and keeps the runtime alive when this
    /// node is not the leader ([`raft::Error::ProposalDropped`]), when `id` is
    /// zero ([`crate::RaftNodeError::ZeroMemberId`]), or when `id` is already
    /// a voter or `endpoint` is empty ([`raft::Error::ConfChangeError`]).
    pub async fn add_voter(
        &self,
        id: u64,
        endpoint: String,
    ) -> Result<(), RaftStateMachineRuntimeError> {
        let endpoint: Arc<str> = endpoint.into();
        self.request(move |reply| Command::AddVoter(id, endpoint, reply))
            .await
    }

    /// Proposes removing one voter from the cluster through the locally
    /// elected leader.
    ///
    /// The same one-change-at-a-time discipline and durability semantics as
    /// [`Self::add_voter`] apply. A removed member's endpoint is dropped from
    /// every coordinator once the change applies. Removing the last voter is
    /// rejected by `raft-rs`; removing this node itself is permitted, after
    /// which it can no longer campaign.
    ///
    /// # Errors
    ///
    /// Returns a routine caller error and keeps the runtime alive when this
    /// node is not the leader ([`raft::Error::ProposalDropped`]), when `id` is
    /// zero ([`crate::RaftNodeError::ZeroMemberId`]), or when `id` is not a
    /// cluster member ([`raft::Error::ConfChangeError`]).
    pub async fn remove_voter(&self, id: u64) -> Result<(), RaftStateMachineRuntimeError> {
        self.request(move |reply| Command::RemoveVoter(id, reply))
            .await
    }

    /// Persists a state-machine snapshot at the latest successfully applied command.
    pub async fn checkpoint(&self) -> Result<(), RaftStateMachineRuntimeError> {
        self.request(Command::Checkpoint).await
    }

    /// Returns the greatest log index applied to the application state machine.
    ///
    /// Combined with an application-level operation id inside proposed commands, this
    /// lets callers wait until a proposal has actually been applied rather than merely
    /// accepted by the local Raft log.
    pub async fn applied_index(&self) -> Result<u64, RaftStateMachineRuntimeError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::AppliedIndex(reply))
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)?;
        result
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)
    }

    /// Requests a graceful stop of the owner task.
    ///
    /// The runtime cancels a transport send that is currently awaiting
    /// completion and does not deliver any remaining outbound messages.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Waits for the owner task and returns its terminal status.
    pub async fn join(self) -> Result<(), RaftStateMachineRuntimeError> {
        self.task
            .await
            .map_err(RaftStateMachineRuntimeError::Task)?
    }

    async fn request<F>(&self, command: F) -> Result<(), RaftStateMachineRuntimeError>
    where
        F: FnOnce(oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>) -> Command,
    {
        self.request_value(command).await
    }

    async fn request_value<F, T>(&self, command: F) -> Result<T, RaftStateMachineRuntimeError>
    where
        F: FnOnce(oneshot::Sender<Result<T, RaftStateMachineRuntimeError>>) -> Command,
    {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)?;
        record_queue_depth(
            "state_machine",
            INBOUND_BUFFER - self.inbox.capacity(),
            COMMAND_BUFFER - self.commands.capacity(),
        );
        result
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)?
    }
}

enum Command {
    Campaign(oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>),
    Propose(
        Vec<u8>,
        oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>,
    ),
    ProposeAndWait(
        Vec<u8>,
        oneshot::Sender<Result<u64, RaftStateMachineRuntimeError>>,
    ),
    AddVoter(
        u64,
        Arc<str>,
        oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>,
    ),
    RemoveVoter(
        u64,
        oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>,
    ),
    Checkpoint(oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>),
    AppliedIndex(oneshot::Sender<u64>),
}

async fn run<M>(
    mut driver: RaftStateMachineDriver<M>,
    transport: Arc<dyn RaftTransport>,
    tick_interval: Duration,
    mut inbound: mpsc::Receiver<RaftMessage>,
    mut commands: mpsc::Receiver<Command>,
    shutdown: CancellationToken,
    applied: watch::Sender<u64>,
) -> Result<(), RaftStateMachineRuntimeError>
where
    M: RaftStateMachine,
{
    let mut ticks = interval(tick_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticks.tick() => {
                record_queue_depth("state_machine", inbound.len(), commands.len());
                if !drive(driver.tick(), &mut driver, transport.as_ref(), &shutdown, &applied).await? {
                    return Ok(());
                }
            }
            Some(message) = inbound.recv() => {
                record_queue_depth("state_machine", inbound.len(), commands.len());
                if !drive(driver.step(message), &mut driver, transport.as_ref(), &shutdown, &applied).await? {
                    return Ok(());
                }
            }
            Some(command) = commands.recv() => match command {
                Command::Campaign(reply) => {
                    record_queue_depth("state_machine", inbound.len(), commands.len());
                    let result =
                        drive(driver.campaign(), &mut driver, transport.as_ref(), &shutdown, &applied)
                            .await
                            .map(|continue_running| (continue_running, ()));
                    if !respond_drive(reply, result)? {
                        return Ok(());
                    }
                }
                Command::Propose(data, reply) => {
                    record_queue_depth("state_machine", inbound.len(), commands.len());
                    let result =
                        propose_command(&mut driver, transport.as_ref(), &shutdown, &applied, |driver| {
                            driver.propose(data)
                        })
                        .await
                        .map(|(continue_running, _)| (continue_running, ()));
                    if !respond_drive(reply, result)? {
                        return Ok(());
                    }
                }
                Command::ProposeAndWait(data, reply) => {
                    record_queue_depth("state_machine", inbound.len(), commands.len());
                    let result =
                        propose_command(&mut driver, transport.as_ref(), &shutdown, &applied, |driver| {
                            driver.propose(data)
                        })
                        .await;
                    if !respond_drive(reply, result)? {
                        return Ok(());
                    }
                }
                Command::AddVoter(id, endpoint, reply) => {
                    record_queue_depth("state_machine", inbound.len(), commands.len());
                    let result =
                        propose_command(&mut driver, transport.as_ref(), &shutdown, &applied, |driver| {
                            driver.add_voter(id, endpoint)
                        })
                        .await
                        .map(|(continue_running, _)| (continue_running, ()));
                    if !respond_drive(reply, result)? {
                        return Ok(());
                    }
                }
                Command::RemoveVoter(id, reply) => {
                    record_queue_depth("state_machine", inbound.len(), commands.len());
                    let result =
                        propose_command(&mut driver, transport.as_ref(), &shutdown, &applied, |driver| {
                            driver.remove_voter(id)
                        })
                        .await
                        .map(|(continue_running, _)| (continue_running, ()));
                    if !respond_drive(reply, result)? {
                        return Ok(());
                    }
                }
                Command::Checkpoint(reply) => {
                    record_queue_depth("state_machine", inbound.len(), commands.len());
                    let result = driver
                        .checkpoint()
                        .map_err(RaftStateMachineRuntimeError::StateMachine);
                    if let Err(error) = &result {
                        record_failure("checkpoint");
                        tracing::error!(
                            target: catga_core::TRACING_TARGET,
                            error = %error,
                            "catga Raft state-machine checkpoint failed"
                        );
                    }
                    respond(reply, result)?;
                }
                Command::AppliedIndex(reply) => {
                    let _ = reply.send(driver.applied_index());
                }
            },
            else => return Ok(()),
        }
    }
}

/// Runs one proposal-style action on the owned driver and, when the proposal
/// was accepted, drives the resulting Raft work before reporting back.
///
/// On success the returned pair carries the continue-running flag and the log
/// index the entry was assigned, which a propose-and-wait caller uses as its
/// applied-index target.
async fn propose_command<M, F>(
    driver: &mut RaftStateMachineDriver<M>,
    transport: &dyn RaftTransport,
    shutdown: &CancellationToken,
    applied: &watch::Sender<u64>,
    action: F,
) -> Result<(bool, u64), RaftStateMachineRuntimeError>
where
    M: RaftStateMachine,
    F: FnOnce(&mut RaftStateMachineDriver<M>) -> Result<(), crate::RaftNodeError>,
{
    let proposed = action(driver).map_err(|error| match error {
        crate::RaftNodeError::Raft(error) => RaftStateMachineRuntimeError::Raft(error),
        other => RaftStateMachineRuntimeError::Node(other),
    });
    match proposed {
        Ok(()) => {
            let assigned = driver.last_log_index();
            drive(Ok(()), driver, transport, shutdown, applied)
                .await
                .map(|continue_running| (continue_running, assigned))
        }
        Err(error) => Err(error),
    }
}

fn respond<T>(
    reply: oneshot::Sender<Result<T, RaftStateMachineRuntimeError>>,
    result: Result<T, RaftStateMachineRuntimeError>,
) -> Result<(), RaftStateMachineRuntimeError> {
    match result {
        Ok(value) => {
            let _ = reply.send(Ok(value));
            Ok(())
        }
        Err(RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::Application(
            error,
        ))) => {
            let _ = reply.send(Err(RaftStateMachineRuntimeError::StateMachine(
                RaftStateMachineError::Application(error.clone()),
            )));
            Err(RaftStateMachineRuntimeError::StateMachine(
                RaftStateMachineError::Application(error),
            ))
        }
        Err(error) => {
            if let Some(copy) = routine_copy(&error) {
                let _ = reply.send(Err(copy));
                return Ok(());
            }
            let _ = reply.send(Err(RaftStateMachineRuntimeError::Stopped));
            Err(error)
        }
    }
}

/// Returns a fresh copy of a routine caller error, or `None` for terminal failures.
///
/// Routine errors describe the caller's situation (not the runtime's health): proposing
/// without leadership, a full pending-commit queue, a checkpoint before the first
/// applied command, or a membership change rejected by pre-validation.
fn routine_copy(error: &RaftStateMachineRuntimeError) -> Option<RaftStateMachineRuntimeError> {
    match error {
        RaftStateMachineRuntimeError::Raft(raft::Error::ProposalDropped) => Some(
            RaftStateMachineRuntimeError::Raft(raft::Error::ProposalDropped),
        ),
        RaftStateMachineRuntimeError::Raft(raft::Error::ConfChangeError(message)) => Some(
            RaftStateMachineRuntimeError::Raft(raft::Error::ConfChangeError(message.clone())),
        ),
        RaftStateMachineRuntimeError::Node(crate::RaftNodeError::PendingCommitCapacity {
            capacity,
        }) => Some(RaftStateMachineRuntimeError::Node(
            crate::RaftNodeError::PendingCommitCapacity {
                capacity: *capacity,
            },
        )),
        RaftStateMachineRuntimeError::Node(crate::RaftNodeError::ZeroMemberId) => Some(
            RaftStateMachineRuntimeError::Node(crate::RaftNodeError::ZeroMemberId),
        ),
        RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::NothingApplied) => Some(
            RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::NothingApplied),
        ),
        _ => None,
    }
}

fn stop_kind_of(error: &RaftStateMachineRuntimeError) -> RaftStopKind {
    match error {
        RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::Application(_)) => {
            RaftStopKind::Application
        }
        RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::Raft(
            raft::Error::Store(_),
        ))
        | RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::Node(_))
        | RaftStateMachineRuntimeError::Node(_)
        | RaftStateMachineRuntimeError::Raft(raft::Error::Store(_)) => RaftStopKind::Storage,
        RaftStateMachineRuntimeError::StateMachine(_) | RaftStateMachineRuntimeError::Raft(_) => {
            RaftStopKind::Raft
        }
        RaftStateMachineRuntimeError::Transport(_) => RaftStopKind::Transport,
        RaftStateMachineRuntimeError::Task(_) => RaftStopKind::Task,
        RaftStateMachineRuntimeError::InvalidTickInterval
        | RaftStateMachineRuntimeError::Stopped
        | RaftStateMachineRuntimeError::Timeout => RaftStopKind::Internal,
    }
}

async fn drive<M>(
    raft_result: raft::Result<()>,
    driver: &mut RaftStateMachineDriver<M>,
    transport: &dyn RaftTransport,
    shutdown: &CancellationToken,
    applied: &watch::Sender<u64>,
) -> Result<bool, RaftStateMachineRuntimeError>
where
    M: RaftStateMachine,
{
    if let Err(error) = raft_result {
        record_failure("raft");
        tracing::error!(
            target: catga_core::TRACING_TARGET,
            error = %error,
            "catga Raft state-machine runtime operation failed"
        );
        return Err(RaftStateMachineRuntimeError::Raft(error));
    }
    if !send_messages(driver, transport, shutdown).await? {
        return Ok(false);
    }
    if let Err(error) = driver.apply_committed() {
        record_failure("apply");
        tracing::error!(
            target: catga_core::TRACING_TARGET,
            error = %error,
            "catga Raft state-machine application failed"
        );
        return Err(RaftStateMachineRuntimeError::StateMachine(error));
    }
    publish_applied(driver, applied);
    send_messages(driver, transport, shutdown).await
}

/// Publishes the driver's latest applied index to propose-and-wait waiters.
fn publish_applied<M>(driver: &RaftStateMachineDriver<M>, applied: &watch::Sender<u64>)
where
    M: RaftStateMachine,
{
    applied.send_if_modified(|current| {
        let index = driver.applied_index();
        if index > *current {
            *current = index;
            true
        } else {
            false
        }
    });
}

async fn send_messages<M>(
    driver: &mut RaftStateMachineDriver<M>,
    transport: &dyn RaftTransport,
    shutdown: &CancellationToken,
) -> Result<bool, RaftStateMachineRuntimeError>
where
    M: RaftStateMachine,
{
    for message in driver.drain_messages() {
        let peer_id = message.to;
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(false),
            result = transport.send(message) => {
                if let Err(error) = result {
                    record_failure("transport");
                    if error.is_retryable() {
                        tracing::debug!(
                            target: catga_core::TRACING_TARGET,
                            peer_id,
                            error = %error,
                            "catga Raft state-machine peer delivery is temporarily unavailable"
                        );
                        driver
                            .report_unreachable(peer_id)
                            .map_err(RaftStateMachineRuntimeError::Raft)?;
                        continue;
                    }
                    tracing::error!(
                        target: catga_core::TRACING_TARGET,
                        error = %error,
                        "catga Raft state-machine transport delivery failed"
                    );
                    return Err(RaftStateMachineRuntimeError::Transport(error));
                }
            }
        }
    }
    Ok(true)
}

fn respond_drive<T>(
    reply: oneshot::Sender<Result<T, RaftStateMachineRuntimeError>>,
    result: Result<(bool, T), RaftStateMachineRuntimeError>,
) -> Result<bool, RaftStateMachineRuntimeError> {
    match result {
        Ok((continue_running, value)) => {
            if continue_running {
                respond(reply, Ok(value))?;
            } else {
                let _ = reply.send(Err(RaftStateMachineRuntimeError::Stopped));
            }
            Ok(continue_running)
        }
        Err(error) => {
            respond(reply, Err(error))?;
            Ok(true)
        }
    }
}
