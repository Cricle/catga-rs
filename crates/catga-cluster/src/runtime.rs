//! Async single-owner runtime for a [`crate::RaftNode`].

use std::{error::Error, fmt, sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;

use crate::{
    RaftClusterNode, RaftCommittedEntry, RaftMessage, RaftNode, RaftNodeError,
    metrics::{record_failure, record_queue_depth, record_runtime_stop},
};

const COMMAND_BUFFER: usize = 64;
const INBOUND_BUFFER: usize = 256;

/// A transport failure classified by whether the Raft owner can safely continue.
///
/// Retryable failures describe a peer that is temporarily unavailable, such as a saturated
/// inbox, timeout, or transient connection failure. The runtime reports that peer unreachable
/// to `raft-rs` and stays alive so its later heartbeats can restore replication. Fatal failures
/// indicate a configuration, authentication, or protocol boundary error and stop the owner task.
///
/// ```
/// use catga_cluster::RaftTransportError;
///
/// let timeout = std::io::Error::new(std::io::ErrorKind::TimedOut, "send timed out");
/// assert!(RaftTransportError::retryable(timeout).is_retryable());
///
/// let rejected = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "peer rejected mTLS");
/// assert!(!RaftTransportError::fatal(rejected).is_retryable());
/// ```
#[derive(Debug)]
pub enum RaftTransportError {
    /// A temporary peer failure that must not stop the Raft owner task.
    Retryable(Box<dyn Error + Send + Sync>),
    /// A non-recoverable transport configuration or protocol failure.
    Fatal(Box<dyn Error + Send + Sync>),
}

impl RaftTransportError {
    /// Classifies `error` as a temporary peer delivery failure.
    pub fn retryable<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Retryable(Box::new(error))
    }

    /// Classifies `error` as a terminal transport failure.
    pub fn fatal<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Fatal(Box::new(error))
    }

    /// Returns whether this failure permits the Raft runtime to continue.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

impl fmt::Display for RaftTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retryable(error) | Self::Fatal(error) => error.fmt(formatter),
        }
    }
}

impl Error for RaftTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Retryable(error) | Self::Fatal(error) => Some(error.as_ref()),
        }
    }
}

/// Result returned by an application-provided Raft transport.
pub type RaftTransportResult = Result<(), RaftTransportError>;

/// Coarse classification of why a Raft runtime owner task stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaftStopKind {
    /// Durable Raft storage failed while reading or writing protocol state.
    Storage,
    /// The application state machine rejected a committed entry.
    Application,
    /// The transport reported a fatal delivery failure.
    Transport,
    /// `raft-rs` rejected a protocol operation.
    Raft,
    /// The owner task panicked or was aborted.
    Task,
    /// A stop that does not fit the other kinds.
    Internal,
}

/// Recorded terminal state of a stopped Raft runtime owner task.
///
/// Produced once when the owner task exits with an error; observers use
/// [`RaftRuntime::stop_reason`] or [`crate::RaftStateMachineRuntime::stop_reason`]
/// to distinguish a graceful shutdown (`None`) from a terminal failure.
#[derive(Clone, Debug)]
pub struct RaftStopReason {
    kind: RaftStopKind,
    detail: Arc<str>,
}

impl RaftStopReason {
    pub(crate) fn new(kind: RaftStopKind, detail: String) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    /// Returns the coarse stop classification.
    pub const fn kind(&self) -> RaftStopKind {
        self.kind
    }

    /// Returns the rendered terminal error.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RaftStopReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.detail)
    }
}

/// Sends wire-level Raft messages to their destination node.
///
/// Implementations should enqueue quickly or apply their own bounded
/// backpressure, because a send is awaited by the single Raft owner task.
/// The runtime cancels an in-flight `send` future when
/// [`RaftRuntime::shutdown`] is called, so implementations must release any
/// resources acquired by a dropped send future.
#[async_trait]
pub trait RaftTransport: Send + Sync {
    /// Delivers one Raft protocol message.
    async fn send(&self, message: RaftMessage) -> RaftTransportResult;
}

/// Errors returned while operating or joining a [`RaftRuntime`].
#[derive(Debug)]
pub enum RaftRuntimeError {
    /// The configured logical Raft clock interval was zero.
    InvalidTickInterval,
    /// The owner task stopped before it could complete a request, including a
    /// request interrupted by [`RaftRuntime::shutdown`].
    Stopped,
    /// `raft-rs` rejected an operation or an inbound protocol message.
    Raft(raft::Error),
    /// The application commit queue could not be refilled from durable Raft storage.
    Node(RaftNodeError),
    /// The configured transport failed while sending an outbound protocol message.
    Transport(RaftTransportError),
    /// The owner task panicked or was aborted.
    Task(TaskError),
}

/// Wrapper for tokio task join errors that can be cloned.
///
/// Since `tokio::task::JoinError` cannot be cloned or constructed externally,
/// we use this wrapper to provide a cloneable representation.
#[derive(Debug, Clone)]
pub struct TaskError {
    is_cancelled: bool,
    is_panic: bool,
}

impl TaskError {
    /// Creates a new cancelled task error.
    pub fn cancelled() -> Self {
        Self {
            is_cancelled: true,
            is_panic: false,
        }
    }

    /// Creates a new panic task error.
    pub fn panic() -> Self {
        Self {
            is_cancelled: false,
            is_panic: true,
        }
    }

    /// Returns true if the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled
    }

    /// Returns true if the task panicked.
    pub fn is_panic(&self) -> bool {
        self.is_panic
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_panic {
            write!(formatter, "task panicked")
        } else {
            write!(formatter, "task cancelled")
        }
    }
}

impl std::error::Error for TaskError {}

impl fmt::Display for RaftRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTickInterval => {
                formatter.write_str("Raft runtime tick interval must be non-zero")
            }
            Self::Stopped => formatter.write_str("Raft runtime stopped"),
            Self::Raft(error) => error.fmt(formatter),
            Self::Node(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::Task(error) => error.fmt(formatter),
        }
    }
}

impl Error for RaftRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Raft(error) => Some(error),
            Self::Node(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::InvalidTickInterval | Self::Stopped => None,
        }
    }
}

/// Drives one [`RaftNode`] on a single Tokio task.
///
/// Network implementations obtain [`Self::inbox`] for inbound Raft frames,
/// while callers use [`Self::campaign`] and [`Self::propose`] for local work.
/// This keeps all `RawNode` mutations inside one task without mutexes.
pub struct RaftRuntime {
    id: u64,
    coordinator: Arc<RaftClusterNode>,
    inbox: mpsc::Sender<RaftMessage>,
    commands: mpsc::Sender<Command>,
    shutdown: CancellationToken,
    terminal: Arc<ArcSwapOption<RaftStopReason>>,
    task: JoinHandle<Result<(), RaftRuntimeError>>,
}

impl RaftRuntime {
    /// Starts a runtime that ticks at `tick_interval` and emits messages through `transport`.
    ///
    /// `tick_interval` must be non-zero. Routine caller errors — a proposal without
    /// leadership or a full pending-commit queue — are returned to that caller and the
    /// owner task keeps running. Terminal failures (durable storage, fatal transport,
    /// internal Raft errors) stop the owner task and are recorded for
    /// [`Self::stop_reason`]; [`Self::join`] returns the terminal error.
    pub fn spawn<T>(
        node: RaftNode,
        transport: Arc<T>,
        tick_interval: Duration,
    ) -> Result<Self, RaftRuntimeError>
    where
        T: RaftTransport + 'static,
    {
        if tick_interval.is_zero() {
            return Err(RaftRuntimeError::InvalidTickInterval);
        }
        let id = node.id();
        let coordinator = node.coordinator();
        let (inbox, inbound) = mpsc::channel(INBOUND_BUFFER);
        let (commands, requests) = mpsc::channel(COMMAND_BUFFER);
        record_queue_depth("raft", 0, 0);
        let shutdown = CancellationToken::new();
        let runtime_shutdown = shutdown.clone();
        let transport: Arc<dyn RaftTransport> = transport;
        let transport: Arc<dyn RaftTransport> = Arc::new(crate::dispatch::PeerDispatcher::new(
            transport,
            runtime_shutdown.clone(),
        ));
        let terminal: Arc<ArcSwapOption<RaftStopReason>> = Arc::new(ArcSwapOption::empty());
        let task_terminal = Arc::clone(&terminal);
        let task = tokio::spawn(async move {
            let result = run(
                node,
                transport,
                tick_interval,
                inbound,
                requests,
                runtime_shutdown,
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
    pub async fn campaign(&self) -> Result<(), RaftRuntimeError> {
        self.request(Command::Campaign).await
    }

    /// Proposes one application command through the locally elected leader.
    pub async fn propose(&self, data: impl Into<Vec<u8>>) -> Result<(), RaftRuntimeError> {
        let data = data.into();
        self.request(move |reply| Command::Propose(data, reply))
            .await
    }

    /// Takes committed normal entries accumulated by the runtime.
    pub async fn drain_committed(&self) -> Result<Vec<RaftCommittedEntry>, RaftRuntimeError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::DrainCommitted(reply))
            .await
            .map_err(|_| RaftRuntimeError::Stopped)?;
        result.await.map_err(|_| RaftRuntimeError::Stopped)?
    }

    /// Requests a graceful stop of the owner task.
    ///
    /// The runtime cancels a transport send that is currently awaiting
    /// completion and does not deliver any remaining outbound messages.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Waits for the owner task and returns its terminal status.
    pub async fn join(self) -> Result<(), RaftRuntimeError> {
        self.task.await.map_err(|e| {
            if e.is_cancelled() {
                RaftRuntimeError::Task(TaskError::cancelled())
            } else {
                RaftRuntimeError::Task(TaskError::panic())
            }
        })?
    }

    async fn request<F>(&self, command: F) -> Result<(), RaftRuntimeError>
    where
        F: FnOnce(oneshot::Sender<Result<(), RaftRuntimeError>>) -> Command,
    {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| RaftRuntimeError::Stopped)?;
        record_queue_depth(
            "raft",
            INBOUND_BUFFER - self.inbox.capacity(),
            COMMAND_BUFFER - self.commands.capacity(),
        );
        result.await.map_err(|_| RaftRuntimeError::Stopped)??;
        Ok(())
    }
}

enum Command {
    Campaign(oneshot::Sender<Result<(), RaftRuntimeError>>),
    Propose(Vec<u8>, oneshot::Sender<Result<(), RaftRuntimeError>>),
    DrainCommitted(oneshot::Sender<Result<Vec<RaftCommittedEntry>, RaftRuntimeError>>),
}

async fn run(
    mut node: RaftNode,
    transport: Arc<dyn RaftTransport>,
    tick_interval: Duration,
    mut inbound: mpsc::Receiver<RaftMessage>,
    mut commands: mpsc::Receiver<Command>,
    shutdown: CancellationToken,
) -> Result<(), RaftRuntimeError> {
    let mut ticks = interval(tick_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticks.tick() => {
                record_queue_depth("raft", inbound.len(), commands.len());
                if !drive(node.tick(), &mut node, transport.as_ref(), &shutdown).await? {
                    return Ok(());
                }
            }
            Some(message) = inbound.recv() => {
                record_queue_depth("raft", inbound.len(), commands.len());
                if !drive(node.step(message), &mut node, transport.as_ref(), &shutdown).await? {
                    return Ok(());
                }
            }
            Some(command) = commands.recv() => {
                record_queue_depth("raft", inbound.len(), commands.len());
                match command {
                    Command::Campaign(reply) => {
                        if !respond_drive(
                            reply,
                            drive(node.campaign(), &mut node, transport.as_ref(), &shutdown)
                                .await,
                        )? {
                            return Ok(());
                        }
                    }
                    Command::Propose(data, reply) => {
                        let proposed = node
                            .propose(data)
                            .map_err(|error| match error {
                                RaftNodeError::Raft(error) => RaftRuntimeError::Raft(error),
                                other => RaftRuntimeError::Node(other),
                            });
                        let result = match proposed {
                            Ok(()) => drive(Ok(()), &mut node, transport.as_ref(), &shutdown).await,
                            Err(error) => Err(error),
                        };
                        if !respond_drive(reply, result)? {
                            return Ok(());
                        }
                    }
                    Command::DrainCommitted(reply) => {
                        let _ = reply.send(node.try_drain_committed().map_err(RaftRuntimeError::Node));
                    }
                }
            }
            else => return Ok(()),
        }
    }
}

fn respond(
    reply: oneshot::Sender<Result<(), RaftRuntimeError>>,
    result: Result<(), RaftRuntimeError>,
) -> Result<(), RaftRuntimeError> {
    match result {
        Ok(()) => {
            let _ = reply.send(Ok(()));
            Ok(())
        }
        Err(error) => {
            // Terminal failure: the caller learns the owner stopped; the real error
            // remains available through join() and stop_reason().
            let _ = reply.send(Err(RaftRuntimeError::Stopped));
            Err(error)
        }
    }
}

async fn drive(
    result: raft::Result<()>,
    node: &mut RaftNode,
    transport: &dyn RaftTransport,
    shutdown: &CancellationToken,
) -> Result<bool, RaftRuntimeError> {
    if let Err(error) = result {
        record_failure("raft");
        tracing::error!(
            target: catga_core::TRACING_TARGET,
            error = %error,
            "catga Raft runtime operation failed"
        );
        return Err(RaftRuntimeError::Raft(error));
    }
    for message in node.drain_messages() {
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
                            "catga Raft peer delivery is temporarily unavailable"
                        );
                        node.report_unreachable(peer_id)
                            .map_err(RaftRuntimeError::Raft)?;
                        continue;
                    }
                    tracing::error!(
                        target: catga_core::TRACING_TARGET,
                        error = %error,
                        "catga Raft transport delivery failed"
                    );
                    return Err(RaftRuntimeError::Transport(error));
                }
            }
        }
    }
    Ok(true)
}

fn respond_drive(
    reply: oneshot::Sender<Result<(), RaftRuntimeError>>,
    result: Result<bool, RaftRuntimeError>,
) -> Result<bool, RaftRuntimeError> {
    match result {
        Ok(continue_running) => {
            if continue_running {
                respond(reply, Ok(()))?;
            } else {
                let _ = reply.send(Err(RaftRuntimeError::Stopped));
            }
            Ok(continue_running)
        }
        Err(error) => {
            if let Some(copy) = routine_copy(&error) {
                let _ = reply.send(Err(copy));
                return Ok(true);
            }
            respond(reply, Err(error))?;
            Ok(true)
        }
    }
}

/// Returns a fresh copy of a routine caller error, or `None` for terminal failures.
///
/// Routine errors describe the caller's situation (not the runtime's health), so the
/// owner task replies and keeps running: proposing without leadership
/// (`ProposalDropped`) and proposing into a full pending-commit queue.
fn routine_copy(error: &RaftRuntimeError) -> Option<RaftRuntimeError> {
    match error {
        RaftRuntimeError::Raft(raft::Error::ProposalDropped) => {
            Some(RaftRuntimeError::Raft(raft::Error::ProposalDropped))
        }
        RaftRuntimeError::Node(RaftNodeError::PendingCommitCapacity { capacity }) => Some(
            RaftRuntimeError::Node(RaftNodeError::PendingCommitCapacity {
                capacity: *capacity,
            }),
        ),
        _ => None,
    }
}

fn stop_kind_of(error: &RaftRuntimeError) -> RaftStopKind {
    match error {
        RaftRuntimeError::Raft(raft::Error::Store(_)) | RaftRuntimeError::Node(_) => {
            RaftStopKind::Storage
        }
        RaftRuntimeError::Raft(_) => RaftStopKind::Raft,
        RaftRuntimeError::Transport(_) => RaftStopKind::Transport,
        RaftRuntimeError::Task(_) => RaftStopKind::Task,
        RaftRuntimeError::InvalidTickInterval | RaftRuntimeError::Stopped => RaftStopKind::Internal,
    }
}
