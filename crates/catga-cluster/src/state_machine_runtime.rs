//! Async single-owner runtime for a deterministic Raft state machine.

use std::{error::Error, fmt, sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;

use crate::{
    RaftClusterNode, RaftMessage, RaftStateMachine, RaftStateMachineDriver, RaftStateMachineError,
    RaftTransport,
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
    /// `raft-rs` rejected an operation or an inbound protocol message.
    Raft(raft::Error),
    /// The state machine or its Raft storage failed while applying an entry.
    StateMachine(RaftStateMachineError),
    /// The configured transport failed while sending an outbound protocol message.
    Transport(Box<dyn Error + Send + Sync>),
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
            Self::Raft(error) => error.fmt(formatter),
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
            Self::StateMachine(error) => Some(error),
            Self::Transport(error) => Some(error.as_ref()),
            Self::Task(error) => Some(error),
            Self::InvalidTickInterval | Self::Stopped => None,
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
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), RaftStateMachineRuntimeError>>,
}

impl RaftStateMachineRuntime {
    /// Starts an owned state-machine runtime with bounded inbound queues.
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
        let shutdown = CancellationToken::new();
        let runtime_shutdown = shutdown.clone();
        let transport: Arc<dyn RaftTransport> = transport;
        let task = tokio::spawn(run(
            driver,
            transport,
            tick_interval,
            inbound,
            requests,
            runtime_shutdown,
        ));
        Ok(Self {
            id,
            coordinator,
            inbox,
            commands,
            shutdown,
            task,
        })
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

    /// Persists a state-machine snapshot at the latest successfully applied command.
    pub async fn checkpoint(&self) -> Result<(), RaftStateMachineRuntimeError> {
        self.request(Command::Checkpoint).await
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
        let (reply, result) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)?;
        result
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)??;
        Ok(())
    }
}

enum Command {
    Campaign(oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>),
    Propose(
        Vec<u8>,
        oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>,
    ),
    Checkpoint(oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>),
}

async fn run<M>(
    mut driver: RaftStateMachineDriver<M>,
    transport: Arc<dyn RaftTransport>,
    tick_interval: Duration,
    mut inbound: mpsc::Receiver<RaftMessage>,
    mut commands: mpsc::Receiver<Command>,
    shutdown: CancellationToken,
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
                if !drive(driver.tick(), &mut driver, transport.as_ref(), &shutdown).await? {
                    return Ok(());
                }
            }
            Some(message) = inbound.recv() => {
                if !drive(driver.step(message), &mut driver, transport.as_ref(), &shutdown).await? {
                    return Ok(());
                }
            }
            Some(command) = commands.recv() => match command {
                Command::Campaign(reply) => {
                    if !respond_drive(
                        reply,
                        drive(driver.campaign(), &mut driver, transport.as_ref(), &shutdown).await,
                    )? {
                        return Ok(());
                    }
                }
                Command::Propose(data, reply) => {
                    if !respond_drive(
                        reply,
                        drive(driver.propose(data), &mut driver, transport.as_ref(), &shutdown).await,
                    )? {
                        return Ok(());
                    }
                }
                Command::Checkpoint(reply) => {
                    respond(reply, driver.checkpoint().map_err(RaftStateMachineRuntimeError::StateMachine))?;
                }
            },
            else => return Ok(()),
        }
    }
}

fn respond(
    reply: oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>,
    result: Result<(), RaftStateMachineRuntimeError>,
) -> Result<(), RaftStateMachineRuntimeError> {
    match result {
        Ok(()) => {
            let _ = reply.send(Ok(()));
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
            let _ = reply.send(Err(RaftStateMachineRuntimeError::Stopped));
            Err(error)
        }
    }
}

async fn drive<M>(
    raft_result: raft::Result<()>,
    driver: &mut RaftStateMachineDriver<M>,
    transport: &dyn RaftTransport,
    shutdown: &CancellationToken,
) -> Result<bool, RaftStateMachineRuntimeError>
where
    M: RaftStateMachine,
{
    raft_result.map_err(RaftStateMachineRuntimeError::Raft)?;
    if !send_messages(driver, transport, shutdown).await? {
        return Ok(false);
    }
    driver
        .apply_committed()
        .map_err(RaftStateMachineRuntimeError::StateMachine)?;
    send_messages(driver, transport, shutdown).await
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
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(false),
            result = transport.send(message) => result.map_err(RaftStateMachineRuntimeError::Transport)?,
        }
    }
    Ok(true)
}

fn respond_drive(
    reply: oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>,
    result: Result<bool, RaftStateMachineRuntimeError>,
) -> Result<bool, RaftStateMachineRuntimeError> {
    match result {
        Ok(continue_running) => {
            if continue_running {
                respond(reply, Ok(()))?;
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
