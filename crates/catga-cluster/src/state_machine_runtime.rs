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
    /// The owner task exited before it could handle a request.
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
        self.request(Command::Propose(data.into())).await
    }

    /// Persists a state-machine snapshot at the latest successfully applied command.
    pub async fn checkpoint(&self) -> Result<(), RaftStateMachineRuntimeError> {
        self.request(Command::Checkpoint).await
    }

    /// Requests a graceful stop of the owner task.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Waits for the owner task and returns its terminal status.
    pub async fn join(self) -> Result<(), RaftStateMachineRuntimeError> {
        self.task
            .await
            .map_err(RaftStateMachineRuntimeError::Task)?
    }

    async fn request(&self, command: Command) -> Result<(), RaftStateMachineRuntimeError> {
        let (reply, result) = oneshot::channel();
        let command = match command {
            Command::Campaign => Command::CampaignWithReply(reply),
            Command::Propose(data) => Command::ProposeWithReply(data, reply),
            Command::Checkpoint => Command::CheckpointWithReply(reply),
            Command::CampaignWithReply(_)
            | Command::ProposeWithReply(_, _)
            | Command::CheckpointWithReply(_) => {
                unreachable!("only public state-machine commands are requested")
            }
        };
        self.commands
            .send(command)
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)?;
        result
            .await
            .map_err(|_| RaftStateMachineRuntimeError::Stopped)??;
        Ok(())
    }
}

enum Command {
    Campaign,
    Propose(Vec<u8>),
    Checkpoint,
    CampaignWithReply(oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>),
    ProposeWithReply(
        Vec<u8>,
        oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>,
    ),
    CheckpointWithReply(oneshot::Sender<Result<(), RaftStateMachineRuntimeError>>),
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
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticks.tick() => drive(driver.tick(), &mut driver, transport.as_ref()).await?,
            Some(message) = inbound.recv() => drive(driver.step(message), &mut driver, transport.as_ref()).await?,
            Some(command) = commands.recv() => match command {
                Command::CampaignWithReply(reply) => {
                    respond(reply, drive(driver.campaign(), &mut driver, transport.as_ref()).await)?;
                }
                Command::ProposeWithReply(data, reply) => {
                    respond(reply, drive(driver.propose(data), &mut driver, transport.as_ref()).await)?;
                }
                Command::CheckpointWithReply(reply) => {
                    respond(reply, driver.checkpoint().map_err(RaftStateMachineRuntimeError::StateMachine))?;
                }
                Command::Campaign | Command::Propose(_) | Command::Checkpoint => {
                    unreachable!("state-machine commands are always paired with a reply")
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
) -> Result<(), RaftStateMachineRuntimeError>
where
    M: RaftStateMachine,
{
    raft_result.map_err(RaftStateMachineRuntimeError::Raft)?;
    send_messages(driver, transport).await?;
    driver
        .apply_committed()
        .map_err(RaftStateMachineRuntimeError::StateMachine)?;
    send_messages(driver, transport).await
}

async fn send_messages<M>(
    driver: &mut RaftStateMachineDriver<M>,
    transport: &dyn RaftTransport,
) -> Result<(), RaftStateMachineRuntimeError>
where
    M: RaftStateMachine,
{
    for message in driver.drain_messages() {
        transport
            .send(message)
            .await
            .map_err(RaftStateMachineRuntimeError::Transport)?;
    }
    Ok(())
}
