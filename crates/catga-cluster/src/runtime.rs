//! Async single-owner runtime for a [`crate::RaftNode`].

use std::{error::Error, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;

use crate::{RaftClusterNode, RaftCommittedEntry, RaftMessage, RaftNode};

const COMMAND_BUFFER: usize = 64;
const INBOUND_BUFFER: usize = 256;

/// Result returned by an application-provided Raft transport.
pub type RaftTransportResult = Result<(), Box<dyn Error + Send + Sync>>;

/// Sends wire-level Raft messages to their destination node.
///
/// Implementations should enqueue quickly or apply their own bounded
/// backpressure, because a send is awaited by the single Raft owner task.
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
    /// The owner task exited before it could handle a request.
    Stopped,
    /// `raft-rs` rejected an operation or an inbound protocol message.
    Raft(raft::Error),
    /// The configured transport failed while sending an outbound protocol message.
    Transport(Box<dyn Error + Send + Sync>),
    /// The owner task panicked or was aborted.
    Task(tokio::task::JoinError),
}

impl fmt::Display for RaftRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTickInterval => {
                formatter.write_str("Raft runtime tick interval must be non-zero")
            }
            Self::Stopped => formatter.write_str("Raft runtime stopped"),
            Self::Raft(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::Task(error) => error.fmt(formatter),
        }
    }
}

impl Error for RaftRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Raft(error) => Some(error),
            Self::Transport(error) => Some(error.as_ref()),
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
    task: JoinHandle<Result<(), RaftRuntimeError>>,
}

impl RaftRuntime {
    /// Starts a runtime that ticks at `tick_interval` and emits messages through `transport`.
    ///
    /// `tick_interval` must be non-zero. The runtime stops on a Raft or transport
    /// failure; [`Self::join`] returns that terminal error.
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
        let shutdown = CancellationToken::new();
        let runtime_shutdown = shutdown.clone();
        let transport: Arc<dyn RaftTransport> = transport;
        let task = tokio::spawn(run(
            node,
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
    pub async fn campaign(&self) -> Result<(), RaftRuntimeError> {
        self.request(Command::Campaign).await
    }

    /// Proposes one application command through the locally elected leader.
    pub async fn propose(&self, data: impl Into<Vec<u8>>) -> Result<(), RaftRuntimeError> {
        self.request(Command::Propose(data.into())).await
    }

    /// Takes committed normal entries accumulated by the runtime.
    pub async fn drain_committed(&self) -> Result<Vec<RaftCommittedEntry>, RaftRuntimeError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::DrainCommitted(reply))
            .await
            .map_err(|_| RaftRuntimeError::Stopped)?;
        result.await.map_err(|_| RaftRuntimeError::Stopped)
    }

    /// Requests a graceful stop of the owner task.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Waits for the owner task and returns its terminal status.
    pub async fn join(self) -> Result<(), RaftRuntimeError> {
        self.task.await.map_err(RaftRuntimeError::Task)?
    }

    async fn request(&self, command: Command) -> Result<(), RaftRuntimeError> {
        let (reply, result) = oneshot::channel();
        let command = match command {
            Command::Campaign => Command::CampaignWithReply(reply),
            Command::Propose(data) => Command::ProposeWithReply(data, reply),
            Command::CampaignWithReply(_)
            | Command::ProposeWithReply(_, _)
            | Command::DrainCommitted(_) => {
                unreachable!("only public Raft commands are requested")
            }
        };
        self.commands
            .send(command)
            .await
            .map_err(|_| RaftRuntimeError::Stopped)?;
        result.await.map_err(|_| RaftRuntimeError::Stopped)??;
        Ok(())
    }
}

enum Command {
    Campaign,
    Propose(Vec<u8>),
    CampaignWithReply(oneshot::Sender<Result<(), RaftRuntimeError>>),
    ProposeWithReply(Vec<u8>, oneshot::Sender<Result<(), RaftRuntimeError>>),
    DrainCommitted(oneshot::Sender<Vec<RaftCommittedEntry>>),
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
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticks.tick() => drive(node.tick(), &mut node, transport.as_ref()).await?,
            Some(message) = inbound.recv() => drive(node.step(message), &mut node, transport.as_ref()).await?,
            Some(command) = commands.recv() => {
                match command {
                    Command::CampaignWithReply(reply) => {
                        respond(
                            reply,
                            drive(node.campaign(), &mut node, transport.as_ref()).await,
                        )?;
                    }
                    Command::ProposeWithReply(data, reply) => {
                        respond(
                            reply,
                            drive(node.propose(data), &mut node, transport.as_ref()).await,
                        )?;
                    }
                    Command::DrainCommitted(reply) => {
                        let _ = reply.send(node.drain_committed());
                    }
                    Command::Campaign | Command::Propose(_) => unreachable!("commands are always paired with a reply"),
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
            let _ = reply.send(Err(RaftRuntimeError::Stopped));
            Err(error)
        }
    }
}

async fn drive(
    result: raft::Result<()>,
    node: &mut RaftNode,
    transport: &dyn RaftTransport,
) -> Result<(), RaftRuntimeError> {
    result.map_err(RaftRuntimeError::Raft)?;
    for message in node.drain_messages() {
        transport
            .send(message)
            .await
            .map_err(RaftRuntimeError::Transport)?;
    }
    Ok(())
}
