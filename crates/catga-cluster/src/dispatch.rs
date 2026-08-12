//! Non-blocking per-peer dispatch between the Raft owner loop and a transport.
//!
//! The owner task advances the Raft logical clock on every tick, so awaiting a
//! transport send inline lets a dead or slow peer stall the entire clock —
//! including election timeouts — turning a single-node outage into a multi-ten-second
//! failover. [`PeerDispatcher`] gives each peer a bounded queue and a serial worker:
//! queueing never blocks the owner, and a full queue fails fast as retryable
//! backpressure (which the owner reports unreachable so Raft backs off). A fatal
//! worker failure is surfaced on the next send so the runtime still stops.

use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{RaftMessage, RaftTransport, RaftTransportError, RaftTransportResult};

/// Per-peer queue bound. Heartbeats dominate traffic; a dead peer fills this in a
/// few seconds, after which sends fail fast until the peer worker drains again.
const PEER_QUEUE_CAPACITY: usize = 64;

struct PeerWorker {
    receiver: mpsc::Receiver<RaftMessage>,
    transport: Arc<dyn RaftTransport>,
    fatal: Arc<Mutex<Option<String>>>,
    shutdown: CancellationToken,
}

impl PeerWorker {
    async fn run(mut self) {
        loop {
            let message = tokio::select! {
                _ = self.shutdown.cancelled() => return,
                message = self.receiver.recv() => message,
            };
            let Some(message) = message else { return };
            // A hung peer send must not outlive the runtime: dropping the
            // in-flight future on shutdown releases the worker promptly.
            let result = tokio::select! {
                _ = self.shutdown.cancelled() => return,
                result = self.transport.send(message) => result,
            };
            match result {
                Ok(()) => {}
                Err(error) if error.is_retryable() => {
                    // Raft regenerates replication traffic from log state
                    // after the owner reports the peer unreachable, so the
                    // frame can be dropped safely.
                }
                Err(error) => {
                    *self.fatal.lock().expect("fatal slot poisoned") = Some(error.to_string());
                    return;
                }
            }
        }
    }
}

/// A [`RaftTransport`] wrapper that decouples the Raft owner loop from peer latency.
///
/// `send` returns once the frame is queued for the peer's serial worker, so the
/// owner task never awaits network I/O. Workers stop when `shutdown` is cancelled:
/// an in-flight send future is dropped and queued but unsent frames are
/// discarded, matching the runtime's shutdown contract.
pub(crate) struct PeerDispatcher {
    inner: Arc<dyn RaftTransport>,
    peers: Mutex<HashMap<u64, mpsc::Sender<RaftMessage>>>,
    fatal: Arc<Mutex<Option<String>>>,
    shutdown: CancellationToken,
}

impl PeerDispatcher {
    /// Wraps `inner` with per-peer bounded dispatch queues.
    pub(crate) fn new(inner: Arc<dyn RaftTransport>, shutdown: CancellationToken) -> Self {
        Self {
            inner,
            peers: Mutex::new(HashMap::new()),
            fatal: Arc::new(Mutex::new(None)),
            shutdown,
        }
    }

    fn peer_sender(&self, id: u64) -> mpsc::Sender<RaftMessage> {
        let mut peers = self.peers.lock().expect("peer map poisoned");
        if let Some(sender) = peers.get(&id) {
            return sender.clone();
        }
        let (sender, receiver) = mpsc::channel(PEER_QUEUE_CAPACITY);
        peers.insert(id, sender.clone());
        let worker = PeerWorker {
            receiver,
            transport: Arc::clone(&self.inner),
            fatal: Arc::clone(&self.fatal),
            shutdown: self.shutdown.clone(),
        };
        tokio::spawn(worker.run());
        sender
    }
}

#[async_trait]
impl RaftTransport for PeerDispatcher {
    /// Queues one frame for the peer's worker without blocking the caller.
    ///
    /// A full peer queue produces a retryable error so the owner reports the peer
    /// unreachable and Raft backs off; a worker-side fatal failure fails all later
    /// sends so the runtime stops observably.
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        if let Some(reason) = self.fatal.lock().expect("fatal slot poisoned").as_ref() {
            return Err(RaftTransportError::fatal(io::Error::other(format!(
                "raft peer transport failed earlier: {reason}"
            ))));
        }
        self.peer_sender(message.to).try_send(message).map_err(|_| {
            RaftTransportError::retryable(io::Error::other("raft peer dispatch queue is full"))
        })
    }
}
