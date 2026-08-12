//! A scripted-fault transport wrapper for multi-voter scenario tests.
//!
//! All faults are structural, never random: a partition matches fixed
//! source/target voter sets, loss drops every Nth message to one voter counted
//! deterministically from the first intercepted send, and delay holds each
//! matching send for a fixed duration before delivery. Intercept counters let
//! tests prove the configured fault actually engaged.

use std::{
    collections::HashSet,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    RaftMessage, RaftStateMachineRuntime, RaftTransport, RaftTransportError, RaftTransportResult,
};

use crate::channel_transport::ChannelTransport;

/// The two voter sets of an armed partition; traffic crossing from one set
/// to the other is severed in both directions.
type PartitionSets = (HashSet<u64>, HashSet<u64>);

/// The currently armed partition, shared by every clone of the transport.
type ArmedPartition = Arc<Mutex<Option<PartitionSets>>>;

/// Counts how many sends each fault intercepted, so tests can assert the
/// scripted fault was actually exercised instead of passing vacuously.
#[derive(Default)]
struct FaultCounters {
    severed: AtomicU64,
    dropped: AtomicU64,
    delayed: AtomicU64,
}

/// Drops every `modulus`-th send addressed to `target`, counted from the
/// first matching send, mirroring a link with deterministic periodic loss.
struct LossFault {
    target: u64,
    modulus: u64,
    sent: AtomicU64,
}

/// Routes sends through the channel hub unless a scripted fault intercepts
/// them. Clone-cheap: every clone shares the hub, faults, and counters.
#[derive(Clone)]
pub(crate) struct FaultTransport {
    hub: ChannelTransport,
    partition: ArmedPartition,
    loss: Option<Arc<LossFault>>,
    delay: Option<(u64, Duration)>,
    counters: Arc<FaultCounters>,
}

impl FaultTransport {
    /// Wraps the hub with no loss or delay; a partition can still be armed
    /// later through [`Self::partition_between`].
    pub(crate) fn new(hub: ChannelTransport) -> Self {
        Self {
            hub,
            partition: Arc::new(Mutex::new(None)),
            loss: None,
            delay: None,
            counters: Arc::new(FaultCounters::default()),
        }
    }

    /// Drops every `modulus`-th message addressed to `target`, like a link
    /// with periodic loss. The modulus must exceed one so delivery, and
    /// therefore convergence, stays possible.
    pub(crate) fn dropping_every_nth_to(mut self, target: u64, modulus: u64) -> Self {
        assert!(
            modulus > 1,
            "a loss modulus above one keeps delivery possible"
        );
        self.loss = Some(Arc::new(LossFault {
            target,
            modulus,
            sent: AtomicU64::new(0),
        }));
        self
    }

    /// Holds every message addressed to `target` for `delay` before delivery.
    /// Delivery happens in the background so pipelined sends keep their
    /// cadence, mirroring link latency rather than a bandwidth cap.
    pub(crate) fn delaying_sends_to(mut self, target: u64, delay: Duration) -> Self {
        self.delay = Some((target, delay));
        self
    }

    /// Cuts all traffic between `left` and `right` in both directions. Sends
    /// across the cut fail as retryable, mirroring dropped packets: the Raft
    /// runtime reports the peer unreachable and keeps retrying on later ticks.
    pub(crate) fn partition_between(&self, left: &[u64], right: &[u64]) {
        *self.partition.lock().expect("partition state poisoned") = Some((
            left.iter().copied().collect(),
            right.iter().copied().collect(),
        ));
    }

    /// Restores traffic cut by [`Self::partition_between`].
    pub(crate) fn heal(&self) {
        *self.partition.lock().expect("partition state poisoned") = None;
    }

    /// Registers a runtime inbox with the underlying hub.
    pub(crate) async fn register(&self, runtime: &RaftStateMachineRuntime) {
        self.hub.register(runtime).await;
    }

    /// Sends refused because they crossed an armed partition.
    pub(crate) fn severed_messages(&self) -> u64 {
        self.counters.severed.load(Ordering::Acquire)
    }

    /// Sends dropped by the deterministic loss fault.
    pub(crate) fn dropped_messages(&self) -> u64 {
        self.counters.dropped.load(Ordering::Acquire)
    }

    /// Sends held back by the delivery-delay fault.
    pub(crate) fn delayed_messages(&self) -> u64 {
        self.counters.delayed.load(Ordering::Acquire)
    }

    fn crosses_partition(&self, from: u64, to: u64) -> bool {
        self.partition
            .lock()
            .expect("partition state poisoned")
            .as_ref()
            .is_some_and(|(left, right)| {
                (left.contains(&from) && right.contains(&to))
                    || (right.contains(&from) && left.contains(&to))
            })
    }
}

#[async_trait]
impl RaftTransport for FaultTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        if self.crosses_partition(message.from, message.to) {
            self.counters.severed.fetch_add(1, Ordering::AcqRel);
            return Err(RaftTransportError::retryable(io::Error::other(
                "partition severs cross-set traffic",
            )));
        }
        if let Some(loss) = &self.loss
            && message.to == loss.target
        {
            let ordinal = loss.sent.fetch_add(1, Ordering::AcqRel) + 1;
            if ordinal % loss.modulus == 0 {
                self.counters.dropped.fetch_add(1, Ordering::AcqRel);
                return Err(RaftTransportError::retryable(io::Error::other(
                    "deterministic loss drops every nth message",
                )));
            }
        }
        if let Some((target, delay)) = self.delay
            && message.to == target
        {
            self.counters.delayed.fetch_add(1, Ordering::AcqRel);
            let hub = self.hub.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                // A peer that vanishes mid-delay is indistinguishable from a
                // packet dropped after the wait, so delivery failure is ignored.
                let _ = hub.send(message).await;
            });
            return Ok(());
        }
        self.hub.send(message).await
    }
}
