use async_trait::async_trait;
use catga_cluster::{RaftMessage, RaftTransport, RaftTransportError, RaftTransportResult};

/// A channel-based transport hub that routes Raft messages between runtimes.
///
/// Sending to a closed inbox (a stopped peer) is a retryable failure, mirroring
/// a fast TCP refusal from a dead process.
#[derive(Clone, Default)]
pub(crate) struct ChannelTransport {
    routes: std::sync::Arc<
        tokio::sync::RwLock<std::collections::HashMap<u64, tokio::sync::mpsc::Sender<RaftMessage>>>,
    >,
}

impl ChannelTransport {
    pub(crate) async fn register(&self, runtime: &catga_cluster::RaftStateMachineRuntime) {
        self.routes
            .write()
            .await
            .insert(runtime.id(), runtime.inbox());
    }
}

#[async_trait]
impl RaftTransport for ChannelTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let route = self.routes.read().await.get(&message.to).cloned();
        let Some(route) = route else {
            return Err(RaftTransportError::retryable(std::io::Error::other(
                "peer never registered",
            )));
        };
        route
            .send(message)
            .await
            .map_err(|_| RaftTransportError::retryable(std::io::Error::other("peer stopped")))?;
        Ok(())
    }
}
