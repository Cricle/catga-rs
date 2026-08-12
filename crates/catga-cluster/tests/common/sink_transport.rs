use async_trait::async_trait;
use catga_cluster::{RaftMessage, RaftTransport, RaftTransportResult};

/// A transport that silently accepts every outbound Raft message.
pub(crate) struct SinkTransport;

#[async_trait]
impl RaftTransport for SinkTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Ok(())
    }
}
