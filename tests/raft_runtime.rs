use std::{collections::HashMap, io, sync::Arc, time::Duration};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftMessage, RaftNode, RaftRuntime, RaftTransport,
};
use tokio::sync::{RwLock, mpsc};

#[derive(Clone, Default)]
struct ChannelTransport {
    routes: Arc<RwLock<HashMap<u64, mpsc::Sender<RaftMessage>>>>,
}

impl ChannelTransport {
    async fn register(&self, node: &RaftRuntime) {
        self.routes.write().await.insert(node.id(), node.inbox());
    }
}

#[async_trait]
impl RaftTransport for ChannelTransport {
    async fn send(
        &self,
        message: RaftMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let route = self
            .routes
            .read()
            .await
            .get(&message.to)
            .cloned()
            .ok_or_else(|| io::Error::other("unknown Raft peer"))?;
        route
            .send(message)
            .await
            .map_err(|_| io::Error::other("Raft peer stopped"))?;
        Ok(())
    }
}

fn members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
        RaftMember::new(3, "http://node-3"),
    ]
}

#[tokio::test]
async fn raft_runtime_owns_ticks_transport_and_committed_entries_without_external_relay() {
    let transport = ChannelTransport::default();
    let cluster_members = members();
    let runtimes = cluster_members
        .iter()
        .map(|member| {
            RaftRuntime::spawn(
                RaftNode::new(member.id(), member.endpoint(), cluster_members.clone()).unwrap(),
                Arc::new(transport.clone()),
                Duration::from_millis(2),
            )
        })
        .collect::<Vec<_>>();
    for runtime in &runtimes {
        transport.register(runtime).await;
    }

    runtimes[0].campaign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtimes.iter().any(|runtime| {
            runtime.coordinator().leader_endpoint().as_deref() != Some("http://node-1")
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    runtimes[0].propose(b"reserve-inventory:10").await.unwrap();
    for runtime in &runtimes {
        let committed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let entries = runtime.drain_committed().await.unwrap();
                if entries
                    .iter()
                    .any(|entry| entry.data == b"reserve-inventory:10")
                {
                    return entries;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            committed
                .iter()
                .any(|entry| entry.data == b"reserve-inventory:10")
        );
    }

    for runtime in runtimes {
        runtime.shutdown();
        runtime.join().await.unwrap();
    }
}
