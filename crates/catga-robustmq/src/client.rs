use catga_core::{CatgaError, CatgaResult, Envelope, ErrorCode};
use robustmq::{MQ9Client, Mailbox, Subscription};

use crate::MailboxPriority;

/// Configuration for a single RobustMQ mq9 mailbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MailboxConfig {
    /// Server URL accepted by the RobustMQ SDK.
    pub server: Box<str>,
    /// Mailbox lifetime in seconds.
    pub ttl_seconds: u64,
    /// Whether the mailbox is publicly discoverable.
    pub public: bool,
    /// Optional discoverable mailbox name.
    pub name: Box<str>,
    /// Optional mailbox description.
    pub description: Box<str>,
}

/// A typed wrapper around the RobustMQ mq9 mailbox client.
pub struct MailboxClient {
    client: MQ9Client,
}

impl MailboxClient {
    /// Connects to a RobustMQ NATS-compatible endpoint.
    pub async fn connect(server: &str) -> CatgaResult<Self> {
        MQ9Client::connect(server)
            .await
            .map(|client| Self { client })
            .map_err(map_error)
    }

    /// Creates a mailbox using the configured retention and visibility.
    pub async fn create(&self, config: &MailboxConfig) -> CatgaResult<Mailbox> {
        self.client
            .create(
                config.ttl_seconds,
                config.public,
                &config.name,
                &config.description,
            )
            .await
            .map_err(map_error)
    }

    /// Sends an envelope payload to a mailbox with explicit priority.
    pub async fn send(
        &self,
        mailbox_id: &str,
        envelope: &Envelope,
        priority: MailboxPriority,
    ) -> CatgaResult<()> {
        self.client
            .send(mailbox_id, envelope.payload(), priority.as_sdk())
            .await
            .map_err(map_error)
    }

    /// Subscribes to push delivery for a mailbox.
    pub async fn subscribe<F, Fut>(
        &self,
        mailbox_id: &str,
        callback: F,
        priority: Option<MailboxPriority>,
        queue_group: &str,
    ) -> CatgaResult<Subscription>
    where
        F: Fn(robustmq::Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.client
            .subscribe(
                mailbox_id,
                callback,
                priority.map(MailboxPriority::as_sdk),
                queue_group,
            )
            .await
            .map_err(map_error)
    }
}

fn map_error(error: robustmq::MQ9Error) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
