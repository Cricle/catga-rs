//! Minimal mq9 control-plane harness for end-to-end adapter contracts.
//!
//! The upstream `robustmq` crate speaks the mq9 mailbox protocol over NATS,
//! while the publicly available RobustMQ broker image exposes other protocols.
//! This harness implements only mailbox creation; message delivery remains on
//! the real NATS server used by the test. It is deliberately test-only and
//! must not be used as a production RobustMQ implementation.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::StreamExt as _;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const MAILBOX_CREATE_SUBJECT: &str = "$mq9.AI.MAILBOX.CREATE";

/// A running, bounded mq9 mailbox-creation control-plane harness.
pub struct Mq9ControlPlane {
    shutdown: CancellationToken,
    task: JoinHandle<CatgaResult<()>>,
    created_mailboxes: Arc<AtomicUsize>,
}

impl Mq9ControlPlane {
    /// Returns the number of mailbox-creation requests served by this harness.
    pub fn created_mailboxes(&self) -> usize {
        self.created_mailboxes.load(Ordering::Relaxed)
    }

    /// Stops the NATS subscription and waits for the harness task to exit.
    pub async fn close(self) -> CatgaResult<()> {
        self.shutdown.cancel();
        self.task.await.map_err(|error| {
            CatgaError::new(ErrorCode::Internal, format!("join mq9 harness: {error}"))
        })?
    }
}

/// Starts the minimal mq9 mailbox-creation control plane on `server`.
pub async fn start(server: &str) -> CatgaResult<Mq9ControlPlane> {
    let client = async_nats::connect(server).await.map_err(|error| {
        CatgaError::new(
            ErrorCode::Unavailable,
            format!("connect mq9 test control plane: {error}"),
        )
    })?;
    let subscription = client
        .subscribe(MAILBOX_CREATE_SUBJECT)
        .await
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("subscribe mq9 mailbox creation: {error}"),
            )
        })?;
    // `async_nats::Client::subscribe` only queues the SUB command locally.  The request
    // client may publish immediately after this function returns, so wait until NATS has
    // processed the subscription before exposing the harness.  Without this barrier a
    // request can be legitimately dropped by core NATS (there is no subscriber yet), which
    // made the real-service contract intermittently time out under CI scheduling pressure.
    client.flush().await.map_err(|error| {
        CatgaError::new(
            ErrorCode::Unavailable,
            format!("flush mq9 mailbox-creation subscription: {error}"),
        )
    })?;
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let created_mailboxes = Arc::new(AtomicUsize::new(0));
    let task_created_mailboxes = Arc::clone(&created_mailboxes);
    let task = tokio::spawn(async move {
        serve_mailbox_creations(client, subscription, task_shutdown, task_created_mailboxes).await
    });
    Ok(Mq9ControlPlane {
        shutdown,
        task,
        created_mailboxes,
    })
}

async fn serve_mailbox_creations(
    client: async_nats::Client,
    mut subscription: async_nats::Subscriber,
    shutdown: CancellationToken,
    created_mailboxes: Arc<AtomicUsize>,
) -> CatgaResult<()> {
    loop {
        let message = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            message = subscription.next() => message.ok_or_else(|| {
                CatgaError::new(ErrorCode::Transient, "mq9 mailbox-creation subscription closed")
            })?,
        };
        let reply = message.reply.ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "mq9 mailbox-creation request has no reply subject",
            )
        })?;
        let sequence = created_mailboxes.fetch_add(1, Ordering::Relaxed) + 1;
        let response = serde_json::to_vec(&serde_json::json!({
            "mail_id": format!("catga-mq9-test-mailbox-{sequence}"),
        }))
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("serialize mq9 mailbox response: {error}"),
            )
        })?;
        client
            .publish(reply, response.into())
            .await
            .map_err(|error| {
                CatgaError::new(
                    ErrorCode::Transient,
                    format!("reply to mq9 mailbox creation: {error}"),
                )
            })?;
        client.flush().await.map_err(|error| {
            CatgaError::new(
                ErrorCode::Transient,
                format!("flush mq9 mailbox response: {error}"),
            )
        })?;
    }
}
