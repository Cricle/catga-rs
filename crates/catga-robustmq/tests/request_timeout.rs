//! End-to-end timeout coverage for RobustMQ request setup.

use std::{future::pending, time::Duration};

use catga_core::{CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata};
use catga_robustmq::{MailboxClient, MailboxConfig, MailboxRequestServer};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

#[tokio::test]
async fn request_timeout_bounds_a_stalled_mailbox_creation() -> CatgaResult<()> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| io_error("bind stalled RobustMQ server", error))?;
    let address = listener
        .local_addr()
        .map_err(|error| io_error("read stalled RobustMQ server address", error))?;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let (reader, mut writer) = socket.into_split();
        writer
            .write_all(
                b"INFO {\"server_id\":\"catga\",\"version\":\"1.0.0\",\"proto\":1,\"max_payload\":1048576}\r\n",
            )
            .await?;

        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await? {
            if line == "PING" {
                writer.write_all(b"PONG\r\n").await?;
            }
            if line.starts_with("PUB $mq9.AI.MAILBOX.CREATE ") {
                pending::<()>().await;
            }
        }
        Ok::<(), std::io::Error>(())
    });

    let client = MailboxClient::connect(&format!("nats://{address}")).await?;
    let request = Envelope::new(
        1,
        "catga.test.request",
        Vec::new(),
        MessageMetadata::new(1, Some(1)),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client.request_to("requests", request, Duration::from_millis(20)),
    )
    .await
    .expect("mailbox request must use its timeout while creating the reply mailbox");

    let error = result.expect_err("the stalled mailbox creation must time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn mailbox_api_validates_local_arguments_before_control_plane_io() -> CatgaResult<()> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| io_error("bind validation RobustMQ server", error))?;
    let address = listener
        .local_addr()
        .map_err(|error| io_error("read validation RobustMQ server address", error))?;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let (reader, mut writer) = socket.into_split();
        writer
            .write_all(
                b"INFO {\"server_id\":\"catga\",\"version\":\"1.0.0\",\"proto\":1,\"max_payload\":1048576}\r\n",
            )
            .await?;

        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await? {
            if line == "PING" {
                writer.write_all(b"PONG\r\n").await?;
            }
        }
        Ok::<(), std::io::Error>(())
    });
    let client = MailboxClient::connect(&format!("nats://{address}")).await?;
    let request = Envelope::new(
        1,
        "catga.test.request",
        Vec::new(),
        MessageMetadata::new(1, Some(1)),
    );

    let empty_mailbox = client
        .request_to("", request.clone(), Duration::from_millis(10))
        .await
        .expect_err("an empty destination must be rejected locally");
    assert_eq!(empty_mailbox.code(), ErrorCode::Validation);

    let zero_timeout = client
        .request_to("requests", request, Duration::ZERO)
        .await
        .expect_err("a zero timeout must be rejected locally");
    assert_eq!(zero_timeout.code(), ErrorCode::Validation);

    let missing_mailbox = require_error(
        MailboxRequestServer::subscribe(client.clone(), "", 1).await,
        "an empty request-server mailbox must be rejected locally",
    )?;
    assert_eq!(missing_mailbox.code(), ErrorCode::Validation);

    let zero_capacity = require_error(
        MailboxRequestServer::subscribe(client.clone(), "requests", 0).await,
        "a zero request-server capacity must be rejected locally",
    )?;
    assert_eq!(zero_capacity.code(), ErrorCode::Validation);

    let invalid_public_config = client
        .create(&MailboxConfig {
            server: "unused-by-an-existing-client".into(),
            ttl_seconds: 60,
            public: true,
            name: "".into(),
            description: "must be rejected by RobustMQ before it is sent".into(),
        })
        .await
        .expect_err("a public mailbox without a name must be rejected locally");
    // The upstream mq9 SDK exposes this as an untyped transport error; the adapter keeps its
    // established transient classification while preserving the actionable validation text.
    assert_eq!(invalid_public_config.code(), ErrorCode::Transient);
    assert!(
        invalid_public_config.message().contains("name is required"),
        "the upstream validation reason must survive error translation",
    );

    drop(client);
    server.abort();
    Ok(())
}

fn io_error(context: &'static str, error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, context).with_details(error.to_string())
}

fn require_error<T>(result: CatgaResult<T>, message: &'static str) -> CatgaResult<CatgaError> {
    match result {
        Ok(_) => Err(CatgaError::new(ErrorCode::Internal, message)),
        Err(error) => Ok(error),
    }
}
