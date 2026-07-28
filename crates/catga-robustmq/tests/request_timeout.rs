//! End-to-end timeout coverage for RobustMQ request setup.

use std::{future::pending, time::Duration};

use catga_core::{CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata};
use catga_robustmq::MailboxClient;
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

fn io_error(context: &'static str, error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, context).with_details(error.to_string())
}
