//! End-to-end timeout coverage for Redis request setup.

use std::{future::pending, time::Duration};

use catga_core::{CatgaError, CatgaResult, Envelope, ErrorCode, EventStore, MessageMetadata};
use catga_redis::{RedisCommandOptions, RedisEventStore, RedisRequestClient};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

#[tokio::test]
async fn request_timeout_bounds_a_stalled_reply_connection() -> CatgaResult<()> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| io_error("bind stalled Redis server", error))?;
    let address = listener
        .local_addr()
        .map_err(|error| io_error("read stalled Redis server address", error))?;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut first_command_byte = [0; 1];
        socket.read_exact(&mut first_command_byte).await?;
        pending::<Result<(), std::io::Error>>().await
    });
    let client = RedisRequestClient::connect(&format!("redis://{address}"))?;
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
    .expect("Redis request must use its timeout while opening the reply subscription");

    let error = result.expect_err("the stalled Redis reply connection must time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn persistence_connection_returns_a_bounded_error_for_a_stalled_response() -> CatgaResult<()>
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| io_error("bind stalled Redis server", error))?;
    let address = listener
        .local_addr()
        .map_err(|error| io_error("read stalled Redis server address", error))?;
    let (stall_commands, wait_for_stall) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut setup_commands = [0; 256];
        if socket.read(&mut setup_commands).await? == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Redis client closed before its setup commands",
            ));
        }
        socket.write_all(b"+OK\r\n+OK\r\n").await?;
        wait_for_stall
            .await
            .map_err(|_| std::io::Error::other("test did not start stalled command"))?;
        let mut first_command_byte = [0; 1];
        socket.read_exact(&mut first_command_byte).await?;
        pending::<Result<(), std::io::Error>>().await
    });

    let store = RedisEventStore::connect_with_options(
        format!("redis://{address}"),
        "timeout-test",
        RedisCommandOptions::new(Duration::from_millis(20))?,
    )
    .await?;
    stall_commands
        .send(())
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "stalled Redis server stopped early"))?;
    let result = tokio::time::timeout(Duration::from_secs(1), store.version("timeout-test"))
        .await
        .expect("ordinary Redis persistence commands must have a finite response timeout");

    let error = match result {
        Ok(_) => panic!("the stalled Redis persistence connection must time out"),
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::Transient);
    server.abort();
    Ok(())
}

fn io_error(context: &'static str, error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, context).with_details(error.to_string())
}
