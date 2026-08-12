//! Shared scripted RESP2 mock Redis server for connection-drop contract tests.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

/// How the mock answers a SUBSCRIBE command.
#[derive(Clone, Copy)]
pub enum SubscribeReply {
    /// Confirm the subscription and keep serving commands.
    Confirm,
    /// Confirm the subscription, then close the connection.
    ConfirmThenClose,
    /// Close the connection without answering, failing the pending subscribe.
    Fail,
}

/// Starts a minimal RESP2 mock that acknowledges setup commands with `+OK`, answers
/// PUBLISH with `:1`, and scripts SUBSCRIBE per `reply`.
///
/// Returns the mock's `redis://` URL and the accept-loop task.
pub async fn spawn_mock_redis(reply: SubscribeReply) -> CatgaResult<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| io_error("bind mock Redis server", error))?;
    let address = listener
        .local_addr()
        .map_err(|error| io_error("read mock Redis server address", error))?;
    let server = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(serve(socket, reply));
        }
    });
    Ok((format!("redis://{address}"), server))
}

async fn serve(mut socket: TcpStream, reply: SubscribeReply) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4_096];
    loop {
        let read = match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        buffer.extend_from_slice(&chunk[..read]);
        while let Some((command, consumed)) = parse_command(&buffer) {
            buffer.drain(..consumed);
            let name = command
                .first()
                .map(|argument| argument.to_ascii_uppercase())
                .unwrap_or_default();
            if name.eq_ignore_ascii_case(b"SUBSCRIBE") {
                match reply {
                    SubscribeReply::Fail => {
                        // Dropping the socket without a response fails the pending subscribe.
                        return;
                    }
                    SubscribeReply::Confirm => {
                        let confirmation = push_confirmation(b"subscribe", command.get(1), 1);
                        if socket.write_all(&confirmation).await.is_err() {
                            return;
                        }
                    }
                    SubscribeReply::ConfirmThenClose => {
                        let confirmation = push_confirmation(b"subscribe", command.get(1), 1);
                        // Dropping the socket right after the confirmation closes the
                        // subscription while the client keeps waiting for a message.
                        let _ = socket.write_all(&confirmation).await;
                        return;
                    }
                }
            } else if name.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
                let confirmation = push_confirmation(b"unsubscribe", command.get(1), 0);
                if socket.write_all(&confirmation).await.is_err() {
                    return;
                }
            } else if name.eq_ignore_ascii_case(b"PUBLISH") {
                if socket.write_all(b":1\r\n").await.is_err() {
                    return;
                }
            } else if socket.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        }
    }
}

/// Builds a RESP2 push confirmation such as `*3 $9 subscribe $<len> <channel> :<count>`.
fn push_confirmation(kind: &[u8], channel: Option<&Vec<u8>>, count: usize) -> Vec<u8> {
    let channel = channel.cloned().unwrap_or_default();
    let mut confirmation = Vec::new();
    confirmation.extend_from_slice(b"*3\r\n$");
    confirmation.extend_from_slice(kind.len().to_string().as_bytes());
    confirmation.extend_from_slice(b"\r\n");
    confirmation.extend_from_slice(kind);
    confirmation.extend_from_slice(b"\r\n$");
    confirmation.extend_from_slice(channel.len().to_string().as_bytes());
    confirmation.extend_from_slice(b"\r\n");
    confirmation.extend_from_slice(&channel);
    confirmation.extend_from_slice(b"\r\n:");
    confirmation.extend_from_slice(count.to_string().as_bytes());
    confirmation.extend_from_slice(b"\r\n");
    confirmation
}

/// Parses one RESP array of bulk strings, returning its arguments and byte count.
fn parse_command(buffer: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    if buffer.first() != Some(&b'*') {
        return None;
    }
    let mut offset = 1;
    let (count, used) = parse_number_line(buffer.get(offset..)?)?;
    offset += used;
    let mut arguments = Vec::with_capacity(count);
    for _ in 0..count {
        if buffer.get(offset) != Some(&b'$') {
            return None;
        }
        offset += 1;
        let (length, used) = parse_number_line(buffer.get(offset..)?)?;
        offset += used;
        let end = offset.checked_add(length)?;
        arguments.push(buffer.get(offset..end)?.to_vec());
        if buffer.get(end) != Some(&b'\r') || buffer.get(end + 1) != Some(&b'\n') {
            return None;
        }
        offset = end + 2;
    }
    Some((arguments, offset))
}

/// Parses `<number>\r\n`, returning the number and the consumed byte count.
fn parse_number_line(buffer: &[u8]) -> Option<(usize, usize)> {
    let end = buffer.windows(2).position(|window| window == b"\r\n")?;
    let number = std::str::from_utf8(buffer.get(..end)?).ok()?.parse().ok()?;
    Some((number, end + 2))
}

fn io_error(context: &'static str, error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, context).with_details(error.to_string())
}
