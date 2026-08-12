//! In-process fake of the RobustMQ mq9 control plane and message plane.
//!
//! The adapter under test talks to RobustMQ through the mq9 SDK, which layers mailbox subjects
//! on the NATS text protocol. This fixture implements the smallest sufficient slice of that
//! protocol (`INFO`/`CONNECT`/`PING`/`PONG`/`SUB`/`UNSUB`/`PUB`/`MSG`) so contract tests drive
//! the real socket, framing, and subscription machinery without a live service. Every wait is
//! condition-based, never sleep-based, so the tests stay deterministic.

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};

const SERVER_INFO: &str = "INFO {\"server_id\":\"fake-mq9\",\"version\":\"1.0.0\",\"proto\":1,\"max_payload\":1048576}\r\n";
const MAILBOX_CREATE_SUBJECT: &str = "$mq9.AI.MAILBOX.CREATE";
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const WAIT_POLL: Duration = Duration::from_millis(5);

/// One frame the broker pushes to a client connection.
enum ServerFrame {
    Raw(&'static [u8]),
    Message {
        subject: String,
        sid: u64,
        payload: Vec<u8>,
    },
}

/// One subscription a client connection registered with the broker.
struct SubscriptionEntry {
    pattern: String,
    queue: Option<String>,
    sid: u64,
    connection: u64,
    frames: mpsc::UnboundedSender<ServerFrame>,
}

#[derive(Default)]
struct BrokerState {
    subscriptions: Vec<SubscriptionEntry>,
    published: Vec<(String, Vec<u8>)>,
    create_error: Option<(String, u32)>,
    created_mailboxes: u64,
}

impl BrokerState {
    /// Delivers one published message to every matching subscription.
    ///
    /// Queue subscriptions share one delivery per (pattern, queue) pair, mirroring NATS queue
    /// group semantics closely enough for the single-member groups used in tests.
    fn deliver(&mut self, subject: &str, payload: &[u8]) {
        let mut served_queues = HashSet::new();
        self.subscriptions.retain(|entry| {
            if !subject_matches(&entry.pattern, subject) {
                return true;
            }
            if let Some(queue) = &entry.queue
                && !served_queues.insert((entry.pattern.clone(), queue.clone()))
            {
                return true;
            }
            entry
                .frames
                .send(ServerFrame::Message {
                    subject: subject.to_owned(),
                    sid: entry.sid,
                    payload: payload.to_vec(),
                })
                .is_ok()
        });
    }
}

/// A hermetic RobustMQ mq9 stand-in listening on an ephemeral localhost port.
pub struct FakeBroker {
    address: SocketAddr,
    state: Arc<Mutex<BrokerState>>,
    acceptor: JoinHandle<()>,
}

impl FakeBroker {
    /// Starts a broker that answers every mailbox creation request successfully.
    pub async fn start() -> Self {
        Self::start_inner().await
    }

    /// Starts a broker that answers mailbox creation with `{"error": ..., "code": ...}`.
    pub async fn start_failing_create(message: &str, code: u32) -> Self {
        let broker = Self::start_inner().await;
        broker.state().create_error = Some((message.to_owned(), code));
        broker
    }

    async fn start_inner() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake broker");
        let address = listener.local_addr().expect("fake broker address");
        let state = Arc::new(Mutex::new(BrokerState::default()));
        let acceptor = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let mut next_connection = 0u64;
                while let Ok((socket, _)) = listener.accept().await {
                    let connection = next_connection;
                    next_connection += 1;
                    tokio::spawn(run_connection(socket, connection, Arc::clone(&state)));
                }
            })
        };
        Self {
            address,
            state,
            acceptor,
        }
    }

    /// The `nats://` URL the adapter connects to.
    pub fn url(&self) -> String {
        format!("nats://{}", self.address)
    }

    /// Blocks until a client registers a subscription for `pattern`.
    pub async fn wait_for_subscription(&self, pattern: &str) {
        self.wait_for(format!("subscription {pattern}"), |state| {
            state
                .subscriptions
                .iter()
                .any(|entry| entry.pattern == pattern)
                .then_some(())
        })
        .await;
    }

    /// Blocks until a client publishes to a subject starting with `prefix`, returning the first
    /// matching `(subject, payload)` pair in arrival order.
    pub async fn wait_for_publish(&self, prefix: &str) -> (String, Vec<u8>) {
        self.wait_for(format!("publish {prefix}*"), |state| {
            state
                .published
                .iter()
                .find(|(subject, _)| subject.starts_with(prefix))
                .cloned()
        })
        .await
    }

    /// Every `(subject, payload)` publish observed by the broker, in arrival order.
    pub fn published(&self) -> Vec<(String, Vec<u8>)> {
        self.state().published.clone()
    }

    fn state(&self) -> MutexGuard<'_, BrokerState> {
        self.state.lock().expect("fake broker state poisoned")
    }

    /// Polls the broker state until `probe` produces a value or the wait deadline expires.
    async fn wait_for<T>(
        &self,
        description: String,
        mut probe: impl FnMut(&BrokerState) -> Option<T>,
    ) -> T {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(found) = probe(&self.state()) {
                return found;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {description}"
            );
            tokio::time::sleep(WAIT_POLL).await;
        }
    }
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.acceptor.abort();
    }
}

/// Speaks the NATS text protocol with one client until the connection drops.
async fn run_connection(socket: TcpStream, connection: u64, state: Arc<Mutex<BrokerState>>) {
    let (reader, mut writer) = socket.into_split();
    if writer.write_all(SERVER_INFO.as_bytes()).await.is_err() {
        return;
    }
    let (frames, mut outgoing) = mpsc::unbounded_channel::<ServerFrame>();
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outgoing.recv().await {
            let bytes = match frame {
                ServerFrame::Raw(bytes) => bytes.to_vec(),
                ServerFrame::Message {
                    subject,
                    sid,
                    payload,
                } => {
                    let mut frame =
                        format!("MSG {subject} {sid} {}\r\n", payload.len()).into_bytes();
                    frame.extend_from_slice(&payload);
                    frame.extend_from_slice(b"\r\n");
                    frame
                }
            };
            if writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let mut reader = BufReader::new(reader);
    let mut header = Vec::new();
    loop {
        header.clear();
        match reader.read_until(b'\n', &mut header).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let header = String::from_utf8_lossy(&header);
        let mut tokens = header.trim_end().split(' ');
        match tokens.next().unwrap_or_default() {
            "CONNECT" | "PONG" => {}
            "PING" => {
                if frames.send(ServerFrame::Raw(b"PONG\r\n")).is_err() {
                    break;
                }
            }
            "SUB" => {
                let Some(subject) = tokens.next().map(str::to_owned) else {
                    continue;
                };
                let (queue, sid) = match (tokens.next(), tokens.next()) {
                    (Some(queue), Some(sid)) => (Some(queue.to_owned()), sid),
                    (Some(sid), None) => (None, sid),
                    _ => continue,
                };
                let Ok(sid) = sid.parse::<u64>() else {
                    continue;
                };
                state
                    .lock()
                    .expect("fake broker state poisoned")
                    .subscriptions
                    .push(SubscriptionEntry {
                        pattern: subject,
                        queue,
                        sid,
                        connection,
                        frames: frames.clone(),
                    });
            }
            "UNSUB" => {
                if let Some(sid) = tokens.next().and_then(|sid| sid.parse::<u64>().ok()) {
                    state
                        .lock()
                        .expect("fake broker state poisoned")
                        .subscriptions
                        .retain(|entry| !(entry.connection == connection && entry.sid == sid));
                }
            }
            "PUB" => {
                let Some(subject) = tokens.next().map(str::to_owned) else {
                    continue;
                };
                let (reply, size) = match (tokens.next(), tokens.next()) {
                    (Some(reply), Some(size)) => (Some(reply.to_owned()), size),
                    (Some(size), None) => (None, size),
                    _ => continue,
                };
                let Ok(size) = size.parse::<usize>() else {
                    continue;
                };
                let mut payload = vec![0u8; size + 2];
                if reader.read_exact(&mut payload).await.is_err() {
                    break;
                }
                payload.truncate(size);
                handle_publish(&state, &subject, reply.as_deref(), &payload);
            }
            _ => {}
        }
    }

    state
        .lock()
        .expect("fake broker state poisoned")
        .subscriptions
        .retain(|entry| entry.connection != connection);
    writer_task.abort();
}

/// Routes one published frame, answering mq9 control-plane requests inline.
fn handle_publish(
    state: &Arc<Mutex<BrokerState>>,
    subject: &str,
    reply: Option<&str>,
    payload: &[u8],
) {
    let mut state = state.lock().expect("fake broker state poisoned");
    state.published.push((subject.to_owned(), payload.to_vec()));
    if subject == MAILBOX_CREATE_SUBJECT {
        let Some(reply) = reply else { return };
        let body = match &state.create_error {
            Some((message, code)) => format!("{{\"error\":\"{message}\",\"code\":{code}}}"),
            None => {
                state.created_mailboxes += 1;
                format!("{{\"mail_id\":\"mailbox-{}\"}}", state.created_mailboxes)
            }
        };
        state.deliver(reply, body.as_bytes());
        return;
    }
    state.deliver(subject, payload);
}

/// Matches a NATS subscription pattern against a published subject.
///
/// Tokens are `.`-separated; `*` matches exactly one token and a trailing `>` matches the rest.
fn subject_matches(pattern: &str, subject: &str) -> bool {
    let mut pattern_tokens = pattern.split('.');
    let mut subject_tokens = subject.split('.');
    loop {
        match (pattern_tokens.next(), subject_tokens.next()) {
            (Some(">"), _) => return true,
            (None, None) => return true,
            (None, Some(_)) | (Some(_), None) => return false,
            (Some(token), Some(subject_token)) => {
                if token != "*" && token != subject_token {
                    return false;
                }
            }
        }
    }
}
