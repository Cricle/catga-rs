//! `claim_due` partial-claim contract under cursor compare-and-set exhaustion.
//!
//! The scheduler commits each claim with an individual KV compare-and-set and advances the
//! shared scan cursor with another. When the cursor update exhausts its bounded retry budget
//! mid-scan, claims committed earlier in the same call are already durable: the call must
//! return them (partial batch) instead of reporting an error that would silently orphan
//! leased work. These tests drive the real `NatsFlowScheduler` against an in-process fake
//! JetStream server that deterministically rejects the cursor CAS after a chosen number of
//! successful writes, so the exhaustion point is exact and the tests never need a broker.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use catga_core::flow::{DueFlowScheduler, FlowScheduler};
use catga_core::{CatgaResult, ErrorCode};
use catga_nats::NatsFlowScheduler;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const SERVER_INFO: &str = "INFO {\"server_id\":\"fake-js\",\"server_name\":\"fake-js\",\"version\":\"2.11.0\",\"proto\":1,\"max_payload\":1048576,\"headers\":true}\r\n";
const MESSAGE_TIMESTAMP: &str = "2025-01-01T00:00:00Z";
const TEST_TIMEOUT: Duration = Duration::from_secs(20);
const WRONG_LAST_SEQUENCE: u64 = 10071;

static BUCKET_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// Returns a process-unique bucket name so concurrent test binaries never collide.
fn unique_bucket() -> String {
    let sequence = BUCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("CAS_CONTRACT_{}_{}", std::process::id(), sequence)
}

/// One stored KV message: the stream sequence doubles as the KV revision.
struct StoredMessage {
    sequence: u64,
    headers: Vec<(String, String)>,
    payload: Vec<u8>,
}

/// A fake stream: subjects it owns plus the last message published per subject.
struct StreamState {
    subjects: Vec<String>,
    next_sequence: u64,
    messages: HashMap<String, StoredMessage>,
}

impl StreamState {
    fn new(subjects: Vec<String>) -> Self {
        Self {
            subjects,
            next_sequence: 1,
            messages: HashMap::new(),
        }
    }

    fn owns(&self, subject: &str) -> bool {
        self.subjects
            .iter()
            .any(|pattern| subject_matches(pattern, subject))
    }
}

/// How the fake rejects compare-and-set writes to the armed subject.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GateMode {
    /// Allow exactly one more successful write, then reject every later write.
    AfterNextWrite,
    /// Reject every write.
    Always,
}

/// Deterministic contention injector for one subject's compare-and-set writes.
struct CasGate {
    subject: String,
    sequence_at_arm: u64,
    mode: GateMode,
}

impl CasGate {
    fn rejects(&self, current_sequence: u64) -> bool {
        match self.mode {
            GateMode::AfterNextWrite => current_sequence > self.sequence_at_arm,
            GateMode::Always => true,
        }
    }
}

/// One client subscription registered with the fake server.
struct Subscription {
    pattern: String,
    sid: u64,
    connection: u64,
    frames: mpsc::UnboundedSender<ServerFrame>,
}

#[derive(Default)]
struct FakeState {
    subscriptions: Vec<Subscription>,
    streams: HashMap<String, StreamState>,
    gate: Option<CasGate>,
}

impl FakeState {
    /// Delivers one server-originated message to every matching subscription.
    fn deliver(
        &mut self,
        subject: &str,
        status: Option<&'static str>,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) {
        self.subscriptions.retain(|entry| {
            if !subject_matches(&entry.pattern, subject) {
                return true;
            }
            entry
                .frames
                .send(ServerFrame::Message {
                    subject: subject.to_owned(),
                    sid: entry.sid,
                    status,
                    headers: headers.clone(),
                    payload: payload.clone(),
                })
                .is_ok()
        });
    }

    fn stream_info_json(&self, name: &str) -> Option<String> {
        let stream = self.streams.get(name)?;
        let subjects = stream
            .subjects
            .iter()
            .map(|subject| format!("\"{subject}\""))
            .collect::<Vec<_>>()
            .join(",");
        Some(format!(
            "{{\"config\":{{\"name\":\"{name}\",\"subjects\":[{subjects}],\"retention\":\"limits\",\"max_consumers\":-1,\"max_msgs\":-1,\"max_bytes\":-1,\"max_age\":0,\"max_msgs_per_subject\":1,\"max_msg_size\":-1,\"discard\":\"new\",\"storage\":\"memory\",\"num_replicas\":1,\"duplicate_window\":0,\"allow_rollup_hdrs\":true,\"deny_delete\":true,\"deny_purge\":false,\"allow_direct\":true}},\"created\":\"{MESSAGE_TIMESTAMP}\",\"state\":{{\"messages\":0,\"bytes\":0,\"first_seq\":0,\"first_ts\":\"{MESSAGE_TIMESTAMP}\",\"last_seq\":0,\"last_ts\":\"{MESSAGE_TIMESTAMP}\",\"consumer_count\":0}}}}"
        ))
    }
}

/// One frame the fake server pushes to a client connection.
enum ServerFrame {
    Raw(&'static [u8]),
    Message {
        subject: String,
        sid: u64,
        status: Option<&'static str>,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    },
}

/// A hermetic JetStream stand-in serving the KV slice the scheduler relies on.
pub struct FakeJetStream {
    address: std::net::SocketAddr,
    state: Arc<Mutex<FakeState>>,
    acceptor: JoinHandle<()>,
}

impl FakeJetStream {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake JetStream server");
        let address = listener.local_addr().expect("fake JetStream address");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let acceptor = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let mut next_connection = 0_u64;
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

    fn url(&self) -> String {
        format!("nats://{}", self.address)
    }

    fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().expect("fake JetStream state poisoned")
    }

    /// Arms the gate on the index cursor of `index_bucket`: the next cursor CAS succeeds,
    /// every later one fails with `wrong last sequence`, exhausting the retry budget on cue.
    fn fail_cursor_cas_after_next_write(&self, index_bucket: &str) {
        let subject = format!("$KV.{index_bucket}.m");
        let mut state = self.state();
        let sequence = state
            .streams
            .get(&format!("KV_{index_bucket}"))
            .and_then(|stream| stream.messages.get(&subject))
            .map_or(0, |message| message.sequence);
        state.gate = Some(CasGate {
            subject,
            sequence_at_arm: sequence,
            mode: GateMode::AfterNextWrite,
        });
    }

    /// Arms the gate so every cursor CAS fails immediately.
    fn fail_cursor_cas_always(&self, index_bucket: &str) {
        let mut state = self.state();
        state.gate = Some(CasGate {
            subject: format!("$KV.{index_bucket}.m"),
            sequence_at_arm: 0,
            mode: GateMode::Always,
        });
    }

    /// Disarms any gate, restoring normal compare-and-set behavior.
    fn clear_gate(&self) {
        self.state().gate = None;
    }
}

impl Drop for FakeJetStream {
    fn drop(&mut self) {
        self.acceptor.abort();
    }
}

/// Speaks the NATS text protocol with one client until the connection drops.
async fn run_connection(socket: TcpStream, connection: u64, state: Arc<Mutex<FakeState>>) {
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
                    status,
                    headers,
                    payload,
                } => {
                    if status.is_none() && headers.is_empty() {
                        let mut frame =
                            format!("MSG {subject} {sid} {}\r\n", payload.len()).into_bytes();
                        frame.extend_from_slice(&payload);
                        frame.extend_from_slice(b"\r\n");
                        frame
                    } else {
                        let mut block = String::from("NATS/1.0");
                        if let Some(status) = status {
                            block.push(' ');
                            block.push_str(status);
                        }
                        block.push_str("\r\n");
                        for (name, value) in &headers {
                            block.push_str(name);
                            block.push_str(": ");
                            block.push_str(value);
                            block.push_str("\r\n");
                        }
                        block.push_str("\r\n");
                        let header_bytes = block.into_bytes();
                        let mut frame = format!(
                            "HMSG {subject} {sid} {} {}\r\n",
                            header_bytes.len(),
                            header_bytes.len() + payload.len()
                        )
                        .into_bytes();
                        frame.extend_from_slice(&header_bytes);
                        frame.extend_from_slice(&payload);
                        frame.extend_from_slice(b"\r\n");
                        frame
                    }
                }
            };
            if writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let header = String::from_utf8_lossy(&line);
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
                let sid = match (tokens.next(), tokens.next()) {
                    (Some(_queue), Some(sid)) => sid,
                    (Some(sid), None) => sid,
                    _ => continue,
                };
                let Ok(sid) = sid.parse::<u64>() else {
                    continue;
                };
                state
                    .lock()
                    .expect("fake JetStream state poisoned")
                    .subscriptions
                    .push(Subscription {
                        pattern: subject,
                        sid,
                        connection,
                        frames: frames.clone(),
                    });
            }
            "UNSUB" => {
                if let Some(sid) = tokens.next().and_then(|sid| sid.parse::<u64>().ok()) {
                    state
                        .lock()
                        .expect("fake JetStream state poisoned")
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
                let mut payload = vec![0_u8; size + 2];
                if reader.read_exact(&mut payload).await.is_err() {
                    break;
                }
                payload.truncate(size);
                handle_message(&state, &subject, reply.as_deref(), Vec::new(), payload);
            }
            "HPUB" => {
                let Some(subject) = tokens.next().map(str::to_owned) else {
                    continue;
                };
                let (reply, header_size, total_size) =
                    match (tokens.next(), tokens.next(), tokens.next()) {
                        (Some(reply), Some(header_size), Some(total_size)) => {
                            (Some(reply.to_owned()), header_size, total_size)
                        }
                        (Some(header_size), Some(total_size), None) => {
                            (None, header_size, total_size)
                        }
                        _ => continue,
                    };
                let (Ok(header_size), Ok(total_size)) =
                    (header_size.parse::<usize>(), total_size.parse::<usize>())
                else {
                    continue;
                };
                let mut payload = vec![0_u8; total_size + 2];
                if reader.read_exact(&mut payload).await.is_err() {
                    break;
                }
                payload.truncate(total_size);
                if header_size > payload.len() {
                    continue;
                }
                let headers = parse_headers(&payload[..header_size]);
                let body = payload.split_off(header_size);
                handle_message(&state, &subject, reply.as_deref(), headers, body);
            }
            _ => {}
        }
    }

    state
        .lock()
        .expect("fake JetStream state poisoned")
        .subscriptions
        .retain(|entry| entry.connection != connection);
    writer_task.abort();
}

/// Parses a `NATS/1.0` header block into name/value pairs.
fn parse_headers(block: &[u8]) -> Vec<(String, String)> {
    let block = String::from_utf8_lossy(block);
    block
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_owned(), value.trim().to_owned()))
        })
        .collect()
}

/// Routes one inbound publish, answering JetStream API and KV write traffic inline.
fn handle_message(
    state: &Arc<Mutex<FakeState>>,
    subject: &str,
    reply: Option<&str>,
    headers: Vec<(String, String)>,
    payload: Vec<u8>,
) {
    let mut state = state.lock().expect("fake JetStream state poisoned");
    if let Some(name) = subject.strip_prefix("$JS.API.STREAM.INFO.") {
        let Some(reply) = reply else { return };
        let body = match state.stream_info_json(name) {
            Some(info) => info,
            None => "{\"error\":{\"code\":404,\"description\":\"stream not found\"}}".to_owned(),
        };
        state.deliver(reply, None, Vec::new(), body.into_bytes());
        return;
    }
    if let Some(name) = subject.strip_prefix("$JS.API.STREAM.CREATE.") {
        let Some(reply) = reply else { return };
        let parsed: serde_json::Value =
            serde_json::from_slice(&payload).unwrap_or(serde_json::Value::Null);
        let subjects = parsed
            .get("subjects")
            .and_then(serde_json::Value::as_array)
            .map(|subjects| {
                subjects
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let name = parsed
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(name)
            .to_owned();
        state
            .streams
            .entry(name.clone())
            .or_insert_with(|| StreamState::new(subjects));
        let info = state
            .stream_info_json(&name)
            .expect("freshly created stream must have info");
        state.deliver(reply, None, Vec::new(), info.into_bytes());
        return;
    }
    if let Some(rest) = subject.strip_prefix("$JS.API.DIRECT.GET.") {
        let Some(reply) = reply else { return };
        let Some((stream_name, target)) = rest.split_once('.') else {
            return;
        };
        let found = state
            .streams
            .get(stream_name)
            .and_then(|stream| stream.messages.get(target))
            .map(|message| {
                (
                    stream_name.to_owned(),
                    message.sequence,
                    message.headers.clone(),
                    message.payload.clone(),
                )
            });
        match found {
            Some((stream_name, sequence, stored_headers, stored_payload)) => {
                let mut response_headers = vec![
                    ("Nats-Stream".to_owned(), stream_name),
                    ("Nats-Subject".to_owned(), target.to_owned()),
                    ("Nats-Sequence".to_owned(), sequence.to_string()),
                    ("Nats-Time-Stamp".to_owned(), MESSAGE_TIMESTAMP.to_owned()),
                ];
                response_headers.extend(stored_headers);
                state.deliver(reply, None, response_headers, stored_payload);
            }
            None => {
                state.deliver(reply, Some("404 No Messages"), Vec::new(), Vec::new());
            }
        }
        return;
    }
    if subject.starts_with("$KV.") {
        let Some(reply) = reply else { return };
        let body = kv_write(&mut state, subject, &headers, payload);
        state.deliver(reply, None, Vec::new(), body.into_bytes());
        return;
    }
    state.deliver(subject, None, headers, payload);
}

/// Applies one KV write with expected-revision semantics, honoring the armed CAS gate.
fn kv_write(
    state: &mut FakeState,
    subject: &str,
    headers: &[(String, String)],
    payload: Vec<u8>,
) -> String {
    let Some((bucket, _)) = subject
        .strip_prefix("$KV.")
        .and_then(|rest| rest.split_once('.'))
    else {
        return "{\"error\":{\"code\":400,\"description\":\"invalid KV subject\"}}".to_owned();
    };
    let stream_name = format!("KV_{bucket}");
    let expected = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("Nats-Expected-Last-Subject-Sequence"))
        .and_then(|(_, value)| value.parse::<u64>().ok());
    let stored_headers = headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("Nats-Expected-Last-Subject-Sequence"))
        .cloned()
        .collect::<Vec<_>>();
    let Some(stream) = state.streams.get_mut(&stream_name) else {
        return "{\"error\":{\"code\":404,\"description\":\"stream not found\"}}".to_owned();
    };
    if !stream.owns(subject) {
        return "{\"error\":{\"code\":404,\"description\":\"no stream matches subject\"}}"
            .to_owned();
    }
    let current = stream
        .messages
        .get(subject)
        .map_or(0, |message| message.sequence);
    if state
        .gate
        .as_ref()
        .is_some_and(|gate| gate.subject == subject && gate.rejects(current))
    {
        return wrong_last_sequence(current);
    }
    if let Some(expected) = expected {
        let matches = if expected == 0 {
            !stream.messages.contains_key(subject)
        } else {
            current == expected
        };
        if !matches {
            return wrong_last_sequence(current);
        }
    }
    let sequence = stream.next_sequence;
    stream.next_sequence += 1;
    stream.messages.insert(
        subject.to_owned(),
        StoredMessage {
            sequence,
            headers: stored_headers,
            payload,
        },
    );
    format!("{{\"stream\":\"{stream_name}\",\"seq\":{sequence}}}")
}

/// Builds the JetStream publish error a contested compare-and-set produces.
fn wrong_last_sequence(current: u64) -> String {
    format!(
        "{{\"error\":{{\"code\":400,\"err_code\":{WRONG_LAST_SEQUENCE},\"description\":\"wrong last sequence: {current}\"}}}}"
    )
}

/// Matches a NATS subscription pattern against a subject token by token.
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

/// Connects a scheduler to the fake server with two due resumes already scheduled.
async fn scheduler_with_two_due(
    fake: &FakeJetStream,
    bucket: &str,
) -> (NatsFlowScheduler, Box<str>, Box<str>) {
    let scheduler = NatsFlowScheduler::connect(&fake.url(), bucket)
        .await
        .expect("connect scheduler to fake JetStream");
    let due = SystemTime::now()
        .checked_sub(Duration::from_secs(1))
        .expect("due time in the past");
    let first = scheduler
        .schedule_resume("flow-a", "state-a", due)
        .await
        .expect("schedule first resume");
    let second = scheduler
        .schedule_resume("flow-b", "state-b", due)
        .await
        .expect("schedule second resume");
    (scheduler, first, second)
}

#[tokio::test]
async fn claim_due_returns_committed_claims_when_cursor_cas_exhausts() -> CatgaResult<()> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let fake = FakeJetStream::start().await;
        let bucket = unique_bucket();
        let index_bucket = format!("{bucket}_IDX");
        let (scheduler, first, second) = scheduler_with_two_due(&fake, &bucket).await;

        // The first cursor advance succeeds and claims one record; every later cursor CAS in
        // this call is rejected, exhausting the bounded retry loop mid-scan.
        fake.fail_cursor_cas_after_next_write(&index_bucket);
        let claimed = scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(60), 4)
            .await?;
        assert_eq!(
            claimed.len(),
            1,
            "a cursor CAS failure must not discard the claim committed before it"
        );
        assert_eq!(claimed[0].schedule_id(), first.as_ref());

        // The partial claim is fully usable: the owner can acknowledge it for real.
        assert!(scheduler.ack_due("worker", &first).await?);

        // With contention gone, the remaining record is claimed on the next poll and the
        // acknowledged one never resurfaces.
        fake.clear_gate();
        let rest = scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(60), 4)
            .await?;
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].schedule_id(), second.as_ref());
        assert!(scheduler.ack_due("worker", &second).await?);
        Ok(())
    })
    .await
    .expect("claim_due CAS-exhaustion test timed out")
}

#[tokio::test]
async fn claim_due_reports_error_only_when_nothing_was_committed() -> CatgaResult<()> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let fake = FakeJetStream::start().await;
        let bucket = unique_bucket();
        let index_bucket = format!("{bucket}_IDX");
        let (scheduler, first, second) = scheduler_with_two_due(&fake, &bucket).await;

        // The very first cursor advance of the call exhausts its retries: no claim could be
        // committed, so the transient failure must surface to the caller.
        fake.fail_cursor_cas_always(&index_bucket);
        let outcome = scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(60), 4)
            .await;
        assert!(
            matches!(outcome, Err(ref error) if error.code() == ErrorCode::Transient),
            "exhaustion before any committed claim must report a transient error"
        );

        // Nothing was claimed behind the caller's back: both records are still claimable.
        fake.clear_gate();
        let mut claimed = scheduler
            .claim_due("worker", SystemTime::now(), Duration::from_secs(60), 4)
            .await?;
        claimed.sort_by(|left, right| left.schedule_id().cmp(right.schedule_id()));
        let mut expected = vec![first, second];
        expected.sort();
        let actual = claimed
            .iter()
            .map(|resume| Box::<str>::from(resume.schedule_id()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        Ok(())
    })
    .await
    .expect("claim_due pre-claim exhaustion test timed out")
}
