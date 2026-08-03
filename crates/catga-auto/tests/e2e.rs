//! End-to-end test: HTTP → command bus → worker → event bus → read model, all over real NATS.
//!
//! This exercises the full CQRS write/read separation through two [`Bus`] instances and an Axum
//! HTTP front end:
//!
//! ```text
//! POST /todos ─▶ command stream ─▶ worker Bus ─▶ event stream ─▶ projection Bus ─▶ read model
//! GET  /todos ─────────────────────────────────────────────────────────────────▶ read model
//! ```
//!
//! `#[ignore]`d by default; provide `CATGA_NATS_URL` and run with
//! `cargo test -p catga-auto --test e2e -- --ignored`.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use catga_auto::Bus;
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{
    CatgaResult, DistributedIdGenerator, Event, Message, SnowflakeIdGenerator, SnowflakeLayout,
    TypedDeliveryHandler, TypedTransport,
};
use catga_nats::{NatsConfig, NatsTransport};
use serde::{Deserialize, Serialize};

type Events = TypedTransport<NatsTransport, MemoryPackCodec>;
type Commands = TypedTransport<NatsTransport, MemoryPackCodec>;
type ReadModel = Arc<RwLock<BTreeMap<u64, TodoView>>>;

// --- Messages -------------------------------------------------------------

#[derive(Clone, MemoryPackable)]
struct CreateTodo {
    id: u64,
    title: String,
}
impl Message for CreateTodo {}

#[derive(Clone, MemoryPackable)]
struct TodoEvent {
    id: u64,
    title: String,
}
impl Message for TodoEvent {}
impl Event for TodoEvent { type TypeId = catga_core::DefaultMessageTypeId; }

#[derive(Clone, Serialize, Deserialize)]
struct TodoView {
    id: u64,
    title: String,
}

#[derive(Deserialize)]
struct CreateTodoInput {
    title: String,
}

// --- Worker: command -> event ---------------------------------------------

struct CommandProcessor {
    events: Events,
}

#[async_trait]
impl TypedDeliveryHandler<CreateTodo> for CommandProcessor {
    async fn handle(&self, command: &CreateTodo) -> CatgaResult<()> {
        self.events
            .publish_reliable_event(&TodoEvent {
                id: command.id,
                title: command.title.clone(),
            })
            .await
    }
}

// --- Projection: event -> read model --------------------------------------

struct Projection {
    model: ReadModel,
}

#[async_trait]
impl TypedDeliveryHandler<TodoEvent> for Projection {
    async fn handle(&self, event: &TodoEvent) -> CatgaResult<()> {
        self.model.write().expect("read model not poisoned").insert(
            event.id,
            TodoView {
                id: event.id,
                title: event.title.clone(),
            },
        );
        Ok(())
    }
}

// --- HTTP front end -------------------------------------------------------

#[derive(Clone)]
struct AppState {
    commands: Commands,
    model: ReadModel,
    ids: Arc<SnowflakeIdGenerator>,
}

async fn create_todo(
    State(state): State<AppState>,
    Json(input): Json<CreateTodoInput>,
) -> Result<StatusCode, StatusCode> {
    let id = state
        .ids
        .next_id()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .commands
        .publish(&CreateTodo {
            id,
            title: input.title,
        })
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(StatusCode::ACCEPTED)
}

async fn list_todos(State(state): State<AppState>) -> Json<Vec<TodoView>> {
    let model = state.model.read().expect("read model not poisoned");
    Json(model.values().cloned().collect())
}

// --- Test -----------------------------------------------------------------

fn nats_url() -> Option<String> {
    std::env::var("CATGA_NATS_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
}

fn unique_stream(prefix: &str) -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("{prefix}_{millis}_{seq}")
}

async fn connect(url: &str, stream: &str, subject: &str, consumer: &str) -> Arc<NatsTransport> {
    Arc::new(
        NatsTransport::connect(NatsConfig {
            server: url.to_string().into(),
            stream: stream.to_string().into(),
            subject: subject.to_string().into(),
            consumer: consumer.to_string().into(),
        })
        .await
        .expect("connect to NATS"),
    )
}

fn ids(worker_id: u32) -> Arc<SnowflakeIdGenerator> {
    Arc::new(SnowflakeIdGenerator::new(worker_id, SnowflakeLayout::default()).expect("ids"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CATGA_NATS_URL pointing at a JetStream server"]
async fn e2e_http_command_to_read_model_over_nats() {
    let Some(url) = nats_url() else { return };

    let commands_stream = unique_stream("E2E_CMDS");
    let events_stream = unique_stream("E2E_EVENTS");

    // Command side: HTTP publishes here; the worker Bus consumes.
    let command_transport = connect(
        &url,
        &commands_stream,
        &format!("{commands_stream}.cmds"),
        "e2e-worker",
    )
    .await;
    // Event side: the worker publishes here; the projection Bus consumes.
    let event_transport = connect(
        &url,
        &events_stream,
        &format!("{events_stream}.events"),
        "e2e-projection",
    )
    .await;

    let read_model: ReadModel = Arc::new(RwLock::new(BTreeMap::new()));

    let worker_bus = Bus::builder(command_transport.clone())
        .endpoint(
            "commands",
            Arc::new(CommandProcessor {
                events: TypedTransport::<NatsTransport, MemoryPackCodec>::new(
                    event_transport.clone(),
                    ids(2),
                ),
            }),
            Arc::new(MemoryPackCodec::default()),
            4,
        )
        .expect("worker endpoint")
        .build();

    let projection_bus = Bus::builder(event_transport.clone())
        .endpoint(
            "events",
            Arc::new(Projection {
                model: read_model.clone(),
            }),
            Arc::new(MemoryPackCodec::default()),
            4,
        )
        .expect("projection endpoint")
        .build();

    // HTTP front end on an ephemeral port.
    let state = AppState {
        commands: TypedTransport::<NatsTransport, MemoryPackCodec>::new(command_transport, ids(1)),
        model: read_model.clone(),
        ids: ids(1),
    };
    let app = Router::new()
        .route("/todos", post(create_todo).get(list_todos))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    const TOTAL: usize = 3;
    let driver = {
        let read_model = read_model.clone();
        let worker_token = worker_bus.shutdown_token();
        let projection_token = projection_bus.shutdown_token();
        async move {
            let client = reqwest::Client::new();
            for i in 0..TOTAL {
                let response = client
                    .post(format!("http://{addr}/todos"))
                    .json(&serde_json::json!({ "title": format!("todo-{i}") }))
                    .send()
                    .await
                    .expect("post");
                assert_eq!(response.status(), StatusCode::ACCEPTED);
            }
            // The write path is asynchronous; poll the read model until projection catches up.
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let response = client
                    .get(format!("http://{addr}/todos"))
                    .send()
                    .await
                    .expect("get");
                let todos: Vec<TodoView> = response.json().await.expect("decode");
                if todos.len() >= TOTAL {
                    assert_eq!(todos.len(), TOTAL);
                    break;
                }
                assert!(Instant::now() < deadline, "read model did not converge");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = read_model;
            worker_token.cancel();
            projection_token.cancel();
        }
    };

    let (worker_runs, projection_runs, ()) = tokio::join!(
        worker_bus.run_until_cancelled(),
        projection_bus.run_until_cancelled(),
        driver
    );
    assert_eq!(worker_runs.expect("worker runs")[0].acknowledged(), TOTAL);
    assert_eq!(
        projection_runs.expect("projection runs")[0].acknowledged(),
        TOTAL
    );
    assert_eq!(read_model.read().expect("read model").len(), TOTAL);
}
