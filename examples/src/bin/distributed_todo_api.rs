//! HTTP API process for the distributed Todo reference application.

use std::{env, net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use catga_axum::{CatgaHttpError, CatgaHttpResult};
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DistributedIdGenerator, Envelope, ErrorCode, MessageMetadata,
    PayloadEncoder, SnowflakeIdGenerator, SnowflakeLayout,
};
use catga_examples::distributed_todo::{CreateTodo, TodoProjection, TodoView};
use catga_nats::{NatsProjectionConfig, NatsProjectionRunner, NatsPublisher, NatsPublisherConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const DEFAULT_COMMAND_STREAM: &str = "TODO_COMMANDS";
const DEFAULT_COMMAND_SUBJECT: &str = "todo.commands";
const DEFAULT_EVENT_STREAM: &str = "TODO_EVENTS";
const DEFAULT_EVENT_PREFIX: &str = "todo.events";
const DEFAULT_CHECKPOINT_BUCKET: &str = "TODO_PROJECTION_CHECKPOINTS";
const DEFAULT_API_ADDR: &str = "127.0.0.1:3000";
const DEFAULT_API_ID_WORKER: u32 = 1;

struct TodoApi {
    publisher: NatsPublisher,
    projection: NatsProjectionRunner<TodoProjection>,
    ids: SnowflakeIdGenerator,
    projection_run: Mutex<()>,
}

struct TodoApiConfig {
    nats_url: Box<str>,
    command_stream: Box<str>,
    command_subject: Box<str>,
    event_stream: Box<str>,
    event_prefix: Box<str>,
    checkpoint_bucket: Box<str>,
    address: SocketAddr,
    id_worker: u32,
}

impl TodoApiConfig {
    fn from_environment() -> CatgaResult<Self> {
        Ok(Self {
            nats_url: environment_or("CATGA_NATS_URL", DEFAULT_NATS_URL),
            command_stream: environment_or("CATGA_TODO_COMMAND_STREAM", DEFAULT_COMMAND_STREAM),
            command_subject: environment_or("CATGA_TODO_COMMAND_SUBJECT", DEFAULT_COMMAND_SUBJECT),
            event_stream: environment_or("CATGA_TODO_EVENT_STREAM", DEFAULT_EVENT_STREAM),
            event_prefix: environment_or("CATGA_TODO_EVENT_PREFIX", DEFAULT_EVENT_PREFIX),
            checkpoint_bucket: environment_or(
                "CATGA_TODO_CHECKPOINT_BUCKET",
                DEFAULT_CHECKPOINT_BUCKET,
            ),
            address: environment_or("CATGA_TODO_API_ADDR", DEFAULT_API_ADDR)
                .parse::<SocketAddr>()
                .map_err(|error| {
                    CatgaError::new(ErrorCode::Validation, "invalid CATGA_TODO_API_ADDR")
                        .with_details(error.to_string())
                })?,
            id_worker: environment_u32("CATGA_TODO_API_ID_WORKER", DEFAULT_API_ID_WORKER)?,
        })
    }
}

impl TodoApi {
    async fn connect(config: TodoApiConfig) -> CatgaResult<Self> {
        let publisher = NatsPublisher::connect(NatsPublisherConfig {
            server: config.nats_url.clone(),
            stream: config.command_stream,
            subject: config.command_subject,
        })
        .await?;
        let projection = NatsProjectionRunner::connect(
            config.nats_url.as_ref(),
            NatsProjectionConfig {
                event_stream: config.event_stream,
                event_subject_prefix: config.event_prefix,
                checkpoint_bucket: config.checkpoint_bucket,
            },
            TodoProjection::default(),
        )
        .await?;
        // The sample keeps its read model in memory, so rebuild it from durable events whenever
        // the API process starts. The projection itself is idempotent for checkpoint retries.
        projection.rebuild().await?;
        Ok(Self {
            publisher,
            projection,
            ids: SnowflakeIdGenerator::new(config.id_worker, SnowflakeLayout::default())?,
            projection_run: Mutex::new(()),
        })
    }

    async fn create(&self, title: Box<str>) -> CatgaResult<AcceptedTodo> {
        let id = self.ids.next_id()?;
        let command = CreateTodo {
            id: id.to_string().into(),
            title,
        };
        command.validate()?;
        let payload = MemoryPackCodec::default().encode_payload(&command)?;
        self.publisher
            .publish(Envelope::new(
                id,
                "todo.create",
                payload,
                MessageMetadata::new(id, Some(id)),
            ))
            .await?;
        Ok(AcceptedTodo {
            id: command.id,
            title: command.title,
        })
    }

    async fn todos(&self) -> CatgaResult<Vec<TodoView>> {
        let _single_projection_run = self.projection_run.lock().await;
        self.projection.run().await?;
        Ok(self.projection.projection().todos().await)
    }
}

#[derive(Deserialize)]
struct NewTodo {
    title: Box<str>,
}

#[derive(Serialize)]
struct AcceptedTodo {
    id: Box<str>,
    title: Box<str>,
}

async fn post_todo(
    State(api): State<Arc<TodoApi>>,
    Json(input): Json<NewTodo>,
) -> CatgaHttpResult<(StatusCode, Json<AcceptedTodo>)> {
    api.create(input.title)
        .await
        .map(|todo| (StatusCode::ACCEPTED, Json(todo)))
        .map_err(CatgaHttpError::from)
}

async fn get_todos(State(api): State<Arc<TodoApi>>) -> CatgaHttpResult<Json<Vec<TodoView>>> {
    api.todos().await.map(Json).map_err(CatgaHttpError::from)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let config = TodoApiConfig::from_environment()?;
    let address = config.address;
    let api = Arc::new(TodoApi::connect(config).await?);
    let app = Router::new()
        .route("/todos", post(post_todo).get(get_todos))
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .with_state(api);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| {
            CatgaError::new(ErrorCode::Unavailable, "bind Todo API listener")
                .with_details(error.to_string())
        })?;

    println!("distributed Todo API listening on http://{address}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|error| {
            CatgaError::new(ErrorCode::Unavailable, "serve Todo API")
                .with_details(error.to_string())
        })
}

fn environment_or(name: &str, default: &str) -> Box<str> {
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .into_boxed_str()
}

fn environment_u32(name: &str, default: u32) -> CatgaResult<u32> {
    match env::var(name) {
        Ok(value) => value.parse::<u32>().map_err(|error| {
            CatgaError::new(ErrorCode::Validation, format!("invalid {name}"))
                .with_details(error.to_string())
        }),
        Err(_) => Ok(default),
    }
}
