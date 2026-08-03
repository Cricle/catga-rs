//! Worker process for the distributed Todo reference application.

use std::{env, sync::Arc};

use catga_core::auto::Bus;
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{CatgaError, CatgaResult, ErrorCode, SnowflakeIdGenerator, SnowflakeLayout};
use catga_examples::distributed_todo::{CreateTodo, TodoWorker};
use catga_nats::{NatsConfig, NatsEventStore, NatsTransport};

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const DEFAULT_COMMAND_STREAM: &str = "TODO_COMMANDS";
const DEFAULT_COMMAND_SUBJECT: &str = "todo.commands";
const DEFAULT_COMMAND_CONSUMER: &str = "todo-worker";
const DEFAULT_EVENT_STREAM: &str = "TODO_EVENTS";
const DEFAULT_EVENT_PREFIX: &str = "todo.events";
const DEFAULT_WORKER_ID: u32 = 2;
const DEFAULT_CONCURRENCY: usize = 16;

struct TodoWorkerConfig {
    nats_url: Box<str>,
    command_stream: Box<str>,
    command_subject: Box<str>,
    command_consumer: Box<str>,
    event_stream: Box<str>,
    event_prefix: Box<str>,
    id_worker: u32,
    concurrency: usize,
}

impl TodoWorkerConfig {
    fn from_environment() -> CatgaResult<Self> {
        Ok(Self {
            nats_url: environment_or("CATGA_NATS_URL", DEFAULT_NATS_URL),
            command_stream: environment_or("CATGA_TODO_COMMAND_STREAM", DEFAULT_COMMAND_STREAM),
            command_subject: environment_or("CATGA_TODO_COMMAND_SUBJECT", DEFAULT_COMMAND_SUBJECT),
            command_consumer: environment_or(
                "CATGA_TODO_COMMAND_CONSUMER",
                DEFAULT_COMMAND_CONSUMER,
            ),
            event_stream: environment_or("CATGA_TODO_EVENT_STREAM", DEFAULT_EVENT_STREAM),
            event_prefix: environment_or("CATGA_TODO_EVENT_PREFIX", DEFAULT_EVENT_PREFIX),
            id_worker: environment_u32("CATGA_TODO_WORKER_ID", DEFAULT_WORKER_ID)?,
            concurrency: environment_usize("CATGA_TODO_WORKER_CONCURRENCY", DEFAULT_CONCURRENCY)?,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let config = TodoWorkerConfig::from_environment()?;
    let events = Arc::new(
        NatsEventStore::connect(
            config.nats_url.as_ref(),
            config.event_stream,
            config.event_prefix,
        )
        .await?,
    );
    let transport = Arc::new(
        NatsTransport::connect(NatsConfig {
            server: config.nats_url,
            stream: config.command_stream,
            subject: config.command_subject,
            consumer: config.command_consumer,
        })
        .await?,
    );
    let worker = Arc::new(TodoWorker::new(
        events,
        Arc::new(SnowflakeIdGenerator::new(
            config.id_worker,
            SnowflakeLayout::default(),
        )?),
    ));
    let bus = Bus::builder(transport)
        .endpoint::<CreateTodo, _, _>(
            "commands",
            worker,
            Arc::new(MemoryPackCodec::default()),
            config.concurrency,
        )?
        .build();

    println!("distributed Todo worker is consuming Todo commands");
    let run = bus.run_until_cancelled();
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            bus.shutdown();
            (&mut run).await?;
        }
    }
    Ok(())
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

fn environment_usize(name: &str, default: usize) -> CatgaResult<usize> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<usize>().map_err(|error| {
            CatgaError::new(ErrorCode::Validation, format!("invalid {name}"))
                .with_details(error.to_string())
        })?,
        Err(_) => default,
    };
    if value == 0 {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "CATGA_TODO_WORKER_CONCURRENCY must be greater than zero",
        ));
    }
    Ok(value)
}
