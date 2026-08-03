//! Shared event and read-model types for the distributed Todo example.

use std::sync::Mutex;

use async_trait::async_trait;
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{
    CatgaError, CatgaResult, Command, DistributedIdGenerator, Envelope, ErrorCode, Event,
    EventStore, Message, MessageMetadata, PayloadDecoder, PayloadEncoder, Projection,
    SnowflakeIdGenerator, StoredEvent, TypedDeliveryHandler, current_transport_context,
};
use serde::{Deserialize, Serialize};

/// A durable command requesting that a worker creates one Todo item.
#[derive(Clone, Deserialize, MemoryPackable)]
pub struct CreateTodo {
    /// Stable Todo identifier allocated by the API.
    pub id: Box<str>,
    /// User-visible task title.
    pub title: Box<str>,
}

impl Message for CreateTodo {}
impl Command for CreateTodo { type TypeId = catga_core::DefaultMessageTypeId; }

impl CreateTodo {
    /// Rejects command values that cannot produce a useful Todo item.
    pub fn validate(&self) -> CatgaResult<()> {
        if self.id.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Todo identifier must not be blank",
            ));
        }
        if self.title.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Todo title must not be blank",
            ));
        }
        Ok(())
    }

    /// Validates this command and converts it to the worker's durable event.
    pub fn into_created(self) -> CatgaResult<TodoCreated> {
        self.validate()?;
        Ok(TodoCreated {
            id: self.id,
            title: self.title,
        })
    }
}

/// The durable event emitted by the Todo worker after it accepts a command.
#[derive(Clone, MemoryPackable)]
pub struct TodoCreated {
    /// Stable Todo identifier allocated by the API.
    pub id: Box<str>,
    /// User-visible task title.
    pub title: Box<str>,
}

impl Message for TodoCreated {}
impl Event for TodoCreated { type TypeId = catga_core::DefaultMessageTypeId; }

/// Typed worker service that stores accepted Todo commands as durable events.
pub struct TodoWorker<S: ?Sized> {
    events: std::sync::Arc<S>,
    ids: std::sync::Arc<SnowflakeIdGenerator>,
}

impl<S: ?Sized> TodoWorker<S> {
    /// Creates a worker from application-owned event storage and ID generation.
    pub fn new(events: std::sync::Arc<S>, ids: std::sync::Arc<SnowflakeIdGenerator>) -> Self {
        Self { events, ids }
    }
}

#[async_trait]
impl<S: ?Sized> TypedDeliveryHandler<CreateTodo> for TodoWorker<S>
where
    S: EventStore,
{
    async fn handle(&self, command: &CreateTodo) -> CatgaResult<()> {
        let created = command.clone().into_created()?;
        let event_id = self.ids.next_id()?;
        let payload = MemoryPackCodec::default().encode_payload(&created)?;
        let correlation_id =
            current_transport_context().and_then(|context| context.correlation_id());
        let event = Envelope::new(
            event_id,
            "todo.created",
            payload,
            MessageMetadata::new(event_id, correlation_id),
        );
        match self
            .events
            .append(created.id.as_ref(), vec![event], Some(-1))
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.code() == ErrorCode::Conflict => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// One Todo item returned by the API read model.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TodoView {
    /// Stable Todo identifier.
    pub id: Box<str>,
    /// User-visible task title.
    pub title: Box<str>,
}

/// In-process read model rebuilt from the durable Todo event stream.
#[derive(Default)]
pub struct TodoProjection {
    todos: Mutex<Vec<TodoView>>,
}

impl TodoProjection {
    /// Returns a snapshot of the Todo read model in event order.
    pub async fn todos(&self) -> Vec<TodoView> {
        self.todos
            .lock()
            .map(|todos| todos.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl Projection for TodoProjection {
    fn name(&self) -> &str {
        "distributed-todo"
    }

    async fn apply(&self, event: &StoredEvent) -> CatgaResult<()> {
        if event.envelope().message_type() != "todo.created" {
            return Ok(());
        }
        let created: TodoCreated =
            MemoryPackCodec::default().decode_payload(event.envelope().payload())?;
        let mut todos = self.todos.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "distributed Todo read model lock is poisoned",
            )
        })?;
        if let Some(existing) = todos.iter_mut().find(|todo| todo.id == created.id) {
            existing.title = created.title;
        } else {
            todos.push(TodoView {
                id: created.id,
                title: created.title,
            });
        }
        Ok(())
    }

    async fn reset(&self) -> CatgaResult<()> {
        let mut todos = self.todos.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "distributed Todo read model lock is poisoned",
            )
        })?;
        todos.clear();
        Ok(())
    }
}
