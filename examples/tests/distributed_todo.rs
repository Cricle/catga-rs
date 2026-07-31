//! Contracts for the distributed Todo sample's shared domain types.

use std::sync::Arc;

use catga_core::{
    Envelope, EventStore, MessageMetadata, PayloadDecoder, PayloadEncoder, Projection,
    SnowflakeIdGenerator, SnowflakeLayout, StoredEvent, TypedDeliveryHandler,
};
use catga_examples::distributed_todo::{CreateTodo, TodoCreated, TodoProjection, TodoWorker};

#[test]
fn create_todo_rejects_blank_titles_before_delivery() {
    let command = CreateTodo {
        id: "todo-42".into(),
        title: "   ".into(),
    };

    let error = command.validate().expect_err("blank titles are invalid");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
}

#[tokio::test]
async fn todo_worker_persists_one_event_for_a_redelivered_command() {
    let store = Arc::new(catga_memory::MemoryEventStore::default());
    let worker = TodoWorker::new(
        Arc::clone(&store),
        Arc::new(
            SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("worker id is valid"),
        ),
    );
    let command = CreateTodo {
        id: "todo-42".into(),
        title: "ship the API".into(),
    };

    worker
        .handle(&command)
        .await
        .expect("first delivery persists");
    worker
        .handle(&command)
        .await
        .expect("redelivery recognizes the existing Todo");

    let page = store
        .read_page("todo-42", 0, 10)
        .await
        .expect("worker stream reads");
    assert_eq!(page.stream().events().len(), 1);
    let created: TodoCreated = catga_codec_memorypack::MemoryPackCodec::default()
        .decode_payload(page.stream().events()[0].envelope().payload())
        .expect("event payload decodes");
    assert_eq!(created.title.as_ref(), "ship the API");
}

#[tokio::test]
async fn todo_projection_exposes_events_written_by_workers() {
    let projection = TodoProjection::default();
    let event = TodoCreated {
        id: "todo-42".into(),
        title: "ship the API".into(),
    };
    let payload = catga_codec_memorypack::MemoryPackCodec::default()
        .encode_payload(&event)
        .expect("event payload encodes");
    projection
        .apply(&StoredEvent::new(
            0,
            Arc::new(Envelope::new(
                1,
                "todo.created",
                payload,
                MessageMetadata::new(1, None),
            )),
            std::time::SystemTime::now(),
        ))
        .await
        .expect("projection accepts TodoCreated");

    assert_eq!(projection.todos().await[0].title.as_ref(), "ship the API");
}

#[tokio::test]
async fn todo_projection_is_idempotent_when_checkpoint_persistence_retries_an_event() {
    let projection = TodoProjection::default();
    let event = TodoCreated {
        id: "todo-43".into(),
        title: "retry projection checkpoint".into(),
    };
    let payload = catga_codec_memorypack::MemoryPackCodec::default()
        .encode_payload(&event)
        .expect("event payload encodes");
    let stored = StoredEvent::new(
        0,
        Arc::new(Envelope::new(
            2,
            "todo.created",
            payload,
            MessageMetadata::new(2, None),
        )),
        std::time::SystemTime::now(),
    );

    projection
        .apply(&stored)
        .await
        .expect("first projection application succeeds");
    projection
        .apply(&stored)
        .await
        .expect("checkpoint retry must be idempotent");

    assert_eq!(projection.todos().await.len(), 1);
}
