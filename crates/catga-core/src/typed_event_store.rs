use std::sync::Arc;

use crate::{
    CatgaResult, DistributedIdGenerator, Envelope, Event, EventStore, PayloadEncoder,
    build_publish_metadata,
};

/// Appends typed events to an application-owned event store.
pub struct TypedEventStore<S: ?Sized, C> {
    store: Arc<S>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: Arc<C>,
}

impl<S: ?Sized, C> Clone for TypedEventStore<S, C> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            id_generator: Arc::clone(&self.id_generator),
            codec: Arc::clone(&self.codec),
        }
    }
}

impl<S: ?Sized, C> TypedEventStore<S, C> {
    /// Creates a typed event-store facade from caller-owned dependencies.
    pub fn new_with_codec(
        store: Arc<S>,
        id_generator: Arc<dyn DistributedIdGenerator>,
        codec: C,
    ) -> Self {
        Self {
            store,
            id_generator,
            codec: Arc::new(codec),
        }
    }
}

impl<S, C> TypedEventStore<S, C>
where
    S: EventStore + ?Sized,
{
    /// Appends one typed event with the caller-selected optimistic stream version.
    pub async fn append_event<E>(
        &self,
        stream_id: &str,
        event: &E,
        expected_version: Option<i64>,
    ) -> CatgaResult<i64>
    where
        E: Event,
        C: PayloadEncoder<E>,
    {
        let (id, metadata) = build_publish_metadata(&*self.id_generator, event)?;
        let envelope = Envelope::versioned(
            id,
            event.message_type(),
            self.codec.encode_payload(event)?,
            metadata,
            event.schema_version(),
        );
        self.store
            .append(stream_id, vec![envelope], expected_version)
            .await
    }

    /// Appends one event only when `stream_id` does not already exist.
    pub async fn append_new_event<E>(&self, stream_id: &str, event: &E) -> CatgaResult<i64>
    where
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.append_event(stream_id, event, Some(-1)).await
    }
}
