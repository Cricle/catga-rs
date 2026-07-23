//! Typed event-to-instance routing for state machines.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    hash::Hash,
    sync::Arc,
};

use catga_core::{CatgaError, CatgaResult, ErrorCode, Event};
use futures::{FutureExt, future::BoxFuture};

use super::{StateMachineExecutor, StateMachineResult, StateMachineState, StateMachineStore};

type InstanceIdResolver<E> = Arc<dyn Fn(&E) -> String + Send + Sync>;
type ErasedEvent = dyn Any + Send + Sync;

trait ErasedEventRoute<S, K, Store>: Send + Sync {
    fn route<'a>(
        &'a self,
        event: &'a ErasedEvent,
    ) -> BoxFuture<'a, CatgaResult<StateMachineResult<K>>>;
}

struct TypedEventRoute<S, K, Store, E> {
    executor: Arc<StateMachineExecutor<S, K, Store>>,
    resolve_instance_id: InstanceIdResolver<E>,
}

impl<S, K, Store, E> ErasedEventRoute<S, K, Store> for TypedEventRoute<S, K, Store, E>
where
    S: StateMachineState<K>,
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Store: StateMachineStore<S> + 'static,
    E: Event,
{
    fn route<'a>(
        &'a self,
        event: &'a ErasedEvent,
    ) -> BoxFuture<'a, CatgaResult<StateMachineResult<K>>> {
        let event = event
            .downcast_ref::<E>()
            .expect("state-machine event type was checked before routing");
        async move {
            let instance_id = (self.resolve_instance_id)(event);
            if instance_id.trim().is_empty() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    format!(
                        "state-machine instance id cannot be empty for event {}",
                        std::any::type_name::<E>()
                    ),
                ));
            }
            self.executor.handle(&instance_id, event).await
        }
        .boxed()
    }
}

/// Routes typed events to state-machine instance ids selected by user code.
///
/// Registration happens during startup; routing reads a fixed type table and does not lock.
pub struct StateMachineEventRouter<S, K, Store> {
    executor: Arc<StateMachineExecutor<S, K, Store>>,
    routes: HashMap<TypeId, Arc<dyn ErasedEventRoute<S, K, Store>>>,
}

impl<S, K, Store> StateMachineEventRouter<S, K, Store>
where
    S: StateMachineState<K>,
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Store: StateMachineStore<S> + 'static,
{
    /// Creates an empty router for one executor.
    pub fn new(executor: Arc<StateMachineExecutor<S, K, Store>>) -> Self {
        Self {
            executor,
            routes: HashMap::new(),
        }
    }

    /// Registers the instance-id selector for one event type.
    pub fn for_event<E, F>(mut self, resolver: F) -> Self
    where
        E: Event,
        F: Fn(&E) -> String + Send + Sync + 'static,
    {
        self.routes.insert(
            TypeId::of::<E>(),
            Arc::new(TypedEventRoute::<S, K, Store, E> {
                executor: Arc::clone(&self.executor),
                resolve_instance_id: Arc::new(resolver),
            }),
        );
        self
    }

    /// Resolves and handles one registered event.
    pub async fn route<E>(&self, event: &E) -> CatgaResult<StateMachineResult<K>>
    where
        E: Event,
    {
        let Some(route) = self.routes.get(&TypeId::of::<E>()) else {
            return Err(CatgaError::new(
                ErrorCode::Unsupported,
                format!(
                    "no state-machine route is registered for event {}",
                    std::any::type_name::<E>()
                ),
            ));
        };
        route.route(event).await
    }
}
