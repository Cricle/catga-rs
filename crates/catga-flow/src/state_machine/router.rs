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
type FallbackInstanceIdResolver = Arc<dyn Fn(&ErasedEvent) -> CatgaResult<String> + Send + Sync>;

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
        let Some(event) = event.downcast_ref::<E>() else {
            return futures::future::ready(Err(CatgaError::new(
                ErrorCode::Internal,
                format!(
                    "state-machine route event type mismatch: expected {}",
                    std::any::type_name::<E>()
                ),
            )))
            .boxed();
        };
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
    fallback: Option<FallbackInstanceIdResolver>,
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
            fallback: None,
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

    /// Configures a resolver used only when an event type has no typed route.
    ///
    /// The resolver receives the erased event so applications can deliberately decide which
    /// otherwise-unregistered event types share a fallback instance-id policy.
    pub fn with_fallback<F>(mut self, resolver: F) -> Self
    where
        F: Fn(&dyn Any) -> CatgaResult<String> + Send + Sync + 'static,
    {
        self.fallback = Some(Arc::new(move |event| resolver(event)));
        self
    }

    /// Resolves and handles one registered event.
    pub async fn route<E>(&self, event: &E) -> CatgaResult<StateMachineResult<K>>
    where
        E: Event,
    {
        if let Some(route) = self.routes.get(&TypeId::of::<E>()) {
            return route.route(event).await;
        }
        let Some(fallback) = &self.fallback else {
            return Err(CatgaError::new(
                ErrorCode::Unsupported,
                format!(
                    "no state-machine route is registered for event {}",
                    std::any::type_name::<E>()
                ),
            ));
        };
        let categories = event.categories();
        let event: &ErasedEvent = event;
        let instance_id = fallback(event)?;
        if instance_id.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "state-machine fallback instance id cannot be empty",
            ));
        }
        self.executor
            .handle_erased(&instance_id, event, categories)
            .await
    }
}
