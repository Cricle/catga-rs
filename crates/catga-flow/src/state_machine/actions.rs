//! Type-erased actions kept behind an immutable state-machine definition.

use std::{
    any::{Any, TypeId},
    sync::Arc,
};

use catga_core::{CatgaResult, Event};
use futures::{FutureExt, future::BoxFuture};

pub(super) type SyncStateAction<S> = Arc<dyn Fn(&mut S) -> CatgaResult<()> + Send + Sync>;
pub(super) type AsyncStateAction<S> =
    Arc<dyn for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
pub(super) type SyncEventAction<S, E> = Arc<dyn Fn(&mut S, &E) -> CatgaResult<()> + Send + Sync>;
pub(super) type AsyncEventAction<S, E> =
    Arc<dyn for<'a> Fn(&'a mut S, &'a E) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
pub(super) type EventGuard<S, E> = Arc<dyn Fn(&S, &E) -> bool + Send + Sync>;
pub(super) type InitialStateFactory<S, E> = Arc<dyn Fn(&E, &str) -> S + Send + Sync>;
pub(super) type ErasedEvent = dyn Any + Send + Sync;

pub(super) enum StateAction<S> {
    Sync(SyncStateAction<S>),
    Async(AsyncStateAction<S>),
}

impl<S> StateAction<S> {
    pub(super) async fn execute(&self, state: &mut S) -> CatgaResult<()> {
        match self {
            Self::Sync(action) => action(state),
            Self::Async(action) => action(state).await,
        }
    }
}

pub(super) struct StateDefinition<S, K> {
    pub(super) on_enter: Option<StateAction<S>>,
    pub(super) on_exit: Option<StateAction<S>>,
    pub(super) transitions: Vec<Arc<dyn ErasedTransition<S, K>>>,
}

impl<S, K> Default for StateDefinition<S, K> {
    fn default() -> Self {
        Self {
            on_enter: None,
            on_exit: None,
            transitions: Vec::new(),
        }
    }
}

pub(super) trait ErasedTransition<S, K>: Send + Sync {
    fn event_type(&self) -> TypeId;
    fn applies(&self, state: &S, event: &ErasedEvent) -> bool;
    fn target(&self) -> Option<&K>;
    fn execute<'a>(
        &'a self,
        state: &'a mut S,
        event: &'a ErasedEvent,
    ) -> BoxFuture<'a, CatgaResult<()>>;
}

pub(super) trait ErasedInitialStateFactory<S, K>: Send + Sync {
    fn event_type(&self) -> TypeId;
    fn initial_state(&self) -> &K;
    fn create(&self, event: &ErasedEvent, instance_id: &str) -> S;
}

pub(super) struct TypedInitialStateFactory<S, K, E> {
    pub(super) initial_state: K,
    pub(super) factory: InitialStateFactory<S, E>,
}

impl<S, K, E> ErasedInitialStateFactory<S, K> for TypedInitialStateFactory<S, K, E>
where
    S: Send + Sync,
    K: Send + Sync,
    E: Event,
{
    fn event_type(&self) -> TypeId {
        TypeId::of::<E>()
    }

    fn initial_state(&self) -> &K {
        &self.initial_state
    }

    fn create(&self, event: &ErasedEvent, instance_id: &str) -> S {
        let event = event
            .downcast_ref::<E>()
            .expect("state-machine initial event type was checked before dispatch");
        (self.factory)(event, instance_id)
    }
}

pub(super) enum EventAction<S, E> {
    None,
    Sync(SyncEventAction<S, E>),
    Async(AsyncEventAction<S, E>),
}

pub(super) struct TypedTransition<S, K, E> {
    pub(super) target: Option<K>,
    pub(super) guard: Option<EventGuard<S, E>>,
    pub(super) action: EventAction<S, E>,
}

impl<S, K, E> TypedTransition<S, K, E> {
    pub(super) fn new() -> Self {
        Self {
            target: None,
            guard: None,
            action: EventAction::None,
        }
    }
}

impl<S, K, E> ErasedTransition<S, K> for TypedTransition<S, K, E>
where
    S: Send + Sync,
    K: Send + Sync,
    E: Event,
{
    fn event_type(&self) -> TypeId {
        TypeId::of::<E>()
    }

    fn applies(&self, state: &S, event: &ErasedEvent) -> bool {
        let event = event
            .downcast_ref::<E>()
            .expect("state-machine event type was checked before dispatch");
        self.guard.as_ref().is_none_or(|guard| guard(state, event))
    }

    fn target(&self) -> Option<&K> {
        self.target.as_ref()
    }

    fn execute<'a>(
        &'a self,
        state: &'a mut S,
        event: &'a ErasedEvent,
    ) -> BoxFuture<'a, CatgaResult<()>> {
        let event = event
            .downcast_ref::<E>()
            .expect("state-machine event type was checked before dispatch");
        match &self.action {
            EventAction::None => futures::future::ready(Ok(())).boxed(),
            EventAction::Sync(action) => futures::future::ready(action(state, event)).boxed(),
            EventAction::Async(action) => action(state, event),
        }
    }
}
