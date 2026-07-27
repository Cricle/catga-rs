//! Type-erased actions kept behind an immutable state-machine definition.

use std::{
    any::{Any, TypeId},
    sync::Arc,
};

use catga_core::{CatgaError, CatgaResult, ErrorCode, Event};
use futures::{FutureExt, future::BoxFuture};

pub(super) type SyncStateAction<S> = Arc<dyn Fn(&mut S) -> CatgaResult<()> + Send + Sync>;
pub(super) type AsyncStateAction<S> =
    Arc<dyn for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
pub(super) type SyncEventAction<S, E> = Arc<dyn Fn(&mut S, &E) -> CatgaResult<()> + Send + Sync>;
pub(super) type AsyncEventAction<S, E> =
    Arc<dyn for<'a> Fn(&'a mut S, &'a E) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
pub(super) type EventGuard<S, E> = Arc<dyn Fn(&S, &E) -> bool + Send + Sync>;
pub(super) type CategoryEventExtractor =
    Arc<dyn for<'a> Fn(&'a ErasedEvent) -> Option<&'a dyn Any> + Send + Sync>;
pub(super) type SyncCategoryEventAction<S> =
    Arc<dyn Fn(&mut S, &dyn Any) -> CatgaResult<()> + Send + Sync>;
pub(super) type AsyncCategoryEventAction<S> =
    Arc<dyn for<'a> Fn(&'a mut S, &'a dyn Any) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
pub(super) type CategoryEventGuard<S> = Arc<dyn Fn(&S, &dyn Any) -> bool + Send + Sync>;
pub(super) type InitialStateFactory<S, E> = Arc<dyn Fn(&E, &str) -> S + Send + Sync>;
pub(crate) type ErasedEvent = dyn Any + Send + Sync;

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
    /// Returns whether this transition selects the supplied concrete type and declared
    /// categories without allocating or discovering additional runtime types.
    fn matches(&self, event_type: TypeId, categories: &[TypeId]) -> bool;
    /// Returns whether this transition is an exact concrete-event transition.
    ///
    /// Exact transitions always receive precedence over category transitions.
    fn is_exact(&self) -> bool;
    fn applies(&self, state: &S, event: &ErasedEvent) -> CatgaResult<bool>;
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
    fn create(&self, event: &ErasedEvent, instance_id: &str) -> CatgaResult<S>;
}

fn typed_event<E>(event: &ErasedEvent) -> CatgaResult<&E>
where
    E: Event,
{
    event.downcast_ref::<E>().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            format!(
                "state-machine event type mismatch: expected {}",
                std::any::type_name::<E>()
            ),
        )
    })
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

    fn create(&self, event: &ErasedEvent, instance_id: &str) -> CatgaResult<S> {
        Ok((self.factory)(typed_event::<E>(event)?, instance_id))
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
    fn matches(&self, event_type: TypeId, _: &[TypeId]) -> bool {
        event_type == TypeId::of::<E>()
    }

    fn is_exact(&self) -> bool {
        true
    }

    fn applies(&self, state: &S, event: &ErasedEvent) -> CatgaResult<bool> {
        let event = typed_event::<E>(event)?;
        Ok(self.guard.as_ref().is_none_or(|guard| guard(state, event)))
    }

    fn target(&self) -> Option<&K> {
        self.target.as_ref()
    }

    fn execute<'a>(
        &'a self,
        state: &'a mut S,
        event: &'a ErasedEvent,
    ) -> BoxFuture<'a, CatgaResult<()>> {
        let event = match typed_event::<E>(event) {
            Ok(event) => event,
            Err(error) => return futures::future::ready(Err(error)).boxed(),
        };
        match &self.action {
            EventAction::None => futures::future::ready(Ok(())).boxed(),
            EventAction::Sync(action) => futures::future::ready(action(state, event)).boxed(),
            EventAction::Async(action) => action(state, event),
        }
    }
}

/// An immutable transition selected by an event's explicitly declared marker category.
pub(super) struct CategoryTransition<S, K> {
    pub(super) category: TypeId,
    pub(super) extractor: CategoryEventExtractor,
    pub(super) target: Option<K>,
    pub(super) guard: Option<CategoryEventGuard<S>>,
    pub(super) action: CategoryEventAction<S>,
}

impl<S, K> CategoryTransition<S, K> {
    pub(super) fn new<C, F>(extractor: F) -> Self
    where
        C: 'static,
        F: for<'a> Fn(&'a ErasedEvent) -> Option<&'a dyn Any> + Send + Sync + 'static,
    {
        Self {
            category: TypeId::of::<C>(),
            extractor: Arc::new(extractor),
            target: None,
            guard: None,
            action: CategoryEventAction::None,
        }
    }
}

pub(super) enum CategoryEventAction<S> {
    None,
    Sync(SyncCategoryEventAction<S>),
    Async(AsyncCategoryEventAction<S>),
}

impl<S, K> ErasedTransition<S, K> for CategoryTransition<S, K>
where
    S: Send + Sync,
    K: Send + Sync,
{
    fn matches(&self, _: TypeId, categories: &[TypeId]) -> bool {
        categories.contains(&self.category)
    }

    fn is_exact(&self) -> bool {
        false
    }

    fn applies(&self, state: &S, event: &ErasedEvent) -> CatgaResult<bool> {
        let Some(extracted) = (self.extractor)(event) else {
            return Ok(false);
        };
        Ok(self
            .guard
            .as_ref()
            .is_none_or(|guard| guard(state, extracted)))
    }

    fn target(&self) -> Option<&K> {
        self.target.as_ref()
    }

    fn execute<'a>(
        &'a self,
        state: &'a mut S,
        event: &'a ErasedEvent,
    ) -> BoxFuture<'a, CatgaResult<()>> {
        let Some(extracted) = (self.extractor)(event) else {
            return futures::future::ready(Err(CatgaError::new(
                ErrorCode::Internal,
                "state-machine category extractor declined a selected event",
            )))
            .boxed();
        };
        match &self.action {
            CategoryEventAction::None => futures::future::ready(Ok(())).boxed(),
            CategoryEventAction::Sync(action) => {
                futures::future::ready(action(state, extracted)).boxed()
            }
            CategoryEventAction::Async(action) => action(state, extracted),
        }
    }
}
