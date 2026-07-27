//! Read-optimized typed state-machine definitions.

use std::{
    any::TypeId,
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
    sync::Arc,
};

use catga_core::{CatgaResult, Event};
use futures::future::BoxFuture;

use super::{
    StateMachineResult, StateMachineState,
    actions::{
        CategoryEventAction, CategoryTransition, ErasedEvent, ErasedInitialStateFactory,
        EventAction, StateAction, StateDefinition, TypedInitialStateFactory, TypedTransition,
    },
};

type DefaultInitialStateFactory<S> = Arc<dyn Fn() -> S + Send + Sync>;

struct MachineDefinition<S, K> {
    initial: K,
    states: HashMap<K, StateDefinition<S, K>>,
    initial_factories: Vec<Arc<dyn ErasedInitialStateFactory<S, K>>>,
    default_initial_factory: Option<DefaultInitialStateFactory<S>>,
}

/// An immutable state-machine configuration optimized for concurrent reads.
pub struct StateMachine<S, K> {
    definition: Arc<MachineDefinition<S, K>>,
}

impl<S, K> Clone for StateMachine<S, K> {
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
        }
    }
}

impl<S, K> StateMachine<S, K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    S: StateMachineState<K>,
{
    /// Starts configuration with the supplied default initial state.
    pub fn builder(initial: K) -> StateMachineBuilder<S, K> {
        StateMachineBuilder {
            initial,
            states: HashMap::new(),
            initial_factories: Vec::new(),
            default_initial_factory: None,
        }
    }

    /// Returns the configured default initial state.
    pub fn initial(&self) -> K {
        self.definition.initial.clone()
    }

    /// Handles an event without persisting the mutable state payload.
    pub async fn handle<E>(&self, state: &mut S, event: &E) -> CatgaResult<StateMachineResult<K>>
    where
        E: Event,
    {
        let categories = event.categories();
        let event: &ErasedEvent = event;
        self.handle_erased(state, event, categories).await
    }

    pub(crate) async fn handle_erased(
        &self,
        state: &mut S,
        event: &ErasedEvent,
        categories: &[TypeId],
    ) -> CatgaResult<StateMachineResult<K>> {
        let previous = state.current_state().clone();
        let Some(definition) = self.definition.states.get(&previous) else {
            return Ok(StateMachineResult::new(previous.clone(), previous, false));
        };
        let mut selected = None;
        for exact in [true, false] {
            for transition in &definition.transitions {
                if transition.is_exact() == exact
                    && transition.matches(event.type_id(), categories)
                    && transition.applies(state, event)?
                {
                    selected = Some(transition);
                    break;
                }
            }
            if selected.is_some() {
                break;
            }
        }
        let Some(transition) = selected else {
            return Ok(StateMachineResult::new(previous.clone(), previous, false));
        };

        if let Some(action) = &definition.on_exit {
            action.execute(state).await?;
        }
        transition.execute(state, event).await?;
        if let Some(target) = transition.target() {
            state.set_current_state(target.clone());
            if let Some(action) = self
                .definition
                .states
                .get(state.current_state())
                .and_then(|next| next.on_enter.as_ref())
            {
                action.execute(state).await?;
            }
        }
        Ok(StateMachineResult::new(
            previous,
            state.current_state().clone(),
            true,
        ))
    }

    pub(crate) fn create_initial_erased(
        &self,
        instance_id: &str,
        event: &ErasedEvent,
    ) -> CatgaResult<Option<S>> {
        if let Some(factory) = self
            .definition
            .initial_factories
            .iter()
            .find(|factory| factory.event_type() == event.type_id())
        {
            let mut state = factory.create(event, instance_id)?;
            state.set_current_state(factory.initial_state().clone());
            return Ok(Some(state));
        }
        let Some(factory) = &self.definition.default_initial_factory else {
            return Ok(None);
        };
        let mut state = factory();
        state.set_current_state(self.definition.initial.clone());
        Ok(Some(state))
    }
}

/// Mutable, startup-only builder for a [`StateMachine`].
pub struct StateMachineBuilder<S, K> {
    initial: K,
    states: HashMap<K, StateDefinition<S, K>>,
    initial_factories: Vec<Arc<dyn ErasedInitialStateFactory<S, K>>>,
    default_initial_factory: Option<DefaultInitialStateFactory<S>>,
}

impl<S, K> StateMachineBuilder<S, K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    S: StateMachineState<K>,
{
    /// Configures one state, creating it when necessary.
    pub fn state(&mut self, state: K) -> StateDefinitionBuilder<'_, S, K> {
        let definition = match self.states.entry(state) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(StateDefinition::default()),
        };
        StateDefinitionBuilder { definition }
    }

    /// Allows an event to create a missing instance in the supplied starting state.
    ///
    /// The factory receives the correlated instance id selected by the caller and must return
    /// the rest of the initial state payload. Re-registering an event type replaces its factory.
    pub fn starts_with<E, F>(&mut self, initial_state: K, factory: F) -> &mut Self
    where
        E: Event,
        F: Fn(&E, &str) -> S + Send + Sync + 'static,
    {
        self.initial_factories
            .retain(|existing| existing.event_type() != TypeId::of::<E>());
        self.initial_factories
            .push(Arc::new(TypedInitialStateFactory::<S, K, E> {
                initial_state,
                factory: Arc::new(factory),
            }));
        self
    }

    /// Allows an event to create a missing instance from this machine's default initial state.
    ///
    /// This is useful when instance hydration and the event that starts the workflow are shared
    /// across several initial transitions.
    pub fn create_instance_from<E, F>(&mut self, factory: F) -> &mut Self
    where
        E: Event,
        F: Fn(&E, &str) -> S + Send + Sync + 'static,
    {
        self.starts_with::<E, _>(self.initial.clone(), factory)
    }

    /// Enables lazy creation with [`Default::default`] when no event-specific factory matches.
    ///
    /// Event-specific factories registered with [`Self::starts_with`] keep precedence. This is an
    /// explicit opt-in so missing instances remain errors for definitions that require a custom
    /// correlation or hydration policy.
    pub fn default_initial_state(&mut self) -> &mut Self
    where
        S: Default + 'static,
    {
        self.default_initial_factory = Some(Arc::new(S::default));
        self
    }

    /// Freezes definitions into a lock-free-read state machine.
    pub fn build(self) -> StateMachine<S, K> {
        StateMachine {
            definition: Arc::new(MachineDefinition {
                initial: self.initial,
                states: self.states,
                initial_factories: self.initial_factories,
                default_initial_factory: self.default_initial_factory,
            }),
        }
    }
}

/// Startup-only actions and transitions for one state.
pub struct StateDefinitionBuilder<'a, S, K> {
    definition: &'a mut StateDefinition<S, K>,
}

impl<'a, S, K> StateDefinitionBuilder<'a, S, K>
where
    K: Clone + Send + Sync + 'static,
    S: StateMachineState<K>,
{
    /// Registers a synchronous entry action.
    pub fn on_enter<F>(&mut self, action: F) -> &mut Self
    where
        F: Fn(&mut S) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.definition.on_enter = Some(StateAction::Sync(Arc::new(action)));
        self
    }

    /// Registers an asynchronous entry action.
    pub fn on_enter_async<F>(&mut self, action: F) -> &mut Self
    where
        F: for<'b> Fn(&'b mut S) -> BoxFuture<'b, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.definition.on_enter = Some(StateAction::Async(Arc::new(action)));
        self
    }

    /// Registers a synchronous exit action.
    pub fn on_exit<F>(&mut self, action: F) -> &mut Self
    where
        F: Fn(&mut S) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.definition.on_exit = Some(StateAction::Sync(Arc::new(action)));
        self
    }

    /// Registers an asynchronous exit action.
    pub fn on_exit_async<F>(&mut self, action: F) -> &mut Self
    where
        F: for<'b> Fn(&'b mut S) -> BoxFuture<'b, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.definition.on_exit = Some(StateAction::Async(Arc::new(action)));
        self
    }

    /// Starts configuring a transition for event type `E`.
    pub fn on<E>(self) -> EventTransitionBuilder<'a, S, K, E>
    where
        E: Event,
    {
        EventTransitionBuilder {
            definition: self.definition,
            transition: TypedTransition::new(),
        }
    }

    /// Starts configuring a transition for the explicit category marker `C`.
    ///
    /// `extractor` receives the erased event only after its [`Event::categories`] declaration
    /// includes `C`. It returns the value exposed to the guard and action, or `None` to leave
    /// the event unhandled. This makes category membership explicit and avoids reflection or a
    /// global type registry.
    pub fn on_category<C, F>(self, extractor: F) -> CategoryTransitionBuilder<'a, S, K, C>
    where
        C: 'static,
        F: for<'b> Fn(&'b ErasedEvent) -> Option<&'b dyn std::any::Any> + Send + Sync + 'static,
    {
        CategoryTransitionBuilder {
            definition: self.definition,
            transition: CategoryTransition::new::<C, F>(extractor),
            marker: std::marker::PhantomData,
        }
    }
}

/// Startup-only fluent builder for a typed transition.
pub struct EventTransitionBuilder<'a, S, K, E>
where
    S: StateMachineState<K>,
    K: Clone + Send + Sync + 'static,
    E: Event,
{
    definition: &'a mut StateDefinition<S, K>,
    transition: TypedTransition<S, K, E>,
}

impl<'a, S, K, E> EventTransitionBuilder<'a, S, K, E>
where
    S: StateMachineState<K>,
    K: Clone + Send + Sync + 'static,
    E: Event,
{
    /// Requires a guard to approve the transition.
    pub fn when<F>(mut self, guard: F) -> Self
    where
        F: Fn(&S, &E) -> bool + Send + Sync + 'static,
    {
        self.transition.guard = Some(Arc::new(guard));
        self
    }

    /// Registers a synchronous transition action.
    pub fn execute<F>(mut self, action: F) -> Self
    where
        F: Fn(&mut S, &E) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.transition.action = EventAction::Sync(Arc::new(action));
        self
    }

    /// Registers an asynchronous transition action.
    pub fn execute_async<F>(mut self, action: F) -> Self
    where
        F: for<'b> Fn(&'b mut S, &'b E) -> BoxFuture<'b, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.transition.action = EventAction::Async(Arc::new(action));
        self
    }

    /// Changes the current state after the transition action succeeds and commits the transition.
    pub fn transition_to(mut self, target: K) -> StateDefinitionBuilder<'a, S, K> {
        self.transition.target = Some(target);
        self.finish()
    }

    /// Commits this transition and returns to the containing state configuration.
    pub fn finish(self) -> StateDefinitionBuilder<'a, S, K> {
        self.definition.transitions.push(Arc::new(self.transition));
        StateDefinitionBuilder {
            definition: self.definition,
        }
    }

    /// Alias for [`Self::finish`] that reads naturally after a transition.
    pub fn and(self) -> StateDefinitionBuilder<'a, S, K> {
        self.finish()
    }
}

/// Startup-only fluent builder for an explicit category transition.
pub struct CategoryTransitionBuilder<'a, S, K, C>
where
    S: StateMachineState<K>,
    K: Clone + Send + Sync + 'static,
    C: 'static,
{
    definition: &'a mut StateDefinition<S, K>,
    transition: CategoryTransition<S, K>,
    marker: std::marker::PhantomData<C>,
}

impl<'a, S, K, C> CategoryTransitionBuilder<'a, S, K, C>
where
    S: StateMachineState<K>,
    K: Clone + Send + Sync + 'static,
    C: 'static,
{
    /// Requires a guard to approve the extracted category value.
    pub fn when<F>(mut self, guard: F) -> Self
    where
        F: Fn(&S, &dyn std::any::Any) -> bool + Send + Sync + 'static,
    {
        self.transition.guard = Some(Arc::new(guard));
        self
    }

    /// Registers a synchronous transition action for the extracted category value.
    pub fn execute<F>(mut self, action: F) -> Self
    where
        F: Fn(&mut S, &dyn std::any::Any) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.transition.action = CategoryEventAction::Sync(Arc::new(action));
        self
    }

    /// Registers an asynchronous transition action for the extracted category value.
    pub fn execute_async<F>(mut self, action: F) -> Self
    where
        F: for<'b> Fn(&'b mut S, &'b dyn std::any::Any) -> BoxFuture<'b, CatgaResult<()>>
            + Send
            + Sync
            + 'static,
    {
        self.transition.action = CategoryEventAction::Async(Arc::new(action));
        self
    }

    /// Changes the current state after the transition action succeeds and commits the transition.
    pub fn transition_to(mut self, target: K) -> StateDefinitionBuilder<'a, S, K> {
        self.transition.target = Some(target);
        self.finish()
    }

    /// Commits this category transition and returns to the containing state configuration.
    pub fn finish(self) -> StateDefinitionBuilder<'a, S, K> {
        self.definition.transitions.push(Arc::new(self.transition));
        StateDefinitionBuilder {
            definition: self.definition,
        }
    }

    /// Alias for [`Self::finish`] that reads naturally after a transition.
    pub fn and(self) -> StateDefinitionBuilder<'a, S, K> {
        self.finish()
    }
}
