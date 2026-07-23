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
        ErasedEvent, ErasedInitialStateFactory, EventAction, StateAction, StateDefinition,
        TypedInitialStateFactory, TypedTransition,
    },
};

struct MachineDefinition<S, K> {
    initial: K,
    states: HashMap<K, StateDefinition<S, K>>,
    initial_factories: Vec<Arc<dyn ErasedInitialStateFactory<S, K>>>,
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
        let previous = state.current_state().clone();
        let Some(definition) = self.definition.states.get(&previous) else {
            return Ok(StateMachineResult::new(previous.clone(), previous, false));
        };
        let event: &ErasedEvent = event;
        let Some(transition) = definition.transitions.iter().find(|transition| {
            transition.event_type() == TypeId::of::<E>() && transition.applies(state, event)
        }) else {
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

    pub(crate) fn create_initial<E>(&self, instance_id: &str, event: &E) -> Option<S>
    where
        E: Event,
    {
        let event: &ErasedEvent = event;
        let factory = self
            .definition
            .initial_factories
            .iter()
            .find(|factory| factory.event_type() == TypeId::of::<E>())?;
        let mut state = factory.create(event, instance_id);
        state.set_current_state(factory.initial_state().clone());
        Some(state)
    }
}

/// Mutable, startup-only builder for a [`StateMachine`].
pub struct StateMachineBuilder<S, K> {
    initial: K,
    states: HashMap<K, StateDefinition<S, K>>,
    initial_factories: Vec<Arc<dyn ErasedInitialStateFactory<S, K>>>,
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

    /// Freezes definitions into a lock-free-read state machine.
    pub fn build(self) -> StateMachine<S, K> {
        StateMachine {
            definition: Arc::new(MachineDefinition {
                initial: self.initial,
                states: self.states,
                initial_factories: self.initial_factories,
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
            transition: Some(TypedTransition::new()),
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
    transition: Option<TypedTransition<S, K, E>>,
}

impl<'a, S, K, E> EventTransitionBuilder<'a, S, K, E>
where
    S: StateMachineState<K>,
    K: Clone + Send + Sync + 'static,
    E: Event,
{
    fn transition_mut(&mut self) -> &mut TypedTransition<S, K, E> {
        self.transition
            .as_mut()
            .expect("transition builder is committed only once")
    }

    fn commit(&mut self) {
        if let Some(transition) = self.transition.take() {
            self.definition.transitions.push(Arc::new(transition));
        }
    }

    /// Requires a guard to approve the transition.
    pub fn when<F>(mut self, guard: F) -> Self
    where
        F: Fn(&S, &E) -> bool + Send + Sync + 'static,
    {
        self.transition_mut().guard = Some(Arc::new(guard));
        self
    }

    /// Registers a synchronous transition action.
    pub fn execute<F>(mut self, action: F) -> Self
    where
        F: Fn(&mut S, &E) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.transition_mut().action = EventAction::Sync(Arc::new(action));
        self
    }

    /// Registers an asynchronous transition action.
    pub fn execute_async<F>(mut self, action: F) -> Self
    where
        F: for<'b> Fn(&'b mut S, &'b E) -> BoxFuture<'b, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.transition_mut().action = EventAction::Async(Arc::new(action));
        self
    }

    /// Changes the current state after the transition action succeeds and commits the transition.
    pub fn transition_to(mut self, target: K) -> StateDefinitionBuilder<'a, S, K> {
        self.transition_mut().target = Some(target);
        self.finish()
    }

    /// Commits this transition and returns to the containing state configuration.
    pub fn finish(mut self) -> StateDefinitionBuilder<'a, S, K> {
        self.commit();
        StateDefinitionBuilder {
            definition: self.definition,
        }
    }

    /// Alias for [`Self::finish`] that reads naturally after a transition.
    pub fn and(self) -> StateDefinitionBuilder<'a, S, K> {
        self.finish()
    }
}
