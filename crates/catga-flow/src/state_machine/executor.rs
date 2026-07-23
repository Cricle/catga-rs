//! Persistent execution for immutable state-machine definitions.

use std::{hash::Hash, sync::Arc};

use catga_core::{CatgaError, CatgaResult, ErrorCode, Event};

use super::{StateMachine, StateMachineResult, StateMachineState, StateMachineStore};

/// Applies events to persistent state-machine instances with optimistic concurrency.
pub struct StateMachineExecutor<S, K, Store> {
    machine: StateMachine<S, K>,
    store: Arc<Store>,
}

impl<S, K, Store> StateMachineExecutor<S, K, Store>
where
    S: StateMachineState<K>,
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Store: StateMachineStore<S>,
{
    /// Creates an executor over an immutable definition and one snapshot store.
    pub fn new(machine: StateMachine<S, K>, store: Arc<Store>) -> Self {
        Self { machine, store }
    }

    /// Creates an instance at version zero when it does not already exist.
    pub async fn initialize(&self, instance_id: &str, state: S) -> CatgaResult<bool> {
        self.store
            .create(super::StateMachineSnapshot::new(instance_id, state))
            .await
    }

    /// Loads, handles, and atomically persists one event.
    pub async fn handle<E>(
        &self,
        instance_id: &str,
        event: &E,
    ) -> CatgaResult<StateMachineResult<K>>
    where
        E: Event,
    {
        let Some(snapshot) = self.store.get(instance_id).await? else {
            let Some(mut state) = self.machine.create_initial(instance_id, event) else {
                return Err(CatgaError::new(
                    ErrorCode::NotFound,
                    format!("state-machine instance '{instance_id}' does not exist"),
                ));
            };
            let result = self.machine.handle(&mut state, event).await?;
            if !result.handled() {
                return Ok(result);
            }
            if self
                .store
                .create(super::StateMachineSnapshot::new(instance_id, state))
                .await?
            {
                return Ok(result);
            }
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                format!("state-machine instance '{instance_id}' was created concurrently"),
            ));
        };
        let mut state = snapshot.state().clone();
        let result = self.machine.handle(&mut state, event).await?;
        if !result.handled() {
            return Ok(result);
        }
        let next = snapshot.next_version(state);
        if self.store.update(snapshot.version(), next).await? {
            return Ok(result);
        }
        Err(CatgaError::new(
            ErrorCode::Conflict,
            format!("state-machine instance '{instance_id}' changed while handling its event"),
        ))
    }

    /// Returns the current persistent snapshot when present.
    pub async fn get(
        &self,
        instance_id: &str,
    ) -> CatgaResult<Option<super::StateMachineSnapshot<S>>> {
        self.store.get(instance_id).await
    }
}
