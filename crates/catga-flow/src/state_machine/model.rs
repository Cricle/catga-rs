//! State-machine values that may be persisted independently of configuration.

use std::time::SystemTime;

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// Mutable state held by a state-machine instance.
pub trait StateMachineState<K>: Clone + Send + Sync + 'static {
    /// Returns the instance's current state discriminator.
    fn current_state(&self) -> &K;

    /// Replaces the instance's current state discriminator.
    fn set_current_state(&mut self, state: K);
}

/// The outcome of attempting to handle one event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateMachineResult<K> {
    previous: K,
    current: K,
    handled: bool,
}

impl<K> StateMachineResult<K> {
    pub(crate) const fn new(previous: K, current: K, handled: bool) -> Self {
        Self {
            previous,
            current,
            handled,
        }
    }

    /// Returns the state before event handling.
    pub fn previous(&self) -> K
    where
        K: Clone,
    {
        self.previous.clone()
    }

    /// Returns the state after event handling.
    pub fn current(&self) -> K
    where
        K: Clone,
    {
        self.current.clone()
    }

    /// Returns whether a configured, unguarded transition handled the event.
    pub const fn handled(&self) -> bool {
        self.handled
    }

    /// Returns whether handling changed the state discriminator.
    pub fn transitioned(&self) -> bool
    where
        K: Eq,
    {
        self.previous != self.current
    }
}

/// Immutable, versioned state-machine data kept by a store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateMachineSnapshot<S> {
    instance_id: Box<str>,
    state: S,
    version: i64,
    created_at: SystemTime,
    updated_at: SystemTime,
}

impl<S> StateMachineSnapshot<S> {
    /// Creates the first version of an instance snapshot.
    pub fn new(instance_id: impl Into<Box<str>>, state: S) -> Self {
        let now = SystemTime::now();
        Self {
            instance_id: instance_id.into(),
            state,
            version: 0,
            created_at: now,
            updated_at: now,
        }
    }

    /// Restores a snapshot with its persisted creation and update timestamps.
    pub fn restore(
        instance_id: impl Into<Box<str>>,
        state: S,
        version: i64,
        created_at: SystemTime,
        updated_at: SystemTime,
    ) -> CatgaResult<Self> {
        if version < 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "state-machine snapshot version cannot be negative",
            ));
        }
        if updated_at < created_at {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "state-machine snapshot update time cannot precede creation time",
            ));
        }
        Ok(Self {
            instance_id: instance_id.into(),
            state,
            version,
            created_at,
            updated_at,
        })
    }

    /// Returns the immutable instance identity.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns the persisted state payload.
    pub const fn state(&self) -> &S {
        &self.state
    }

    /// Returns the optimistic concurrency version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns when this instance snapshot was first persisted.
    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns when this state was last successfully persisted.
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Produces the next version with an updated state payload.
    ///
    /// Returns [`ErrorCode::Conflict`] when the version cannot advance beyond `i64::MAX`.
    pub fn next_version(&self, state: S) -> CatgaResult<Self> {
        let now = SystemTime::now();
        let version = self.version.checked_add(1).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Conflict,
                "state-machine snapshot version cannot advance beyond i64::MAX",
            )
        })?;
        Ok(Self {
            instance_id: self.instance_id.clone(),
            state,
            version,
            created_at: self.created_at,
            updated_at: now.max(self.updated_at),
        })
    }

    /// Returns whether `next` is the exact representable successor of `expected`.
    ///
    /// State-machine stores use this to reject same-version writes at `i64::MAX` and every other
    /// non-successor update before persisting it.
    pub const fn is_next_version(expected: i64, next: i64) -> bool {
        matches!(expected.checked_add(1), Some(candidate) if candidate == next)
    }
}
