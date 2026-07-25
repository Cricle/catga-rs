use std::{collections::HashMap, sync::Arc, time::SystemTime};

use arc_swap::{ArcSwap, Guard};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tokio::sync::broadcast;

use crate::FlowDefinition;

const RELOAD_EVENT_BUFFER: usize = 64;

type Definitions = HashMap<Box<str>, Arc<VersionedFlowDefinition>>;
type Versions = HashMap<Box<str>, u64>;

/// An immutable flow definition paired with its registry version.
pub struct VersionedFlowDefinition {
    definition: Arc<FlowDefinition>,
    version: u64,
}

impl VersionedFlowDefinition {
    fn new(definition: Arc<FlowDefinition>, version: u64) -> Self {
        Self {
            definition,
            version,
        }
    }

    /// Returns the immutable flow definition.
    pub fn definition(&self) -> &FlowDefinition {
        &self.definition
    }

    pub(crate) fn shared_definition(&self) -> Arc<FlowDefinition> {
        Arc::clone(&self.definition)
    }

    /// Returns the monotonic version assigned by successful reloads.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

/// A notification emitted after a flow definition has been atomically replaced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowReloaded {
    flow_name: Box<str>,
    old_version: u64,
    new_version: u64,
    reloaded_at: SystemTime,
}

impl FlowReloaded {
    /// Returns the name of the reloaded flow.
    pub fn flow_name(&self) -> &str {
        &self.flow_name
    }

    /// Returns the version that was visible immediately before reload.
    pub const fn old_version(&self) -> u64 {
        self.old_version
    }

    /// Returns the version made visible by the reload.
    pub const fn new_version(&self) -> u64 {
        self.new_version
    }

    /// Returns when the replacement became visible.
    pub const fn reloaded_at(&self) -> SystemTime {
        self.reloaded_at
    }
}

/// A copy-on-write registry of named, immutable durable flow definitions.
///
/// Reads acquire an `ArcSwap` snapshot without a mutex. Registration and reload clone only the
/// small definition map; individual definitions and their handlers stay shared through `Arc`.
pub struct FlowRegistry {
    definitions: ArcSwap<Definitions>,
    notifications: broadcast::Sender<FlowReloaded>,
}

/// A standalone, lock-free version map for flow definitions.
///
/// Applications that coordinate reloads externally can use this directly. The integrated
/// [`FlowRegistry`] already versions its immutable definition snapshots atomically.
pub struct FlowVersionManager {
    versions: ArcSwap<Versions>,
}

impl Default for FlowVersionManager {
    fn default() -> Self {
        Self {
            versions: ArcSwap::from_pointee(HashMap::new()),
        }
    }
}

impl FlowVersionManager {
    /// Returns the current version for `flow_name`, or zero when it has not been assigned.
    pub fn current(&self, flow_name: &str) -> u64 {
        self.versions.load().get(flow_name).copied().unwrap_or(0)
    }

    /// Assigns an explicit version to `flow_name`.
    pub fn set(&self, flow_name: &str, version: u64) -> CatgaResult<()> {
        validate_name(flow_name)?;
        self.replace(flow_name, version);
        Ok(())
    }

    /// Increments and returns the version for `flow_name`.
    pub fn increment(&self, flow_name: &str) -> CatgaResult<u64> {
        validate_name(flow_name)?;
        loop {
            let current = self.versions.load_full();
            let next_version = current
                .get(flow_name)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| {
                    CatgaError::new(ErrorCode::Conflict, "flow definition version overflowed")
                })?;
            let mut next = (*current).clone();
            next.insert(flow_name.into(), next_version);
            let previous =
                Guard::into_inner(self.versions.compare_and_swap(&current, Arc::new(next)));
            if Arc::ptr_eq(&previous, &current) {
                return Ok(next_version);
            }
        }
    }

    fn replace(&self, flow_name: &str, version: u64) {
        loop {
            let current = self.versions.load_full();
            let mut next = (*current).clone();
            next.insert(flow_name.into(), version);
            let previous =
                Guard::into_inner(self.versions.compare_and_swap(&current, Arc::new(next)));
            if Arc::ptr_eq(&previous, &current) {
                return;
            }
        }
    }
}

impl Default for FlowRegistry {
    fn default() -> Self {
        let (notifications, _) = broadcast::channel(RELOAD_EVENT_BUFFER);
        Self {
            definitions: ArcSwap::from_pointee(HashMap::new()),
            notifications,
        }
    }
}

impl FlowRegistry {
    /// Registers a definition without changing an existing reload version.
    pub fn register(&self, definition: FlowDefinition) -> CatgaResult<()> {
        self.replace(definition, false).map(|_| ())
    }

    /// Atomically replaces a definition, increments its version, and broadcasts the replacement.
    pub fn reload(&self, definition: FlowDefinition) -> CatgaResult<FlowReloaded> {
        self.replace(definition, true)?.ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "a flow reload must produce a replacement notification",
            )
        })
    }

    /// Returns the current immutable definition snapshot for `flow_name`.
    pub fn get(&self, flow_name: &str) -> Option<Arc<VersionedFlowDefinition>> {
        self.definitions.load().get(flow_name).cloned()
    }

    /// Returns whether a definition is registered under `flow_name`.
    pub fn contains(&self, flow_name: &str) -> bool {
        self.definitions.load().contains_key(flow_name)
    }

    /// Returns all currently registered names in unspecified order.
    pub fn names(&self) -> Vec<Box<str>> {
        self.definitions.load().keys().cloned().collect()
    }

    /// Removes a definition and returns whether it was present when removal committed.
    pub fn unregister(&self, flow_name: &str) -> bool {
        loop {
            let current = self.definitions.load_full();
            if !current.contains_key(flow_name) {
                return false;
            }
            let mut next = (*current).clone();
            let removed = next.remove(flow_name).is_some();
            let previous =
                Guard::into_inner(self.definitions.compare_and_swap(&current, Arc::new(next)));
            if Arc::ptr_eq(&previous, &current) {
                return removed;
            }
        }
    }

    /// Subscribes to successful reload notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<FlowReloaded> {
        self.notifications.subscribe()
    }

    fn replace(
        &self,
        definition: FlowDefinition,
        reload: bool,
    ) -> CatgaResult<Option<FlowReloaded>> {
        let flow_name: Box<str> = definition.name().into();
        validate_name(&flow_name)?;
        let definition = Arc::new(definition);
        loop {
            let current = self.definitions.load_full();
            let old_version = current
                .get(flow_name.as_ref())
                .map_or(0, |value| value.version());
            let version = if reload {
                old_version.checked_add(1).ok_or_else(|| {
                    CatgaError::new(ErrorCode::Conflict, "flow definition version overflowed")
                })?
            } else {
                old_version
            };
            let mut next = (*current).clone();
            next.insert(
                flow_name.clone(),
                Arc::new(VersionedFlowDefinition::new(
                    Arc::clone(&definition),
                    version,
                )),
            );
            let previous =
                Guard::into_inner(self.definitions.compare_and_swap(&current, Arc::new(next)));
            if !Arc::ptr_eq(&previous, &current) {
                continue;
            }
            if !reload {
                return Ok(None);
            }
            let event = FlowReloaded {
                flow_name,
                old_version,
                new_version: version,
                reloaded_at: SystemTime::now(),
            };
            let _ = self.notifications.send(event.clone());
            return Ok(Some(event));
        }
    }
}

fn validate_name(flow_name: &str) -> CatgaResult<()> {
    if flow_name.is_empty() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "a flow definition requires a non-empty name",
        ));
    }
    Ok(())
}
