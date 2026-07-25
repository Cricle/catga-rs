//! Lock-light in-memory state-machine snapshots.

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use catga_core::CatgaResult;
use catga_flow::{StateMachineSnapshot, StateMachineStore};
use dashmap::{DashMap, mapref::entry::Entry};

/// In-memory state-machine storage with per-instance pointer CAS updates.
pub struct MemoryStateMachines<S> {
    snapshots: DashMap<Box<str>, Arc<SnapshotSlot<S>>>,
}

impl<S> Default for MemoryStateMachines<S> {
    fn default() -> Self {
        Self {
            snapshots: DashMap::new(),
        }
    }
}

struct SnapshotSlot<S> {
    snapshot: ArcSwap<StateMachineSnapshot<S>>,
}

impl<S> SnapshotSlot<S> {
    fn new(snapshot: StateMachineSnapshot<S>) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(snapshot),
        }
    }

    fn replace(
        &self,
        expected: &Arc<StateMachineSnapshot<S>>,
        next: StateMachineSnapshot<S>,
    ) -> bool {
        let next = Arc::new(next);
        let previous = self.snapshot.compare_and_swap(expected, next);
        Arc::ptr_eq(&*previous, expected)
    }
}

#[async_trait]
impl<S> StateMachineStore<S> for MemoryStateMachines<S>
where
    S: Clone + Send + Sync + 'static,
{
    async fn create(&self, snapshot: StateMachineSnapshot<S>) -> CatgaResult<bool> {
        Ok(match self.snapshots.entry(snapshot.instance_id().into()) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(SnapshotSlot::new(snapshot)));
                true
            }
            Entry::Occupied(_) => false,
        })
    }

    async fn get(&self, instance_id: &str) -> CatgaResult<Option<StateMachineSnapshot<S>>> {
        Ok(self
            .snapshots
            .get(instance_id)
            .map(|slot| (*slot.snapshot.load_full()).clone()))
    }

    async fn update(
        &self,
        expected_version: i64,
        next: StateMachineSnapshot<S>,
    ) -> CatgaResult<bool> {
        if next.version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        let Some(slot) = self
            .snapshots
            .get(next.instance_id())
            .map(|entry| Arc::clone(&entry))
        else {
            return Ok(false);
        };
        loop {
            let current = slot.snapshot.load_full();
            if current.version() != expected_version {
                return Ok(false);
            }
            if slot.replace(&current, next.clone()) {
                return Ok(true);
            }
        }
    }
}
