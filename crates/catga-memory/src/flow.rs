//! Lock-free, optimistic in-memory storage for durable flow state.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use catga_core::CatgaResult;
use catga_flow::{FlowState, FlowStatus, FlowStore};
use dashmap::{DashMap, mapref::entry::Entry};

/// An in-memory flow store with sharded indexes and per-flow CAS updates.
#[derive(Default)]
pub struct MemoryFlows {
    flows: DashMap<Box<str>, Arc<FlowSlot>>,
}

struct FlowSlot {
    state: ArcSwap<FlowState>,
}

impl FlowSlot {
    fn new(state: FlowState) -> Self {
        Self {
            state: ArcSwap::from_pointee(state),
        }
    }

    fn replace(&self, expected: &Arc<FlowState>, next: FlowState) -> Option<FlowState> {
        let next = Arc::new(next);
        let previous = self.state.compare_and_swap(expected, Arc::clone(&next));
        Arc::ptr_eq(&*previous, expected).then(|| (*next).clone())
    }
}

#[async_trait]
impl FlowStore for MemoryFlows {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        state.validate()?;
        Ok(match self.flows.entry(state.id().into()) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(FlowSlot::new(state)));
                true
            }
            Entry::Occupied(_) => false,
        })
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
        next.validate()?;
        if !FlowState::is_next_version(expected_version, next.version()) {
            return Ok(false);
        }
        let Some(slot) = self.flows.get(next.id()).map(|entry| Arc::clone(&entry)) else {
            return Ok(false);
        };
        loop {
            let current = slot.state.load_full();
            if current.version() != expected_version {
                return Ok(false);
            }
            if slot.replace(&current, next.clone()).is_some() {
                return Ok(true);
            }
        }
    }

    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>> {
        let state = self
            .flows
            .get(id)
            .map(|slot| (*slot.state.load_full()).clone());
        if let Some(state) = &state {
            state.validate()?;
        }
        Ok(state)
    }

    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>> {
        let now = SystemTime::now();
        for slot in self.flows.iter().map(|entry| Arc::clone(&entry)) {
            let current = slot.state.load_full();
            current.validate()?;
            if current.flow_type() != flow_type
                || current.status() != FlowStatus::Running
                || !is_stale(current.heartbeat(), now, stale_after)
            {
                continue;
            }
            let next = (*current).clone().claimed_by(owner).next_version()?;
            if let Some(claimed) = slot.replace(&current, next) {
                return Ok(Some(claimed));
            }
        }
        Ok(None)
    }

    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let Some(slot) = self.flows.get(id).map(|entry| Arc::clone(&entry)) else {
            return Ok(false);
        };
        loop {
            let current = slot.state.load_full();
            if current.owner() != Some(owner) || current.version() != version {
                return Ok(false);
            }
            if slot
                .replace(
                    &current,
                    (*current).clone().heartbeated_at(SystemTime::now()),
                )
                .is_some()
            {
                return Ok(true);
            }
        }
    }
}

fn is_stale(heartbeat: SystemTime, now: SystemTime, stale_after: Duration) -> bool {
    now.duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}
