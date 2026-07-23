use std::{sync::atomic::{AtomicU64, Ordering}, time::SystemTime};

use async_trait::async_trait;
use catga_core::CatgaResult;
use dashmap::DashMap;

/// Schedules durable flow resumption without coupling flows to a job system.
#[async_trait]
pub trait FlowScheduler: Send + Sync {
    /// Schedules `flow_id` for resumption at `due_at` and returns a cancellation identity.
    async fn schedule_resume(&self, flow_id: &str, due_at: SystemTime) -> CatgaResult<Box<str>>;

    /// Cancels a scheduled resume when it has not started.
    async fn cancel_resume(&self, schedule_id: &str) -> CatgaResult<bool>;
}

/// One due flow-resume request emitted by [`MemoryFlowScheduler`].
#[derive(Clone, Debug)]
pub struct ScheduledResume {
    schedule_id: Box<str>,
    flow_id: Box<str>,
    due_at: SystemTime,
}

impl ScheduledResume {
    /// Returns the scheduler-specific cancellation identity.
    pub fn schedule_id(&self) -> &str {
        &self.schedule_id
    }

    /// Returns the flow that should be resumed.
    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }

    /// Returns when this resume becomes eligible.
    pub const fn due_at(&self) -> SystemTime {
        self.due_at
    }
}

/// A deterministic in-memory scheduler for local development and integration tests.
#[derive(Default)]
pub struct MemoryFlowScheduler {
    next_id: AtomicU64,
    schedules: DashMap<Box<str>, ScheduledResume>,
}

impl MemoryFlowScheduler {
    /// Removes and returns every schedule due no later than `now`.
    pub fn take_due(&self, now: SystemTime) -> Vec<ScheduledResume> {
        let due_ids: Vec<Box<str>> = self
            .schedules
            .iter()
            .filter(|entry| entry.due_at <= now)
            .map(|entry| entry.key().clone())
            .collect();
        due_ids
            .into_iter()
            .filter_map(|id| self.schedules.remove(&id).map(|(_, schedule)| schedule))
            .collect()
    }
}

#[async_trait]
impl FlowScheduler for MemoryFlowScheduler {
    async fn schedule_resume(&self, flow_id: &str, due_at: SystemTime) -> CatgaResult<Box<str>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let schedule_id: Box<str> = format!("flow-resume-{id}").into();
        self.schedules.insert(
            schedule_id.clone(),
            ScheduledResume {
                schedule_id: schedule_id.clone(),
                flow_id: flow_id.into(),
                due_at,
            },
        );
        Ok(schedule_id)
    }

    async fn cancel_resume(&self, schedule_id: &str) -> CatgaResult<bool> {
        Ok(self.schedules.remove(schedule_id).is_some())
    }
}
