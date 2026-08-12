//! Observable probe over the recording state machine.
//!
//! This module references [`crate::recording_machine`]: every test binary that
//! includes it must also declare `mod recording_machine;` from `common/`.

use std::sync::{Arc, atomic::AtomicU64};

use catga_core::ConsensusStateMachine;

use crate::recording_machine::RecordingMachine;

/// Observable handles into one node's state machine.
#[derive(Clone, Default)]
pub(crate) struct MachineProbe {
    pub(crate) sum: Arc<AtomicU64>,
    pub(crate) applied: Arc<AtomicU64>,
    pub(crate) snapshot_calls: Arc<AtomicU64>,
}

impl MachineProbe {
    /// Builds a fresh machine sharing this probe's counters.
    pub(crate) fn machine(&self) -> impl ConsensusStateMachine + 'static {
        RecordingMachine::new(
            Arc::clone(&self.sum),
            Arc::clone(&self.applied),
            Arc::clone(&self.snapshot_calls),
        )
    }
}
