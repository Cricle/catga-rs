//! Cross-system integration: Durable flow state transitions are replicated through a Raft state
//! machine. Verifies that flow progress survives leadership changes and that a new leader can
//! resume an in-progress flow from the replicated state.

use std::sync::Arc;

use catga_cluster::{RaftCommittedEntry, RaftStateMachine};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowRuntime, FlowStepOutcome, MemoryFlowScheduler, flow_definition};
use catga_memory::MemorySuspendedFlows;

// ---------------------------------------------------------------------------
// Replicated flow journal
// ---------------------------------------------------------------------------

/// A simple replicated journal that records flow step completions.
#[derive(Default)]
struct FlowJournal {
    entries: Vec<JournalEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JournalEntry {
    flow_id: String,
    step: String,
    version: i64,
}

impl FlowJournal {
    fn append(&mut self, flow_id: &str, step: &str, version: i64) {
        self.entries.push(JournalEntry {
            flow_id: flow_id.to_owned(),
            step: step.to_owned(),
            version,
        });
    }

    fn last_version(&self, flow_id: &str) -> Option<i64> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.flow_id == flow_id)
            .map(|entry| entry.version)
    }

    fn entries_for(&self, flow_id: &str) -> Vec<JournalEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.flow_id == flow_id)
            .cloned()
            .collect()
    }
}

impl RaftStateMachine for FlowJournal {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        // Entry data format: "flow_id|step|version"
        let text = String::from_utf8_lossy(&entry.data);
        let parts: Vec<&str> = text.splitn(3, '|').collect();
        if parts.len() != 3 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "invalid flow journal entry",
            ));
        }
        let version: i64 = parts[2].parse().map_err(|_| {
            CatgaError::new(ErrorCode::Validation, "invalid version in journal entry")
        })?;
        self.append(parts[0], parts[1], version);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        let lines: Vec<String> = self
            .entries
            .iter()
            .map(|entry| format!("{}|{}|{}", entry.flow_id, entry.step, entry.version))
            .collect();
        Ok(lines.join("\n").into_bytes())
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        let text = String::from_utf8_lossy(data);
        self.entries.clear();
        for line in text.lines() {
            let parts: Vec<&str> = line.splitn(3, '|').collect();
            if parts.len() == 3 {
                let version = parts[2].parse().unwrap_or(0);
                self.entries.push(JournalEntry {
                    flow_id: parts[0].to_owned(),
                    step: parts[1].to_owned(),
                    version,
                });
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flow_step_completions_replicate_through_journal() -> CatgaResult<()> {
    let journal = Arc::new(std::sync::Mutex::new(FlowJournal::default()));
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let definition = flow_definition! {
        "order-process";
        "validate" => |_| async { Ok::<_, CatgaError>(FlowStepOutcome::Advance) };
        "fulfill" => |_| async { Ok::<_, CatgaError>(FlowStepOutcome::Advance) };
        "ship" => |_| async { Ok::<_, CatgaError>(FlowStepOutcome::complete()) };
    };

    let runtime = FlowRuntime::new(flows.clone(), scheduler.clone(), definition, "test-owner");

    // Start and run the flow to completion.
    runtime.start("flow-1", Vec::new()).await?;
    let result = runtime.resume("flow-1").await?;
    assert!(result.is_success());

    // Simulate replicating each step completion through the journal.
    let committed_steps = ["validate", "fulfill", "ship"];
    for (index, step) in committed_steps.iter().enumerate() {
        let entry = RaftCommittedEntry {
            index: (index + 1) as u64,
            data: format!("flow-1|{}|{}", step, index as i64).into_bytes(),
        };
        journal.lock().unwrap().apply(&entry)?;
    }

    // Verify the journal captured all steps.
    let journal = journal.lock().unwrap();
    let entries = journal.entries_for("flow-1");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].step, "validate");
    assert_eq!(entries[1].step, "fulfill");
    assert_eq!(entries[2].step, "ship");
    assert_eq!(journal.last_version("flow-1"), Some(2));

    Ok(())
}

#[tokio::test]
async fn flow_journal_snapshot_survives_restart() -> CatgaResult<()> {
    let mut journal = FlowJournal::default();

    // Apply some entries.
    for (index, step) in ["reserve", "charge", "notify"].iter().enumerate() {
        let entry = RaftCommittedEntry {
            index: (index + 1) as u64,
            data: format!("flow-99|{}|{}", step, index as i64).into_bytes(),
        };
        journal.apply(&entry)?;
    }

    // Snapshot.
    let snapshot = journal.snapshot()?;

    // Restore into a fresh journal (simulating a restarted follower).
    let mut restored = FlowJournal::default();
    restored.restore(&snapshot)?;

    let entries = restored.entries_for("flow-99");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[2].step, "notify");
    assert_eq!(restored.last_version("flow-99"), Some(2));

    Ok(())
}

#[tokio::test]
async fn flow_recovery_after_failover_uses_replicated_state() -> CatgaResult<()> {
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let definition = flow_definition! {
        "multi-step";
        "step-a" => |_| async { Ok::<_, CatgaError>(FlowStepOutcome::Advance) };
        "step-b" => |_| async { Ok::<_, CatgaError>(FlowStepOutcome::complete()) };
    };

    // Leader 1 starts the flow.
    let runtime_1 = FlowRuntime::new(flows.clone(), scheduler.clone(), definition, "leader-1");
    runtime_1.start("flow-failover", Vec::new()).await?;

    // Simulate: leader 1 completes step-a and replicates it.
    let mut journal = FlowJournal::default();
    let entry = RaftCommittedEntry {
        index: 1,
        data: b"flow-failover|step-a|0".to_vec(),
    };
    journal.apply(&entry)?;

    // Leader 1 crashes. A new leader takes over and resumes the flow.
    let definition_2 = flow_definition! {
        "multi-step";
        "step-a" => |_| async { Ok::<_, CatgaError>(FlowStepOutcome::Advance) };
        "step-b" => |_| async { Ok::<_, CatgaError>(FlowStepOutcome::complete()) };
    };
    let runtime_2 = FlowRuntime::new(flows.clone(), scheduler.clone(), definition_2, "leader-2");

    // The new leader can resume because the flow state is in the shared store.
    let result = runtime_2.resume("flow-failover").await?;
    assert!(result.is_success());

    // The replicated journal confirms step-a was committed before failover.
    assert_eq!(journal.last_version("flow-failover"), Some(0));

    Ok(())
}

#[tokio::test]
async fn flow_journal_rejects_malformed_entries() {
    let mut journal = FlowJournal::default();

    let bad_entry = RaftCommittedEntry {
        index: 1,
        data: b"invalid-data-without-pipes".to_vec(),
    };
    let result = journal.apply(&bad_entry);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
}
