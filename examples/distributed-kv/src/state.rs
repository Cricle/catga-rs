//! Shared replicated state and the backend-agnostic state machine for the KV store.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use std::sync::Arc;

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};
use serde::{Deserialize, Serialize};

const APPLIED_OP_WINDOW: usize = 4096;
const APPLY_POLL_INTERVAL: Duration = Duration::from_millis(2);

/// One replicated write command carried by a committed Raft entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum KvCommand {
    Put {
        op_id: u64,
        key: String,
        value: String,
    },
}

/// Applied read model shared between the Raft state machine and HTTP handlers.
pub(crate) struct SharedState {
    values: RwLock<BTreeMap<String, String>>,
    applied_index: AtomicU64,
    applied_ops: Mutex<VecDeque<u64>>,
}

impl SharedState {
    pub(crate) fn new() -> Self {
        Self {
            values: RwLock::new(BTreeMap::new()),
            applied_index: AtomicU64::new(0),
            applied_ops: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        self.values.read().ok()?.get(key).cloned()
    }

    pub(crate) fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    pub(crate) fn is_applied(&self, op_id: u64) -> bool {
        self.applied_ops
            .lock()
            .map(|ops| ops.contains(&op_id))
            .unwrap_or(false)
    }

    pub(crate) async fn wait_applied(&self, op_id: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.is_applied(op_id) {
                return true;
            }
            if Instant::now() >= deadline {
                return self.is_applied(op_id);
            }
            tokio::time::sleep(APPLY_POLL_INTERVAL).await;
        }
    }

    fn record_applied(&self, op_id: u64) {
        if let Ok(mut ops) = self.applied_ops.lock() {
            if ops.len() >= APPLIED_OP_WINDOW {
                ops.pop_front();
            }
            ops.push_back(op_id);
        }
    }
}

fn lock_error() -> CatgaError {
    CatgaError::new(ErrorCode::Internal, "kv state lock poisoned")
}

/// The deterministic state machine owned by the consensus backend.
///
/// It implements only the backend-agnostic [`ConsensusStateMachine`] contract;
/// `node.rs` adapts it to whichever backend was selected.
pub(crate) struct KvMachine {
    state: Arc<SharedState>,
}

impl KvMachine {
    pub(crate) fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

impl ConsensusStateMachine for KvMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        let command: KvCommand = serde_json::from_slice(data).map_err(|error| {
            CatgaError::new(
                ErrorCode::SerializationFailed,
                format!("invalid kv command: {error}"),
            )
        })?;
        match command {
            KvCommand::Put { op_id, key, value } => {
                self.state
                    .values
                    .write()
                    .map_err(|_| lock_error())?
                    .insert(key.clone(), value.clone());
                self.state.applied_index.store(index, Ordering::Release);
                self.state.record_applied(op_id);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        let values = self.state.values.read().map_err(|_| lock_error())?;
        serde_json::to_vec(&*values).map_err(|error| {
            CatgaError::new(
                ErrorCode::SerializationFailed,
                format!("kv snapshot encode failed: {error}"),
            )
        })
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let values: BTreeMap<String, String> = serde_json::from_slice(bytes).map_err(|error| {
            CatgaError::new(
                ErrorCode::SerializationFailed,
                format!("kv snapshot decode failed: {error}"),
            )
        })?;
        *self.state.values.write().map_err(|_| lock_error())? = values;
        Ok(())
    }
}
