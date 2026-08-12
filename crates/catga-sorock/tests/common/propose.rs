//! Proposal retry helper for tests that must survive leader-election races.
//!
//! This module references [`crate::harness`]: every test binary that includes
//! it must also declare `mod harness;` from `common/`.

use std::time::Duration;

use catga_core::ConsensusRuntime;
use catga_sorock::SorockRuntime;

use crate::harness::WAIT_TIMEOUT;

/// Retries a proposal until the group elects a leader and commits it.
///
/// sorock rejects writes fast while no leader is known, so a proposal issued
/// right after bootstrap or a restart needs a bounded retry loop. A rejected
/// proposal was never queued, so retrying cannot double-apply.
pub(crate) async fn propose_eventually(runtime: &SorockRuntime, value: u64) {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match runtime.propose(value.to_le_bytes().to_vec()).await {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "proposal of {value} did not succeed: {error}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}
