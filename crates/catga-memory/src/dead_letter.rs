use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, DeadLetter, DeadLetterStore, ErrorCode};
use dashmap::DashMap;

/// A bounded process-local dead-letter queue for development and deterministic tests.
pub struct MemoryDeadLetters {
    capacity: usize,
    used: AtomicUsize,
    next_id: AtomicU64,
    letters: DashMap<u64, DeadLetter>,
}

impl MemoryDeadLetters {
    /// Creates a bounded queue that rejects writes once it reaches `capacity`.
    pub fn new(capacity: usize) -> CatgaResult<Self> {
        if capacity == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "dead-letter capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            used: AtomicUsize::new(0),
            next_id: AtomicU64::new(0),
            letters: DashMap::with_capacity(capacity),
        })
    }
}

#[async_trait]
impl DeadLetterStore for MemoryDeadLetters {
    async fn enqueue(&self, letter: DeadLetter) -> CatgaResult<()> {
        if self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (used < self.capacity).then_some(used + 1)
            })
            .is_err()
        {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "dead-letter capacity is exhausted",
            ));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.letters.insert(id, letter);
        Ok(())
    }

    async fn list(&self, limit: usize) -> CatgaResult<Vec<DeadLetter>> {
        let mut letters: Vec<_> = self
            .letters
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        letters.sort_unstable_by_key(|(id, _)| *id);
        Ok(letters
            .into_iter()
            .take(limit)
            .map(|(_, letter)| letter)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryDeadLetters;
    use catga_core::{
        DeadLetter, DeadLetterDiagnostics, DeadLetterStore, Envelope, ErrorCode, MessageMetadata,
    };

    #[tokio::test]
    async fn memory_dead_letters_preserve_failure_diagnostics() -> catga_core::CatgaResult<()> {
        let store = MemoryDeadLetters::new(1)?;
        let envelope = Envelope::new(
            3,
            "tests.dead-letter",
            vec![],
            MessageMetadata::new(3, None),
        );
        let diagnostics =
            DeadLetterDiagnostics::try_at(123, ErrorCode::Timeout, "consumer.handle")?;
        let letter = DeadLetter::try_with_diagnostics(envelope, "expired", 2, diagnostics)?;

        store.enqueue(letter).await?;
        let retained = store.list(1).await?;
        assert_eq!(retained[0].diagnostics().failed_at_unix_ms(), 123);
        assert_eq!(retained[0].diagnostics().error_code(), ErrorCode::Timeout);
        assert_eq!(retained[0].diagnostics().stage(), "consumer.handle");
        Ok(())
    }
}
