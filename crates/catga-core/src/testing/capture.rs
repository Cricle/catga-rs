//! Concurrently safe message capture for test assertions.

use dashmap::DashMap;

/// Concurrently safe message capture for test assertions.
#[derive(Default)]
pub struct MessageCapture<T> {
    published: DashMap<u64, T>,
    consumed: DashMap<u64, T>,
    next: std::sync::atomic::AtomicU64,
}

impl<T> MessageCapture<T> {
    /// Records a published message.
    pub fn record_published(&self, value: T) {
        self.published.insert(
            self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            value,
        );
    }
    /// Records a consumed message.
    pub fn record_consumed(&self, value: T) {
        self.consumed.insert(
            self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            value,
        );
    }
    /// Returns published values in recording order.
    pub fn published(&self) -> Vec<T>
    where
        T: Clone,
    {
        captured_in_order(&self.published)
    }

    /// Returns consumed values in recording order.
    pub fn consumed(&self) -> Vec<T>
    where
        T: Clone,
    {
        captured_in_order(&self.consumed)
    }

    /// Clears captured values without resetting the concurrent sequence source.
    pub fn clear(&self) {
        self.published.clear();
        self.consumed.clear();
    }
}

fn captured_in_order<T>(values: &DashMap<u64, T>) -> Vec<T>
where
    T: Clone,
{
    let mut values: Vec<_> = values
        .iter()
        .map(|entry| (*entry.key(), entry.value().clone()))
        .collect();
    values.sort_unstable_by_key(|(sequence, _)| *sequence);
    values.into_iter().map(|(_, value)| value).collect()
}
