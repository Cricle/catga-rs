use crate::{CatgaResult, PersistentSubscription, SubscriptionCheckpoint, SubscriptionStore};
use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry};

/// A shard-locked, process-local persistent subscription and competing-lease store.
#[derive(Default)]
pub struct MemorySubscriptions {
    subscriptions: DashMap<Box<str>, PersistentSubscription>,
    checkpoints: DashMap<Box<str>, DashMap<Box<str>, SubscriptionCheckpoint>>,
    leases: DashMap<Box<str>, Box<str>>,
}

#[async_trait]
impl SubscriptionStore for MemorySubscriptions {
    async fn save(&self, subscription: PersistentSubscription) -> CatgaResult<()> {
        self.subscriptions
            .insert(subscription.name().into(), subscription);
        Ok(())
    }

    async fn load(&self, name: &str) -> CatgaResult<Option<PersistentSubscription>> {
        Ok(self
            .subscriptions
            .get(name)
            .map(|subscription| subscription.clone()))
    }

    async fn delete(&self, name: &str) -> CatgaResult<()> {
        self.subscriptions.remove(name);
        self.checkpoints.remove(name);
        self.leases.remove(name);
        Ok(())
    }

    async fn list(&self) -> CatgaResult<Vec<PersistentSubscription>> {
        let mut subscriptions: Vec<_> = self
            .subscriptions
            .iter()
            .map(|subscription| subscription.clone())
            .collect();
        subscriptions.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        Ok(subscriptions)
    }

    async fn save_checkpoint(&self, checkpoint: SubscriptionCheckpoint) -> CatgaResult<()> {
        self.checkpoints
            .entry(checkpoint.subscription_name().into())
            .or_default()
            .insert(checkpoint.stream_id().into(), checkpoint);
        Ok(())
    }

    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<SubscriptionCheckpoint>> {
        Ok(self
            .checkpoints
            .get(subscription_name)
            .and_then(|streams| streams.get(stream_id).map(|checkpoint| checkpoint.clone())))
    }

    async fn try_acquire(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<bool> {
        Ok(match self.leases.entry(subscription_name.into()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(consumer_id.into());
                true
            }
        })
    }

    async fn release(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<()> {
        if let Entry::Occupied(entry) = self.leases.entry(subscription_name.into())
            && entry.get().as_ref() == consumer_id
        {
            entry.remove();
        }
        Ok(())
    }
}
