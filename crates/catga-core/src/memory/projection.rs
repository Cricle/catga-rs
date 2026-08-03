use crate::{CatgaResult, ProjectionCheckpoint, ProjectionCheckpointStore};
use async_trait::async_trait;
use dashmap::DashMap;

/// A shard-locked, process-local store of immutable projection checkpoints.
#[derive(Default)]
pub struct MemoryProjectionCheckpoints {
    projections: DashMap<Box<str>, DashMap<Box<str>, ProjectionCheckpoint>>,
}

#[async_trait]
impl ProjectionCheckpointStore for MemoryProjectionCheckpoints {
    async fn save(&self, checkpoint: ProjectionCheckpoint) -> CatgaResult<()> {
        self.projections
            .entry(checkpoint.projection_name().into())
            .or_default()
            .insert(checkpoint.stream_id().into(), checkpoint);
        Ok(())
    }

    async fn load(
        &self,
        projection_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<ProjectionCheckpoint>> {
        Ok(self
            .projections
            .get(projection_name)
            .and_then(|streams| streams.get(stream_id).map(|checkpoint| checkpoint.clone())))
    }

    async fn delete(&self, projection_name: &str, stream_id: &str) -> CatgaResult<()> {
        if let Some(streams) = self.projections.get(projection_name) {
            streams.remove(stream_id);
        }
        Ok(())
    }

    async fn delete_all(&self, projection_name: &str) -> CatgaResult<()> {
        self.projections.remove(projection_name);
        Ok(())
    }
}
