//! JetStream KV durable flow state with a revision-safe type index.

use std::{
    error::Error as _,
    time::{Duration, SystemTime},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackSerialize, MemoryPackSerializer, MemoryPackable,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowState, FlowStatus, FlowStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::record::{create_record, decode_record};

const MAX_CAS_RETRIES: usize = 8;
const MAX_INDEX_PAGE_ENTRIES: usize = 32;

#[derive(Clone, Copy, Debug, Default, Deserialize, MemoryPackable, Serialize)]
struct TypeIndex {
    tail_page: u64,
    scan_page: u64,
    scan_offset: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, MemoryPackable, Serialize)]
struct IndexMarker {
    page: u64,
}

enum IndexedFlow {
    Candidate(Box<str>),
    Advanced,
    Absent,
}

/// A JetStream KV-backed flow store with a paged, compact flow-type index.
///
/// Flow identities and types are SHA-256-derived keys. Each type page has a fixed number of
/// identities, and a revision-safe cursor visits one candidate at a time. This bounds recovery
/// work while eventually revisiting every indexed flow without scanning the state bucket. Terminal
/// and dangling identities are pruned best-effort, so cleanup races never change a committed flow
/// transition into an error.
pub struct NatsFlows {
    states: kv::Store,
    index: kv::Store,
}

impl NatsFlows {
    /// Connects to `server`, provisioning a state bucket named `bucket` and its type index.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let states = open_bucket(&context, bucket.as_ref()).await?;
        let index = open_bucket(&context, &format!("{bucket}_IDX")).await?;
        Ok(Self { states, index })
    }

    async fn state_entry(&self, id: &str) -> CatgaResult<Option<kv::Entry>> {
        self.states.entry(&flow_key(id)).await.map_err(map_error)
    }

    async fn get_state(&self, id: &str) -> CatgaResult<Option<(kv::Entry, FlowState)>> {
        let Some(entry) = self.state_entry(id).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        let state = decode_flow_state(decode_record(&entry.value)?.payload())?;
        Ok(Some((entry, state)))
    }

    async fn index_flow(&self, flow_type: &str, id: &str) -> CatgaResult<()> {
        let metadata_key = type_metadata_key(flow_type);
        let marker_key = type_marker_key(flow_type, id);
        for _ in 0..MAX_CAS_RETRIES {
            let entry = self.index.entry(&metadata_key).await.map_err(map_error)?;
            let Some(entry) = entry else {
                if create(&self.index, &metadata_key, &TypeIndex::default()).await? {
                    continue;
                }
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                if create(&self.index, &metadata_key, &TypeIndex::default()).await? {
                    continue;
                }
                continue;
            }
            let metadata_record = decode_record(&entry.value)?;
            let metadata = decode::<TypeIndex>(metadata_record.payload())?;
            let marker_entry = self.index.entry(&marker_key).await.map_err(map_error)?;
            let Some(marker_entry) = marker_entry else {
                if create(
                    &self.index,
                    &marker_key,
                    &IndexMarker {
                        page: metadata.tail_page,
                    },
                )
                .await?
                {
                    continue;
                }
                continue;
            };
            if matches!(
                marker_entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                if create(
                    &self.index,
                    &marker_key,
                    &IndexMarker {
                        page: metadata.tail_page,
                    },
                )
                .await?
                {
                    continue;
                }
                continue;
            }
            let marker_record = decode_record(&marker_entry.value)?;
            let marker = decode::<IndexMarker>(marker_record.payload())?;
            let page_key = type_page_key(flow_type, marker.page);
            let page = self.index.entry(&page_key).await.map_err(map_error)?;
            let Some(page) = page else {
                if create(&self.index, &page_key, &vec![Box::<str>::from(id)]).await? {
                    return Ok(());
                }
                continue;
            };
            if matches!(page.operation, kv::Operation::Delete | kv::Operation::Purge) {
                if create(&self.index, &page_key, &vec![Box::<str>::from(id)]).await? {
                    return Ok(());
                }
                continue;
            }
            let page_record = decode_record(&page.value)?;
            let mut values = decode::<Vec<Box<str>>>(page_record.payload())?;
            validate_index_page(&values)?;
            if values.iter().any(|value| value.as_ref() == id) {
                return Ok(());
            }
            if values.len() < MAX_INDEX_PAGE_ENTRIES {
                values.push(id.into());
                if compare_and_set(
                    &self.index,
                    &page_key,
                    page_record.with_payload(&encode(&values)?),
                    page.revision,
                )
                .await?
                {
                    return Ok(());
                }
                continue;
            }
            if marker.page != metadata.tail_page {
                let next = IndexMarker {
                    page: metadata.tail_page,
                };
                if compare_and_set(
                    &self.index,
                    &marker_key,
                    marker_record.with_payload(&encode(&next)?),
                    marker_entry.revision,
                )
                .await?
                {
                    continue;
                }
                continue;
            }
            let next_page = metadata.tail_page.checked_add(1).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Transient,
                    "NATS flow type index page limit reached",
                )
            })?;
            let next = TypeIndex {
                tail_page: next_page,
                ..metadata
            };
            if compare_and_set(
                &self.index,
                &metadata_key,
                metadata_record.with_payload(&encode(&next)?),
                entry.revision,
            )
            .await?
            {
                continue;
            }
        }
        Err(cas_error("index flow"))
    }

    async fn next_indexed_flow(&self, flow_type: &str) -> CatgaResult<IndexedFlow> {
        let metadata_key = type_metadata_key(flow_type);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.index.entry(&metadata_key).await.map_err(map_error)? else {
                return Ok(IndexedFlow::Absent);
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(IndexedFlow::Absent);
            }
            let metadata_record = decode_record(&entry.value)?;
            let metadata = decode::<TypeIndex>(metadata_record.payload())?;
            let page_key = type_page_key(flow_type, metadata.scan_page);
            let page = self.index.entry(&page_key).await.map_err(map_error)?;
            let candidate = match page {
                Some(page) if matches!(page.operation, kv::Operation::Put) => {
                    let page_record = decode_record(&page.value)?;
                    let ids = decode::<Vec<Box<str>>>(page_record.payload())?;
                    validate_index_page(&ids)?;
                    let offset = usize::try_from(metadata.scan_offset).map_err(|_| {
                        CatgaError::new(ErrorCode::Internal, "NATS flow index offset is invalid")
                    })?;
                    ids.get(offset).cloned()
                }
                _ => None,
            };
            let next = next_index_cursor(&metadata, candidate.is_some())?;
            if compare_and_set(
                &self.index,
                &metadata_key,
                metadata_record.with_payload(&encode(&next)?),
                entry.revision,
            )
            .await?
            {
                return Ok(match candidate {
                    Some(candidate) => IndexedFlow::Candidate(candidate),
                    None => IndexedFlow::Advanced,
                });
            }
        }
        Err(cas_error("advance flow index cursor"))
    }

    async fn prune_index(&self, flow_type: &str, id: &str) -> CatgaResult<()> {
        let marker_key = type_marker_key(flow_type, id);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(marker_entry) = self.index.entry(&marker_key).await.map_err(map_error)? else {
                return Ok(());
            };
            if matches!(
                marker_entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(());
            }
            let marker_record = decode_record(&marker_entry.value)?;
            let marker = decode::<IndexMarker>(marker_record.payload())?;
            let page_key = type_page_key(flow_type, marker.page);
            let page = self.index.entry(&page_key).await.map_err(map_error)?;
            if let Some(page) = page.filter(|page| matches!(page.operation, kv::Operation::Put)) {
                let page_record = decode_record(&page.value)?;
                let mut ids = decode::<Vec<Box<str>>>(page_record.payload())?;
                validate_index_page(&ids)?;
                let original_len = ids.len();
                ids.retain(|indexed_id| indexed_id.as_ref() != id);
                if ids.len() != original_len {
                    if compare_and_set(
                        &self.index,
                        &page_key,
                        page_record.with_payload(&encode(&ids)?),
                        page.revision,
                    )
                    .await?
                    {
                        continue;
                    }
                    continue;
                }
            }
            self.index
                .delete_expect_revision(&marker_key, Some(marker_entry.revision))
                .await
                .map_err(map_error)?;
            return Ok(());
        }
        Err(cas_error("prune flow index"))
    }

    async fn replace(
        &self,
        id: &str,
        expected_revision: u64,
        next: &FlowState,
    ) -> CatgaResult<bool> {
        compare_and_set(
            &self.states,
            &flow_key(id),
            encode(next)?,
            expected_revision,
        )
        .await
    }
}

#[async_trait]
impl FlowStore for NatsFlows {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        match self.get_state(state.id()).await? {
            Some((_, current)) if current.flow_type() == state.flow_type() => {
                if current.status().is_terminal() {
                    let _ = self.prune_index(current.flow_type(), current.id()).await;
                } else {
                    self.index_flow(current.flow_type(), current.id()).await?;
                }
                return Ok(false);
            }
            Some(_) => return Ok(false),
            None => {}
        }
        if !state.status().is_terminal() {
            self.index_flow(state.flow_type(), state.id()).await?;
        }
        let created = create(&self.states, &flow_key(state.id()), &state).await?;
        if !created
            && let Some((_, current)) = self.get_state(state.id()).await?
            && current.flow_type() == state.flow_type()
        {
            if current.status().is_terminal() {
                let _ = self.prune_index(current.flow_type(), current.id()).await;
            } else {
                self.index_flow(current.flow_type(), current.id()).await?;
            }
        }
        Ok(created)
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
        if !FlowState::is_next_version(expected_version, next.version()) {
            return Ok(false);
        }
        let Some((entry, current)) = self.get_state(next.id()).await? else {
            return Ok(false);
        };
        if current.version() != expected_version {
            return Ok(false);
        }
        let updated = self.replace(next.id(), entry.revision, &next).await?;
        if updated && next.status().is_terminal() {
            let _ = self.prune_index(next.flow_type(), next.id()).await;
        }
        Ok(updated)
    }

    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>> {
        self.get_state(id)
            .await
            .map(|value| value.map(|(_, state)| state))
    }

    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>> {
        let now = SystemTime::now();
        for _ in 0..MAX_INDEX_PAGE_ENTRIES {
            let id = match self.next_indexed_flow(flow_type).await? {
                IndexedFlow::Candidate(id) => id,
                IndexedFlow::Advanced => continue,
                IndexedFlow::Absent => return Ok(None),
            };
            let Some((entry, current)) = self.get_state(&id).await? else {
                let _ = self.prune_index(flow_type, &id).await;
                continue;
            };
            if current.flow_type() != flow_type || current.status().is_terminal() {
                let _ = self.prune_index(flow_type, &id).await;
                continue;
            }
            if current.status() != FlowStatus::Running
                || !is_stale(current.heartbeat(), now, stale_after)
            {
                continue;
            }
            let next = current.claimed_by(owner).next_version()?;
            if self.replace(&id, entry.revision, &next).await? {
                return Ok(Some(next));
            }
        }
        Ok(None)
    }

    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let Some((entry, current)) = self.get_state(id).await? else {
            return Ok(false);
        };
        if current.owner() != Some(owner) || current.version() != version {
            return Ok(false);
        }
        self.replace(
            id,
            entry.revision,
            &current.heartbeated_at(SystemTime::now()),
        )
        .await
    }
}

/// Opens a one-history KV bucket, tolerating a concurrent provisioner.
pub(crate) async fn open_bucket(
    context: &jetstream::Context,
    bucket: &str,
) -> CatgaResult<kv::Store> {
    crate::kv::open_or_create(context, bucket)
        .await
        .map_err(map_error)
}

async fn create<T: MemoryPackSerialize>(
    store: &kv::Store,
    key: &str,
    value: &T,
) -> CatgaResult<bool> {
    let record = create_record(&encode(value)?);
    match store.update(key, record.value().to_vec().into(), 0).await {
        Ok(_) => Ok(true),
        Err(error) if is_revision_conflict(&error) => Ok(false),
        Err(error) => {
            let reported = map_error(error);
            let committed = match store.entry(key).await {
                Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) => {
                    record.matches(&decode_record(&entry.value)?)
                }
                _ => false,
            };
            if committed { Ok(true) } else { Err(reported) }
        }
    }
}

async fn compare_and_set(
    store: &kv::Store,
    key: &str,
    value: Vec<u8>,
    revision: u64,
) -> CatgaResult<bool> {
    match store.update(key, value.clone().into(), revision).await {
        Ok(_) => Ok(true),
        Err(error) if is_revision_conflict(&error) => Ok(false),
        Err(error) => {
            let reported = map_error(error);
            let committed = matches!(store.entry(key).await, Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) && entry.value.as_ref() == value.as_slice());
            if committed { Ok(true) } else { Err(reported) }
        }
    }
}

fn flow_key(id: &str) -> String {
    format!("f{}", hex::encode(Sha256::digest(id.as_bytes())))
}
fn type_metadata_key(flow_type: &str) -> String {
    format!("m{}", hex::encode(Sha256::digest(flow_type.as_bytes())))
}
fn type_page_key(flow_type: &str, page: u64) -> String {
    format!(
        "p{}.{page}",
        hex::encode(Sha256::digest(flow_type.as_bytes()))
    )
}
fn type_marker_key(flow_type: &str, id: &str) -> String {
    format!(
        "i{}.{}",
        hex::encode(Sha256::digest(flow_type.as_bytes())),
        hex::encode(Sha256::digest(id.as_bytes()))
    )
}
fn next_index_cursor(metadata: &TypeIndex, consumed: bool) -> CatgaResult<TypeIndex> {
    if consumed {
        return Ok(TypeIndex {
            scan_offset: metadata.scan_offset.checked_add(1).ok_or_else(|| {
                CatgaError::new(ErrorCode::Internal, "NATS flow index offset overflowed")
            })?,
            ..*metadata
        });
    }
    let scan_page = if metadata.scan_page >= metadata.tail_page {
        0
    } else {
        metadata.scan_page.checked_add(1).ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "NATS flow index page overflowed")
        })?
    };
    Ok(TypeIndex {
        scan_page,
        scan_offset: 0,
        ..*metadata
    })
}
fn validate_index_page(ids: &[Box<str>]) -> CatgaResult<()> {
    if ids.len() > MAX_INDEX_PAGE_ENTRIES {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS flow index page exceeds its entry limit",
        ));
    }
    Ok(())
}
fn encode<T: MemoryPackSerialize>(value: &T) -> CatgaResult<Vec<u8>> {
    MemoryPackSerializer::serialize(value)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}
fn decode<T: MemoryPackDeserialize>(value: &[u8]) -> CatgaResult<T> {
    MemoryPackSerializer::deserialize(value)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}

fn decode_flow_state(value: &[u8]) -> CatgaResult<FlowState> {
    MemoryPackSerializer::deserialize_bounded(value, FlowState::memorypack_decode_limits()?)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}
fn is_revision_conflict(error: &kv::UpdateError) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<jetstream::context::PublishError>())
        .is_some_and(|source| {
            source.kind() == jetstream::context::PublishErrorKind::WrongLastSequence
        })
}
fn is_stale(heartbeat: SystemTime, now: SystemTime, stale_after: Duration) -> bool {
    now.duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}
fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("NATS flow {operation} compare-and-set did not stabilize"),
    )
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
