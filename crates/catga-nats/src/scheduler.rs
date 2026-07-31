//! JetStream KV-backed durable flow-resume scheduling.

use std::{
    error::Error as _,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackSerialize, MemoryPackSerializer, MemoryPackable,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{DueFlowScheduler, FlowScheduler, ScheduledResume};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::record::{create_record, decode_record};

const MAX_CAS_RETRIES: usize = 8;
const MAX_INDEX_PAGE_ENTRIES: usize = 32;
const MAX_CLAIM_SCAN_ENTRIES: usize = MAX_INDEX_PAGE_ENTRIES;
const RECORD_VERSION: u8 = 1;

#[derive(Clone, Debug, Deserialize, MemoryPackable, Serialize)]
struct StoredSchedule {
    version: u8,
    schedule_id: Box<str>,
    flow_id: Box<str>,
    state_id: Box<str>,
    due_at_millis: u64,
    owner: Option<Box<str>>,
    lease_until_millis: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, MemoryPackable, Serialize)]
struct ScheduleIndex {
    tail_page: u64,
    scan_page: u64,
    scan_offset: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, MemoryPackable, Serialize)]
struct IndexMarker {
    page: u64,
}

enum ClaimAttempt {
    Claimed(ScheduledResume),
    Examined,
    Missing,
}

enum IndexCursorStep {
    Candidate(Box<str>),
    Advanced,
    Exhausted,
}

/// A bounded, at-least-once flow scheduler backed by JetStream key-value buckets.
///
/// The SHA-256 key for each flow-state target is both the schedule record key and the sole
/// uniqueness authority. A second bucket stores only a fixed-size, cursor-driven page index; its
/// stale entries are harmless because every claim reloads the authoritative record. Each
/// `claim_due` call inspects at most the smaller of its requested limit and 32 index entries.
/// Record transitions use JetStream expected-revision updates, so only the current owner can
/// acknowledge, release, or renew a lease.
pub struct NatsFlowScheduler {
    schedules: kv::Store,
    index: kv::Store,
}

impl NatsFlowScheduler {
    /// Connects to `server`, provisioning the schedule bucket named `bucket` and its index bucket.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let schedules = open_bucket(&context, bucket.as_ref()).await?;
        let index = open_bucket(&context, &format!("{bucket}_IDX")).await?;
        Ok(Self { schedules, index })
    }

    async fn load_schedule_by_key(
        &self,
        key: &str,
    ) -> CatgaResult<Option<(kv::Entry, StoredSchedule)>> {
        let Some(entry) = self.schedules.entry(key).await.map_err(map_error)? else {
            return Ok(None);
        };
        if !matches!(entry.operation, kv::Operation::Put) {
            return Ok(None);
        }
        let schedule = decode::<StoredSchedule>(decode_record(&entry.value)?.payload())?;
        if schedule.version != RECORD_VERSION || schedule_key(&schedule.schedule_id) != Some(key) {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "NATS scheduler record has an unsupported version or mismatched identity",
            ));
        }
        Ok(Some((entry, schedule)))
    }

    async fn index_schedule(&self, key: &str) -> CatgaResult<()> {
        let marker_key = marker_key(key);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(metadata_entry) = self.index.entry(metadata_key()).await.map_err(map_error)?
            else {
                if create(&self.index, metadata_key(), &ScheduleIndex::default()).await? {
                    continue;
                }
                continue;
            };
            if !matches!(metadata_entry.operation, kv::Operation::Put) {
                if create(&self.index, metadata_key(), &ScheduleIndex::default()).await? {
                    continue;
                }
                continue;
            }
            let metadata_record = decode_record(&metadata_entry.value)?;
            let metadata = decode::<ScheduleIndex>(metadata_record.payload())?;
            let marker = self.index.entry(&marker_key).await.map_err(map_error)?;
            let Some(marker) =
                marker.filter(|marker| matches!(marker.operation, kv::Operation::Put))
            else {
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
            let marker_record = decode_record(&marker.value)?;
            let marker_value = decode::<IndexMarker>(marker_record.payload())?;
            let page_key = page_key(marker_value.page);
            let page = self.index.entry(&page_key).await.map_err(map_error)?;
            let Some(page) = page.filter(|page| matches!(page.operation, kv::Operation::Put))
            else {
                if create(&self.index, &page_key, &vec![Box::<str>::from(key)]).await? {
                    return Ok(());
                }
                continue;
            };
            let page_record = decode_record(&page.value)?;
            let mut values = decode::<Vec<Box<str>>>(page_record.payload())?;
            validate_page(&values)?;
            if values.iter().any(|value| value.as_ref() == key) {
                return Ok(());
            }
            if values.len() < MAX_INDEX_PAGE_ENTRIES {
                values.push(key.into());
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
            if marker_value.page != metadata.tail_page {
                let next_marker = IndexMarker {
                    page: metadata.tail_page,
                };
                if compare_and_set(
                    &self.index,
                    &marker_key,
                    marker_record.with_payload(&encode(&next_marker)?),
                    marker.revision,
                )
                .await?
                {
                    continue;
                }
                continue;
            }
            let next_tail = metadata.tail_page.checked_add(1).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Transient,
                    "NATS scheduler index page limit reached",
                )
            })?;
            let next = ScheduleIndex {
                tail_page: next_tail,
                ..metadata
            };
            if compare_and_set(
                &self.index,
                metadata_key(),
                metadata_record.with_payload(&encode(&next)?),
                metadata_entry.revision,
            )
            .await?
            {
                continue;
            }
        }
        Err(cas_error("index schedule"))
    }

    async fn next_indexed_schedule(&self) -> CatgaResult<IndexCursorStep> {
        for _ in 0..MAX_CAS_RETRIES {
            let Some(metadata_entry) = self.index.entry(metadata_key()).await.map_err(map_error)?
            else {
                return Ok(IndexCursorStep::Exhausted);
            };
            if !matches!(metadata_entry.operation, kv::Operation::Put) {
                return Ok(IndexCursorStep::Exhausted);
            }
            let metadata_record = decode_record(&metadata_entry.value)?;
            let metadata = decode::<ScheduleIndex>(metadata_record.payload())?;
            let page = self
                .index
                .entry(page_key(metadata.scan_page))
                .await
                .map_err(map_error)?;
            let candidate = match page.filter(|page| matches!(page.operation, kv::Operation::Put)) {
                Some(page) => {
                    let page_record = decode_record(&page.value)?;
                    let values = decode::<Vec<Box<str>>>(page_record.payload())?;
                    validate_page(&values)?;
                    values
                        .get(usize::try_from(metadata.scan_offset).map_err(|_| {
                            CatgaError::new(
                                ErrorCode::Internal,
                                "NATS scheduler index offset is invalid",
                            )
                        })?)
                        .cloned()
                }
                None => None,
            };
            let next = next_cursor(&metadata, candidate.is_some())?;
            if compare_and_set(
                &self.index,
                metadata_key(),
                metadata_record.with_payload(&encode(&next)?),
                metadata_entry.revision,
            )
            .await?
            {
                return Ok(match candidate {
                    Some(schedule_id) => IndexCursorStep::Candidate(schedule_id),
                    None => IndexCursorStep::Advanced,
                });
            }
        }
        Err(cas_error("advance schedule index cursor"))
    }

    async fn claim_schedule(
        &self,
        key: &str,
        owner: &str,
        now_millis: u64,
        lease_until_millis: u64,
    ) -> CatgaResult<ClaimAttempt> {
        let Some((entry, mut schedule)) = self.load_schedule_by_key(key).await? else {
            return Ok(ClaimAttempt::Missing);
        };
        if schedule.due_at_millis > now_millis
            || schedule
                .lease_until_millis
                .is_some_and(|lease_until| lease_until > now_millis)
        {
            return Ok(ClaimAttempt::Examined);
        }
        schedule.owner = Some(owner.into());
        schedule.lease_until_millis = Some(lease_until_millis);
        let value = decode_record(&entry.value)?.with_payload(&encode(&schedule)?);
        if !compare_and_set(&self.schedules, key, value, entry.revision).await? {
            return Ok(ClaimAttempt::Examined);
        }
        Ok(ClaimAttempt::Claimed(ScheduledResume::new(
            schedule.schedule_id,
            schedule.flow_id,
            schedule.state_id,
            from_millis(schedule.due_at_millis)?,
        )))
    }
}

#[async_trait]
impl FlowScheduler for NatsFlowScheduler {
    async fn schedule_resume(
        &self,
        flow_id: &str,
        state_id: &str,
        due_at: SystemTime,
    ) -> CatgaResult<Box<str>> {
        let due_at_millis = to_millis(due_at)?;
        let key = target_record_key(&target_bytes(flow_id, state_id)?);
        let schedule_id: Box<str> = format!("{key}:{}", Uuid::new_v4()).into();
        self.index_schedule(&key).await?;
        let schedule = StoredSchedule {
            version: RECORD_VERSION,
            schedule_id: schedule_id.clone(),
            flow_id: flow_id.into(),
            state_id: state_id.into(),
            due_at_millis,
            owner: None,
            lease_until_millis: None,
        };
        if create_or_restore(&self.schedules, &key, &schedule).await? {
            Ok(schedule_id)
        } else {
            Err(CatgaError::new(
                ErrorCode::Conflict,
                "a resume is already scheduled for this flow state",
            ))
        }
    }

    async fn cancel_resume(&self, schedule_id: &str) -> CatgaResult<bool> {
        let Some(key) = schedule_key(schedule_id) else {
            return Ok(false);
        };
        let Some((entry, schedule)) = self.load_schedule_by_key(key).await? else {
            return Ok(false);
        };
        if schedule.schedule_id.as_ref() != schedule_id || schedule.owner.is_some() {
            return Ok(false);
        }
        if !delete_if_revision(&self.schedules, key, entry.revision).await? {
            return Ok(false);
        }
        Ok(true)
    }
}

#[async_trait]
impl DueFlowScheduler for NatsFlowScheduler {
    async fn claim_due(
        &self,
        owner: &str,
        now: SystemTime,
        lease_for: Duration,
        limit: usize,
    ) -> CatgaResult<Vec<ScheduledResume>> {
        if lease_for.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due-work lease duration must be greater than zero",
            ));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now_millis = to_millis(now)?;
        let lease_until_millis = now_millis
            .checked_add(duration_millis(lease_for)?)
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "due-work lease exceeds Unix time")
            })?;
        let scan_limit = limit.min(MAX_CLAIM_SCAN_ENTRIES);
        let mut claimed = Vec::with_capacity(scan_limit);
        let mut inspected = 0_usize;
        for _ in 0..MAX_CLAIM_SCAN_ENTRIES {
            if inspected >= scan_limit || claimed.len() >= scan_limit {
                break;
            }
            let key = match self.next_indexed_schedule().await? {
                IndexCursorStep::Candidate(key) => key,
                IndexCursorStep::Advanced => continue,
                IndexCursorStep::Exhausted => break,
            };
            inspected = inspected.saturating_add(1);
            match self
                .claim_schedule(&key, owner, now_millis, lease_until_millis)
                .await?
            {
                ClaimAttempt::Claimed(schedule) => claimed.push(schedule),
                ClaimAttempt::Examined => {}
                ClaimAttempt::Missing => {}
            }
        }
        Ok(claimed)
    }

    async fn ack_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        let Some(key) = schedule_key(schedule_id) else {
            return Ok(false);
        };
        let Some((entry, schedule)) = self.load_schedule_by_key(key).await? else {
            return Ok(false);
        };
        if schedule.schedule_id.as_ref() != schedule_id || schedule.owner.as_deref() != Some(owner)
        {
            return Ok(false);
        }
        if !delete_if_revision(&self.schedules, key, entry.revision).await? {
            return Ok(false);
        }
        Ok(true)
    }

    async fn release_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        let Some(key) = schedule_key(schedule_id) else {
            return Ok(false);
        };
        let Some((entry, mut schedule)) = self.load_schedule_by_key(key).await? else {
            return Ok(false);
        };
        if schedule.schedule_id.as_ref() != schedule_id || schedule.owner.as_deref() != Some(owner)
        {
            return Ok(false);
        }
        schedule.owner = None;
        schedule.lease_until_millis = None;
        let value = decode_record(&entry.value)?.with_payload(&encode(&schedule)?);
        compare_and_set(&self.schedules, key, value, entry.revision).await
    }

    async fn renew_due(
        &self,
        owner: &str,
        schedule_id: &str,
        now: SystemTime,
        lease_for: Duration,
    ) -> CatgaResult<bool> {
        if lease_for.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due-work lease duration must be greater than zero",
            ));
        }
        let now_millis = to_millis(now)?;
        let lease_until_millis = now_millis
            .checked_add(duration_millis(lease_for)?)
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "due-work lease exceeds Unix time")
            })?;
        let Some(key) = schedule_key(schedule_id) else {
            return Ok(false);
        };
        let Some((entry, mut schedule)) = self.load_schedule_by_key(key).await? else {
            return Ok(false);
        };
        if schedule.schedule_id.as_ref() != schedule_id || schedule.owner.as_deref() != Some(owner)
        {
            return Ok(false);
        }
        if schedule
            .lease_until_millis
            .is_none_or(|lease_until| lease_until <= now_millis)
        {
            return Ok(false);
        }
        schedule.lease_until_millis = Some(lease_until_millis);
        let value = decode_record(&entry.value)?.with_payload(&encode(&schedule)?);
        compare_and_set(&self.schedules, key, value, entry.revision).await
    }
}

async fn open_bucket(context: &jetstream::Context, bucket: &str) -> CatgaResult<kv::Store> {
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

async fn create_or_restore<T: MemoryPackSerialize>(
    store: &kv::Store,
    key: &str,
    value: &T,
) -> CatgaResult<bool> {
    let Some(entry) = store.entry(key).await.map_err(map_error)? else {
        return create(store, key, value).await;
    };
    if matches!(entry.operation, kv::Operation::Put) {
        return Ok(false);
    }
    let record = create_record(&encode(value)?);
    compare_and_set(store, key, record.value().to_vec(), entry.revision).await
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

async fn delete_if_revision(store: &kv::Store, key: &str, revision: u64) -> CatgaResult<bool> {
    match store.delete_expect_revision(key, Some(revision)).await {
        Ok(()) => Ok(true),
        Err(error) if is_revision_conflict(&error) => Ok(false),
        Err(error) => {
            let reported = map_error(error);
            let deleted = !matches!(store.entry(key).await, Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put));
            if deleted { Ok(true) } else { Err(reported) }
        }
    }
}

fn target_record_key(target: &[u8]) -> String {
    format!("r{}", hex::encode(Sha256::digest(target)))
}

fn schedule_key(schedule_id: &str) -> Option<&str> {
    let (key, generation) = schedule_id.rsplit_once(':')?;
    if key.len() != 65
        || !key.starts_with('r')
        || !key[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
        || Uuid::parse_str(generation).is_err()
    {
        return None;
    }
    Some(key)
}

const fn metadata_key() -> &'static str {
    "m"
}

fn page_key(page: u64) -> String {
    format!("p{page}")
}

fn marker_key(key: &str) -> String {
    format!("i{}", hex::encode(Sha256::digest(key.as_bytes())))
}

fn target_bytes(flow_id: &str, state_id: &str) -> CatgaResult<Vec<u8>> {
    let flow_len = u64::try_from(flow_id.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "flow identifier is too long for NATS",
        )
    })?;
    let state_len = u64::try_from(state_id.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "state identifier is too long for NATS",
        )
    })?;
    let capacity = 16_usize
        .checked_add(flow_id.len())
        .and_then(|value| value.checked_add(state_id.len()))
        .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "scheduler target is too long"))?;
    let mut target = Vec::with_capacity(capacity);
    target.extend_from_slice(&flow_len.to_be_bytes());
    target.extend_from_slice(flow_id.as_bytes());
    target.extend_from_slice(&state_len.to_be_bytes());
    target.extend_from_slice(state_id.as_bytes());
    Ok(target)
}

fn next_cursor(index: &ScheduleIndex, consumed: bool) -> CatgaResult<ScheduleIndex> {
    if consumed {
        return Ok(ScheduleIndex {
            scan_offset: index.scan_offset.checked_add(1).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "NATS scheduler index offset overflowed",
                )
            })?,
            ..*index
        });
    }
    let scan_page = if index.scan_page >= index.tail_page {
        0
    } else {
        index.scan_page.checked_add(1).ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "NATS scheduler index page overflowed")
        })?
    };
    Ok(ScheduleIndex {
        scan_page,
        scan_offset: 0,
        ..*index
    })
}

fn validate_page(values: &[Box<str>]) -> CatgaResult<()> {
    if values.len() > MAX_INDEX_PAGE_ENTRIES {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS scheduler index page exceeds its entry limit",
        ));
    }
    Ok(())
}

fn to_millis(value: SystemTime) -> CatgaResult<u64> {
    let elapsed = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "due time precedes the Unix epoch"))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "due time exceeds NATS range"))
}

fn duration_millis(value: Duration) -> CatgaResult<u64> {
    u64::try_from(value.as_millis())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "lease duration exceeds NATS range"))
}

fn from_millis(value: u64) -> CatgaResult<SystemTime> {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(value))
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "NATS scheduler due time is out of range",
            )
        })
}

fn encode<T: MemoryPackSerialize>(value: &T) -> CatgaResult<Vec<u8>> {
    MemoryPackSerializer::serialize(value)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}

fn decode<T: MemoryPackDeserialize>(value: &[u8]) -> CatgaResult<T> {
    MemoryPackSerializer::deserialize(value)
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

fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("NATS scheduler {operation} compare-and-set did not stabilize"),
    )
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
