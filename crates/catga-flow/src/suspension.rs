use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};
use catga_core::CatgaError;
use catga_core::{CatgaResult, ErrorCode};
use serde::{Deserialize, Serialize};

use crate::{
    FlowState,
    memorypack::{
        DurationWire, ErrorWire, TimeWire, decode_duration, decode_error, decode_time,
        encode_duration, encode_error, encode_time,
    },
};

/// The policy used to decide when a wait condition is complete.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WaitPolicy {
    /// Every expected child must succeed.
    All,
    /// The first successful child completes the condition.
    Any,
}

/// Maximum number of durable child identities or retained results in one wait condition.
pub const MAX_WAIT_CHILDREN: usize = 1_024;
/// Maximum byte length of one successful child payload retained by a wait condition.
pub const MAX_WAIT_RESULT_BYTES: usize = 64 * 1_024;
/// Maximum successful rollback-capable steps retained by one durable flow.
pub const MAX_FLOW_COMPENSATIONS: usize = 1_024;

/// Persisted state of one stable child-launch intent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FlowChildLaunchState {
    /// No process currently owns launching this child.
    Pending,
    /// One runtime holds the launch claim until `expires_at`.
    Claimed {
        /// Runtime owner that obtained the claim.
        owner: Box<str>,
        /// Wall-clock deadline after which another runtime may retry the stable child identity.
        expires_at: SystemTime,
    },
    /// A launcher successfully accepted the stable child identity.
    Launched,
}

/// One bounded, stable child identity retained by a durable wait condition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FlowChildLaunch {
    child_id: Box<str>,
    state: FlowChildLaunchState,
}

impl FlowChildLaunch {
    fn pending(child_id: impl Into<Box<str>>) -> Self {
        Self {
            child_id: child_id.into(),
            state: FlowChildLaunchState::Pending,
        }
    }

    /// Returns the stable application-supplied child identity.
    pub fn child_id(&self) -> &str {
        &self.child_id
    }

    /// Returns the persisted launch state.
    pub const fn state(&self) -> &FlowChildLaunchState {
        &self.state
    }
}

/// One immutable child result recorded against a wait condition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WaitResult {
    child_id: Box<str>,
    payload: Option<Arc<[u8]>>,
    error: Option<CatgaError>,
}

impl WaitResult {
    fn success(child_id: impl Into<Box<str>>, payload: impl Into<Arc<[u8]>>) -> Self {
        Self {
            child_id: child_id.into(),
            payload: Some(payload.into()),
            error: None,
        }
    }

    fn failure(child_id: impl Into<Box<str>>, error: CatgaError) -> Self {
        Self {
            child_id: child_id.into(),
            payload: None,
            error: Some(error),
        }
    }

    /// Returns the child identity that produced this result.
    pub fn child_id(&self) -> &str {
        &self.child_id
    }

    /// Returns whether the child completed successfully.
    pub const fn is_success(&self) -> bool {
        self.error.is_none()
    }

    /// Returns the successful result payload without copying it.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// Returns shared ownership of the successful payload.
    pub fn shared_payload(&self) -> Option<Arc<[u8]>> {
        self.payload.as_ref().map(Arc::clone)
    }

    /// Returns the failure associated with this child, when any.
    pub fn error(&self) -> Option<&CatgaError> {
        self.error.as_ref()
    }
}

/// Immutable persisted state for a set of child-flow results.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WaitCondition {
    correlation_id: Box<str>,
    policy: WaitPolicy,
    expected_count: u32,
    results: Arc<[WaitResult]>,
    created_at: SystemTime,
    timeout: Duration,
    child_launches: Arc<[FlowChildLaunch]>,
}

impl WaitCondition {
    /// Creates an empty condition waiting for `expected_count` distinct child results.
    pub fn new(
        correlation_id: impl Into<Box<str>>,
        policy: WaitPolicy,
        expected_count: u32,
        created_at: SystemTime,
        timeout: Duration,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            policy,
            expected_count,
            results: Arc::from([]),
            created_at,
            timeout,
            child_launches: Arc::from([]),
        }
    }

    /// Creates a wait condition that launches exactly the supplied stable child identities.
    ///
    /// Child identities are persisted before launch. They must be unique, non-empty, and no more
    /// than [`MAX_WAIT_CHILDREN`]. A caller can recover a parent after a crash by invoking
    /// [`crate::FlowRuntime::launch_waiting_children`] again with an idempotent launcher.
    pub fn for_children<I, Id>(
        correlation_id: impl Into<Box<str>>,
        policy: WaitPolicy,
        child_ids: I,
        created_at: SystemTime,
        timeout: Duration,
    ) -> CatgaResult<Self>
    where
        I: IntoIterator<Item = Id>,
        Id: Into<Box<str>>,
    {
        let mut children = Vec::new();
        for child_id in child_ids {
            let child_id = child_id.into();
            if child_id.is_empty()
                || children
                    .iter()
                    .any(|child: &FlowChildLaunch| child.child_id == child_id)
            {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "flow child identities must be non-empty and unique",
                ));
            }
            if children.len() == MAX_WAIT_CHILDREN {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "flow child wait exceeds the supported child limit",
                ));
            }
            children.push(FlowChildLaunch::pending(child_id));
        }
        if children.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "a durable child wait requires at least one child identity",
            ));
        }
        Ok(Self {
            correlation_id: correlation_id.into(),
            policy,
            expected_count: u32::try_from(children.len()).unwrap_or(u32::MAX),
            results: Arc::from([]),
            created_at,
            timeout,
            child_launches: Arc::from(children),
        })
    }

    /// Returns the stable condition identity.
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    /// Returns the configured completion policy.
    pub const fn policy(&self) -> WaitPolicy {
        self.policy
    }

    /// Returns the number of distinct child results expected.
    pub const fn expected_count(&self) -> u32 {
        self.expected_count
    }

    /// Returns the number of distinct child results recorded.
    pub fn completed_count(&self) -> u32 {
        u32::try_from(self.results.len()).unwrap_or(u32::MAX)
    }

    /// Returns recorded child results in insertion order.
    pub fn results(&self) -> &[WaitResult] {
        &self.results
    }

    /// Returns stable child launch intents, or an empty slice for an externally completed wait.
    pub fn child_launches(&self) -> &[FlowChildLaunch] {
        &self.child_launches
    }

    /// Returns when the condition was created.
    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns the maximum time child results may take.
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns whether the condition is expired at `now`.
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        now.duration_since(self.created_at)
            .is_ok_and(|elapsed| elapsed >= self.timeout)
    }

    /// Adds a successful child result unless the child has already reported.
    pub fn record_success(
        &self,
        child_id: impl Into<Box<str>>,
        payload: impl Into<Arc<[u8]>>,
    ) -> Self {
        let child_id = child_id.into();
        let payload: Arc<[u8]> = payload.into();
        if payload.len() > MAX_WAIT_RESULT_BYTES
            || self.results.len() >= MAX_WAIT_CHILDREN
            || self.results.len() >= usize::try_from(self.expected_count).unwrap_or(usize::MAX)
            || !self.accepts_child(&child_id)
            || self
                .results
                .iter()
                .any(|result| result.child_id == child_id)
        {
            return self.clone();
        }
        let mut results = Vec::with_capacity(self.results.len().saturating_add(1));
        results.extend(self.results.iter().cloned());
        results.push(WaitResult::success(child_id, payload));
        Self {
            results: Arc::from(results),
            ..self.clone()
        }
    }

    /// Adds a failed child result unless the child has already reported.
    pub fn record_failure(&self, child_id: impl Into<Box<str>>, error: CatgaError) -> Self {
        let child_id = child_id.into();
        if self.results.len() >= MAX_WAIT_CHILDREN
            || self.results.len() >= usize::try_from(self.expected_count).unwrap_or(usize::MAX)
            || !self.accepts_child(&child_id)
            || self
                .results
                .iter()
                .any(|result| result.child_id == child_id)
        {
            return self.clone();
        }
        let mut results = Vec::with_capacity(self.results.len().saturating_add(1));
        results.extend(self.results.iter().cloned());
        results.push(WaitResult::failure(child_id, error));
        Self {
            results: Arc::from(results),
            ..self.clone()
        }
    }

    /// Returns whether this wait accepts a completion from `child_id`.
    ///
    /// Generic external waits created with [`Self::new`] accept any child identity. Durable child
    /// fan-out accepts only its persisted child identities.
    pub fn accepts_child(&self, child_id: &str) -> bool {
        self.child_launches.is_empty()
            || self
                .child_launches
                .iter()
                .any(|child| child.child_id.as_ref() == child_id)
    }

    /// Returns whether `payload_len` is safe to retain for this condition.
    pub const fn accepts_payload_len(&self, payload_len: usize) -> bool {
        payload_len <= MAX_WAIT_RESULT_BYTES
    }

    pub(crate) fn claim_next_child(
        &self,
        owner: impl Into<Box<str>>,
        now: SystemTime,
        claim_for: Duration,
    ) -> Option<(Box<str>, Self)> {
        let expires_at = now.checked_add(claim_for)?;
        let owner = owner.into();
        let index = self
            .child_launches
            .iter()
            .position(|child| match child.state {
                FlowChildLaunchState::Pending => true,
                FlowChildLaunchState::Claimed { expires_at, .. } => expires_at <= now,
                FlowChildLaunchState::Launched => false,
            })?;
        let child_id = self.child_launches[index].child_id.clone();
        let mut children: Vec<FlowChildLaunch> = self.child_launches.iter().cloned().collect();
        children[index].state = FlowChildLaunchState::Claimed { owner, expires_at };
        Some((
            child_id,
            Self {
                child_launches: Arc::from(children),
                ..self.clone()
            },
        ))
    }

    pub(crate) fn mark_child_launched(&self, child_id: &str, owner: &str) -> Option<Self> {
        let index = self.child_launches.iter().position(|child| {
            child.child_id.as_ref() == child_id
                && matches!(
                    &child.state,
                    FlowChildLaunchState::Claimed {
                        owner: claim_owner,
                        ..
                    } if claim_owner.as_ref() == owner
                )
        })?;
        let mut children: Vec<FlowChildLaunch> = self.child_launches.iter().cloned().collect();
        children[index].state = FlowChildLaunchState::Launched;
        Some(Self {
            child_launches: Arc::from(children),
            ..self.clone()
        })
    }

    pub(crate) fn release_child_claim(&self, child_id: &str, owner: &str) -> Option<Self> {
        let index = self.child_launches.iter().position(|child| {
            child.child_id.as_ref() == child_id
                && matches!(
                    &child.state,
                    FlowChildLaunchState::Claimed {
                        owner: claim_owner,
                        ..
                    } if claim_owner.as_ref() == owner
                )
        })?;
        let mut children: Vec<FlowChildLaunch> = self.child_launches.iter().cloned().collect();
        children[index].state = FlowChildLaunchState::Pending;
        Some(Self {
            child_launches: Arc::from(children),
            ..self.clone()
        })
    }

    /// Validates the durable wait identity, expected child count, and bounded payload shape.
    pub fn validate(&self) -> CatgaResult<()> {
        if self.correlation_id.is_empty()
            || self.expected_count == 0
            || self.expected_count as usize > MAX_WAIT_CHILDREN
            || self.results.len() > MAX_WAIT_CHILDREN
            || self.results.len() > self.expected_count as usize
            || self.results.iter().any(|result| {
                result
                    .payload()
                    .is_some_and(|payload| payload.len() > MAX_WAIT_RESULT_BYTES)
            })
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow wait condition requires a correlation, at least one expected child, and supported bounds",
            ));
        }
        if !self.child_launches.is_empty()
            && (self.child_launches.len() > MAX_WAIT_CHILDREN
                || self.child_launches.len() != self.expected_count as usize
                || self
                    .child_launches
                    .iter()
                    .any(|child| child.child_id.is_empty())
                || self
                    .child_launches
                    .iter()
                    .enumerate()
                    .any(|(index, child)| {
                        self.child_launches[..index]
                            .iter()
                            .any(|previous| previous.child_id == child.child_id)
                    }))
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow child launch intents are invalid",
            ));
        }
        Ok(())
    }
}

/// Immutable flow state plus the named step needed to resume it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FlowContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<WaitCondition>,
    resume_at: Option<SystemTime>,
    schedule_id: Option<Box<str>>,
    compensations: Arc<[Box<str>]>,
    created_at: SystemTime,
    updated_at: SystemTime,
}

#[derive(Default, MemoryPackable)]
struct WaitResultWire {
    child_id: String,
    payload: Option<Vec<u8>>,
    error: Option<ErrorWire>,
}

#[derive(Default, MemoryPackable)]
struct FlowChildLaunchWire {
    child_id: String,
    state: u8,
    owner: Option<String>,
    expires_at: Option<TimeWire>,
}

#[derive(Default, MemoryPackable)]
struct WaitConditionWire {
    correlation_id: String,
    policy: u8,
    expected_count: u32,
    results: Vec<WaitResultWire>,
    created_at: TimeWire,
    timeout: DurationWire,
    child_launches: Vec<FlowChildLaunchWire>,
}

#[derive(MemoryPackable)]
struct FlowContinuationWire {
    state: FlowState,
    step_name: String,
    wait: Option<WaitConditionWire>,
    resume_at: Option<TimeWire>,
    schedule_id: Option<String>,
    compensations: Vec<String>,
    created_at: TimeWire,
    updated_at: TimeWire,
}

impl MemoryPackSerialize for WaitResult {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        WaitResultWire::from(self).serialize(writer)
    }
}

impl MemoryPackDeserialize for WaitResult {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        WaitResultWire::deserialize(reader)?.try_into()
    }
}

impl From<&WaitResult> for WaitResultWire {
    fn from(value: &WaitResult) -> Self {
        Self {
            child_id: value.child_id.to_string(),
            payload: value.payload.as_deref().map(ToOwned::to_owned),
            error: value.error.as_ref().map(encode_error),
        }
    }
}

impl TryFrom<WaitResultWire> for WaitResult {
    type Error = MemoryPackError;

    fn try_from(value: WaitResultWire) -> Result<Self, Self::Error> {
        Ok(Self {
            child_id: value.child_id.into_boxed_str(),
            payload: value.payload.map(Arc::from),
            error: value.error.map(decode_error).transpose()?,
        })
    }
}

impl MemoryPackSerialize for FlowChildLaunch {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        FlowChildLaunchWire::from(self).serialize(writer)
    }
}

impl MemoryPackDeserialize for FlowChildLaunch {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        FlowChildLaunchWire::deserialize(reader)?.try_into()
    }
}

impl From<&FlowChildLaunch> for FlowChildLaunchWire {
    fn from(value: &FlowChildLaunch) -> Self {
        match &value.state {
            FlowChildLaunchState::Pending => Self {
                child_id: value.child_id.to_string(),
                state: 0,
                owner: None,
                expires_at: None,
            },
            FlowChildLaunchState::Claimed { owner, expires_at } => Self {
                child_id: value.child_id.to_string(),
                state: 1,
                owner: Some(owner.to_string()),
                expires_at: Some(encode_time(*expires_at)),
            },
            FlowChildLaunchState::Launched => Self {
                child_id: value.child_id.to_string(),
                state: 2,
                owner: None,
                expires_at: None,
            },
        }
    }
}

impl TryFrom<FlowChildLaunchWire> for FlowChildLaunch {
    type Error = MemoryPackError;

    fn try_from(value: FlowChildLaunchWire) -> Result<Self, Self::Error> {
        let state = match (value.state, value.owner, value.expires_at) {
            (0, None, None) => FlowChildLaunchState::Pending,
            (1, Some(owner), Some(expires_at)) => FlowChildLaunchState::Claimed {
                owner: owner.into_boxed_str(),
                expires_at: decode_time(expires_at)?,
            },
            (2, None, None) => FlowChildLaunchState::Launched,
            _ => {
                return Err(MemoryPackError::DeserializationError(
                    "invalid flow child launch state".into(),
                ));
            }
        };
        Ok(Self {
            child_id: value.child_id.into_boxed_str(),
            state,
        })
    }
}

impl MemoryPackSerialize for WaitCondition {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        WaitConditionWire::try_from(self)?.serialize(writer)
    }
}

impl MemoryPackDeserialize for WaitCondition {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        WaitConditionWire::deserialize(reader)?.try_into()
    }
}

impl TryFrom<&WaitCondition> for WaitConditionWire {
    type Error = MemoryPackError;

    fn try_from(value: &WaitCondition) -> Result<Self, Self::Error> {
        Ok(Self {
            correlation_id: value.correlation_id.to_string(),
            policy: encode_wait_policy(value.policy),
            expected_count: value.expected_count,
            results: value.results.iter().map(WaitResultWire::from).collect(),
            created_at: encode_time(value.created_at),
            timeout: encode_duration(value.timeout),
            child_launches: value
                .child_launches
                .iter()
                .map(FlowChildLaunchWire::from)
                .collect(),
        })
    }
}

impl TryFrom<WaitConditionWire> for WaitCondition {
    type Error = MemoryPackError;

    fn try_from(value: WaitConditionWire) -> Result<Self, Self::Error> {
        let condition = Self {
            correlation_id: value.correlation_id.into_boxed_str(),
            policy: decode_wait_policy(value.policy)?,
            expected_count: value.expected_count,
            results: Arc::from(
                value
                    .results
                    .into_iter()
                    .map(WaitResult::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            created_at: decode_time(value.created_at)?,
            timeout: decode_duration(value.timeout),
            child_launches: Arc::from(
                value
                    .child_launches
                    .into_iter()
                    .map(FlowChildLaunch::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        };
        condition.validate().map_err(|error| {
            MemoryPackError::DeserializationError(format!("invalid flow wait condition: {error:?}"))
        })?;
        Ok(condition)
    }
}

impl MemoryPackSerialize for FlowContinuation {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        FlowContinuationWire::try_from(self)?.serialize(writer)
    }
}

impl MemoryPackDeserialize for FlowContinuation {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        FlowContinuationWire::deserialize(reader)?.try_into()
    }
}

impl TryFrom<&FlowContinuation> for FlowContinuationWire {
    type Error = MemoryPackError;

    fn try_from(value: &FlowContinuation) -> Result<Self, Self::Error> {
        Ok(Self {
            state: value.state.clone(),
            step_name: value.step_name.to_string(),
            wait: value
                .wait
                .as_ref()
                .map(WaitConditionWire::try_from)
                .transpose()?,
            resume_at: value.resume_at.map(encode_time),
            schedule_id: value.schedule_id.as_deref().map(str::to_owned),
            compensations: value
                .compensations
                .iter()
                .map(ToString::to_string)
                .collect(),
            created_at: encode_time(value.created_at),
            updated_at: encode_time(value.updated_at),
        })
    }
}

impl TryFrom<FlowContinuationWire> for FlowContinuation {
    type Error = MemoryPackError;

    fn try_from(value: FlowContinuationWire) -> Result<Self, Self::Error> {
        let continuation = Self {
            state: value.state,
            step_name: value.step_name.into_boxed_str(),
            wait: value.wait.map(WaitConditionWire::try_into).transpose()?,
            resume_at: value.resume_at.map(decode_time).transpose()?,
            schedule_id: value.schedule_id.map(String::into_boxed_str),
            compensations: Arc::from(
                value
                    .compensations
                    .into_iter()
                    .map(String::into_boxed_str)
                    .collect::<Vec<_>>(),
            ),
            created_at: decode_time(value.created_at)?,
            updated_at: decode_time(value.updated_at)?,
        };
        continuation.validate().map_err(|error| {
            MemoryPackError::DeserializationError(format!("invalid flow continuation: {error:?}"))
        })?;
        Ok(continuation)
    }
}

fn encode_wait_policy(value: WaitPolicy) -> u8 {
    match value {
        WaitPolicy::All => 0,
        WaitPolicy::Any => 1,
    }
}

fn decode_wait_policy(value: u8) -> Result<WaitPolicy, MemoryPackError> {
    match value {
        0 => Ok(WaitPolicy::All),
        1 => Ok(WaitPolicy::Any),
        value => Err(MemoryPackError::DeserializationError(format!(
            "invalid wait policy: {value}"
        ))),
    }
}

impl FlowContinuation {
    /// Creates a continuation ready to execute `step_name`.
    pub fn new(state: FlowState, step_name: impl Into<Box<str>>) -> Self {
        let now = SystemTime::now();
        Self {
            state,
            step_name: step_name.into(),
            wait: None,
            resume_at: None,
            schedule_id: None,
            compensations: Arc::from([]),
            created_at: now,
            updated_at: now,
        }
    }

    /// Creates a continuation suspended on `wait` at `step_name`.
    pub fn waiting(state: FlowState, step_name: impl Into<Box<str>>, wait: WaitCondition) -> Self {
        let now = SystemTime::now();
        Self {
            state,
            step_name: step_name.into(),
            wait: Some(wait),
            resume_at: None,
            schedule_id: None,
            compensations: Arc::from([]),
            created_at: now,
            updated_at: now,
        }
    }

    /// Validates the durable continuation shape before it crosses a persistence boundary.
    pub fn validate(&self) -> CatgaResult<()> {
        self.state.validate()?;
        if let Some(wait) = &self.wait {
            wait.validate()?;
        }
        Ok(())
    }

    /// Returns the immutable durable flow state.
    pub fn state(&self) -> &FlowState {
        &self.state
    }

    /// Returns the registered step name to execute or resume.
    pub fn step_name(&self) -> &str {
        &self.step_name
    }

    /// Returns the active wait condition, when this continuation is waiting.
    pub fn wait(&self) -> Option<&WaitCondition> {
        self.wait.as_ref()
    }

    /// Returns the requested resume time, when this continuation is delayed.
    pub const fn resume_at(&self) -> Option<SystemTime> {
        self.resume_at
    }

    /// Returns when this durable continuation was created.
    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns when this durable continuation was most recently changed.
    ///
    /// Legacy frames without this field decode with their creation time, so discovery callers can
    /// safely use this value while old durable records are migrated lazily on their next write.
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Returns the cancellation identity of the delayed resume, when it has been scheduled.
    pub fn schedule_id(&self) -> Option<&str> {
        self.schedule_id.as_deref()
    }

    /// Returns forward steps whose rollback action is still required, in completion order.
    ///
    /// The runtime consumes the last entry first. The returned names identify handlers in the
    /// currently registered flow definition and are retained across process restart.
    pub fn compensation_steps(&self) -> &[Box<str>] {
        &self.compensations
    }

    /// Returns a delayed copy that will resume at `resume_at`.
    pub fn delayed_until(self, resume_at: SystemTime) -> Self {
        Self {
            resume_at: Some(resume_at),
            wait: None,
            schedule_id: None,
            ..self.touch()
        }
    }

    pub(crate) fn with_schedule_id(self, schedule_id: impl Into<Box<str>>) -> Self {
        Self {
            schedule_id: Some(schedule_id.into()),
            ..self.touch()
        }
    }

    /// Returns a copy waiting on `wait`.
    pub fn with_wait(self, wait: WaitCondition) -> Self {
        Self {
            wait: Some(wait),
            resume_at: None,
            schedule_id: None,
            ..self.touch()
        }
    }

    /// Returns a ready-to-run copy at the next registered step.
    pub fn at_step(self, step_name: impl Into<Box<str>>) -> Self {
        Self {
            step_name: step_name.into(),
            wait: None,
            resume_at: None,
            schedule_id: None,
            ..self.touch()
        }
    }

    /// Clears delay and wait metadata before executing a ready continuation.
    pub fn ready(self) -> Self {
        Self {
            wait: None,
            resume_at: None,
            schedule_id: None,
            ..self.touch()
        }
    }

    /// Returns a copy whose flow state has been atomically advanced by a store.
    pub fn with_state(self, state: FlowState) -> Self {
        Self {
            state,
            ..self.touch()
        }
    }

    pub(crate) fn record_compensation(self, step_name: impl Into<Box<str>>) -> CatgaResult<Self> {
        if self.compensations.len() == MAX_FLOW_COMPENSATIONS {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow compensation stack exceeds the supported bound",
            ));
        }
        let mut compensations = Vec::with_capacity(self.compensations.len().saturating_add(1));
        compensations.extend(self.compensations.iter().cloned());
        compensations.push(step_name.into());
        Ok(Self {
            compensations: Arc::from(compensations),
            ..self.touch()
        })
    }

    pub(crate) fn next_compensation(&self) -> Option<&str> {
        self.compensations.last().map(AsRef::as_ref)
    }

    pub(crate) fn complete_compensation(self) -> Self {
        let mut compensations: Vec<Box<str>> = self.compensations.iter().cloned().collect();
        let _ = compensations.pop();
        Self {
            compensations: Arc::from(compensations),
            ..self.touch()
        }
    }

    fn touch(mut self) -> Self {
        self.updated_at = SystemTime::now();
        self
    }
}
