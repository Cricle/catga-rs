//! MemoryPack wire representation for durable suspension types.
//!
//! The wire structs mirror the domain types in [`crate::suspension`] with a stable, compact
//! encoding. Serialization impls live here so the domain module stays focused on behavior; the
//! domain fields are `pub(crate)` so these conversions can map between the two representations.

use std::sync::Arc;

use crate::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};

use crate::flow::{
    state::FlowState,
    suspension::{
        FlowChildLaunch, FlowChildLaunchState, FlowContinuation, WaitCondition, WaitPolicy,
        WaitResult,
    },
    memorypack::{
        DurationWire, ErrorWire, TimeWire, decode_duration, decode_error, decode_time,
        encode_duration, encode_error, encode_time,
    },
};

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

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;
    use crate::codec::memorypack::MemoryPackSerializer;
    use crate::{CatgaError, ErrorCode};

    fn wait_with_claimed_child() -> WaitCondition {
        let wait = WaitCondition::for_children(
            "parent",
            WaitPolicy::All,
            ["child-a", "child-b"],
            UNIX_EPOCH,
            Duration::from_secs(30),
        )
        .expect("child identities are valid")
        .record_success("child-a", [1_u8, 2])
        .record_failure(
            "child-b",
            CatgaError::new(ErrorCode::Unavailable, "child unavailable"),
        );
        let (_, claimed) = wait
            .claim_next_child("worker", UNIX_EPOCH, Duration::from_secs(5))
            .expect("first persisted child can be claimed");
        claimed
    }

    #[test]
    fn wait_and_continuation_wires_round_trip_results_and_child_launches() {
        let claimed = wait_with_claimed_child();
        let bytes = MemoryPackSerializer::serialize(&claimed).expect("wait serializes");
        assert_eq!(
            MemoryPackSerializer::deserialize::<WaitCondition>(&bytes).expect("wait deserializes"),
            claimed
        );

        let launched = claimed
            .mark_child_launched("child-a", "worker")
            .expect("owner launches the claimed child");
        let bytes = MemoryPackSerializer::serialize(&launched).expect("launched wait serializes");
        assert_eq!(
            MemoryPackSerializer::deserialize::<WaitCondition>(&bytes)
                .expect("launched wait deserializes"),
            launched
        );

        let state = FlowState::new("flow", "checkout", [9_u8], "worker").suspended();
        let continuation = FlowContinuation::waiting(state, "wait-payment", launched);
        let bytes =
            MemoryPackSerializer::serialize(&continuation).expect("continuation serializes");
        assert_eq!(
            MemoryPackSerializer::deserialize::<FlowContinuation>(&bytes)
                .expect("continuation deserializes"),
            continuation
        );
    }

    #[test]
    fn suspension_wires_reject_invalid_child_states_and_wait_policies() {
        assert!(matches!(
            decode_wait_policy(7),
            Err(MemoryPackError::DeserializationError(message)) if message.contains("wait policy")
        ));
        let invalid = FlowChildLaunchWire {
            child_id: "child".into(),
            state: 1,
            owner: None,
            expires_at: None,
        };
        assert!(matches!(
            FlowChildLaunch::try_from(invalid),
            Err(MemoryPackError::DeserializationError(message)) if message.contains("child launch")
        ));
    }
}
