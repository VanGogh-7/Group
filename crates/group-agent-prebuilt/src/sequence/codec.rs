use super::{SequenceApprovalRequest, state::SequenceState};
use crate::{AgentSnapshot, AgentStopReason, state::AgentState};
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointState, CodecDescriptor, EncodedValue,
    InterruptPayload, SnapshotError,
};
use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    First,
    Second,
    Stopped,
}
/// Independent sequence snapshot; existing standalone Agent formats are unchanged.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SequenceSnapshot {
    phase: Phase,
    first: AgentSnapshot,
    second: Option<AgentSnapshot>,
}
/// Version-one JSON codec for sequence state and stage-scoped approvals.
#[derive(Clone, Copy, Debug, Default)]
pub struct SequenceSnapshotCodec;
fn interrupt_descriptor() -> CodecDescriptor {
    CodecDescriptor::new("group-agent-prebuilt-agent-sequence-approval", 1, "json")
}
impl CheckpointCodec<SequenceSnapshot> for SequenceSnapshotCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group-agent-prebuilt-agent-sequence-state", 1, "json")
    }
    fn encode_snapshot(
        &self,
        snapshot: &SequenceSnapshot,
    ) -> Result<Vec<u8>, CheckpointCodecError> {
        SequenceState::restore(snapshot)
            .map_err(|e| CheckpointCodecError::with_source("invalid sequence snapshot", e))?;
        serde_json::to_vec(snapshot)
            .map_err(|e| CheckpointCodecError::with_source("sequence encoding failed", e))
    }
    fn decode_snapshot(&self, bytes: &[u8]) -> Result<SequenceSnapshot, CheckpointCodecError> {
        let snapshot = serde_json::from_slice(bytes)
            .map_err(|e| CheckpointCodecError::with_source("sequence decoding failed", e))?;
        SequenceState::restore(&snapshot)
            .map_err(|e| CheckpointCodecError::with_source("invalid sequence snapshot", e))?;
        Ok(snapshot)
    }
    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let request = payload
            .downcast_ref::<SequenceApprovalRequest>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        let bytes = serde_json::to_vec(request).map_err(|e| {
            CheckpointCodecError::with_source("sequence approval encoding failed", e)
        })?;
        Ok(EncodedValue::new(interrupt_descriptor(), bytes))
    }
    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        if value.descriptor() != &interrupt_descriptor() {
            return Err(CheckpointCodecError::message(
                "unsupported sequence interrupt descriptor",
            ));
        }
        let request: SequenceApprovalRequest =
            serde_json::from_slice(value.bytes()).map_err(|e| {
                CheckpointCodecError::with_source("sequence approval decoding failed", e)
            })?;
        if request.request.pending_calls().is_empty() {
            return Err(CheckpointCodecError::message("empty sequence approval"));
        }
        Ok(InterruptPayload::new(request))
    }
}
fn valid_stage(state: &AgentState) -> Result<(), SnapshotError> {
    let invalid = || SnapshotError::message("invalid saved sequence stage");
    if !state.usage_is_aligned() || state.messages().is_empty() {
        return Err(invalid());
    }
    super::state::validate_transcript(state.messages(), true)
        .map_err(|e| SnapshotError::with_source("invalid saved transcript", e))?;
    if state.model_rounds() == 0 {
        if state.stop_reason().is_some() {
            return Err(invalid());
        }
        super::state::admit(state.messages())
            .map_err(|e| SnapshotError::with_source("invalid initial stage transcript", e))?;
        return Ok(());
    }
    let tail = state.messages().last().ok_or_else(invalid)?;
    match state.stop_reason() {
        Some(AgentStopReason::FinalAnswer)
            if tail
                .as_assistant()
                .is_some_and(|m| m.tool_calls().is_empty()) => {}
        Some(AgentStopReason::MaxRounds) if tail.as_tool().is_some() => {}
        None if tail.as_tool().is_some()
            || tail
                .as_assistant()
                .is_some_and(|m| !m.tool_calls().is_empty()) => {}
        _ => return Err(invalid()),
    }
    Ok(())
}
impl CheckpointState for SequenceState {
    type Snapshot = SequenceSnapshot;
    fn snapshot(&self) -> Result<SequenceSnapshot, SnapshotError> {
        Ok(SequenceSnapshot {
            phase: if self.stopped {
                Phase::Stopped
            } else if self.second.is_some() {
                Phase::Second
            } else {
                Phase::First
            },
            first: self.first.snapshot()?,
            second: self.second.as_ref().map(AgentState::snapshot).transpose()?,
        })
    }
    fn restore(snapshot: &SequenceSnapshot) -> Result<Self, SnapshotError> {
        let first = AgentState::restore(&snapshot.first)?;
        let second = snapshot
            .second
            .as_ref()
            .map(AgentState::restore)
            .transpose()?;
        valid_stage(&first)?;
        if let Some(second) = &second {
            valid_stage(second)?;
        }
        let valid = match snapshot.phase {
            Phase::First => {
                second.is_none() && first.stop_reason() != Some(AgentStopReason::MaxRounds)
            }
            Phase::Second => {
                first.stop_reason() == Some(AgentStopReason::FinalAnswer)
                    && second.as_ref().is_some_and(|s| s.stop_reason().is_none())
            }
            Phase::Stopped => {
                if let Some(second) = &second {
                    first.stop_reason() == Some(AgentStopReason::FinalAnswer)
                        && second.stop_reason().is_some()
                } else {
                    first.stop_reason() == Some(AgentStopReason::MaxRounds)
                }
            }
        };
        if !valid {
            return Err(SnapshotError::message("invalid sequence phase"));
        }
        Ok(Self {
            first,
            second,
            stopped: snapshot.phase == Phase::Stopped,
            sink: None,
            stage_sinks: None,
        })
    }
}
