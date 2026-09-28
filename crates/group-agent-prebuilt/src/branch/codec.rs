use super::{BranchApprovalRequest, BranchTarget, state::BranchState};
use crate::{AgentSnapshot, AgentStopReason, BranchError, BranchErrorKind, state::AgentState};
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointState, CodecDescriptor, EncodedValue,
    InterruptPayload, SnapshotError,
};
use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    First,
    Selected,
    Stopped,
}
/// Independent branch snapshot; existing standalone Agent formats are unchanged.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchSnapshot {
    phase: Phase,
    first: AgentSnapshot,
    selection: Option<BranchTarget>,
    downstream: Option<AgentSnapshot>,
}
/// Version-one JSON codec for branch state and stage-scoped approvals.
#[derive(Clone, Copy, Debug, Default)]
pub struct BranchSnapshotCodec;
fn interrupt_descriptor() -> CodecDescriptor {
    CodecDescriptor::new("group-agent-prebuilt-agent-branch-approval", 1, "json")
}
impl CheckpointCodec<BranchSnapshot> for BranchSnapshotCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group-agent-prebuilt-agent-branch-state", 1, "json")
    }
    fn encode_snapshot(&self, snapshot: &BranchSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        BranchState::restore(snapshot).map_err(|e| {
            CheckpointCodecError::with_source(
                "invalid branch snapshot",
                BranchError::source_error(BranchErrorKind::InvalidState, e),
            )
        })?;
        serde_json::to_vec(snapshot).map_err(|e| {
            CheckpointCodecError::with_source(
                "branch encoding failed",
                BranchError::source_error(BranchErrorKind::InvalidState, e),
            )
        })
    }
    fn decode_snapshot(&self, bytes: &[u8]) -> Result<BranchSnapshot, CheckpointCodecError> {
        let snapshot = serde_json::from_slice(bytes).map_err(|e| {
            CheckpointCodecError::with_source(
                "branch decoding failed",
                BranchError::source_error(BranchErrorKind::InvalidState, e),
            )
        })?;
        BranchState::restore(&snapshot).map_err(|e| {
            CheckpointCodecError::with_source(
                "invalid branch snapshot",
                BranchError::source_error(BranchErrorKind::InvalidState, e),
            )
        })?;
        Ok(snapshot)
    }
    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let request = payload
            .downcast_ref::<BranchApprovalRequest>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        let bytes = serde_json::to_vec(request).map_err(|e| {
            CheckpointCodecError::with_source(
                "branch approval encoding failed",
                BranchError::source_error(BranchErrorKind::InvalidState, e),
            )
        })?;
        Ok(EncodedValue::new(interrupt_descriptor(), bytes))
    }
    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        if value.descriptor() != &interrupt_descriptor() {
            return Err(CheckpointCodecError::message(
                "unsupported branch interrupt descriptor",
            ));
        }
        let request: BranchApprovalRequest =
            serde_json::from_slice(value.bytes()).map_err(|e| {
                CheckpointCodecError::with_source(
                    "branch approval decoding failed",
                    BranchError::source_error(BranchErrorKind::InvalidState, e),
                )
            })?;
        if request.request.pending_calls().is_empty() {
            return Err(CheckpointCodecError::message("empty branch approval"));
        }
        Ok(InterruptPayload::new(request))
    }
}
fn valid_stage(state: &AgentState) -> Result<(), SnapshotError> {
    let invalid = || SnapshotError::message("invalid saved branch stage");
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
impl CheckpointState for BranchState {
    type Snapshot = BranchSnapshot;
    fn snapshot(&self) -> Result<BranchSnapshot, SnapshotError> {
        Ok(BranchSnapshot {
            phase: if self.stopped {
                Phase::Stopped
            } else if self.downstream.is_some() {
                Phase::Selected
            } else {
                Phase::First
            },
            first: self.first.snapshot()?,
            selection: self.selection,
            downstream: self
                .downstream
                .as_ref()
                .map(AgentState::snapshot)
                .transpose()?,
        })
    }
    fn restore(snapshot: &BranchSnapshot) -> Result<Self, SnapshotError> {
        let first = AgentState::restore(&snapshot.first)?;
        let downstream = snapshot
            .downstream
            .as_ref()
            .map(AgentState::restore)
            .transpose()?;
        valid_stage(&first)?;
        if let Some(downstream) = &downstream {
            valid_stage(downstream)?;
        }
        let valid = match (snapshot.phase, snapshot.selection) {
            (Phase::First, None) => {
                downstream.is_none() && first.stop_reason() != Some(AgentStopReason::MaxRounds)
            }
            (Phase::Selected, Some(BranchTarget::B | BranchTarget::C)) => {
                first.stop_reason() == Some(AgentStopReason::FinalAnswer)
                    && downstream
                        .as_ref()
                        .is_some_and(|s| s.stop_reason().is_none())
            }
            (Phase::Stopped, None) => {
                downstream.is_none() && first.stop_reason() == Some(AgentStopReason::MaxRounds)
            }
            (Phase::Stopped, Some(BranchTarget::Complete)) => {
                downstream.is_none() && first.stop_reason() == Some(AgentStopReason::FinalAnswer)
            }
            (Phase::Stopped, Some(BranchTarget::B | BranchTarget::C)) => {
                first.stop_reason() == Some(AgentStopReason::FinalAnswer)
                    && downstream
                        .as_ref()
                        .is_some_and(|s| s.stop_reason().is_some())
            }
            _ => false,
        };
        if !valid {
            return Err(SnapshotError::message("invalid branch phase"));
        }
        Ok(Self {
            first,
            downstream,
            selection: snapshot.selection,
            stopped: snapshot.phase == Phase::Stopped,
            sink: None,
            stage_sinks: None,
        })
    }
}
