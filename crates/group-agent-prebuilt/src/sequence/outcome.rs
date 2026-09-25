use super::{AgentStage, AgentStageId, SequenceError, SequenceErrorKind, state::SequenceState};
use crate::{AgentOutcome, AgentStopReason};
/// Completed sequence, including a normal early stop at either stage's round cap.
#[derive(Clone, Debug, PartialEq)]
pub struct SequenceOutcome {
    first: AgentOutcome,
    second: Option<AgentOutcome>,
    stopping_stage: AgentStageId,
}
impl SequenceOutcome {
    pub(crate) fn from_state(
        state: SequenceState,
        stages: &[AgentStage; 2],
    ) -> Result<Self, SequenceError> {
        super::nodes::guard(&state, stages, false)
            .map_err(|e| SequenceError::node(SequenceErrorKind::InvalidState, e))?;
        if !state.stopped {
            return Err(SequenceError::new(SequenceErrorKind::InvalidState));
        }
        let first = stages[0]
            .contract()
            .validate(AgentOutcome::from_completed_state(state.first))
            .map_err(|e| {
                SequenceError::source_error(SequenceErrorKind::Output, e)
                    .at_stage(stages[0].id.clone())
            })?;
        let second = state
            .second
            .map(|s| {
                stages[1]
                    .contract()
                    .validate(AgentOutcome::from_completed_state(s))
            })
            .transpose()
            .map_err(|e| {
                SequenceError::source_error(SequenceErrorKind::Output, e)
                    .at_stage(stages[1].id.clone())
            })?;
        let stopping_stage = stages[usize::from(second.is_some())].id.clone();
        Ok(Self {
            first,
            second,
            stopping_stage,
        })
    }
    pub fn final_message(&self) -> Option<&group_agent_model::AssistantMessage> {
        self.second
            .as_ref()
            .and_then(crate::AgentOutcome::final_message)
    }
    pub fn first(&self) -> &AgentOutcome {
        &self.first
    }
    pub fn second(&self) -> Option<&AgentOutcome> {
        self.second.as_ref()
    }
    pub fn stopping_stage(&self) -> &AgentStageId {
        &self.stopping_stage
    }
    pub fn stop_reason(&self) -> AgentStopReason {
        self.second.as_ref().unwrap_or(&self.first).stop_reason()
    }
    pub fn final_output(&self) -> Option<&group_agent_model::ValidatedJsonOutput> {
        self.second
            .as_ref()
            .and_then(AgentOutcome::structured_output)
    }
}
