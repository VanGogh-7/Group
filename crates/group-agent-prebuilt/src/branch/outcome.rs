use super::BranchTarget;
use super::{AgentStage, AgentStageId, BranchError, BranchErrorKind, state::BranchState};
use crate::{AgentOutcome, AgentStopReason};
/// Completed branch, including a normal early stop at either stage's round cap.
#[derive(Clone, Debug, PartialEq)]
pub struct BranchOutcome {
    first: AgentOutcome,
    downstream: Option<AgentOutcome>,
    stopping_stage: AgentStageId,
    selected: Option<BranchTarget>,
}
impl BranchOutcome {
    pub(crate) fn from_state(
        state: BranchState,
        stages: &[AgentStage; 3],
    ) -> Result<Self, BranchError> {
        super::nodes::guard(&state, stages, false)
            .map_err(|e| BranchError::node(BranchErrorKind::InvalidState, e))?;
        if !state.stopped {
            return Err(BranchError::new(BranchErrorKind::InvalidState));
        }
        let first = stages[0]
            .contract()
            .validate(AgentOutcome::from_completed_state(state.first))
            .map_err(|e| {
                BranchError::source_error(BranchErrorKind::Output, e).at_stage(stages[0].id.clone())
            })?;
        let downstream = state
            .downstream
            .map(|s| {
                stages[state
                    .selection
                    .and_then(BranchTarget::index)
                    .expect("selected stage")]
                .contract()
                .validate(AgentOutcome::from_completed_state(s))
            })
            .transpose()
            .map_err(|e| {
                BranchError::source_error(BranchErrorKind::Output, e).at_stage(
                    stages[state
                        .selection
                        .and_then(BranchTarget::index)
                        .expect("selected stage")]
                    .id
                    .clone(),
                )
            })?;
        let stopping_stage = stages[state.selection.and_then(BranchTarget::index).unwrap_or(0)]
            .id
            .clone();
        Ok(Self {
            first,
            downstream,
            stopping_stage,
            selected: state.selection,
        })
    }
    pub fn final_message(&self) -> Option<&group_agent_model::AssistantMessage> {
        self.final_agent()
            .and_then(crate::AgentOutcome::final_message)
    }
    pub fn selected(&self) -> Option<BranchTarget> {
        self.selected
    }
    fn final_agent(&self) -> Option<&AgentOutcome> {
        if self.stop_reason() != AgentStopReason::FinalAnswer {
            return None;
        }
        if self.selected == Some(BranchTarget::Complete) {
            Some(&self.first)
        } else {
            self.downstream.as_ref()
        }
    }
    pub fn first(&self) -> &AgentOutcome {
        &self.first
    }
    pub fn downstream(&self) -> Option<&AgentOutcome> {
        self.downstream.as_ref()
    }
    pub fn stopping_stage(&self) -> &AgentStageId {
        &self.stopping_stage
    }
    pub fn stop_reason(&self) -> AgentStopReason {
        self.downstream
            .as_ref()
            .unwrap_or(&self.first)
            .stop_reason()
    }
    pub fn final_output(&self) -> Option<&group_agent_model::ValidatedJsonOutput> {
        self.final_agent().and_then(AgentOutcome::structured_output)
    }
}
