use super::{BranchError, BranchErrorKind, BranchSelection, BranchTarget};
use crate::{
    AgentStopReason,
    state::{AgentState, AgentUpdate},
};
use group_agent_core::{GraphState, StateError};
use group_agent_model::{ChatRequest, Message};
pub(crate) struct BranchState {
    pub first: AgentState,
    pub downstream: Option<AgentState>,
    pub stopped: bool,
    pub selection: Option<BranchTarget>,
    pub(crate) sink: Option<std::sync::Arc<dyn super::BranchEventSink>>,
    pub(crate) stage_sinks: Option<[std::sync::Arc<dyn crate::AgentEventSink>; 3]>,
}
pub(crate) enum BranchUpdate {
    Stage { index: usize, update: AgentUpdate },
    Select(BranchSelection),
}
pub(crate) fn validate_transcript(
    messages: &[Message],
    allow_pending: bool,
) -> Result<(), BranchError> {
    ChatRequest::new(messages.to_vec())
        .validate()
        .map_err(|e| BranchError::source_error(BranchErrorKind::InvalidState, e))?;
    let mut pending = std::collections::BTreeSet::new();
    for message in messages {
        if let Some(tool) = message.as_tool() {
            if !pending.remove(tool.tool_call_id()) {
                return Err(BranchError::new(BranchErrorKind::InvalidState));
            }
        } else {
            if !pending.is_empty() {
                return Err(BranchError::new(BranchErrorKind::InvalidState));
            }
            if let Some(assistant) = message.as_assistant() {
                pending.extend(assistant.tool_calls().iter().map(|c| c.id().clone()));
            }
        }
    }
    if !pending.is_empty()
        && (!allow_pending || !messages.last().is_some_and(|m| m.as_assistant().is_some()))
    {
        return Err(BranchError::new(BranchErrorKind::InvalidState));
    }
    Ok(())
}
pub(crate) fn admit(messages: &[Message]) -> Result<(), BranchError> {
    validate_transcript(messages, false)
}
impl BranchState {
    pub fn set_sink(
        &mut self,
        sink: std::sync::Arc<dyn super::BranchEventSink>,
        stages: &[super::AgentStage; 3],
    ) {
        let sinks = [
            super::event::stage_sink(stages[0].id.clone(), sink.clone()),
            super::event::stage_sink(stages[1].id.clone(), sink.clone()),
            super::event::stage_sink(stages[2].id.clone(), sink.clone()),
        ];
        self.first.set_sink(sinks[0].clone());
        if let Some(downstream) = &mut self.downstream {
            downstream.set_sink(
                sinks[self
                    .selection
                    .and_then(BranchTarget::index)
                    .expect("selected stage")]
                .clone(),
            );
        }
        self.sink = Some(sink);
        self.stage_sinks = Some(sinks);
    }

    pub fn new(messages: Vec<Message>) -> Result<Self, BranchError> {
        admit(&messages)?;
        Ok(Self {
            first: AgentState::new(messages),
            downstream: None,
            stopped: false,
            selection: None,
            sink: None,
            stage_sinks: None,
        })
    }
    pub fn stage(&self, index: usize) -> Result<&AgentState, StateError> {
        match index {
            0 => Ok(&self.first),
            i if self.selection.and_then(BranchTarget::index) == Some(i) => self
                .downstream
                .as_ref()
                .ok_or_else(|| StateError::message("downstream stage not initialized")),
            _ => Err(StateError::message("invalid stage")),
        }
    }
}
impl GraphState for BranchState {
    type Update = BranchUpdate;
    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        if self.stopped {
            return Err(StateError::message("branch already stopped"));
        }
        match update {
            BranchUpdate::Stage { index, update } => {
                let state = if index == 0 && self.selection.is_none() {
                    &mut self.first
                } else if self.selection.and_then(BranchTarget::index) == Some(index) {
                    self.downstream
                        .as_mut()
                        .ok_or_else(|| StateError::message("downstream stage missing"))?
                } else {
                    return Err(StateError::message("inactive stage update"));
                };
                state.apply(update)?;
                self.stopped = match state.stop_reason() {
                    Some(AgentStopReason::MaxRounds) => true,
                    Some(AgentStopReason::FinalAnswer) => index != 0,
                    _ => false,
                };
            }
            BranchUpdate::Select(selection) => {
                if self.first.stop_reason() != Some(AgentStopReason::FinalAnswer)
                    || self.selection.is_some()
                    || self.downstream.is_some()
                {
                    return Err(StateError::message("invalid selection phase"));
                }
                let target = selection.target();
                let messages = match selection {
                    BranchSelection::B(messages) | BranchSelection::C(messages) => Some(messages),
                    BranchSelection::Complete => None,
                };
                if let Some(messages) = messages {
                    admit(&messages)
                        .map_err(|e| StateError::with_source("invalid selected messages", e))?;
                    let mut downstream = AgentState::new(messages);
                    if let Some(sinks) = &self.stage_sinks {
                        downstream.set_sink(sinks[target.index().expect("selected stage")].clone());
                    }
                    self.downstream = Some(downstream);
                } else {
                    self.stopped = true;
                }
                self.selection = Some(target);
            }
        }
        Ok(())
    }
}
