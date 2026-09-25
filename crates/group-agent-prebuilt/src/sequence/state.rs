use super::{SequenceError, SequenceErrorKind};
use crate::{
    AgentStopReason,
    state::{AgentState, AgentUpdate},
};
use group_agent_core::{GraphState, StateError};
use group_agent_model::{ChatRequest, Message};
pub(crate) struct SequenceState {
    pub first: AgentState,
    pub second: Option<AgentState>,
    pub stopped: bool,
    pub(crate) sink: Option<std::sync::Arc<dyn super::SequenceEventSink>>,
    pub(crate) stage_sinks: Option<[std::sync::Arc<dyn crate::AgentEventSink>; 2]>,
}
pub(crate) enum SequenceUpdate {
    Stage { index: usize, update: AgentUpdate },
    Handoff(Vec<Message>),
}
pub(crate) fn validate_transcript(
    messages: &[Message],
    allow_pending: bool,
) -> Result<(), SequenceError> {
    ChatRequest::new(messages.to_vec())
        .validate()
        .map_err(|e| SequenceError::source_error(SequenceErrorKind::InvalidState, e))?;
    let mut pending = std::collections::BTreeSet::new();
    for message in messages {
        if let Some(tool) = message.as_tool() {
            if !pending.remove(tool.tool_call_id()) {
                return Err(SequenceError::new(SequenceErrorKind::InvalidState));
            }
        } else {
            if !pending.is_empty() {
                return Err(SequenceError::new(SequenceErrorKind::InvalidState));
            }
            if let Some(assistant) = message.as_assistant() {
                pending.extend(assistant.tool_calls().iter().map(|c| c.id().clone()));
            }
        }
    }
    if !pending.is_empty()
        && (!allow_pending || !messages.last().is_some_and(|m| m.as_assistant().is_some()))
    {
        return Err(SequenceError::new(SequenceErrorKind::InvalidState));
    }
    Ok(())
}
pub(crate) fn admit(messages: &[Message]) -> Result<(), SequenceError> {
    validate_transcript(messages, false)
}
impl SequenceState {
    pub fn set_sink(
        &mut self,
        sink: std::sync::Arc<dyn super::SequenceEventSink>,
        stages: &[super::AgentStage; 2],
    ) {
        let sinks = [
            super::event::stage_sink(stages[0].id.clone(), sink.clone()),
            super::event::stage_sink(stages[1].id.clone(), sink.clone()),
        ];
        self.first.set_sink(sinks[0].clone());
        if let Some(second) = &mut self.second {
            second.set_sink(sinks[1].clone());
        }
        self.sink = Some(sink);
        self.stage_sinks = Some(sinks);
    }

    pub fn new(messages: Vec<Message>) -> Result<Self, SequenceError> {
        admit(&messages)?;
        Ok(Self {
            first: AgentState::new(messages),
            second: None,
            stopped: false,
            sink: None,
            stage_sinks: None,
        })
    }
    pub fn stage(&self, index: usize) -> Result<&AgentState, StateError> {
        match index {
            0 => Ok(&self.first),
            1 => self
                .second
                .as_ref()
                .ok_or_else(|| StateError::message("second stage not initialized")),
            _ => Err(StateError::message("invalid stage")),
        }
    }
}
impl GraphState for SequenceState {
    type Update = SequenceUpdate;
    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        if self.stopped {
            return Err(StateError::message("sequence already stopped"));
        }
        match update {
            SequenceUpdate::Stage { index, update } => {
                let state = if index == 0 && self.second.is_none() {
                    &mut self.first
                } else if index == 1 {
                    self.second
                        .as_mut()
                        .ok_or_else(|| StateError::message("second stage missing"))?
                } else {
                    return Err(StateError::message("inactive stage update"));
                };
                state.apply(update)?;
                self.stopped = match state.stop_reason() {
                    Some(AgentStopReason::MaxRounds) => true,
                    Some(AgentStopReason::FinalAnswer) => index == 1,
                    _ => false,
                };
            }
            SequenceUpdate::Handoff(messages) => {
                if self.first.stop_reason() != Some(AgentStopReason::FinalAnswer)
                    || self.second.is_some()
                {
                    return Err(StateError::message("invalid handoff phase"));
                }
                let mut second = AgentState::new(messages);
                if let Some(sinks) = &self.stage_sinks {
                    second.set_sink(sinks[1].clone());
                }
                self.second = Some(second);
            }
        }
        Ok(())
    }
}
