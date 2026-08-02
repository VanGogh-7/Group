use group_agent_core::{GraphState, StateError};
use group_agent_model::{AssistantMessage, Message, TokenUsage, ToolCall, ToolMessage};

use crate::AgentStopReason;

pub(crate) struct AgentState {
    messages: Vec<Message>,
    model_rounds: usize,
    usage_by_round: Vec<Option<TokenUsage>>,
    stop_reason: Option<AgentStopReason>,
}

impl AgentState {
    pub(crate) const fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            model_rounds: 0,
            usage_by_round: Vec::new(),
            stop_reason: None,
        }
    }

    pub(crate) fn messages(&self) -> &[Message] {
        &self.messages
    }

    pub(crate) const fn model_rounds(&self) -> usize {
        self.model_rounds
    }

    pub(crate) const fn stop_reason(&self) -> Option<AgentStopReason> {
        self.stop_reason
    }

    pub(crate) fn usage_is_aligned(&self) -> bool {
        self.usage_by_round.len() == self.model_rounds
    }

    pub(crate) fn pending_tool_calls(&self) -> Option<&[ToolCall]> {
        self.messages
            .last()
            .and_then(Message::as_assistant)
            .map(AssistantMessage::tool_calls)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<Message>,
        usize,
        Vec<Option<TokenUsage>>,
        Option<AgentStopReason>,
    ) {
        (
            self.messages,
            self.model_rounds,
            self.usage_by_round,
            self.stop_reason,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_test_parts(
        messages: Vec<Message>,
        model_rounds: usize,
        usage_by_round: Vec<Option<TokenUsage>>,
        stop_reason: Option<AgentStopReason>,
    ) -> Self {
        Self {
            messages,
            model_rounds,
            usage_by_round,
            stop_reason,
        }
    }
}

pub(crate) enum AgentUpdate {
    ModelCompleted {
        message: AssistantMessage,
        usage: Option<TokenUsage>,
    },
    ToolsCompleted {
        messages: Vec<ToolMessage>,
        reached_max_rounds: bool,
    },
}

impl GraphState for AgentState {
    type Update = AgentUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        match update {
            AgentUpdate::ModelCompleted { message, usage } => {
                if self.stop_reason.is_some() {
                    return Err(StateError::message("agent is already stopped"));
                }
                if self.usage_by_round.len() != self.model_rounds {
                    return Err(StateError::message("agent usage and round counts diverged"));
                }
                let model_rounds = self
                    .model_rounds
                    .checked_add(1)
                    .ok_or_else(|| StateError::message("agent model round counter overflow"))?;
                let stop_reason = message
                    .tool_calls()
                    .is_empty()
                    .then_some(AgentStopReason::FinalAnswer);

                self.messages.push(Message::Assistant(message));
                self.model_rounds = model_rounds;
                self.usage_by_round.push(usage);
                self.stop_reason = stop_reason;
                Ok(())
            }
            AgentUpdate::ToolsCompleted {
                messages,
                reached_max_rounds,
            } => {
                if self.stop_reason.is_some() {
                    return Err(StateError::message("agent is already stopped"));
                }
                if self.model_rounds == 0 || self.usage_by_round.len() != self.model_rounds {
                    return Err(StateError::message("agent model round state is invalid"));
                }
                let calls = self.pending_tool_calls().ok_or_else(|| {
                    StateError::message("tool update requires a preceding assistant message")
                })?;
                if calls.is_empty() {
                    return Err(StateError::message(
                        "tool update requires at least one pending tool call",
                    ));
                }
                if calls.len() != messages.len()
                    || calls
                        .iter()
                        .zip(&messages)
                        .any(|(call, message)| call.id() != message.tool_call_id())
                {
                    return Err(StateError::message(
                        "tool update messages do not match pending calls",
                    ));
                }

                self.messages
                    .extend(messages.into_iter().map(Message::Tool));
                if reached_max_rounds {
                    self.stop_reason = Some(AgentStopReason::MaxRounds);
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use group_agent_core::GraphState as _;
    use group_agent_model::{
        AssistantMessage, Message, ToolCall, ToolCallId, ToolMessage, ToolName, ToolResult,
    };

    use super::{AgentState, AgentUpdate};
    use crate::AgentStopReason;

    fn call(id: &str) -> ToolCall {
        ToolCall::new(
            ToolCallId::new(id).expect("valid call id"),
            ToolName::new("tool").expect("valid tool name"),
            "{}".parse().expect("valid arguments"),
        )
    }

    fn state_with_pending_calls() -> AgentState {
        let mut state = AgentState::new(vec![Message::user("question")]);
        state
            .apply(AgentUpdate::ModelCompleted {
                message: AssistantMessage::new(Vec::new(), vec![call("a"), call("b")]),
                usage: None,
            })
            .expect("model update commits");
        state
    }

    #[test]
    fn tool_update_validates_entire_batch_before_mutation() {
        let mut state = state_with_pending_calls();
        let result = state.apply(AgentUpdate::ToolsCompleted {
            messages: vec![
                ToolMessage::new(ToolCallId::new("a").unwrap(), ToolResult::text("success")),
                ToolMessage::new(
                    ToolCallId::new("wrong").unwrap(),
                    ToolResult::text("must not commit"),
                ),
            ],
            reached_max_rounds: true,
        });

        assert!(result.is_err());
        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.model_rounds, 1);
        assert_eq!(state.usage_by_round, [None]);
        assert_eq!(state.stop_reason, None);
    }

    #[test]
    fn tool_update_commits_success_and_business_messages_together() {
        let mut state = state_with_pending_calls();
        state
            .apply(AgentUpdate::ToolsCompleted {
                messages: vec![
                    ToolMessage::new(ToolCallId::new("a").unwrap(), ToolResult::text("success")),
                    ToolMessage::new(
                        ToolCallId::new("b").unwrap(),
                        ToolResult::error_text("business"),
                    ),
                ],
                reached_max_rounds: true,
            })
            .expect("complete batch commits");

        assert_eq!(state.messages.len(), 4);
        assert_eq!(state.model_rounds, 1);
        assert_eq!(state.usage_by_round, [None]);
        assert_eq!(state.stop_reason, Some(AgentStopReason::MaxRounds));
        assert!(!state.messages[2].as_tool().unwrap().result().is_error());
        assert!(state.messages[3].as_tool().unwrap().result().is_error());
    }
}
