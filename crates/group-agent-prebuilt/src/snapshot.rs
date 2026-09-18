use std::fmt;

use group_agent_core::{CheckpointState, SnapshotError};
use group_agent_model::{Message, TokenUsage};
use serde::{Deserialize, Serialize};

use crate::AgentStopReason;
use crate::state::AgentState;

/// Immutable snapshot of the private Agent state retained by a durable
/// checkpoint.
///
/// The snapshot mirrors the private Agent State exactly so a checkpointed
/// invocation can resume, replay, or fork from a committed boundary. Its
/// `Debug` implementation reports field counts only and never exposes message
/// content.
#[derive(Clone, Serialize, Deserialize)]
pub struct AgentSnapshot {
    messages: Vec<Message>,
    model_rounds: usize,
    usage_by_round: Vec<Option<TokenUsage>>,
    stop_reason: Option<AgentStopReason>,
}

impl AgentSnapshot {
    pub(crate) const fn from_parts(
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
}

impl fmt::Debug for AgentSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentSnapshot")
            .field("message_count", &self.messages.len())
            .field("model_rounds", &self.model_rounds)
            .field("usage_rounds", &self.usage_by_round.len())
            .field("stop_reason", &self.stop_reason)
            .finish()
    }
}

impl CheckpointState for AgentState {
    type Snapshot = AgentSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(AgentSnapshot::from_parts(
            self.messages().to_vec(),
            self.model_rounds(),
            self.usage_by_round().to_vec(),
            self.stop_reason(),
        ))
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        let (messages, model_rounds, usage_by_round, stop_reason) = snapshot.clone().into_parts();
        Ok(AgentState::from_parts(
            messages,
            model_rounds,
            usage_by_round,
            stop_reason,
        ))
    }
}

#[cfg(test)]
mod tests {
    use group_agent_core::{CheckpointState as _, GraphState as _};
    use group_agent_model::{
        AssistantMessage, ContentPart, Message, TokenUsage, ToolCall, ToolCallId, ToolMessage,
        ToolName, ToolResult,
    };

    use super::AgentSnapshot;
    use crate::AgentStopReason;
    use crate::state::{AgentState, AgentUpdate};

    fn call(id: &str) -> ToolCall {
        ToolCall::new(
            ToolCallId::new(id).expect("valid call id"),
            ToolName::new("tool").expect("valid tool name"),
            r#"{"query":"SECRET_ARGUMENT"}"#.parse().expect("valid arguments"),
        )
    }

    fn mid_run_state() -> AgentState {
        let mut state = AgentState::new(vec![Message::user("SECRET_QUESTION")]);
        state
            .apply(AgentUpdate::ModelCompleted {
                message: AssistantMessage::new(
                    vec![ContentPart::text("SECRET_ASSISTANT_TEXT")],
                    vec![call("a"), call("b")],
                ),
                usage: Some(TokenUsage::from_parts(Some(7), Some(3), Some(10)).unwrap()),
            })
            .expect("model update commits");
        state
            .apply(AgentUpdate::ToolsCompleted {
                messages: vec![
                    ToolMessage::new(
                        ToolCallId::new("a").unwrap(),
                        ToolResult::text("SECRET_TOOL_RESULT"),
                    ),
                    ToolMessage::new(
                        ToolCallId::new("b").unwrap(),
                        ToolResult::error_text("SECRET_BUSINESS_ERROR"),
                    ),
                ],
                reached_max_rounds: false,
            })
            .expect("tool update commits");
        state
    }

    fn assert_states_equal(left: &AgentState, right: &AgentState) {
        assert_eq!(left.messages(), right.messages());
        assert_eq!(left.model_rounds(), right.model_rounds());
        assert_eq!(left.usage_by_round(), right.usage_by_round());
        assert_eq!(left.stop_reason(), right.stop_reason());
    }

    #[test]
    fn snapshot_restore_roundtrip_preserves_mid_run_state() {
        let state = mid_run_state();

        let snapshot = state.snapshot().expect("snapshot succeeds");
        let restored = AgentState::restore(&snapshot).expect("restore succeeds");

        assert_states_equal(&state, &restored);
    }

    #[test]
    fn snapshot_restore_roundtrip_preserves_stopped_state() {
        let mut state = AgentState::new(vec![Message::user("question")]);
        state
            .apply(AgentUpdate::ModelCompleted {
                message: AssistantMessage::text("final answer"),
                usage: None,
            })
            .expect("model update commits");
        assert_eq!(state.stop_reason(), Some(AgentStopReason::FinalAnswer));

        let snapshot = state.snapshot().expect("snapshot succeeds");
        let restored = AgentState::restore(&snapshot).expect("restore succeeds");

        assert_states_equal(&state, &restored);
        assert_eq!(restored.stop_reason(), Some(AgentStopReason::FinalAnswer));
    }

    #[test]
    fn snapshot_serde_roundtrip_is_canonical() {
        let snapshot = mid_run_state().snapshot().expect("snapshot succeeds");

        let first = serde_json::to_vec(&snapshot).expect("snapshot serializes");
        let decoded: AgentSnapshot = serde_json::from_slice(&first).expect("snapshot decodes");
        let second = serde_json::to_vec(&decoded).expect("decoded snapshot serializes");

        assert_eq!(first, second);
        let restored = AgentState::restore(&decoded).expect("decoded snapshot restores");
        assert_states_equal(&mid_run_state(), &restored);
    }

    #[test]
    fn snapshot_debug_does_not_expose_message_content() {
        let snapshot = mid_run_state().snapshot().expect("snapshot succeeds");

        let rendered = format!("{snapshot:?}");

        assert!(rendered.contains("AgentSnapshot"));
        for marker in [
            "SECRET_QUESTION",
            "SECRET_ASSISTANT_TEXT",
            "SECRET_ARGUMENT",
            "SECRET_TOOL_RESULT",
            "SECRET_BUSINESS_ERROR",
        ] {
            assert!(
                !rendered.contains(marker),
                "Debug must not contain {marker}"
            );
        }
    }
}
