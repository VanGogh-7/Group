use crate::{AgentError, AgentOutcome};
use group_agent_model::ChatRequest;

/// Kept separate from Copy AgentConfig and omitted from snapshots.
#[derive(Clone, Default)]
pub(crate) struct OutputContract {
    #[cfg(feature = "structured-output")]
    pub(crate) value: Option<group_agent_model::StructuredOutput>,
}
impl OutputContract {
    pub(crate) fn request(&self, request: ChatRequest) -> ChatRequest {
        #[cfg(feature = "structured-output")]
        if let Some(output) = &self.value {
            return request.with_structured_output(output.clone());
        }
        request
    }
    pub(crate) fn version(&self, approval: bool) -> Option<String> {
        #[cfg(not(feature = "structured-output"))]
        let _ = approval;
        #[cfg(feature = "structured-output")]
        if let Some(output) = &self.value {
            let mode = if approval {
                "approval/structured"
            } else {
                "structured"
            };
            return Some(format!(
                "group-agent-prebuilt/tool-calling-agent/{mode}/1/{}",
                output.contract_id()
            ));
        }
        None
    }
    pub(crate) fn validate(&self, outcome: AgentOutcome) -> Result<AgentOutcome, AgentError> {
        #[cfg(feature = "structured-output")]
        {
            use group_agent_model::{ChatResponse, FinishReason};
            let mut outcome = outcome;
            if let Some(contract) = &self.value
                && outcome.stop_reason() == crate::AgentStopReason::FinalAnswer
            {
                let message = outcome.final_message().ok_or_else(|| {
                    AgentError::from_output(group_agent_model::StructuredOutputError::Incomplete)
                })?;
                // Live responses were checked before commit. Completed restores execute
                // no model node, so conversion must independently check their saved text.
                let response = ChatResponse::new(message.clone(), FinishReason::Stop);
                outcome.output = contract
                    .validate_response(&response)
                    .map_err(AgentError::from_output)?;
            }
            Ok(outcome)
        }
        #[cfg(not(feature = "structured-output"))]
        Ok(outcome)
    }
}
