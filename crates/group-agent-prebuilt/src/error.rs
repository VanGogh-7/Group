use std::error::Error as StdError;
use std::fmt;

use group_agent_core::{GraphBuildError, GraphCompileError, GraphRunError};
use group_agent_tool::ToolBatchReport;

/// Experimental failure while constructing a [`crate::ToolCallingAgent`].
///
/// This type retains the concrete Core graph error as its source. Default
/// formatting reports only the construction phase and does not format the
/// source chain.
#[non_exhaustive]
pub enum AgentBuildError {
    /// The model does not support the requested output contract.
    #[cfg(feature = "structured-output")]
    OutputConfiguration(group_agent_model::ModelError),
    /// Registering the private model graph failed.
    GraphBuild(GraphBuildError),
    /// Compiling the private model graph failed.
    GraphCompile(GraphCompileError),
}

impl fmt::Display for AgentBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "structured-output")]
            Self::OutputConfiguration(_) => {
                formatter.write_str("agent output configuration failed")
            }
            Self::GraphBuild(_) => formatter.write_str("agent graph construction failed"),
            Self::GraphCompile(_) => formatter.write_str("agent graph compilation failed"),
        }
    }
}

impl fmt::Debug for AgentBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = match self {
            #[cfg(feature = "structured-output")]
            Self::OutputConfiguration(_) => "output",
            Self::GraphBuild(_) => "build",
            Self::GraphCompile(_) => "compile",
        };
        formatter
            .debug_struct("AgentBuildError")
            .field("phase", &phase)
            .field("has_source", &true)
            .finish()
    }
}

impl StdError for AgentBuildError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            #[cfg(feature = "structured-output")]
            Self::OutputConfiguration(source) => Some(source),
            Self::GraphBuild(source) => Some(source),
            Self::GraphCompile(source) => Some(source),
        }
    }
}

impl From<GraphBuildError> for AgentBuildError {
    fn from(source: GraphBuildError) -> Self {
        Self::GraphBuild(source)
    }
}

impl From<GraphCompileError> for AgentBuildError {
    fn from(source: GraphCompileError) -> Self {
        Self::GraphCompile(source)
    }
}

/// Experimental failure from a [`crate::ToolCallingAgent`] invocation,
/// including the checkpoint-enabled `invoke_with_checkpoint`, `resume`,
/// `replay`, and `fork` methods.
///
/// For graph execution failures the immediate source is the concrete Core
/// [`GraphRunError`]; for an internal conversion invariant failure it is a
/// private typed error. Default formatting does not traverse or format that
/// source, so model
/// messages, prompts, definitions, and lower-level source messages remain
/// excluded unless an application deliberately traverses the chain. This
/// error does not expose internal committed Agent State or a transcript. In a
/// multi-round invocation, earlier Tools may already have executed and caused
/// external side effects before a later failure; the error does not imply
/// non-execution or make a blind retry safe. Cancellation, timeout, and Future
/// drop release local ownership only and do not prove a remote operation or
/// its effects were undone. A committed transcript is never returned through
/// this error; a failure from a checkpoint-enabled invocation may follow
/// durably persisted super-steps, which remain available through the
/// configured checkpointer.
///
/// `GraphRunError` remains the immediate source of graph failures. Structured batch inspection
/// is available only when a Tool infrastructure failure produced a complete
/// current-batch report; it is not a transcript accessor:
///
/// ```
/// # use std::error::Error as _;
/// # use async_trait::async_trait;
/// # use group_agent_core::GraphRunError;
/// # use group_agent_model::{AssistantMessage, ChatModel, ChatModelAdapter, ChatResponse, FinishReason, Message, ModelCapabilities, ModelError, ModelId, ModelMetadata, ProviderId, ToolCallId, ToolResult, ValidatedChatRequest};
/// # use group_agent_prebuilt::{AgentConfig, ToolCallingAgent};
/// # use group_agent_tool::{ToolRegistry, ToolRuntime};
/// # struct OfflineModel { metadata: ModelMetadata }
/// # #[async_trait]
/// # impl ChatModelAdapter for OfflineModel {
/// #     fn metadata(&self) -> &ModelMetadata { &self.metadata }
/// #     async fn complete_raw(&self, _request: ValidatedChatRequest) -> Result<ChatResponse, ModelError> {
/// #         Ok(ChatResponse::new(AssistantMessage::text("unused"), FinishReason::Stop))
/// #     }
/// # }
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let model = ChatModel::from_adapter(OfflineModel { metadata: ModelMetadata::new(
/// #     ProviderId::new("offline")?, ModelId::new("scripted")?, ModelCapabilities::new(),
/// # )})?;
/// # let agent = ToolCallingAgent::new(model, ToolRuntime::new(ToolRegistry::empty()), AgentConfig::new(1)?)?;
/// let invalid = vec![Message::tool(
///     ToolCallId::new("unknown-call")?,
///     ToolResult::text("offline"),
/// )];
/// let error = agent.invoke(invalid).await.unwrap_err();
/// assert!(error.source().unwrap().is::<GraphRunError>());
/// assert!(error.tool_batch_report().is_none());
/// # Ok(())
/// # }
/// ```
pub struct AgentError {
    source: AgentErrorSource,
}

/// Private immediate-source classification for [`AgentError`].
enum AgentErrorSource {
    #[cfg(feature = "structured-output")]
    Output(group_agent_model::StructuredOutputError),
    Graph(GraphRunError),
    UnknownOutcome(UnknownExecutionOutcome),
}

/// Private invariant failure: Core returned an execution outcome kind this
/// crate does not convert.
#[derive(Debug)]
struct UnknownExecutionOutcome;

impl fmt::Display for UnknownExecutionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("unknown Core execution outcome kind")
    }
}

impl StdError for UnknownExecutionOutcome {}

impl AgentError {
    #[cfg(feature = "structured-output")]
    pub(crate) fn from_output(source: group_agent_model::StructuredOutputError) -> Self {
        Self {
            source: AgentErrorSource::Output(source),
        }
    }
    pub(crate) const fn from_graph(source: GraphRunError) -> Self {
        Self {
            source: AgentErrorSource::Graph(source),
        }
    }

    pub(crate) const fn unknown_outcome() -> Self {
        Self {
            source: AgentErrorSource::UnknownOutcome(UnknownExecutionOutcome),
        }
    }

    /// Returns the complete ordered current Tool batch report for an
    /// infrastructure failure, when one was produced.
    ///
    /// This experimental accessor borrows the report retained by the private
    /// Agent failure source. It does not clone results or expose the wrapper.
    /// It returns `None` for non-Tool failures, batch-configuration failures
    /// without a report, and errors that occur after a successful Tool batch;
    /// it is not an accessor for a committed transcript or ToolMessages.
    #[must_use]
    pub fn tool_batch_report(&self) -> Option<&ToolBatchReport> {
        let mut current: Option<&(dyn StdError + 'static)> = StdError::source(self);
        while let Some(error) = current {
            if let Some(failure) = error.downcast_ref::<AgentToolBatchFailure>() {
                return Some(failure.report());
            }
            current = error.source();
        }
        None
    }
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("agent invocation failed")
    }
}

impl fmt::Debug for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentError")
            .field(
                "has_graph_source",
                &matches!(self.source, AgentErrorSource::Graph(_)),
            )
            .finish()
    }
}

impl StdError for AgentError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.source {
            #[cfg(feature = "structured-output")]
            AgentErrorSource::Output(source) => Some(source),
            AgentErrorSource::Graph(source) => Some(source),
            AgentErrorSource::UnknownOutcome(source) => Some(source),
        }
    }
}

pub(crate) struct AgentToolBatchFailure {
    report: ToolBatchReport,
}

impl AgentToolBatchFailure {
    pub(crate) const fn new(report: ToolBatchReport) -> Self {
        Self { report }
    }

    const fn report(&self) -> &ToolBatchReport {
        &self.report
    }
}

impl fmt::Display for AgentToolBatchFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("agent tool batch infrastructure failure")
    }
}

impl fmt::Debug for AgentToolBatchFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentToolBatchFailure")
            .field("has_ordered_report", &true)
            .finish()
    }
}

impl StdError for AgentToolBatchFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.report
            .results()
            .iter()
            .find_map(|result| result.as_ref().err())
            .map(|error| error as &dyn StdError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_outcome_error_is_typed_and_payload_free() {
        let error = AgentError::unknown_outcome();

        assert_eq!(error.to_string(), "agent invocation failed");
        assert_eq!(
            format!("{error:?}"),
            "AgentError { has_graph_source: false }"
        );
        assert!(error.tool_batch_report().is_none());

        let source = error
            .source()
            .expect("unknown outcome carries a typed source");
        assert_eq!(source.to_string(), "unknown Core execution outcome kind");
        assert!(source.source().is_none());
    }
}
