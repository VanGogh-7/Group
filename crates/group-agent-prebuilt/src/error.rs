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
    /// Registering the private model graph failed.
    GraphBuild(GraphBuildError),
    /// Compiling the private model graph failed.
    GraphCompile(GraphCompileError),
}

impl fmt::Display for AgentBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GraphBuild(_) => formatter.write_str("agent graph construction failed"),
            Self::GraphCompile(_) => formatter.write_str("agent graph compilation failed"),
        }
    }
}

impl fmt::Debug for AgentBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = match self {
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

/// Experimental failure from [`crate::ToolCallingAgent::invoke`] or
/// [`crate::ToolCallingAgent::invoke_with_control`].
///
/// The immediate source is always the concrete Core [`GraphRunError`].
/// Default formatting does not traverse or format that source, so model
/// messages, prompts, definitions, and lower-level source messages remain
/// excluded unless an application deliberately traverses the chain. This
/// error does not expose internal committed Agent State or a transcript. In a
/// multi-round invocation, earlier Tools may already have executed and caused
/// external side effects before a later failure; the error does not imply
/// non-execution or make a blind retry safe. Cancellation, timeout, and Future
/// drop release local ownership only and do not prove a remote operation or
/// its effects were undone. No committed transcript is durably persisted or
/// returned through this error.
///
/// `GraphRunError` remains the immediate source. Structured batch inspection
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
    source: GraphRunError,
}

impl AgentError {
    pub(crate) const fn from_graph(source: GraphRunError) -> Self {
        Self { source }
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
        let mut current: Option<&(dyn StdError + 'static)> = Some(&self.source);
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
            .field("has_graph_source", &true)
            .finish()
    }
}

impl StdError for AgentError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
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
