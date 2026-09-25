//! Experimental fixed two-stage composition over one Core graph and lineage.
mod codec;
mod durable;
mod durable_outcome;
mod error;
mod event;
mod streaming;
pub use codec::{SequenceSnapshot, SequenceSnapshotCodec};
pub use durable_outcome::{
    SequenceForkReport, SequenceInterrupted, SequenceReplayReport, SequenceRunOutcome,
};
pub use event::{SequenceEventSink, SequenceEventStream, SequenceStreamEvent};
mod nodes;
mod outcome;
mod state;
use crate::{AgentConfig, structured_output::OutputContract};
pub use error::{HandoffError, SequenceBuildError, SequenceError, SequenceErrorKind};
use group_agent_core::{CompiledGraph, EventConfig, RunConfig, RunControl};
use group_agent_model::{ChatModel, Message, StructuredOutput, ValidatedJsonOutput};
use group_agent_tool::ToolRuntime;
pub use outcome::SequenceOutcome;
use serde::{Deserialize, Serialize};
use state::SequenceState;
use std::{fmt, sync::Arc};

/// Stable logical stage identity. Does not create a child Core run or thread.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentStageId(String);
impl AgentStageId {
    pub fn new(value: impl Into<String>) -> Result<Self, SequenceBuildError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(SequenceError::new(SequenceErrorKind::Configuration));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for AgentStageId {
    type Error = SequenceBuildError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<AgentStageId> for String {
    fn from(value: AgentStageId) -> String {
        value.0
    }
}
impl fmt::Debug for AgentStageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentStageId")
            .field("bytes", &self.0.len())
            .finish()
    }
}
/// Owned immutable stage configuration, separate from standalone private graphs.
#[derive(Clone)]
pub struct AgentStage {
    pub(crate) id: AgentStageId,
    pub(crate) model: ChatModel,
    pub(crate) tools: ToolRuntime,
    pub(crate) config: AgentConfig,
    pub(crate) output: StructuredOutput,
}
impl AgentStage {
    pub fn new(
        id: AgentStageId,
        model: ChatModel,
        tools: ToolRuntime,
        config: AgentConfig,
        output: StructuredOutput,
    ) -> Result<Self, SequenceBuildError> {
        if !model.metadata().capabilities().structured_output() {
            return Err(SequenceError::source_error(
                SequenceErrorKind::Configuration,
                group_agent_model::ModelError::unsupported(
                    group_agent_model::ModelCapability::StructuredOutput,
                    model.metadata(),
                ),
            ));
        }
        Ok(Self {
            id,
            model,
            tools,
            config,
            output,
        })
    }
    pub fn id(&self) -> &AgentStageId {
        &self.id
    }
    pub(crate) fn contract(&self) -> OutputContract {
        OutputContract {
            value: Some(self.output.clone()),
        }
    }
}
impl fmt::Debug for AgentStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentStage")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
/// Synchronous, bounded, side-effect-free application mapping. May rerun if unsaved.
pub trait HandoffMapper: Send + Sync {
    fn map(&self, output: &ValidatedJsonOutput) -> Result<Vec<Message>, HandoffError>;
}
impl<F> HandoffMapper for F
where
    F: Fn(&ValidatedJsonOutput) -> Result<Vec<Message>, HandoffError> + Send + Sync,
{
    fn map(&self, output: &ValidatedJsonOutput) -> Result<Vec<Message>, HandoffError> {
        self(output)
    }
}
/// Two independent conversations executed by one compiled Core graph.
#[derive(Clone)]
pub struct AgentSequence {
    pub(crate) graph: Arc<CompiledGraph<SequenceState>>,
    pub(crate) stages: Arc<[AgentStage; 2]>,
    pub(crate) run_config: RunConfig,
}
impl AgentSequence {
    pub fn new(
        revision: impl Into<String>,
        first: AgentStage,
        second: AgentStage,
        mapper: Arc<dyn HandoffMapper>,
    ) -> Result<Self, SequenceBuildError> {
        let revision = AgentStageId::new(revision)?;
        if first.id == second.id {
            return Err(SequenceError::new(SequenceErrorKind::Configuration));
        }
        let steps = first
            .config
            .max_rounds()
            .checked_add(second.config.max_rounds())
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| SequenceError::new(SequenceErrorKind::Configuration))?;
        let stages = Arc::new([first, second]);
        let graph = nodes::compile(&revision, stages.clone(), mapper)?;
        Ok(Self {
            graph: Arc::new(graph),
            stages,
            run_config: RunConfig::new(steps),
        })
    }
    pub async fn invoke(&self, messages: Vec<Message>) -> Result<SequenceOutcome, SequenceError> {
        self.invoke_with_control(messages, EventConfig::default(), RunControl::default())
            .await
    }
    pub async fn invoke_with_control(
        &self,
        messages: Vec<Message>,
        events: EventConfig,
        control: RunControl,
    ) -> Result<SequenceOutcome, SequenceError> {
        if self.stages.iter().any(|s| s.config.tool_approval()) {
            return Err(SequenceError::new(SequenceErrorKind::Configuration));
        }
        let state = SequenceState::new(messages)?;
        let report = self
            .graph
            .invoke_with_control(state, self.run_config.clone(), events, control)
            .await
            .map_err(SequenceError::graph)?;
        SequenceOutcome::from_state(report.into_final_state(), &self.stages)
    }
}

/// Persisted, stage-scoped approval request. Inspect calls explicitly to render UI.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SequenceApprovalRequest {
    pub(crate) stage: AgentStageId,
    pub(crate) request: crate::AgentApprovalRequest,
}
impl SequenceApprovalRequest {
    pub fn stage(&self) -> &AgentStageId {
        &self.stage
    }
    pub fn request(&self) -> &crate::AgentApprovalRequest {
        &self.request
    }
}
/// One-attempt approval decision for the saved active stage.
#[derive(Clone, Debug)]
pub struct SequenceApprovalDecision {
    pub(crate) stage: AgentStageId,
    pub(crate) decision: crate::AgentApprovalDecision,
}
impl SequenceApprovalDecision {
    pub fn new(stage: AgentStageId, decision: crate::AgentApprovalDecision) -> Self {
        Self { stage, decision }
    }
}
