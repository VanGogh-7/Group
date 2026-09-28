//! Experimental fixed conditional composition over one Core graph and lineage.
mod codec;
mod durable;
mod durable_outcome;
mod error;
mod event;
mod streaming;
pub use codec::{BranchSnapshot, BranchSnapshotCodec};
pub use durable_outcome::{
    BranchForkReport, BranchInterrupted, BranchReplayReport, BranchRunOutcome,
};
pub use event::{BranchEventSink, BranchEventStream, BranchStreamEvent};
mod nodes;
mod outcome;
mod state;
pub use error::{BranchBuildError, BranchError, BranchErrorKind, BranchSelectionError};
use group_agent_core::{CompiledGraph, EventConfig, RunConfig, RunControl};
use group_agent_model::{Message, ValidatedJsonOutput};
pub use outcome::BranchOutcome;
use serde::{Deserialize, Serialize};
use state::BranchState;
use std::{fmt, sync::Arc};

use crate::{AgentStage, AgentStageId};

/// Fixed construction-time branch destination, without input payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchTarget {
    B,
    C,
    Complete,
}
impl BranchTarget {
    pub(crate) fn index(self) -> Option<usize> {
        match self {
            Self::B => Some(1),
            Self::C => Some(2),
            Self::Complete => None,
        }
    }
}
/// Application selection and isolated downstream input. Debug omits messages.
pub enum BranchSelection {
    B(Vec<Message>),
    C(Vec<Message>),
    Complete,
}
impl BranchSelection {
    pub fn target(&self) -> BranchTarget {
        match self {
            Self::B(_) => BranchTarget::B,
            Self::C(_) => BranchTarget::C,
            Self::Complete => BranchTarget::Complete,
        }
    }
}
impl fmt::Debug for BranchSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BranchSelection")
            .field("target", &self.target())
            .finish_non_exhaustive()
    }
}

/// Synchronous, bounded, side-effect-free application mapping. May rerun if unsaved.
pub trait BranchSelector: Send + Sync {
    fn select(&self, output: &ValidatedJsonOutput)
    -> Result<BranchSelection, BranchSelectionError>;
}
impl<F> BranchSelector for F
where
    F: Fn(&ValidatedJsonOutput) -> Result<BranchSelection, BranchSelectionError> + Send + Sync,
{
    fn select(
        &self,
        output: &ValidatedJsonOutput,
    ) -> Result<BranchSelection, BranchSelectionError> {
        self(output)
    }
}
/// One first stage and at most one selected conversation executed by one compiled Core graph.
#[derive(Clone)]
pub struct AgentBranch {
    pub(crate) graph: Arc<CompiledGraph<BranchState>>,
    pub(crate) stages: Arc<[AgentStage; 3]>,
    pub(crate) run_config: RunConfig,
}
impl AgentBranch {
    pub fn new(
        revision: impl Into<String>,
        first: AgentStage,
        b: AgentStage,
        c: AgentStage,
        selector: Arc<dyn BranchSelector>,
    ) -> Result<Self, BranchBuildError> {
        let revision = AgentStageId::new(revision)
            .map_err(|e| BranchError::source_error(BranchErrorKind::Configuration, e))?;
        if first.id == b.id || first.id == c.id || b.id == c.id {
            return Err(BranchError::new(BranchErrorKind::Configuration));
        }
        let steps = first
            .config
            .max_rounds()
            .checked_add(b.config.max_rounds().max(c.config.max_rounds()))
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| BranchError::new(BranchErrorKind::Configuration))?;
        let stages = Arc::new([first, b, c]);
        let graph = nodes::compile(&revision, stages.clone(), selector)?;
        Ok(Self {
            graph: Arc::new(graph),
            stages,
            run_config: RunConfig::new(steps),
        })
    }
    pub async fn invoke(&self, messages: Vec<Message>) -> Result<BranchOutcome, BranchError> {
        self.invoke_with_control(messages, EventConfig::default(), RunControl::default())
            .await
    }
    pub async fn invoke_with_control(
        &self,
        messages: Vec<Message>,
        events: EventConfig,
        control: RunControl,
    ) -> Result<BranchOutcome, BranchError> {
        if self.stages.iter().any(|s| s.config.tool_approval()) {
            return Err(BranchError::new(BranchErrorKind::Configuration));
        }
        let state = BranchState::new(messages)?;
        let report = self
            .graph
            .invoke_with_control(state, self.run_config.clone(), events, control)
            .await
            .map_err(BranchError::graph)?;
        BranchOutcome::from_state(report.into_final_state(), &self.stages)
    }
}

/// Persisted, stage-scoped approval request. Inspect calls explicitly to render UI.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BranchApprovalRequest {
    pub(crate) stage: AgentStageId,
    pub(crate) request: crate::AgentApprovalRequest,
}
impl BranchApprovalRequest {
    pub fn stage(&self) -> &AgentStageId {
        &self.stage
    }
    pub fn request(&self) -> &crate::AgentApprovalRequest {
        &self.request
    }
}
/// One-attempt approval decision for the saved active stage.
#[derive(Clone, Debug)]
pub struct BranchApprovalDecision {
    pub(crate) stage: AgentStageId,
    pub(crate) decision: crate::AgentApprovalDecision,
}
impl BranchApprovalDecision {
    pub fn new(stage: AgentStageId, decision: crate::AgentApprovalDecision) -> Self {
        Self { stage, decision }
    }
}
