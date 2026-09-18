use std::fmt;

use group_agent_core::{
    BranchId, CheckpointId, ExecutionOutcome, ForkReport, InterruptReport, ReplayReport, RunId,
    ThreadId,
};
use group_agent_model::AssistantMessage;

use crate::state::AgentState;
use crate::{AgentApprovalRequest, AgentError, AgentOutcome, AgentStopReason};

/// Experimental outcome of one checkpoint-enabled, resumed, or forked Agent
/// run.
///
/// The private Agent State is converted eagerly into opaque public wrappers
/// and never appears in this API.
#[non_exhaustive]
pub enum AgentRunOutcome {
    /// The run reached a normal stop condition.
    Completed(AgentOutcome),
    /// The run suspended at a durable interrupt boundary.
    Interrupted(AgentInterrupted),
}

impl AgentRunOutcome {
    pub(crate) fn from_execution(
        outcome: ExecutionOutcome<AgentState>,
    ) -> Result<Self, AgentError> {
        match outcome {
            ExecutionOutcome::Completed(report) => Ok(Self::Completed(
                AgentOutcome::from_completed_state(report.into_final_state()),
            )),
            ExecutionOutcome::Interrupted(report) => {
                Ok(Self::Interrupted(AgentInterrupted::from_report(&report)))
            }
            // `ExecutionOutcome` is non_exhaustive; an unknown future kind is
            // a typed invariant failure, never a silent misclassification.
            _ => Err(AgentError::unknown_outcome()),
        }
    }

    /// Returns whether the run reached a normal stop condition.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    /// Returns whether the run suspended at an interrupt boundary.
    #[must_use]
    pub const fn is_interrupted(&self) -> bool {
        matches!(self, Self::Interrupted(_))
    }

    /// Returns the completed outcome when the run finished normally.
    #[must_use]
    pub const fn as_completed(&self) -> Option<&AgentOutcome> {
        match self {
            Self::Completed(outcome) => Some(outcome),
            Self::Interrupted(_) => None,
        }
    }

    /// Returns the interruption metadata when the run suspended.
    #[must_use]
    pub const fn as_interrupted(&self) -> Option<&AgentInterrupted> {
        match self {
            Self::Completed(_) => None,
            Self::Interrupted(interrupted) => Some(interrupted),
        }
    }

    /// Returns why a completed run stopped normally.
    #[must_use]
    pub const fn stop_reason(&self) -> Option<AgentStopReason> {
        match self {
            Self::Completed(outcome) => Some(outcome.stop_reason()),
            Self::Interrupted(_) => None,
        }
    }

    /// Returns the final assistant answer of a completed run.
    #[must_use]
    pub fn final_message(&self) -> Option<&AssistantMessage> {
        self.as_completed().and_then(AgentOutcome::final_message)
    }
}

impl fmt::Debug for AgentRunOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(outcome) => formatter.debug_tuple("Completed").field(outcome).finish(),
            Self::Interrupted(interrupted) => formatter
                .debug_tuple("Interrupted")
                .field(interrupted)
                .finish(),
        }
    }
}

/// Experimental metadata of a durably suspended Agent run.
///
/// An approval-enabled Agent suspends when its Tool node is reached without
/// an approval decision; the downcast approval request is then exposed
/// through [`Self::approval_request`]. Any other interrupt payload stays
/// behind the stored checkpoint and is not exposed here.
pub struct AgentInterrupted {
    thread_id: ThreadId,
    checkpoint_id: CheckpointId,
    approval_request: Option<AgentApprovalRequest>,
}

impl AgentInterrupted {
    fn from_report(report: &InterruptReport<AgentState>) -> Self {
        Self {
            thread_id: report.thread_id().clone(),
            checkpoint_id: report.checkpoint_id(),
            approval_request: report
                .interrupt()
                .payload()
                .downcast_ref::<AgentApprovalRequest>()
                .cloned(),
        }
    }

    /// Returns the logical thread that owns the suspended checkpoint.
    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    /// Returns the saved interrupted checkpoint identifier.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the pending Tool approval request when the suspension was
    /// raised for Tool approval.
    ///
    /// The calls are exposed through the explicit
    /// [`AgentApprovalRequest::pending_calls`] accessor so an application can
    /// present them to a human approver before resuming with an
    /// [`crate::AgentApprovalDecision`].
    #[must_use]
    pub const fn approval_request(&self) -> Option<&AgentApprovalRequest> {
        self.approval_request.as_ref()
    }
}

impl fmt::Debug for AgentInterrupted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentInterrupted")
            .field("thread_id", &self.thread_id)
            .field("checkpoint_id", &self.checkpoint_id)
            .field("has_approval_request", &self.approval_request.is_some())
            .finish()
    }
}

/// Experimental report of one exact read-only historical replay.
///
/// The replayed run is eagerly converted into an [`AgentRunOutcome`] so the
/// private Agent State never appears in this API.
pub struct AgentReplayReport {
    run_id: RunId,
    source_thread_id: ThreadId,
    source_checkpoint_id: CheckpointId,
    source_step: usize,
    source_superstep: usize,
    steps: usize,
    outcome: AgentRunOutcome,
}

impl AgentReplayReport {
    pub(crate) fn from_replay(report: ReplayReport<AgentState>) -> Self {
        Self {
            run_id: report.run_id(),
            source_thread_id: report.source_thread_id().clone(),
            source_checkpoint_id: report.source_checkpoint_id(),
            source_step: report.source_step(),
            source_superstep: report.source_superstep(),
            steps: report.steps(),
            outcome: AgentRunOutcome::Completed(AgentOutcome::from_completed_state(
                report.into_final_state(),
            )),
        }
    }

    /// Returns the invocation identifier assigned to the replay.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the logical thread containing the source checkpoint.
    #[must_use]
    pub const fn source_thread_id(&self) -> &ThreadId {
        &self.source_thread_id
    }

    /// Returns the exact historical checkpoint that was replayed.
    #[must_use]
    pub const fn source_checkpoint_id(&self) -> CheckpointId {
        self.source_checkpoint_id
    }

    /// Returns the cumulative node count at the source checkpoint.
    #[must_use]
    pub const fn source_step(&self) -> usize {
        self.source_step
    }

    /// Returns the cumulative super-step count at the source checkpoint.
    #[must_use]
    pub const fn source_superstep(&self) -> usize {
        self.source_superstep
    }

    /// Returns the cumulative number of executed nodes after replay.
    #[must_use]
    pub const fn steps(&self) -> usize {
        self.steps
    }

    /// Returns the replayed run outcome.
    #[must_use]
    pub const fn outcome(&self) -> &AgentRunOutcome {
        &self.outcome
    }
}

impl fmt::Debug for AgentReplayReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentReplayReport")
            .field("run_id", &self.run_id)
            .field("source_thread_id", &self.source_thread_id)
            .field("source_checkpoint_id", &self.source_checkpoint_id)
            .field("source_step", &self.source_step)
            .field("source_superstep", &self.source_superstep)
            .field("steps", &self.steps)
            .field("outcome", &self.outcome)
            .finish()
    }
}

/// Experimental report of one explicit writable historical branch.
///
/// The branch run is eagerly converted into an [`AgentRunOutcome`] so the
/// private Agent State never appears in this API.
pub struct AgentForkReport {
    branch_id: BranchId,
    source_thread_id: ThreadId,
    source_checkpoint_id: CheckpointId,
    run_id: RunId,
    outcome: AgentRunOutcome,
}

impl AgentForkReport {
    pub(crate) fn from_fork(report: ForkReport<AgentState>) -> Result<Self, AgentError> {
        Ok(Self {
            branch_id: report.branch_id(),
            source_thread_id: report.source_thread_id().clone(),
            source_checkpoint_id: report.source_checkpoint_id(),
            run_id: report.run_id(),
            outcome: AgentRunOutcome::from_execution(report.into_outcome())?,
        })
    }

    /// Returns the branch that received the new checkpoints.
    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    /// Returns the logical thread containing the source checkpoint.
    #[must_use]
    pub const fn source_thread_id(&self) -> &ThreadId {
        &self.source_thread_id
    }

    /// Returns the exact historical checkpoint the branch started from.
    #[must_use]
    pub const fn source_checkpoint_id(&self) -> CheckpointId {
        self.source_checkpoint_id
    }

    /// Returns the invocation identifier assigned to the branch run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the branch run outcome.
    #[must_use]
    pub const fn outcome(&self) -> &AgentRunOutcome {
        &self.outcome
    }
}

impl fmt::Debug for AgentForkReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentForkReport")
            .field("branch_id", &self.branch_id)
            .field("source_thread_id", &self.source_thread_id)
            .field("source_checkpoint_id", &self.source_checkpoint_id)
            .field("run_id", &self.run_id)
            .field("outcome", &self.outcome)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use group_agent_core::{BranchId, CheckpointId, GraphState as _, RunId, ThreadId};
    use group_agent_model::{AssistantMessage, Message, ToolCall, ToolCallId, ToolName};

    use super::{AgentForkReport, AgentInterrupted, AgentReplayReport, AgentRunOutcome};
    use crate::state::{AgentState, AgentUpdate};
    use crate::{AgentApprovalRequest, AgentOutcome, AgentStopReason};

    fn secret_outcome() -> AgentOutcome {
        let mut state = AgentState::new(vec![Message::user("SECRET_QUESTION")]);
        state
            .apply(AgentUpdate::ModelCompleted {
                message: AssistantMessage::text("SECRET_ANSWER"),
                usage: None,
            })
            .expect("model update commits");
        AgentOutcome::from_completed_state(state)
    }

    fn interrupted() -> AgentInterrupted {
        AgentInterrupted {
            thread_id: ThreadId::from("interrupted-thread"),
            checkpoint_id: CheckpointId::new(),
            approval_request: None,
        }
    }

    fn assert_redacted(rendered: String) {
        for marker in ["SECRET_QUESTION", "SECRET_ANSWER"] {
            assert!(
                !rendered.contains(marker),
                "Debug must not contain {marker}"
            );
        }
    }

    #[test]
    fn completed_run_outcome_mirrors_agent_outcome_accessors() {
        let outcome = AgentRunOutcome::Completed(secret_outcome());

        assert!(outcome.is_completed());
        assert!(!outcome.is_interrupted());
        assert!(outcome.as_interrupted().is_none());
        assert_eq!(outcome.stop_reason(), Some(AgentStopReason::FinalAnswer));
        assert_eq!(
            outcome.final_message().map(AssistantMessage::text_content),
            Some("SECRET_ANSWER".to_owned()),
        );
        assert_eq!(
            outcome.as_completed().map(AgentOutcome::model_rounds),
            Some(1)
        );
    }

    #[test]
    fn interrupted_run_outcome_carries_only_checkpoint_identity() {
        let outcome = AgentRunOutcome::Interrupted(interrupted());

        assert!(!outcome.is_completed());
        assert!(outcome.is_interrupted());
        assert!(outcome.as_completed().is_none());
        assert_eq!(outcome.stop_reason(), None);
        assert!(outcome.final_message().is_none());
        let interrupted = outcome.as_interrupted().expect("interrupted variant");
        assert_eq!(interrupted.thread_id().as_str(), "interrupted-thread");
    }

    #[test]
    fn run_outcome_debug_is_redacted() {
        assert_redacted(format!(
            "{:?}",
            AgentRunOutcome::Completed(secret_outcome())
        ));
        let interrupted_debug = format!("{:?}", AgentRunOutcome::Interrupted(interrupted()));
        assert!(interrupted_debug.contains("Interrupted"));
        assert!(interrupted_debug.contains("interrupted-thread"));
    }

    #[test]
    fn interrupted_outcome_exposes_the_approval_request_with_redacted_debug() {
        let request = AgentApprovalRequest::new(vec![ToolCall::new(
            ToolCallId::new("SECRET_CALL").expect("valid call id"),
            ToolName::new("SECRET_TOOL").expect("valid tool name"),
            serde_json::json!({"q": "SECRET_ARGUMENT"}),
        )]);
        let outcome = AgentRunOutcome::Interrupted(AgentInterrupted {
            thread_id: ThreadId::from("approval-thread"),
            checkpoint_id: CheckpointId::new(),
            approval_request: Some(request.clone()),
        });

        let suspension = outcome.as_interrupted().expect("interrupted variant");
        assert_eq!(suspension.approval_request(), Some(&request));
        assert_eq!(
            suspension
                .approval_request()
                .map(|request| request.pending_calls().len()),
            Some(1)
        );
        assert!(interrupted().approval_request().is_none());

        let rendered = format!("{outcome:?}");
        assert!(rendered.contains("has_approval_request: true"));
        for marker in ["SECRET_CALL", "SECRET_TOOL", "SECRET_ARGUMENT"] {
            assert!(
                !rendered.contains(marker),
                "Debug must not contain {marker}"
            );
        }
    }

    #[test]
    fn replay_report_debug_is_redacted() {
        let report = AgentReplayReport {
            run_id: RunId::new(),
            source_thread_id: ThreadId::from("source-thread"),
            source_checkpoint_id: CheckpointId::new(),
            source_step: 2,
            source_superstep: 1,
            steps: 3,
            outcome: AgentRunOutcome::Completed(secret_outcome()),
        };

        let rendered = format!("{report:?}");

        assert!(rendered.contains("AgentReplayReport"));
        assert!(rendered.contains("source-thread"));
        assert_redacted(rendered);
    }

    #[test]
    fn fork_report_debug_is_redacted() {
        let report = AgentForkReport {
            branch_id: BranchId::new(),
            source_thread_id: ThreadId::from("source-thread"),
            source_checkpoint_id: CheckpointId::new(),
            run_id: RunId::new(),
            outcome: AgentRunOutcome::Completed(secret_outcome()),
        };

        let rendered = format!("{report:?}");

        assert!(rendered.contains("AgentForkReport"));
        assert_redacted(rendered);
    }
}
