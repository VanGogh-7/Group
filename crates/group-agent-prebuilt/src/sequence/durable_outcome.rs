use std::fmt;

use group_agent_core::{
    BranchId, CheckpointId, ExecutionOutcome, ForkReport, InterruptReport, ReplayReport, RunId,
    ThreadId,
};
use group_agent_model::AssistantMessage;

use super::AgentStage;
use super::state::SequenceState;
use super::{SequenceApprovalRequest, SequenceError, SequenceErrorKind, SequenceOutcome};
use crate::AgentStopReason;

/// Experimental outcome of one checkpoint-enabled, resumed, or forked Agent
/// run.
///
/// The private Agent State is converted eagerly into opaque public wrappers
/// and never appears in this API.
#[non_exhaustive]
pub enum SequenceRunOutcome {
    /// The run reached a normal stop condition.
    Completed(SequenceOutcome),
    /// The run suspended at a durable interrupt boundary.
    Interrupted(SequenceInterrupted),
}

impl SequenceRunOutcome {
    pub(crate) fn from_execution(
        outcome: ExecutionOutcome<SequenceState>,
        output: &[AgentStage; 2],
    ) -> Result<Self, SequenceError> {
        match outcome {
            ExecutionOutcome::Completed(report) => Ok(Self::Completed(
                SequenceOutcome::from_state(report.into_final_state(), output)?,
            )),
            ExecutionOutcome::Interrupted(report) => {
                Ok(Self::Interrupted(SequenceInterrupted::from_report(&report)))
            }
            // `ExecutionOutcome` is non_exhaustive; an unknown future kind is
            // a typed invariant failure, never a silent misclassification.
            _ => Err(SequenceError::new(SequenceErrorKind::InvalidState)),
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
    pub const fn as_completed(&self) -> Option<&SequenceOutcome> {
        match self {
            Self::Completed(outcome) => Some(outcome),
            Self::Interrupted(_) => None,
        }
    }

    /// Returns the interruption metadata when the run suspended.
    #[must_use]
    pub const fn as_interrupted(&self) -> Option<&SequenceInterrupted> {
        match self {
            Self::Completed(_) => None,
            Self::Interrupted(interrupted) => Some(interrupted),
        }
    }

    /// Returns why a completed run stopped normally.
    #[must_use]
    pub fn stop_reason(&self) -> Option<AgentStopReason> {
        match self {
            Self::Completed(outcome) => Some(outcome.stop_reason()),
            Self::Interrupted(_) => None,
        }
    }

    /// Returns the final assistant answer of a completed run.
    #[must_use]
    pub fn final_message(&self) -> Option<&AssistantMessage> {
        self.as_completed().and_then(SequenceOutcome::final_message)
    }
}

impl fmt::Debug for SequenceRunOutcome {
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
#[derive(Clone, PartialEq)]
pub struct SequenceInterrupted {
    thread_id: ThreadId,
    checkpoint_id: CheckpointId,
    approval_request: Option<SequenceApprovalRequest>,
}

impl SequenceInterrupted {
    fn from_report(report: &InterruptReport<SequenceState>) -> Self {
        Self {
            thread_id: report.thread_id().clone(),
            checkpoint_id: report.checkpoint_id(),
            approval_request: report
                .interrupt()
                .payload()
                .downcast_ref::<SequenceApprovalRequest>()
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
    /// [`SequenceApprovalRequest::request`] accessor so an application can
    /// present them to a human approver before resuming with an
    /// [`super::SequenceApprovalDecision`].
    #[must_use]
    pub const fn approval_request(&self) -> Option<&SequenceApprovalRequest> {
        self.approval_request.as_ref()
    }
}

impl fmt::Debug for SequenceInterrupted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SequenceInterrupted")
            .field("thread_id", &self.thread_id)
            .field("checkpoint_id", &self.checkpoint_id)
            .field("has_approval_request", &self.approval_request.is_some())
            .finish()
    }
}

/// Experimental report of one exact read-only historical replay.
///
/// The replayed run is eagerly converted into an [`SequenceRunOutcome`] so the
/// private Agent State never appears in this API.
pub struct SequenceReplayReport {
    run_id: RunId,
    source_thread_id: ThreadId,
    source_checkpoint_id: CheckpointId,
    source_step: usize,
    source_superstep: usize,
    steps: usize,
    outcome: SequenceRunOutcome,
}

impl SequenceReplayReport {
    pub(crate) fn from_replay(
        report: ReplayReport<SequenceState>,
        output: &[AgentStage; 2],
    ) -> Result<Self, SequenceError> {
        Ok(Self {
            run_id: report.run_id(),
            source_thread_id: report.source_thread_id().clone(),
            source_checkpoint_id: report.source_checkpoint_id(),
            source_step: report.source_step(),
            source_superstep: report.source_superstep(),
            steps: report.steps(),
            outcome: SequenceRunOutcome::Completed(SequenceOutcome::from_state(
                report.into_final_state(),
                output,
            )?),
        })
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
    pub const fn outcome(&self) -> &SequenceRunOutcome {
        &self.outcome
    }
}

impl fmt::Debug for SequenceReplayReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SequenceReplayReport")
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
/// The branch run is eagerly converted into an [`SequenceRunOutcome`] so the
/// private Agent State never appears in this API.
pub struct SequenceForkReport {
    branch_id: BranchId,
    source_thread_id: ThreadId,
    source_checkpoint_id: CheckpointId,
    run_id: RunId,
    outcome: SequenceRunOutcome,
}

impl SequenceForkReport {
    pub(crate) fn from_fork(
        report: ForkReport<SequenceState>,
        output: &[AgentStage; 2],
    ) -> Result<Self, SequenceError> {
        Ok(Self {
            branch_id: report.branch_id(),
            source_thread_id: report.source_thread_id().clone(),
            source_checkpoint_id: report.source_checkpoint_id(),
            run_id: report.run_id(),
            outcome: SequenceRunOutcome::from_execution(report.into_outcome(), output)?,
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
    pub const fn outcome(&self) -> &SequenceRunOutcome {
        &self.outcome
    }
}

impl fmt::Debug for SequenceForkReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SequenceForkReport")
            .field("branch_id", &self.branch_id)
            .field("source_thread_id", &self.source_thread_id)
            .field("source_checkpoint_id", &self.source_checkpoint_id)
            .field("run_id", &self.run_id)
            .field("outcome", &self.outcome)
            .finish()
    }
}
