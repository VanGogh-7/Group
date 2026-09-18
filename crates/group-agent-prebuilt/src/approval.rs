use std::fmt;

use group_agent_model::ToolCall;
use serde::{Deserialize, Serialize};

/// Experimental durable interrupt payload listing the ToolCalls awaiting
/// human approval.
///
/// An approval-enabled [`crate::ToolCallingAgent`] durably suspends before
/// executing any Tool side effect and stores the exact pending calls of the
/// current round in this payload. Applications inspect the calls through the
/// explicit [`Self::pending_calls`] accessor to render an approval prompt;
/// `Debug` deliberately reports only the pending call count so accidental
/// formatting never exposes call identities or arguments. Serde decoding
/// applies the Model types' validated constructors, so bytes that would
/// construct an invalid ToolCall fail decode.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentApprovalRequest {
    pending_calls: Vec<ToolCall>,
}

impl AgentApprovalRequest {
    pub(crate) const fn new(pending_calls: Vec<ToolCall>) -> Self {
        Self { pending_calls }
    }

    /// Returns the exact pending ToolCalls of the suspended round, in
    /// model-produced order.
    ///
    /// This accessor deliberately exposes the calls so an application can
    /// present them to a human approver; they remain excluded from `Debug`
    /// formatting.
    #[must_use]
    pub fn pending_calls(&self) -> &[ToolCall] {
        &self.pending_calls
    }
}

impl fmt::Debug for AgentApprovalRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentApprovalRequest")
            .field("pending_calls", &self.pending_calls.len())
            .finish()
    }
}

/// Experimental in-memory resume decision for one suspended approval batch.
///
/// The decision is supplied as a Core resume value and is never serialized:
/// `Approve` executes the pending batch as usual, while `Reject` converts
/// every pending call into a business-error ToolMessage and lets the Agent
/// loop continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentApprovalDecision {
    /// Execute the pending Tool batch.
    Approve,
    /// Convert every pending call into a business-error ToolMessage and
    /// continue the loop without executing any Tool.
    Reject,
}
