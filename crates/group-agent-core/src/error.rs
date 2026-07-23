use thiserror::Error;

use crate::NodeId;

/// An error raised while modifying a graph builder.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphBuildError {
    /// A node identifier was registered more than once.
    #[error("node `{node_id}` is already registered")]
    DuplicateNode { node_id: NodeId },
    /// A normal node attempted to use a reserved identifier.
    #[error("node identifier `{node_id}` is reserved")]
    ReservedNodeId { node_id: NodeId },
}

/// An error found while compiling graph topology.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphCompileError {
    /// An edge endpoint does not name a registered or reserved node.
    #[error("edge `{from}` -> `{to}` references unknown node `{node_id}`")]
    UnknownNode {
        from: NodeId,
        to: NodeId,
        node_id: NodeId,
    },
    /// `START` has no outgoing edge.
    #[error("START must have exactly one outgoing edge")]
    MissingStartEdge,
    /// `START` has an incoming edge.
    #[error("START cannot have an incoming edge from `{from}`")]
    StartHasIncoming { from: NodeId },
    /// `END` has an outgoing edge.
    #[error("END cannot have an outgoing edge to `{to}`")]
    EndHasOutgoing { to: NodeId },
    /// `START` has more than one outgoing edge.
    #[error("START has {count} outgoing edges; exactly one is allowed")]
    MultipleStartEdges { count: usize },
    /// A normal node has more than one fixed successor.
    #[error("node `{node_id}` has {count} outgoing fixed edges; at most one is allowed")]
    MultipleOutgoingEdges { node_id: NodeId, count: usize },
    /// A registered node cannot be reached from `START`.
    #[error("node `{node_id}` is unreachable from START")]
    UnreachableNode { node_id: NodeId },
    /// No directed path from `START` reaches `END`.
    #[error("END is not reachable from START")]
    NoReachableEnd,
}

/// An error returned by a node implementation.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum NodeError {
    /// A node-specific failure with a human-readable message.
    #[error("{message}")]
    Failed { message: String },
}

impl NodeError {
    /// Creates a node failure.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }
}

/// An error produced while applying a state update.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StateError {
    /// The state rejected an update.
    #[error("{message}")]
    UpdateRejected { message: String },
}

impl StateError {
    /// Creates an update rejection.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self::UpdateRejected {
            message: message.into(),
        }
    }
}

/// An error raised during graph execution.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphRunError {
    /// The configured step limit was reached before `END`.
    #[error(
        "maximum step count {max_steps} reached before executing node `{node_id}` at step {step}"
    )]
    MaxStepsExceeded {
        max_steps: usize,
        node_id: NodeId,
        step: usize,
    },
    /// A node failed.
    #[error("node `{node_id}` failed at step {step}: {source}")]
    NodeFailed {
        node_id: NodeId,
        step: usize,
        #[source]
        source: NodeError,
    },
    /// A state update could not be applied.
    #[error("state update from node `{node_id}` failed at step {step}: {source}")]
    StateUpdateFailed {
        node_id: NodeId,
        step: usize,
        #[source]
        source: StateError,
    },
}
