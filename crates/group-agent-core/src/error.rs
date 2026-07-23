use std::error::Error as StdError;

use thiserror::Error;

use crate::NodeId;

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// An error raised while modifying a graph builder.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GraphBuildError {
    /// A node identifier was registered more than once.
    #[error("node `{node_id}` is already registered")]
    DuplicateNode { node_id: NodeId },
    /// A normal node attempted to use a reserved identifier.
    #[error("node identifier `{node_id}` is reserved")]
    ReservedNodeId { node_id: NodeId },
    /// A conditional edge declared no possible target.
    #[error("conditional edge from `{source_node}` must declare at least one allowed target")]
    EmptyConditionalTargets { source_node: NodeId },
    /// A conditional edge declared the same target more than once.
    #[error("conditional edge from `{source_node}` declares duplicate target `{target}`")]
    DuplicateConditionalTarget { source_node: NodeId, target: NodeId },
    /// A source attempted to register more than one conditional router.
    #[error("node `{source_node}` already has a conditional router")]
    MultipleConditionalRouters { source_node: NodeId },
}

/// An error found while compiling graph topology.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GraphCompileError {
    /// A fixed edge endpoint does not name a registered or reserved node.
    #[error("edge `{from}` -> `{to}` references unknown node `{node_id}`")]
    UnknownNode {
        from: NodeId,
        to: NodeId,
        node_id: NodeId,
    },
    /// A conditional edge source is not a registered normal node.
    #[error("conditional edge source `{source_node}` is not a registered node")]
    UnknownConditionalSource { source_node: NodeId },
    /// A conditional target does not name a registered node or `END`.
    #[error("conditional edge from `{source_node}` references unknown target `{target}`")]
    UnknownConditionalTarget { source_node: NodeId, target: NodeId },
    /// `START` has no outgoing fixed edge.
    #[error("START must have exactly one outgoing fixed edge")]
    MissingStartEdge,
    /// `START` has an incoming edge.
    #[error("START cannot have an incoming edge from `{from}`")]
    StartHasIncoming { from: NodeId },
    /// `START` attempted to use conditional routing.
    #[error("START cannot have a conditional edge")]
    StartHasConditionalEdge,
    /// `END` has a fixed outgoing edge.
    #[error("END cannot have an outgoing edge to `{to}`")]
    EndHasOutgoing { to: NodeId },
    /// `END` attempted to use conditional routing.
    #[error("END cannot have a conditional edge")]
    EndHasConditionalEdge,
    /// `START` has more than one fixed outgoing edge.
    #[error("START has {count} outgoing fixed edges; exactly one is allowed")]
    MultipleStartEdges { count: usize },
    /// A normal node has more than one fixed successor.
    #[error("node `{node_id}` has {count} outgoing fixed edges; at most one is allowed")]
    MultipleOutgoingEdges { node_id: NodeId, count: usize },
    /// A node has both fixed and conditional routing.
    #[error("node `{node_id}` cannot have both fixed and conditional outgoing edges")]
    MixedOutgoingEdgeKinds { node_id: NodeId },
    /// A reachable normal node has no successor.
    #[error("node `{node_id}` has no outgoing fixed or conditional edge")]
    MissingOutgoingEdge { node_id: NodeId },
    /// A registered node cannot be reached from `START`.
    #[error("node `{node_id}` is unreachable from START")]
    UnreachableNode { node_id: NodeId },
    /// No possible directed route from `START` reaches `END`.
    #[error("END is not reachable from START")]
    NoReachableEnd,
}

macro_rules! define_source_error {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Error)]
        #[error("{message}")]
        pub struct $name {
            message: String,
            #[source]
            source: Option<BoxedError>,
        }

        impl $name {
            /// Creates an error containing only a message.
            #[must_use]
            pub fn message(message: impl Into<String>) -> Self {
                Self {
                    message: message.into(),
                    source: None,
                }
            }

            /// Creates an error that preserves an underlying source error.
            #[must_use]
            pub fn with_source<E>(message: impl Into<String>, source: E) -> Self
            where
                E: Into<Box<dyn StdError + Send + Sync + 'static>>,
            {
                Self {
                    message: message.into(),
                    source: Some(source.into()),
                }
            }

            /// Creates a message-only error.
            ///
            /// This is a compatibility alias for [`Self::message`].
            #[must_use]
            pub fn new(message: impl Into<String>) -> Self {
                Self::message(message)
            }

            /// Returns the framework-level error message.
            #[must_use]
            pub fn as_message(&self) -> &str {
                &self.message
            }
        }
    };
}

define_source_error!(NodeError, "An error returned by a node implementation.");
define_source_error!(
    StateError,
    "An error produced while applying a state update."
);
define_source_error!(
    RouteError,
    "An error returned by a synchronous conditional router."
);

/// An error raised during graph execution.
#[derive(Debug, Error)]
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
    /// A conditional router failed.
    #[error("conditional router for node `{node_id}` failed at step {step}: {source}")]
    RouteFailed {
        node_id: NodeId,
        step: usize,
        #[source]
        source: RouteError,
    },
    /// A conditional router returned a target outside its declared whitelist.
    #[error(
        "conditional router for node `{node_id}` selected undeclared target `{target}` at step {step}"
    )]
    InvalidRouteTarget {
        node_id: NodeId,
        target: NodeId,
        step: usize,
    },
}
