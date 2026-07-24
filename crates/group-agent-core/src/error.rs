use std::error::Error as StdError;
use std::time::Duration;

use thiserror::Error;

use crate::{CheckpointId, GraphVersion, NodeId, RunId, ThreadId};

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
    /// A fan-out edge declared no targets.
    #[error("fan-out edge from `{source_node}` must declare at least one target")]
    EmptyFanOutTargets { source_node: NodeId },
    /// A fan-out edge declared the same target more than once.
    #[error("fan-out edge from `{source_node}` declares duplicate target `{target}`")]
    DuplicateFanOutTarget { source_node: NodeId, target: NodeId },
    /// A source attempted to register more than one fan-out transition.
    #[error("node `{source_node}` already has a fan-out transition")]
    MultipleFanOutTransitions { source_node: NodeId },
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
    /// A fan-out source is not a registered normal node.
    #[error("fan-out source `{source_node}` is not a registered node")]
    UnknownFanOutSource { source_node: NodeId },
    /// A fan-out target does not name a registered node or `END`.
    #[error("fan-out from `{source_node}` references unknown target `{target}`")]
    UnknownFanOutTarget { source_node: NodeId, target: NodeId },
    /// `START` has no outgoing fixed edge.
    #[error("START must have exactly one outgoing fixed edge")]
    MissingStartEdge,
    /// `START` has an incoming edge.
    #[error("START cannot have an incoming edge from `{from}`")]
    StartHasIncoming { from: NodeId },
    /// `START` attempted to use conditional routing.
    #[error("START cannot have a conditional edge")]
    StartHasConditionalEdge,
    /// `START` attempted to use fan-out.
    #[error("START cannot have a fan-out transition")]
    StartHasFanOut,
    /// `END` has a fixed outgoing edge.
    #[error("END cannot have an outgoing edge to `{to}`")]
    EndHasOutgoing { to: NodeId },
    /// `END` attempted to use conditional routing.
    #[error("END cannot have a conditional edge")]
    EndHasConditionalEdge,
    /// `END` attempted to use fan-out.
    #[error("END cannot have a fan-out transition")]
    EndHasFanOut,
    /// `START` has more than one fixed outgoing edge.
    #[error("START has {count} outgoing fixed edges; exactly one is allowed")]
    MultipleStartEdges { count: usize },
    /// A normal node has more than one fixed successor.
    #[error("node `{node_id}` has {count} outgoing fixed edges; at most one is allowed")]
    MultipleOutgoingEdges { node_id: NodeId, count: usize },
    /// A node declared more than one transition kind.
    #[error("node `{node_id}` cannot combine fixed, fan-out, and conditional transitions")]
    MixedOutgoingEdgeKinds { node_id: NodeId },
    /// A reachable normal node has no successor.
    #[error("node `{node_id}` has no outgoing fixed, fan-out, or conditional transition")]
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
define_source_error!(
    SnapshotError,
    "An error produced while creating or restoring a checkpoint snapshot."
);
define_source_error!(
    CheckpointerError,
    "An error returned by a checkpoint storage implementation."
);

/// Why a checkpoint cannot be resumed by a compiled graph.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointIncompatibility {
    /// The checkpoint was created by an unversioned graph.
    #[error("checkpoint has no graph version")]
    UnversionedCheckpoint,
    /// The target compiled graph has no explicit version.
    #[error("compiled graph has no graph version")]
    UnversionedGraph,
    /// The explicit graph versions differ.
    #[error("checkpoint graph version `{checkpoint}` does not match compiled graph `{compiled}`")]
    GraphVersionMismatch {
        checkpoint: GraphVersion,
        compiled: GraphVersion,
    },
    /// A custom store returned a checkpoint from another thread.
    #[error("checkpoint belongs to thread `{actual_thread}`")]
    ThreadMismatch { actual_thread: ThreadId },
    /// The restored frontier names an unknown executable node.
    #[error("checkpoint frontier contains unknown node `{node_id}`")]
    UnknownFrontierNode { node_id: NodeId },
    /// `START` cannot appear in a checkpoint frontier.
    #[error("checkpoint frontier contains START")]
    StartInFrontier,
    /// `END` is represented by an empty frontier and cannot appear explicitly.
    #[error("checkpoint frontier contains END")]
    EndInFrontier,
    /// A completed checkpoint retained executable frontier nodes.
    #[error("completed checkpoint has a non-empty frontier")]
    CompletedWithFrontier,
    /// An incomplete checkpoint has no node from which to continue.
    #[error("incomplete checkpoint has an empty frontier")]
    IncompleteWithoutFrontier,
}

/// An error raised during graph execution.
#[derive(Debug, Error)]
pub enum GraphRunError {
    /// Cancellation was requested for the invocation.
    #[error("run `{run_id}` was cancelled at step {step} near node {node_id:?}")]
    Cancelled {
        run_id: RunId,
        node_id: Option<NodeId>,
        step: usize,
    },
    /// The configured run timeout elapsed.
    #[error("run `{run_id}` timed out after {timeout:?} at step {step} near node {node_id:?}")]
    RunTimedOut {
        run_id: RunId,
        timeout: Duration,
        node_id: Option<NodeId>,
        step: usize,
    },
    /// The configured timeout for one node elapsed.
    #[error("node `{node_id}` in run `{run_id}` timed out after {timeout:?} at step {step}")]
    NodeTimedOut {
        run_id: RunId,
        timeout: Duration,
        node_id: NodeId,
        step: usize,
    },
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
    /// A parallel state-update batch could not be applied.
    #[error("parallel state-update batch from nodes {node_ids:?} failed at step {step}: {source}")]
    StateBatchUpdateFailed {
        node_ids: Vec<NodeId>,
        step: usize,
        #[source]
        source: StateError,
    },
    /// Creating a checkpoint snapshot failed.
    #[error(
        "snapshot creation for thread `{thread_id}` in run `{run_id}` failed after super-step \
         {superstep} at step {step}: {source}"
    )]
    SnapshotFailed {
        run_id: RunId,
        thread_id: ThreadId,
        superstep: usize,
        step: usize,
        #[source]
        source: SnapshotError,
    },
    /// The checkpoint thread advanced beyond this invocation's expected parent.
    #[error(
        "checkpoint conflict for thread `{thread_id}` in run `{run_id}` after super-step \
         {superstep} at step {step}: expected parent {expected_parent:?}, current latest is \
         {actual_parent:?}"
    )]
    CheckpointConflict {
        run_id: RunId,
        thread_id: ThreadId,
        superstep: usize,
        step: usize,
        expected_parent: Option<CheckpointId>,
        actual_parent: Option<CheckpointId>,
    },
    /// A checkpoint idempotency key was reused for different metadata.
    #[error(
        "checkpoint idempotency key `{checkpoint_id}` for thread `{thread_id}` in run `{run_id}` \
         conflicts with an existing request"
    )]
    CheckpointIdConflict {
        run_id: RunId,
        thread_id: ThreadId,
        checkpoint_id: CheckpointId,
        superstep: usize,
        step: usize,
    },
    /// Saving a checkpoint failed.
    #[error(
        "checkpoint save for thread `{thread_id}` in run `{run_id}` failed after super-step \
         {superstep} at step {step}: {source}"
    )]
    CheckpointSaveFailed {
        run_id: RunId,
        thread_id: ThreadId,
        superstep: usize,
        step: usize,
        #[source]
        source: CheckpointerError,
    },
    /// Loading checkpoint storage failed.
    #[error("checkpoint load for thread `{thread_id}` in run `{run_id}` failed: {source}")]
    CheckpointLoadFailed {
        run_id: RunId,
        thread_id: ThreadId,
        checkpoint_id: Option<CheckpointId>,
        #[source]
        source: CheckpointerError,
    },
    /// No checkpoint matched the requested resume target.
    #[error("checkpoint {checkpoint_id:?} was not found for thread `{thread_id}`")]
    CheckpointNotFound {
        run_id: RunId,
        thread_id: ThreadId,
        checkpoint_id: Option<CheckpointId>,
    },
    /// The selected checkpoint is not the thread's latest checkpoint.
    #[error(
        "checkpoint `{checkpoint_id}` is not latest for thread `{thread_id}`; latest is \
         {latest_checkpoint_id:?}"
    )]
    ResumeConflict {
        run_id: RunId,
        thread_id: ThreadId,
        checkpoint_id: CheckpointId,
        latest_checkpoint_id: Option<CheckpointId>,
        step: usize,
    },
    /// The selected checkpoint is incompatible with the compiled graph.
    #[error("checkpoint `{checkpoint_id}` for thread `{thread_id}` is incompatible: {reason}")]
    CheckpointIncompatible {
        run_id: RunId,
        thread_id: ThreadId,
        checkpoint_id: CheckpointId,
        step: usize,
        reason: CheckpointIncompatibility,
    },
    /// Restoring state from a checkpoint snapshot failed.
    #[error(
        "restore from checkpoint `{checkpoint_id}` for thread `{thread_id}` failed at step \
         {step}: {source}"
    )]
    RestoreFailed {
        run_id: RunId,
        thread_id: ThreadId,
        checkpoint_id: CheckpointId,
        superstep: usize,
        step: usize,
        #[source]
        source: SnapshotError,
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
