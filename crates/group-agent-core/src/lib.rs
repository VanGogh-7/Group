//! A strongly typed asynchronous state-graph runtime.
//!
//! Nodes in one super-step inspect the same immutable state and return updates.
//! The runtime owns state mutation, deterministically commits sequential or
//! parallel update batches, and then selects fixed, fan-out, or conditional
//! successors.

mod checkpoint;
mod context;
mod edge;
mod error;
mod event;
mod graph;
mod node;
mod runtime;
mod state;

pub use checkpoint::{
    Checkpoint, CheckpointConfig, CheckpointId, CheckpointPolicy, CheckpointRequest,
    CheckpointState, Checkpointer, InMemoryCheckpointer, ThreadId,
};
pub use context::{NodeContext, RunConfig, RunControl};
pub use edge::{END, NodeId, START};
pub use error::{
    CheckpointerError, GraphBuildError, GraphCompileError, GraphRunError, NodeError, RouteError,
    SnapshotError, StateError,
};
pub use event::{EventConfig, EventRetention, EventSink, GraphEvent, RunFailure, RunId};
pub use graph::{CompiledGraph, StateGraph};
pub use node::Node;
pub use runtime::RunReport;
pub use state::{GraphState, NodeUpdate};
