//! A strongly typed asynchronous state-graph runtime.
//!
//! Nodes inspect immutable state and return updates. The runtime owns state
//! mutation and applies each update before selecting a fixed or conditional
//! successor.

mod context;
mod edge;
mod error;
mod event;
mod graph;
mod node;
mod runtime;
mod state;

pub use context::{NodeContext, RunConfig};
pub use edge::{END, NodeId, START};
pub use error::{
    GraphBuildError, GraphCompileError, GraphRunError, NodeError, RouteError, StateError,
};
pub use event::{EventConfig, EventRetention, EventSink, GraphEvent, RunFailure, RunId};
pub use graph::{CompiledGraph, StateGraph};
pub use node::Node;
pub use runtime::RunReport;
pub use state::GraphState;
