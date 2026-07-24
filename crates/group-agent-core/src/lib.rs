//! A strongly typed asynchronous state-graph runtime.
//!
//! Nodes in one super-step inspect the same immutable state and return updates.
//! The runtime owns state mutation, deterministically commits sequential or
//! parallel update batches, and then selects fixed, fan-out, or conditional
//! successors.

mod checkpoint;
mod checkpoint_codec;
mod checkpoint_record;
mod checkpoint_store;
mod context;
mod edge;
mod error;
mod event;
mod graph;
mod id;
mod interrupt;
mod node;
mod path;
mod runtime;
mod state;

pub use checkpoint::{
    Checkpoint, CheckpointConfig, CheckpointPolicy, CheckpointRequest, CheckpointState,
    CheckpointWriteError, Checkpointer, GraphVersion, InMemoryCheckpointer, RecordCheckpointer,
    ResumeConfig, ResumeTarget, ThreadId,
};
pub use checkpoint_codec::{
    CheckpointCodec, CheckpointCodecError, CheckpointEncodingError, CheckpointReconstructionError,
    CodecDescriptor, EncodedValue,
};
pub use checkpoint_record::{
    CheckpointFormatVersion, CheckpointRecord, CheckpointRecordError, CheckpointRecordInterrupt,
    CheckpointRecordParts,
};
pub use checkpoint_store::{CheckpointStore, InMemoryCheckpointStore};
pub use context::{NodeContext, RunConfig, RunControl};
pub use edge::{END, NodeId, START};
pub use error::{
    CheckpointIncompatibility, CheckpointerError, GraphBuildError, GraphCompileError,
    GraphRunError, NodeError, RouteError, SnapshotError, StateError,
};
pub use event::{EventConfig, EventRetention, EventSink, GraphEvent, RunFailure};
pub use graph::{CompiledGraph, StateGraph};
pub use id::{CheckpointId, InterruptId, RunId};
pub use interrupt::{
    CheckpointInterrupt, ExecutionOutcome, InterruptPayload, InterruptReport, InterruptRequest,
    NodeOutcome, ResumeValue, ResumeValueError,
};
pub use node::{InterruptibleNode, Node};
pub use path::{GraphPath, NodePath};
pub use runtime::RunReport;
pub use state::{GraphState, NodeUpdate};
