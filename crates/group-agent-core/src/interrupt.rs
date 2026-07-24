use std::any::{Any, type_name};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::{CheckpointId, GraphEvent, GraphState, NodeId, NodePath, RunId, ThreadId};

static NEXT_INTERRUPT_ID: AtomicU64 = AtomicU64::new(1);

/// Identifies one node suspension request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InterruptId(u64);

impl InterruptId {
    /// Creates an identifier for custom checkpoint tooling.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn next() -> Self {
        Self(NEXT_INTERRUPT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for InterruptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone)]
struct TypedValue {
    value: Arc<dyn Any + Send + Sync>,
    type_name: &'static str,
}

impl TypedValue {
    fn new<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            value: Arc::new(value),
            type_name: type_name::<T>(),
        }
    }

    fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.value.downcast_ref()
    }

    fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.value, &other.value)
    }
}

impl fmt::Debug for TypedValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedValue")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

/// Type-erased, reference-counted payload presented to an interrupt handler.
#[derive(Clone, Debug)]
pub struct InterruptPayload(TypedValue);

impl InterruptPayload {
    /// Stores a typed payload behind a shared allocation.
    #[must_use]
    pub fn new<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self(TypedValue::new(value))
    }

    /// Returns the payload when its concrete type is `T`.
    #[must_use]
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.0.downcast_ref()
    }

    /// Returns the payload's concrete Rust type name.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        self.0.type_name
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}

/// Type-erased value supplied when resuming an interrupted checkpoint.
#[derive(Clone, Debug)]
pub struct ResumeValue(TypedValue);

impl ResumeValue {
    /// Stores a typed resume value behind a shared allocation.
    #[must_use]
    pub fn new<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self(TypedValue::new(value))
    }

    /// Returns the value when its concrete type is `T`.
    #[must_use]
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.0.downcast_ref()
    }

    /// Returns the value's concrete Rust type name.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        self.0.type_name
    }
}

/// A typed resume-value access failure inside a node.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ResumeValueError {
    /// The current node execution has no resume value.
    #[error("resume value is missing; expected type `{expected}`")]
    Missing { expected: &'static str },
    /// The supplied resume value has a different concrete type.
    #[error("resume value type mismatch: expected `{expected}`, actual `{actual}`")]
    TypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
}

/// A node request to suspend execution with a typed payload.
#[derive(Clone, Debug)]
pub struct InterruptRequest {
    id: InterruptId,
    payload: InterruptPayload,
}

impl InterruptRequest {
    /// Creates a new interrupt request and assigns it a fresh identifier.
    #[must_use]
    pub fn new<T>(payload: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            id: InterruptId::next(),
            payload: InterruptPayload::new(payload),
        }
    }

    /// Returns this request's identifier.
    #[must_use]
    pub const fn id(&self) -> InterruptId {
        self.id
    }

    /// Returns the shared typed payload.
    #[must_use]
    pub const fn payload(&self) -> &InterruptPayload {
        &self.payload
    }

    pub(crate) fn into_checkpoint(self, node_path: NodePath) -> CheckpointInterrupt {
        CheckpointInterrupt {
            id: self.id,
            node_path,
            payload: self.payload,
        }
    }
}

/// The successful result of an interruptible node.
#[derive(Debug)]
#[non_exhaustive]
pub enum NodeOutcome<U> {
    /// Continue execution with a state update.
    Update(U),
    /// Suspend execution without applying an update.
    Interrupt(InterruptRequest),
}

impl<U> NodeOutcome<U> {
    /// Creates an update outcome.
    #[must_use]
    pub const fn update(update: U) -> Self {
        Self::Update(update)
    }

    /// Creates an interrupt outcome with a typed payload.
    #[must_use]
    pub fn interrupt<T>(payload: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self::Interrupt(InterruptRequest::new(payload))
    }
}

impl<U> From<U> for NodeOutcome<U> {
    fn from(update: U) -> Self {
        Self::Update(update)
    }
}

/// Interrupt metadata retained by a checkpoint.
#[derive(Clone, Debug)]
pub struct CheckpointInterrupt {
    id: InterruptId,
    node_path: NodePath,
    payload: InterruptPayload,
}

impl CheckpointInterrupt {
    /// Returns the interrupt identifier.
    #[must_use]
    pub const fn id(&self) -> InterruptId {
        self.id
    }

    /// Returns the node that must be re-executed on resume.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        self.node_path.leaf()
    }

    /// Returns the complete structured path of the interrupted node.
    #[must_use]
    pub const fn node_path(&self) -> &NodePath {
        &self.node_path
    }

    /// Returns the shared typed payload.
    #[must_use]
    pub const fn payload(&self) -> &InterruptPayload {
        &self.payload
    }

    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.id == other.id
            && self.node_path == other.node_path
            && self.payload.ptr_eq(&other.payload)
    }
}

/// A successful graph invocation that either completed or suspended.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ExecutionOutcome<S>
where
    S: GraphState,
{
    /// The graph reached an empty frontier.
    Completed(crate::RunReport<S>),
    /// The graph saved an interrupted checkpoint and suspended.
    Interrupted(InterruptReport<S>),
}

impl<S> ExecutionOutcome<S>
where
    S: GraphState,
{
    /// Returns the invocation identifier.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        match self {
            Self::Completed(report) => report.run_id(),
            Self::Interrupted(report) => report.run_id(),
        }
    }

    /// Returns the last committed state.
    #[must_use]
    pub const fn state(&self) -> &S {
        match self {
            Self::Completed(report) => report.final_state(),
            Self::Interrupted(report) => report.state(),
        }
    }

    /// Returns the completed state for compatibility with checkpoint calls.
    ///
    /// For an interrupted outcome this is the last committed state at the
    /// suspension boundary.
    #[must_use]
    pub const fn final_state(&self) -> &S {
        self.state()
    }

    /// Returns the cumulative number of committed node updates.
    #[must_use]
    pub const fn steps(&self) -> usize {
        match self {
            Self::Completed(report) => report.steps(),
            Self::Interrupted(report) => report.steps(),
        }
    }

    /// Returns node attempts observed by this invocation.
    #[must_use]
    pub fn visited_nodes(&self) -> &[NodePath] {
        match self {
            Self::Completed(report) => report.visited_nodes(),
            Self::Interrupted(report) => report.visited_nodes(),
        }
    }

    /// Returns retained lifecycle events.
    #[must_use]
    pub fn events(&self) -> &[GraphEvent] {
        match self {
            Self::Completed(report) => report.events(),
            Self::Interrupted(report) => report.events(),
        }
    }

    /// Returns the completed report, when execution reached an empty frontier.
    #[must_use]
    pub const fn as_completed(&self) -> Option<&crate::RunReport<S>> {
        match self {
            Self::Completed(report) => Some(report),
            Self::Interrupted(_) => None,
        }
    }

    /// Returns the interruption report, when execution suspended.
    #[must_use]
    pub const fn as_interrupted(&self) -> Option<&InterruptReport<S>> {
        match self {
            Self::Completed(_) => None,
            Self::Interrupted(report) => Some(report),
        }
    }
}

/// Successful suspension information returned after its checkpoint is saved.
#[derive(Clone, Debug)]
pub struct InterruptReport<S>
where
    S: GraphState,
{
    pub(crate) run_id: RunId,
    pub(crate) state: S,
    pub(crate) steps: usize,
    pub(crate) superstep: usize,
    pub(crate) visited_nodes: Vec<NodePath>,
    pub(crate) events: Vec<GraphEvent>,
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) thread_id: ThreadId,
    pub(crate) interrupt: CheckpointInterrupt,
}

impl<S> InterruptReport<S>
where
    S: GraphState,
{
    /// Returns the invocation identifier.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the last committed state.
    #[must_use]
    pub const fn state(&self) -> &S {
        &self.state
    }

    /// Returns the cumulative committed-node count.
    #[must_use]
    pub const fn steps(&self) -> usize {
        self.steps
    }

    /// Returns the cumulative committed super-step count.
    #[must_use]
    pub const fn superstep(&self) -> usize {
        self.superstep
    }

    /// Returns node attempts observed by this invocation.
    #[must_use]
    pub fn visited_nodes(&self) -> &[NodePath] {
        &self.visited_nodes
    }

    /// Returns retained lifecycle events.
    #[must_use]
    pub fn events(&self) -> &[GraphEvent] {
        &self.events
    }

    /// Returns the saved interrupted checkpoint identifier.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the logical thread containing the checkpoint.
    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    /// Returns the interrupt metadata and payload.
    #[must_use]
    pub const fn interrupt(&self) -> &CheckpointInterrupt {
        &self.interrupt
    }
}
