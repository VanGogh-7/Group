use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use thiserror::Error;

use crate::{CheckpointerError, GraphState, NodeId, RunId, SnapshotError};

static NEXT_CHECKPOINT_ID: AtomicU64 = AtomicU64::new(1);

/// Identifies a durable logical execution thread across one or more runs.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ThreadId(Arc<str>);

impl ThreadId {
    /// Creates a thread identifier.
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// Returns this identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ThreadId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ThreadId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl AsRef<str> for ThreadId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ThreadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Identifies one checkpoint within a checkpointer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointId(u64);

impl CheckpointId {
    /// Creates an identifier for custom checkpointer implementations.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn next() -> Self {
        Self(NEXT_CHECKPOINT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for CheckpointId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// State capability required only by checkpoint-enabled invocations.
///
/// The snapshot type does not need to implement `Clone` or Serde. Checkpoints
/// retain it behind an [`Arc`]. The restore method reserves the state boundary
/// needed by future resume support; the current Runtime does not call it.
pub trait CheckpointState: GraphState {
    /// Immutable state representation retained by a checkpoint.
    type Snapshot: Send + Sync + 'static;

    /// Creates a snapshot from the current committed state.
    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError>;

    /// Reconstructs state from a snapshot for future resume implementations.
    ///
    /// Stage 6 stores this capability but does not expose resume execution.
    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError>
    where
        Self: Sized;
}

/// Controls when a checkpoint-enabled invocation saves.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckpointPolicy {
    /// Save after every successful super-step.
    #[default]
    EverySuperstep,
    /// Save only the completed checkpoint at the end of the run.
    FinalOnly,
}

/// Immutable checkpoint data returned by a [`Checkpointer`].
#[derive(Clone, Debug)]
pub struct Checkpoint<T>
where
    T: Send + Sync + 'static,
{
    id: CheckpointId,
    thread_id: ThreadId,
    run_id: RunId,
    parent_id: Option<CheckpointId>,
    superstep: usize,
    step: usize,
    snapshot: Arc<T>,
    next_frontier: Vec<NodeId>,
    completed: bool,
}

impl<T> Checkpoint<T>
where
    T: Send + Sync + 'static,
{
    /// Returns this checkpoint's identifier.
    #[must_use]
    pub const fn id(&self) -> CheckpointId {
        self.id
    }

    /// Returns the logical thread that owns this checkpoint.
    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    /// Returns the invocation that created this checkpoint.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the previous checkpoint in this thread, if any.
    #[must_use]
    pub const fn parent_id(&self) -> Option<CheckpointId> {
        self.parent_id
    }

    /// Returns the one-based super-step number.
    #[must_use]
    pub const fn superstep(&self) -> usize {
        self.superstep
    }

    /// Returns the cumulative number of executed nodes.
    #[must_use]
    pub const fn step(&self) -> usize {
        self.step
    }

    /// Returns a shared reference-counted snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &Arc<T> {
        &self.snapshot
    }

    /// Returns the stable next frontier.
    #[must_use]
    pub fn next_frontier(&self) -> &[NodeId] {
        &self.next_frontier
    }

    /// Returns whether execution reached an empty frontier.
    #[must_use]
    pub const fn completed(&self) -> bool {
        self.completed
    }
}

/// A checkpoint write prepared by the Runtime after a successful super-step.
///
/// The identifier is an idempotency key for this write. `expected_parent`
/// names the state lineage on which the Runtime executed. A checkpointer must
/// compare it with the thread's current latest checkpoint atomically with
/// insertion.
#[derive(Debug)]
pub struct CheckpointRequest<T>
where
    T: Send + Sync + 'static,
{
    checkpoint_id: CheckpointId,
    expected_parent: Option<CheckpointId>,
    thread_id: ThreadId,
    run_id: RunId,
    superstep: usize,
    step: usize,
    snapshot: Arc<T>,
    next_frontier: Vec<NodeId>,
    completed: bool,
}

pub(crate) struct CheckpointLineage {
    checkpoint_id: CheckpointId,
    expected_parent: Option<CheckpointId>,
    thread_id: ThreadId,
    run_id: RunId,
}

impl CheckpointLineage {
    pub(crate) fn new(
        checkpoint_id: CheckpointId,
        expected_parent: Option<CheckpointId>,
        thread_id: ThreadId,
        run_id: RunId,
    ) -> Self {
        Self {
            checkpoint_id,
            expected_parent,
            thread_id,
            run_id,
        }
    }
}

impl<T> CheckpointRequest<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(
        lineage: CheckpointLineage,
        superstep: usize,
        step: usize,
        snapshot: Arc<T>,
        next_frontier: Vec<NodeId>,
        completed: bool,
    ) -> Self {
        Self {
            checkpoint_id: lineage.checkpoint_id,
            expected_parent: lineage.expected_parent,
            thread_id: lineage.thread_id,
            run_id: lineage.run_id,
            superstep,
            step,
            snapshot,
            next_frontier,
            completed,
        }
    }

    /// Returns the idempotency key assigned to this write.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the checkpoint on which this execution was based.
    #[must_use]
    pub const fn expected_parent(&self) -> Option<CheckpointId> {
        self.expected_parent
    }

    /// Returns the target logical thread.
    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    /// Returns the invocation creating this checkpoint.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the one-based super-step number.
    #[must_use]
    pub const fn superstep(&self) -> usize {
        self.superstep
    }

    /// Returns the cumulative executed-node count.
    #[must_use]
    pub const fn step(&self) -> usize {
        self.step
    }

    /// Returns the shared snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &Arc<T> {
        &self.snapshot
    }

    /// Returns the stable next frontier.
    #[must_use]
    pub fn next_frontier(&self) -> &[NodeId] {
        &self.next_frontier
    }

    /// Returns whether this request represents completed execution.
    #[must_use]
    pub const fn completed(&self) -> bool {
        self.completed
    }

    /// Finalizes this request after the store has accepted its CAS condition.
    #[must_use]
    pub fn into_checkpoint(self) -> Checkpoint<T> {
        Checkpoint {
            id: self.checkpoint_id,
            thread_id: self.thread_id,
            run_id: self.run_id,
            parent_id: self.expected_parent,
            superstep: self.superstep,
            step: self.step,
            snapshot: self.snapshot,
            next_frontier: self.next_frontier,
            completed: self.completed,
        }
    }
}

/// A failure to atomically write a checkpoint.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CheckpointWriteError {
    /// The thread advanced beyond the lineage on which this request executed.
    #[error(
        "checkpoint parent conflict: expected {expected_parent:?}, current latest is \
         {actual_parent:?}"
    )]
    Conflict {
        expected_parent: Option<CheckpointId>,
        actual_parent: Option<CheckpointId>,
    },
    /// The storage implementation failed.
    #[error(transparent)]
    Failed(#[from] CheckpointerError),
}

/// Asynchronous, replaceable checkpoint persistence boundary.
#[async_trait]
pub trait Checkpointer<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    /// Saves one prepared checkpoint and returns the stored immutable value.
    async fn save(
        &self,
        request: CheckpointRequest<T>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointWriteError>;

    /// Returns the latest checkpoint for a logical thread.
    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError>;

    /// Returns a thread's checkpoints from oldest to newest.
    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError>;
}

/// Checkpoint options for one invocation.
#[derive(Clone)]
pub struct CheckpointConfig<T>
where
    T: Send + Sync + 'static,
{
    thread_id: ThreadId,
    checkpointer: Arc<dyn Checkpointer<T>>,
    policy: CheckpointPolicy,
    expected_parent: Option<CheckpointId>,
}

impl<T> CheckpointConfig<T>
where
    T: Send + Sync + 'static,
{
    /// Creates an opt-in checkpoint configuration for new state.
    ///
    /// The invocation explicitly expects no existing parent checkpoint. Use
    /// [`Self::with_expected_parent`] when execution is based on a checkpoint.
    #[must_use]
    pub fn new(
        thread_id: impl Into<ThreadId>,
        checkpointer: Arc<dyn Checkpointer<T>>,
        policy: CheckpointPolicy,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            checkpointer,
            policy,
            expected_parent: None,
        }
    }

    /// Sets the checkpoint on which this invocation's state is based.
    #[must_use]
    pub fn with_expected_parent(mut self, expected_parent: Option<CheckpointId>) -> Self {
        self.expected_parent = expected_parent;
        self
    }

    /// Returns the logical execution thread.
    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    /// Returns the save policy.
    #[must_use]
    pub const fn policy(&self) -> CheckpointPolicy {
        self.policy
    }

    /// Returns the checkpoint on which this invocation's state is based.
    #[must_use]
    pub const fn expected_parent(&self) -> Option<CheckpointId> {
        self.expected_parent
    }

    pub(crate) fn checkpointer(&self) -> &dyn Checkpointer<T> {
        self.checkpointer.as_ref()
    }
}

impl<T> fmt::Debug for CheckpointConfig<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointConfig")
            .field("thread_id", &self.thread_id)
            .field("policy", &self.policy)
            .field("expected_parent", &self.expected_parent)
            .finish_non_exhaustive()
    }
}

/// Thread-safe, process-local checkpoint storage.
///
/// The store holds snapshots behind `Arc` and takes its mutex only while
/// assigning metadata or cloning stored `Arc` handles.
pub struct InMemoryCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    state: Mutex<InMemoryState<T>>,
}

struct InMemoryState<T>
where
    T: Send + Sync + 'static,
{
    histories: HashMap<ThreadId, Vec<Arc<Checkpoint<T>>>>,
    by_id: HashMap<CheckpointId, Arc<Checkpoint<T>>>,
}

impl<T> InMemoryCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    /// Creates an empty in-memory checkpointer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(InMemoryState {
                histories: HashMap::new(),
                by_id: HashMap::new(),
            }),
        }
    }
}

impl<T> Default for InMemoryCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for InMemoryCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryCheckpointer")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T> Checkpointer<T> for InMemoryCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    async fn save(
        &self,
        request: CheckpointRequest<T>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointWriteError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CheckpointerError::message("in-memory checkpoint lock was poisoned"))?;

        if let Some(checkpoint) = state.by_id.get(&request.checkpoint_id()) {
            return Ok(Arc::clone(checkpoint));
        }

        let actual_parent = state
            .histories
            .get(request.thread_id())
            .and_then(|history| history.last())
            .map(|checkpoint| checkpoint.id());
        if actual_parent != request.expected_parent() {
            return Err(CheckpointWriteError::Conflict {
                expected_parent: request.expected_parent(),
                actual_parent,
            });
        }

        let thread_id = request.thread_id().clone();
        let checkpoint = Arc::new(request.into_checkpoint());
        state
            .histories
            .entry(thread_id)
            .or_default()
            .push(Arc::clone(&checkpoint));
        state.by_id.insert(checkpoint.id(), Arc::clone(&checkpoint));
        Ok(checkpoint)
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CheckpointerError::message("in-memory checkpoint lock was poisoned"))?;
        Ok(state
            .histories
            .get(thread_id)
            .and_then(|history| history.last())
            .cloned())
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CheckpointerError::message("in-memory checkpoint lock was poisoned"))?;
        Ok(state.histories.get(thread_id).cloned().unwrap_or_default())
    }
}
