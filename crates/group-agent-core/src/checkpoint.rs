use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use thiserror::Error;

use crate::{
    CheckpointInterrupt, CheckpointerError, EventConfig, GraphState, NodePath, ResumeValue,
    RunConfig, RunControl, RunId, SnapshotError,
};

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

/// Explicit compatibility version assigned to a compiled graph.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct GraphVersion(Arc<str>);

impl GraphVersion {
    /// Creates a graph version.
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// Returns this version as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for GraphVersion {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for GraphVersion {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for GraphVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// State capability required only by checkpoint-enabled invocations.
///
/// The snapshot type does not need to implement `Clone` or Serde. Checkpoints
/// retain it behind an [`Arc`]. The restore method reserves the state boundary
/// needed by checkpoint resume support.
pub trait CheckpointState: GraphState {
    /// Immutable state representation retained by a checkpoint.
    type Snapshot: Send + Sync + 'static;

    /// Creates a snapshot from the current committed state.
    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError>;

    /// Reconstructs state from a checkpoint snapshot.
    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError>
    where
        Self: Sized;
}

/// Controls committed-boundary saves.
///
/// Interrupted checkpoints are mandatory and are saved under either policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckpointPolicy {
    /// Save after every successful super-step.
    #[default]
    EverySuperstep,
    /// Save only the completed checkpoint, plus any mandatory interrupt.
    FinalOnly,
}

/// Immutable completed, resumable, or interrupted checkpoint data.
#[derive(Clone, Debug)]
pub struct Checkpoint<T>
where
    T: Send + Sync + 'static,
{
    id: CheckpointId,
    thread_id: ThreadId,
    run_id: RunId,
    parent_id: Option<CheckpointId>,
    graph_version: Option<GraphVersion>,
    superstep: usize,
    step: usize,
    snapshot: Arc<T>,
    next_frontier: Vec<NodePath>,
    completed: bool,
    interrupt: Option<CheckpointInterrupt>,
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

    /// Returns the state-lineage parent on which this checkpoint executed.
    ///
    /// This is not inferred from storage insertion order.
    #[must_use]
    pub const fn parent_id(&self) -> Option<CheckpointId> {
        self.parent_id
    }

    /// Returns the committed super-step count.
    ///
    /// A zero-node completed checkpoint uses zero. Boundaries after executing
    /// nodes start at one.
    #[must_use]
    pub const fn superstep(&self) -> usize {
        self.superstep
    }

    /// Returns the graph compatibility version, if the graph was versioned.
    #[must_use]
    pub const fn graph_version(&self) -> Option<&GraphVersion> {
        self.graph_version.as_ref()
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
    pub fn next_frontier(&self) -> &[NodePath] {
        &self.next_frontier
    }

    /// Returns whether execution reached an empty frontier.
    #[must_use]
    pub const fn completed(&self) -> bool {
        self.completed
    }

    /// Returns suspension metadata when this is an interrupted checkpoint.
    #[must_use]
    pub const fn interrupt(&self) -> Option<&CheckpointInterrupt> {
        self.interrupt.as_ref()
    }

    /// Returns whether this checkpoint represents a node suspension.
    #[must_use]
    pub const fn interrupted(&self) -> bool {
        self.interrupt.is_some()
    }
}

/// A checkpoint write prepared by the Runtime at a committed or interrupt
/// boundary.
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
    graph_version: Option<GraphVersion>,
    thread_id: ThreadId,
    run_id: RunId,
    superstep: usize,
    step: usize,
    snapshot: Arc<T>,
    next_frontier: Vec<NodePath>,
    completed: bool,
    interrupt: Option<CheckpointInterrupt>,
}

pub(crate) struct CheckpointLineage {
    checkpoint_id: CheckpointId,
    expected_parent: Option<CheckpointId>,
    graph_version: Option<GraphVersion>,
    thread_id: ThreadId,
    run_id: RunId,
}

impl CheckpointLineage {
    pub(crate) fn new(
        checkpoint_id: CheckpointId,
        expected_parent: Option<CheckpointId>,
        graph_version: Option<GraphVersion>,
        thread_id: ThreadId,
        run_id: RunId,
    ) -> Self {
        Self {
            checkpoint_id,
            expected_parent,
            graph_version,
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
        next_frontier: Vec<NodePath>,
        completed: bool,
        interrupt: Option<CheckpointInterrupt>,
    ) -> Self {
        Self {
            checkpoint_id: lineage.checkpoint_id,
            expected_parent: lineage.expected_parent,
            graph_version: lineage.graph_version,
            thread_id: lineage.thread_id,
            run_id: lineage.run_id,
            superstep,
            step,
            snapshot,
            next_frontier,
            completed,
            interrupt,
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

    /// Returns the graph compatibility version recorded by this write.
    #[must_use]
    pub const fn graph_version(&self) -> Option<&GraphVersion> {
        self.graph_version.as_ref()
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

    /// Returns the committed super-step count.
    ///
    /// A zero-node terminal request uses zero. Boundaries after executing nodes
    /// start at one.
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
    pub fn next_frontier(&self) -> &[NodePath] {
        &self.next_frontier
    }

    /// Returns whether this request represents completed execution.
    #[must_use]
    pub const fn completed(&self) -> bool {
        self.completed
    }

    /// Returns suspension metadata when this request saves an interrupt.
    #[must_use]
    pub const fn interrupt(&self) -> Option<&CheckpointInterrupt> {
        self.interrupt.as_ref()
    }

    /// Finalizes this request after the store has accepted its CAS condition.
    #[must_use]
    pub fn into_checkpoint(self) -> Checkpoint<T> {
        Checkpoint {
            id: self.checkpoint_id,
            thread_id: self.thread_id,
            run_id: self.run_id,
            parent_id: self.expected_parent,
            graph_version: self.graph_version,
            superstep: self.superstep,
            step: self.step,
            snapshot: self.snapshot,
            next_frontier: self.next_frontier,
            completed: self.completed,
            interrupt: self.interrupt,
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
    /// An idempotency key was reused for different request metadata.
    #[error("checkpoint idempotency key `{checkpoint_id}` was reused with different metadata")]
    IdempotencyConflict { checkpoint_id: CheckpointId },
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
    ///
    /// Implementations must treat `checkpoint_id` as an idempotency key. An
    /// exact replay, including the same snapshot `Arc`, should return the
    /// original result even after the thread latest has advanced. Reusing the
    /// identifier with different lineage, boundary, frontier, completion,
    /// version, interrupt, or snapshot metadata must return
    /// [`CheckpointWriteError::IdempotencyConflict`].
    async fn save(
        &self,
        request: CheckpointRequest<T>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointWriteError>;

    /// Returns the latest checkpoint for a logical thread.
    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError>;

    /// Gets one checkpoint only when it belongs to the supplied thread.
    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        Ok(self
            .history(thread_id)
            .await?
            .into_iter()
            .find(|checkpoint| checkpoint.id() == checkpoint_id))
    }

    /// Returns a thread's checkpoints from oldest to newest.
    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError>;
}

/// Selects the checkpoint loaded by a resume invocation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResumeTarget {
    /// Load the current latest checkpoint for the thread.
    #[default]
    Latest,
    /// Load a particular checkpoint, which must also still be latest.
    Checkpoint(CheckpointId),
}

/// Centralized configuration for one resume invocation.
#[derive(Clone)]
pub struct ResumeConfig<T>
where
    T: Send + Sync + 'static,
{
    thread_id: ThreadId,
    checkpointer: Arc<dyn Checkpointer<T>>,
    target: ResumeTarget,
    checkpoint_policy: CheckpointPolicy,
    run_config: RunConfig,
    event_config: EventConfig,
    control: RunControl,
    resume_value: Option<ResumeValue>,
}

pub(crate) struct ResumeParts<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) thread_id: ThreadId,
    pub(crate) checkpointer: Arc<dyn Checkpointer<T>>,
    pub(crate) target: ResumeTarget,
    pub(crate) checkpoint_policy: CheckpointPolicy,
    pub(crate) run_config: RunConfig,
    pub(crate) event_config: EventConfig,
    pub(crate) control: RunControl,
    pub(crate) resume_value: Option<ResumeValue>,
}

impl<T> ResumeConfig<T>
where
    T: Send + Sync + 'static,
{
    /// Creates a latest-checkpoint resume configuration.
    #[must_use]
    pub fn new(thread_id: impl Into<ThreadId>, checkpointer: Arc<dyn Checkpointer<T>>) -> Self {
        Self {
            thread_id: thread_id.into(),
            checkpointer,
            target: ResumeTarget::Latest,
            checkpoint_policy: CheckpointPolicy::EverySuperstep,
            run_config: RunConfig::default(),
            event_config: EventConfig::default(),
            control: RunControl::default(),
            resume_value: None,
        }
    }

    /// Loads a specific checkpoint instead of the thread's latest selection.
    #[must_use]
    pub const fn with_checkpoint_id(mut self, checkpoint_id: CheckpointId) -> Self {
        self.target = ResumeTarget::Checkpoint(checkpoint_id);
        self
    }

    /// Sets the policy for checkpoints created after resuming.
    #[must_use]
    pub const fn with_checkpoint_policy(mut self, policy: CheckpointPolicy) -> Self {
        self.checkpoint_policy = policy;
        self
    }

    /// Sets the additional node budget for this resume call.
    #[must_use]
    pub fn with_run_config(mut self, config: RunConfig) -> Self {
        self.run_config = config;
        self
    }

    /// Sets event delivery and successful-report retention.
    #[must_use]
    pub fn with_event_config(mut self, config: EventConfig) -> Self {
        self.event_config = config;
        self
    }

    /// Sets cancellation and timeout controls.
    #[must_use]
    pub fn with_control(mut self, control: RunControl) -> Self {
        self.control = control;
        self
    }

    /// Supplies the typed value consumed by an interrupted checkpoint's node.
    #[must_use]
    pub fn with_resume_value<TValue>(mut self, value: TValue) -> Self
    where
        TValue: Send + Sync + 'static,
    {
        self.resume_value = Some(ResumeValue::new(value));
        self
    }

    /// Supplies an already type-erased resume value.
    #[must_use]
    pub fn with_shared_resume_value(mut self, value: ResumeValue) -> Self {
        self.resume_value = Some(value);
        self
    }

    /// Returns whether a resume value has been configured.
    #[must_use]
    pub const fn has_resume_value(&self) -> bool {
        self.resume_value.is_some()
    }

    /// Returns the logical thread to resume.
    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    /// Returns the selected checkpoint target.
    #[must_use]
    pub const fn target(&self) -> ResumeTarget {
        self.target
    }

    pub(crate) fn into_parts(self) -> ResumeParts<T> {
        ResumeParts {
            thread_id: self.thread_id,
            checkpointer: self.checkpointer,
            target: self.target,
            checkpoint_policy: self.checkpoint_policy,
            run_config: self.run_config,
            event_config: self.event_config,
            control: self.control,
            resume_value: self.resume_value,
        }
    }
}

impl<T> fmt::Debug for ResumeConfig<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumeConfig")
            .field("thread_id", &self.thread_id)
            .field("target", &self.target)
            .field("checkpoint_policy", &self.checkpoint_policy)
            .field("run_config", &self.run_config)
            .field("event_config", &self.event_config)
            .field("control", &self.control)
            .field("has_resume_value", &self.resume_value.is_some())
            .finish_non_exhaustive()
    }
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
            if checkpoint_matches_request(checkpoint, &request) {
                return Ok(Arc::clone(checkpoint));
            }
            return Err(CheckpointWriteError::IdempotencyConflict {
                checkpoint_id: request.checkpoint_id(),
            });
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

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CheckpointerError::message("in-memory checkpoint lock was poisoned"))?;
        Ok(state
            .by_id
            .get(&checkpoint_id)
            .filter(|checkpoint| checkpoint.thread_id() == thread_id)
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

fn checkpoint_matches_request<T>(checkpoint: &Checkpoint<T>, request: &CheckpointRequest<T>) -> bool
where
    T: Send + Sync + 'static,
{
    checkpoint.id == request.checkpoint_id
        && checkpoint.parent_id == request.expected_parent
        && checkpoint.graph_version == request.graph_version
        && checkpoint.thread_id == request.thread_id
        && checkpoint.run_id == request.run_id
        && checkpoint.superstep == request.superstep
        && checkpoint.step == request.step
        && checkpoint.next_frontier == request.next_frontier
        && checkpoint.completed == request.completed
        && match (&checkpoint.interrupt, &request.interrupt) {
            (Some(checkpoint), Some(request)) => checkpoint.matches(request),
            (None, None) => true,
            _ => false,
        }
        && Arc::ptr_eq(&checkpoint.snapshot, &request.snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        checkpoint_id: CheckpointId,
        expected_parent: Option<CheckpointId>,
        run_id: RunId,
        step: usize,
        snapshot: Arc<usize>,
    ) -> CheckpointRequest<usize> {
        CheckpointRequest::new(
            CheckpointLineage::new(
                checkpoint_id,
                expected_parent,
                Some(GraphVersion::from("test-v1")),
                ThreadId::from("thread"),
                run_id,
            ),
            step,
            step,
            snapshot,
            if step == 2 {
                Vec::new()
            } else {
                vec![NodePath::from("next")]
            },
            step == 2,
            None,
        )
    }

    #[tokio::test]
    async fn identical_checkpoint_request_replay_returns_original_arc() {
        let store = InMemoryCheckpointer::new();
        let run_id = RunId::next();
        let snapshot = Arc::new(1);
        let checkpoint_id = CheckpointId::next();
        let first = store
            .save(request(
                checkpoint_id,
                None,
                run_id,
                1,
                Arc::clone(&snapshot),
            ))
            .await
            .expect("initial save should succeed");
        let replay = store
            .save(request(checkpoint_id, None, run_id, 1, snapshot))
            .await
            .expect("identical replay should succeed");
        assert!(Arc::ptr_eq(&first, &replay));
        assert!(
            store
                .get(&ThreadId::from("other-thread"), checkpoint_id)
                .await
                .expect("cross-thread get should succeed")
                .is_none()
        );
        assert!(Arc::ptr_eq(
            &first,
            &store
                .get(&ThreadId::from("thread"), checkpoint_id)
                .await
                .expect("same-thread get should succeed")
                .expect("checkpoint should exist")
        ));
        assert_eq!(
            store
                .history(&ThreadId::from("thread"))
                .await
                .expect("history should load")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn replay_still_returns_original_after_latest_advances() {
        let store = InMemoryCheckpointer::new();
        let run_id = RunId::next();
        let first_snapshot = Arc::new(1);
        let first_id = CheckpointId::next();
        let first = store
            .save(request(
                first_id,
                None,
                run_id,
                1,
                Arc::clone(&first_snapshot),
            ))
            .await
            .expect("first save should succeed");
        let second_id = CheckpointId::next();
        let second = store
            .save(request(second_id, Some(first.id()), run_id, 2, Arc::new(2)))
            .await
            .expect("second save should advance latest");
        let replay = store
            .save(request(first_id, None, run_id, 1, first_snapshot))
            .await
            .expect("old operation replay should remain idempotent");
        assert!(Arc::ptr_eq(&first, &replay));
        assert_eq!(
            store
                .latest(&ThreadId::from("thread"))
                .await
                .expect("latest should load")
                .expect("latest should exist")
                .id(),
            second.id()
        );
    }

    #[tokio::test]
    async fn duplicate_checkpoint_id_with_different_metadata_is_rejected() {
        let store = InMemoryCheckpointer::new();
        let run_id = RunId::next();
        let snapshot = Arc::new(1);
        let checkpoint_id = CheckpointId::next();
        store
            .save(request(
                checkpoint_id,
                None,
                run_id,
                1,
                Arc::clone(&snapshot),
            ))
            .await
            .expect("initial save should succeed");
        let error = store
            .save(request(checkpoint_id, None, run_id, 9, snapshot))
            .await
            .expect_err("different metadata must be rejected");
        assert!(matches!(
            error,
            CheckpointWriteError::IdempotencyConflict {
                checkpoint_id: actual
            } if actual == checkpoint_id
        ));
    }
}
