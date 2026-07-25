use std::collections::{HashMap, hash_map::Entry};
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use thiserror::Error;

use crate::{
    BranchId, CheckpointCodec, CheckpointEncodingError, CheckpointFormatVersion, CheckpointId,
    CheckpointInterrupt, CheckpointReconstructionError, CheckpointRecord,
    CheckpointRecordInterrupt, CheckpointRecordParts, CheckpointStore, CheckpointerError,
    EncodedValue, EventConfig, GraphState, InMemoryCheckpointStore, NodePath, ResumeValue,
    RunConfig, RunControl, RunId, SnapshotError,
};

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
    /// Reconstructs a typed checkpoint from a validated storage record.
    ///
    /// Codec work runs synchronously in the caller and must be performed
    /// outside storage locks.
    pub fn from_record(
        record: &CheckpointRecord,
        codec: &dyn CheckpointCodec<T>,
    ) -> Result<Self, CheckpointReconstructionError> {
        if record.format_version() != CheckpointFormatVersion::CURRENT {
            return Err(CheckpointReconstructionError::FormatVersion {
                actual: record.format_version(),
                supported: CheckpointFormatVersion::CURRENT,
            });
        }
        let expected = codec.snapshot_descriptor();
        let actual = record.snapshot().descriptor();
        if actual.encoding() != expected.encoding() {
            return Err(CheckpointReconstructionError::SnapshotEncoding {
                expected: Arc::from(expected.encoding()),
                actual: Arc::from(actual.encoding()),
            });
        }
        if actual.schema() != expected.schema()
            || actual.schema_version() != expected.schema_version()
        {
            return Err(CheckpointReconstructionError::SnapshotSchema {
                expected,
                actual: actual.clone(),
            });
        }
        let snapshot = codec
            .decode_snapshot(record.snapshot().bytes())
            .map(Arc::new)
            .map_err(|source| CheckpointReconstructionError::Snapshot { source })?;
        let interrupt = record
            .interrupt()
            .map(|interrupt| {
                if interrupt.payload().descriptor().encoding() != expected.encoding() {
                    return Err(CheckpointReconstructionError::InterruptEncoding {
                        expected: Arc::from(expected.encoding()),
                        actual: Arc::from(interrupt.payload().descriptor().encoding()),
                    });
                }
                codec
                    .decode_interrupt(interrupt.payload())
                    .map(|payload| {
                        CheckpointInterrupt::from_parts(
                            interrupt.id(),
                            interrupt.node_path().clone(),
                            payload,
                        )
                    })
                    .map_err(|source| CheckpointReconstructionError::Interrupt { source })
            })
            .transpose()?;
        let superstep = usize::try_from(record.superstep()).map_err(|_| {
            CheckpointReconstructionError::CounterOutOfRange {
                field: "superstep",
                value: record.superstep(),
            }
        })?;
        let step = usize::try_from(record.step()).map_err(|_| {
            CheckpointReconstructionError::CounterOutOfRange {
                field: "step",
                value: record.step(),
            }
        })?;
        Ok(Self {
            id: record.id(),
            thread_id: record.thread_id().clone(),
            run_id: record.run_id(),
            parent_id: record.parent_id(),
            graph_version: record.graph_version().cloned(),
            superstep,
            step,
            snapshot,
            next_frontier: record.next_frontier().to_vec(),
            completed: record.completed(),
            interrupt,
        })
    }

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

    /// Encodes this typed request as one storage-neutral record.
    pub fn to_record(
        &self,
        codec: &dyn CheckpointCodec<T>,
    ) -> Result<CheckpointRecord, CheckpointEncodingError> {
        let snapshot_descriptor = codec.snapshot_descriptor();
        let snapshot = codec
            .encode_snapshot(&self.snapshot)
            .map(|bytes| EncodedValue::new(snapshot_descriptor.clone(), bytes))
            .map_err(|source| CheckpointEncodingError::Snapshot { source })?;
        let interrupt = self
            .interrupt
            .as_ref()
            .map(|interrupt| {
                codec
                    .encode_interrupt(interrupt.payload())
                    .and_then(|payload| {
                        if payload.descriptor().encoding() == snapshot_descriptor.encoding() {
                            Ok(payload)
                        } else {
                            Err(crate::CheckpointCodecError::message(format!(
                                "interrupt encoding `{}` does not match codec encoding `{}`",
                                payload.descriptor().encoding(),
                                snapshot_descriptor.encoding()
                            )))
                        }
                    })
                    .map(|payload| {
                        CheckpointRecordInterrupt::new(
                            interrupt.id(),
                            interrupt.node_path().clone(),
                            payload,
                        )
                    })
                    .map_err(|source| CheckpointEncodingError::Interrupt { source })
            })
            .transpose()?;
        let superstep =
            u64::try_from(self.superstep).map_err(|source| CheckpointEncodingError::Snapshot {
                source: crate::CheckpointCodecError::with_source(
                    "checkpoint superstep exceeds durable u64 range",
                    source,
                ),
            })?;
        let step =
            u64::try_from(self.step).map_err(|source| CheckpointEncodingError::Snapshot {
                source: crate::CheckpointCodecError::with_source(
                    "checkpoint step exceeds durable u64 range",
                    source,
                ),
            })?;
        CheckpointRecord::try_from_parts(CheckpointRecordParts {
            format_version: CheckpointFormatVersion::CURRENT,
            checkpoint_id: self.checkpoint_id,
            thread_id: self.thread_id.clone(),
            run_id: self.run_id,
            parent_id: self.expected_parent,
            graph_version: self.graph_version.clone(),
            superstep,
            step,
            snapshot,
            next_frontier: self.next_frontier.clone(),
            completed: self.completed,
            interrupt,
        })
        .map_err(|source| CheckpointEncodingError::Snapshot {
            source: crate::CheckpointCodecError::with_source(
                "runtime produced invalid checkpoint record metadata",
                source,
            ),
        })
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
    /// A requested branch identifier already exists.
    #[error("checkpoint branch `{branch_id}` already exists")]
    BranchAlreadyExists { branch_id: BranchId },
    /// A requested checkpoint branch does not exist for the logical thread.
    #[error("checkpoint branch `{branch_id}` was not found")]
    BranchNotFound { branch_id: BranchId },
    /// The selected source checkpoint does not exist for branch creation.
    #[error("source checkpoint `{checkpoint_id}` was not found for branch `{branch_id}`")]
    BranchSourceNotFound {
        branch_id: BranchId,
        checkpoint_id: CheckpointId,
    },
    /// The checkpointer does not implement the additive branch capability.
    #[error("checkpoint branch operations are not supported")]
    BranchUnsupported,
    /// Typed checkpoint data could not be encoded for storage.
    #[error(transparent)]
    Encoding(#[from] CheckpointEncodingError),
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
    /// exact replay with the same stable encoded Record content should return
    /// the original result even after the thread latest has advanced. Snapshot
    /// and payload `Arc` identity is irrelevant. Reusing the identifier with
    /// different lineage, boundary, frontier, completion, version, interrupt,
    /// descriptor, or encoded bytes must return
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

    /// Creates a writable branch whose initial head is `source_checkpoint_id`.
    ///
    /// A duplicate identifier returns
    /// [`CheckpointWriteError::BranchAlreadyExists`], even for an exact repeat.
    async fn create_branch(
        &self,
        _thread_id: &ThreadId,
        _branch_id: BranchId,
        _source_checkpoint_id: CheckpointId,
    ) -> Result<BranchHead, CheckpointWriteError> {
        Err(CheckpointWriteError::BranchUnsupported)
    }

    /// Saves one checkpoint using the branch's independent head CAS.
    async fn save_branch(
        &self,
        _branch_id: BranchId,
        _request: CheckpointRequest<T>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointWriteError> {
        Err(CheckpointWriteError::BranchUnsupported)
    }

    /// Returns a branch's current head checkpoint.
    ///
    /// An absent branch or a branch owned by another thread returns `None`.
    async fn branch_head(
        &self,
        _thread_id: &ThreadId,
        _branch_id: BranchId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        Err(CheckpointerError::message(
            "checkpoint branch operations are not supported",
        ))
    }

    /// Returns the source checkpoint followed by branch descendants.
    ///
    /// An absent branch or a branch owned by another thread returns an empty
    /// collection.
    async fn branch_history(
        &self,
        _thread_id: &ThreadId,
        _branch_id: BranchId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError> {
        Err(CheckpointerError::message(
            "checkpoint branch operations are not supported",
        ))
    }
}

/// Stable metadata for one explicit branch head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchHead {
    branch_id: BranchId,
    thread_id: ThreadId,
    source_checkpoint_id: CheckpointId,
    checkpoint_id: CheckpointId,
}

impl BranchHead {
    /// Constructs branch metadata returned by storage adapters.
    #[must_use]
    pub const fn new(
        branch_id: BranchId,
        thread_id: ThreadId,
        source_checkpoint_id: CheckpointId,
        checkpoint_id: CheckpointId,
    ) -> Self {
        Self {
            branch_id,
            thread_id,
            source_checkpoint_id,
            checkpoint_id,
        }
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    #[must_use]
    pub const fn source_checkpoint_id(&self) -> CheckpointId {
        self.source_checkpoint_id
    }

    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }
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
    branch_id: Option<BranchId>,
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
    pub(crate) branch_id: Option<BranchId>,
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
            branch_id: None,
        }
    }

    /// Loads a specific checkpoint instead of the thread's latest selection.
    #[must_use]
    pub const fn with_checkpoint_id(mut self, checkpoint_id: CheckpointId) -> Self {
        self.target = ResumeTarget::Checkpoint(checkpoint_id);
        self
    }

    /// Selects an explicit branch whose own latest head must be resumed.
    #[must_use]
    pub const fn with_branch_id(mut self, branch_id: BranchId) -> Self {
        self.branch_id = Some(branch_id);
        self
    }

    /// Returns the selected branch, or `None` for the default thread head.
    #[must_use]
    pub const fn branch_id(&self) -> Option<BranchId> {
        self.branch_id
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
            branch_id: self.branch_id,
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
            .field("branch_id", &self.branch_id)
            .finish_non_exhaustive()
    }
}

/// Centralized configuration for one read-only historical replay.
///
/// Replay always names an exact checkpoint and never selects or updates the
/// thread's latest checkpoint. The configured checkpointer is used only for
/// loading that source checkpoint.
#[derive(Clone)]
pub struct ReplayConfig<T>
where
    T: Send + Sync + 'static,
{
    thread_id: ThreadId,
    checkpoint_id: CheckpointId,
    checkpointer: Arc<dyn Checkpointer<T>>,
    run_config: RunConfig,
    event_config: EventConfig,
    control: RunControl,
    resume_value: Option<ResumeValue>,
}

/// Configuration for explicitly forking one exact historical checkpoint.
#[derive(Clone)]
pub struct ForkConfig<T>
where
    T: Send + Sync + 'static,
{
    thread_id: ThreadId,
    checkpoint_id: CheckpointId,
    branch_id: BranchId,
    checkpointer: Arc<dyn Checkpointer<T>>,
    checkpoint_policy: CheckpointPolicy,
    run_config: RunConfig,
    event_config: EventConfig,
    control: RunControl,
    resume_value: Option<ResumeValue>,
}

pub(crate) struct ForkParts<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) thread_id: ThreadId,
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) branch_id: BranchId,
    pub(crate) checkpointer: Arc<dyn Checkpointer<T>>,
    pub(crate) checkpoint_policy: CheckpointPolicy,
    pub(crate) run_config: RunConfig,
    pub(crate) event_config: EventConfig,
    pub(crate) control: RunControl,
    pub(crate) resume_value: Option<ResumeValue>,
}

impl<T> ForkConfig<T>
where
    T: Send + Sync + 'static,
{
    /// Creates a fork configuration with a newly generated branch identifier.
    #[must_use]
    pub fn new(
        thread_id: impl Into<ThreadId>,
        checkpoint_id: CheckpointId,
        checkpointer: Arc<dyn Checkpointer<T>>,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            checkpoint_id,
            branch_id: BranchId::next(),
            checkpointer,
            checkpoint_policy: CheckpointPolicy::EverySuperstep,
            run_config: RunConfig::default(),
            event_config: EventConfig::default(),
            control: RunControl::default(),
            resume_value: None,
        }
    }

    /// Uses an application-selected branch identifier.
    #[must_use]
    pub const fn with_branch_id(mut self, branch_id: BranchId) -> Self {
        self.branch_id = branch_id;
        self
    }

    #[must_use]
    pub const fn with_checkpoint_policy(mut self, policy: CheckpointPolicy) -> Self {
        self.checkpoint_policy = policy;
        self
    }

    #[must_use]
    pub fn with_run_config(mut self, config: RunConfig) -> Self {
        self.run_config = config;
        self
    }

    #[must_use]
    pub fn with_event_config(mut self, config: EventConfig) -> Self {
        self.event_config = config;
        self
    }

    #[must_use]
    pub fn with_control(mut self, control: RunControl) -> Self {
        self.control = control;
        self
    }

    #[must_use]
    pub fn with_resume_value<TValue>(mut self, value: TValue) -> Self
    where
        TValue: Send + Sync + 'static,
    {
        self.resume_value = Some(ResumeValue::new(value));
        self
    }

    #[must_use]
    pub fn with_shared_resume_value(mut self, value: ResumeValue) -> Self {
        self.resume_value = Some(value);
        self
    }

    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) fn into_parts(self) -> ForkParts<T> {
        ForkParts {
            thread_id: self.thread_id,
            checkpoint_id: self.checkpoint_id,
            branch_id: self.branch_id,
            checkpointer: self.checkpointer,
            checkpoint_policy: self.checkpoint_policy,
            run_config: self.run_config,
            event_config: self.event_config,
            control: self.control,
            resume_value: self.resume_value,
        }
    }
}

impl<T> fmt::Debug for ForkConfig<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForkConfig")
            .field("thread_id", &self.thread_id)
            .field("checkpoint_id", &self.checkpoint_id)
            .field("branch_id", &self.branch_id)
            .field("checkpoint_policy", &self.checkpoint_policy)
            .field("run_config", &self.run_config)
            .field("event_config", &self.event_config)
            .field("control", &self.control)
            .field("has_resume_value", &self.resume_value.is_some())
            .finish_non_exhaustive()
    }
}

pub(crate) struct ReplayParts<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) thread_id: ThreadId,
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) checkpointer: Arc<dyn Checkpointer<T>>,
    pub(crate) run_config: RunConfig,
    pub(crate) event_config: EventConfig,
    pub(crate) control: RunControl,
    pub(crate) resume_value: Option<ResumeValue>,
}

impl<T> ReplayConfig<T>
where
    T: Send + Sync + 'static,
{
    /// Creates a replay configuration for one exact historical checkpoint.
    #[must_use]
    pub fn new(
        thread_id: impl Into<ThreadId>,
        checkpoint_id: CheckpointId,
        checkpointer: Arc<dyn Checkpointer<T>>,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            checkpoint_id,
            checkpointer,
            run_config: RunConfig::default(),
            event_config: EventConfig::default(),
            control: RunControl::default(),
            resume_value: None,
        }
    }

    /// Sets the additional node budget for this replay call.
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

    /// Supplies an already type-erased replay resume value.
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

    /// Returns the source logical thread.
    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    /// Returns the exact source checkpoint.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    pub(crate) fn into_parts(self) -> ReplayParts<T> {
        ReplayParts {
            thread_id: self.thread_id,
            checkpoint_id: self.checkpoint_id,
            checkpointer: self.checkpointer,
            run_config: self.run_config,
            event_config: self.event_config,
            control: self.control,
            resume_value: self.resume_value,
        }
    }
}

impl<T> fmt::Debug for ReplayConfig<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplayConfig")
            .field("thread_id", &self.thread_id)
            .field("checkpoint_id", &self.checkpoint_id)
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
    branch_id: Option<BranchId>,
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
            branch_id: None,
        }
    }

    /// Sets the checkpoint on which this invocation's state is based.
    #[must_use]
    pub fn with_expected_parent(mut self, expected_parent: Option<CheckpointId>) -> Self {
        self.expected_parent = expected_parent;
        self
    }

    /// Routes writes through an explicit branch head rather than the default head.
    ///
    /// The caller must also provide the current branch head through
    /// [`Self::with_expected_parent`]; selecting a branch does not infer it.
    #[must_use]
    pub const fn with_branch_id(mut self, branch_id: BranchId) -> Self {
        self.branch_id = Some(branch_id);
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

    #[must_use]
    pub const fn branch_id(&self) -> Option<BranchId> {
        self.branch_id
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
            .field("branch_id", &self.branch_id)
            .finish_non_exhaustive()
    }
}

/// Adapts a storage-neutral record store to the typed Runtime checkpointer port.
///
/// Encoding and decoding happen before or after store calls and never while the
/// store's lock is held. Decoded checkpoints are cached by ID so repeated
/// latest/get/history calls share their Snapshot `Arc`.
type DecodedCheckpoints<T> = HashMap<CheckpointId, (CheckpointRecord, Arc<Checkpoint<T>>)>;

pub struct RecordCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    store: Arc<dyn CheckpointStore>,
    codec: Arc<dyn CheckpointCodec<T>>,
    decoded: Mutex<DecodedCheckpoints<T>>,
}

impl<T> RecordCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    /// Creates a typed adapter over a public record store and codec.
    #[must_use]
    pub fn new(store: Arc<dyn CheckpointStore>, codec: Arc<dyn CheckpointCodec<T>>) -> Self {
        Self {
            store,
            codec,
            decoded: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the underlying storage-neutral port.
    #[must_use]
    pub const fn store(&self) -> &Arc<dyn CheckpointStore> {
        &self.store
    }

    fn cached(
        &self,
        record: &CheckpointRecord,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        let cache = self
            .decoded
            .lock()
            .map_err(|_| CheckpointerError::message("decoded checkpoint cache was poisoned"))?;
        match cache.get(&record.id()) {
            Some((cached_record, checkpoint)) if cached_record == record => {
                Ok(Some(Arc::clone(checkpoint)))
            }
            Some(_) => Err(CheckpointerError::message(format!(
                "checkpoint store returned conflicting content for id `{}`",
                record.id()
            ))),
            None => Ok(None),
        }
    }

    fn cache(
        &self,
        record: CheckpointRecord,
        checkpoint: Arc<Checkpoint<T>>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointerError> {
        let mut cache = self
            .decoded
            .lock()
            .map_err(|_| CheckpointerError::message("decoded checkpoint cache was poisoned"))?;
        match cache.entry(checkpoint.id()) {
            Entry::Vacant(entry) => {
                entry.insert((record, Arc::clone(&checkpoint)));
                Ok(checkpoint)
            }
            Entry::Occupied(entry) if entry.get().0 == record => Ok(Arc::clone(&entry.get().1)),
            Entry::Occupied(entry) => Err(CheckpointerError::message(format!(
                "checkpoint store returned conflicting content for id `{}`",
                entry.key()
            ))),
        }
    }

    fn decode(&self, record: &CheckpointRecord) -> Result<Arc<Checkpoint<T>>, CheckpointerError> {
        if let Some(checkpoint) = self.cached(record)? {
            return Ok(checkpoint);
        }
        let checkpoint =
            Checkpoint::from_record(record, self.codec.as_ref()).map_err(|source| {
                CheckpointerError::with_source(
                    format!("checkpoint `{}` reconstruction failed", record.id()),
                    source,
                )
            })?;
        self.cache(record.clone(), Arc::new(checkpoint))
    }
}

impl<T> fmt::Debug for RecordCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordCheckpointer")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T> Checkpointer<T> for RecordCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    async fn save(
        &self,
        request: CheckpointRequest<T>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointWriteError> {
        let record = request.to_record(self.codec.as_ref())?;
        let stored = self.store.save(record.clone()).await?;
        if stored.as_ref() != &record {
            return Err(CheckpointWriteError::Failed(CheckpointerError::message(
                "checkpoint store returned content different from the submitted record",
            )));
        }
        if let Some(checkpoint) = self.cached(&record).map_err(CheckpointWriteError::Failed)? {
            return Ok(checkpoint);
        }
        self.cache(record, Arc::new(request.into_checkpoint()))
            .map_err(CheckpointWriteError::Failed)
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.store
            .latest(thread_id)
            .await?
            .map(|record| self.decode(&record))
            .transpose()
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.store
            .get(thread_id, checkpoint_id)
            .await?
            .map(|record| self.decode(&record))
            .transpose()
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.store
            .history(thread_id)
            .await?
            .iter()
            .map(|record| self.decode(record))
            .collect()
    }

    async fn create_branch(
        &self,
        thread_id: &ThreadId,
        branch_id: BranchId,
        source_checkpoint_id: CheckpointId,
    ) -> Result<BranchHead, CheckpointWriteError> {
        self.store
            .create_branch(thread_id, branch_id, source_checkpoint_id)
            .await
    }

    async fn save_branch(
        &self,
        branch_id: BranchId,
        request: CheckpointRequest<T>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointWriteError> {
        let record = request.to_record(self.codec.as_ref())?;
        let stored = self.store.save_branch(branch_id, record.clone()).await?;
        if stored.as_ref() != &record {
            return Err(CheckpointWriteError::Failed(CheckpointerError::message(
                "checkpoint store returned content different from the submitted branch record",
            )));
        }
        if let Some(checkpoint) = self.cached(&record).map_err(CheckpointWriteError::Failed)? {
            return Ok(checkpoint);
        }
        self.cache(record, Arc::new(request.into_checkpoint()))
            .map_err(CheckpointWriteError::Failed)
    }

    async fn branch_head(
        &self,
        thread_id: &ThreadId,
        branch_id: BranchId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.store
            .branch_head(thread_id, branch_id)
            .await?
            .map(|record| self.decode(&record))
            .transpose()
    }

    async fn branch_history(
        &self,
        thread_id: &ThreadId,
        branch_id: BranchId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.store
            .branch_history(thread_id, branch_id)
            .await?
            .iter()
            .map(|record| self.decode(record))
            .collect()
    }
}

/// Typed in-memory checkpointer backed by storage-neutral records.
pub struct InMemoryCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    records: Arc<InMemoryCheckpointStore>,
    adapter: RecordCheckpointer<T>,
}

impl<T> InMemoryCheckpointer<T>
where
    T: Send + Sync + 'static,
{
    /// Creates an empty record store using the supplied Snapshot/payload codec.
    #[must_use]
    pub fn new<C>(codec: C) -> Self
    where
        C: CheckpointCodec<T> + 'static,
    {
        Self::with_codec(Arc::new(codec))
    }

    /// Creates an empty record store using a shared codec.
    #[must_use]
    pub fn with_codec(codec: Arc<dyn CheckpointCodec<T>>) -> Self {
        let records = Arc::new(InMemoryCheckpointStore::new());
        let store = Arc::clone(&records) as Arc<dyn CheckpointStore>;
        Self {
            records,
            adapter: RecordCheckpointer::new(store, codec),
        }
    }

    /// Returns the underlying storage-neutral in-memory store.
    #[must_use]
    pub const fn record_store(&self) -> &Arc<InMemoryCheckpointStore> {
        &self.records
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
        self.adapter.save(request).await
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.adapter.latest(thread_id).await
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.adapter.get(thread_id, checkpoint_id).await
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.adapter.history(thread_id).await
    }

    async fn create_branch(
        &self,
        thread_id: &ThreadId,
        branch_id: BranchId,
        source_checkpoint_id: CheckpointId,
    ) -> Result<BranchHead, CheckpointWriteError> {
        self.adapter
            .create_branch(thread_id, branch_id, source_checkpoint_id)
            .await
    }

    async fn save_branch(
        &self,
        branch_id: BranchId,
        request: CheckpointRequest<T>,
    ) -> Result<Arc<Checkpoint<T>>, CheckpointWriteError> {
        self.adapter.save_branch(branch_id, request).await
    }

    async fn branch_head(
        &self,
        thread_id: &ThreadId,
        branch_id: BranchId,
    ) -> Result<Option<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.adapter.branch_head(thread_id, branch_id).await
    }

    async fn branch_history(
        &self,
        thread_id: &ThreadId,
        branch_id: BranchId,
    ) -> Result<Vec<Arc<Checkpoint<T>>>, CheckpointerError> {
        self.adapter.branch_history(thread_id, branch_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct UsizeCodec;

    impl CheckpointCodec<usize> for UsizeCodec {
        fn snapshot_descriptor(&self) -> crate::CodecDescriptor {
            crate::CodecDescriptor::new("group.test.usize", 1, "group.test.le-usize-v1")
        }

        fn encode_snapshot(
            &self,
            snapshot: &usize,
        ) -> Result<Vec<u8>, crate::CheckpointCodecError> {
            Ok(snapshot.to_le_bytes().to_vec())
        }

        fn decode_snapshot(&self, bytes: &[u8]) -> Result<usize, crate::CheckpointCodecError> {
            let bytes: [u8; size_of::<usize>()] = bytes
                .try_into()
                .map_err(|_| crate::CheckpointCodecError::message("invalid usize bytes"))?;
            Ok(usize::from_le_bytes(bytes))
        }
    }

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

    fn record(checkpoint_id: CheckpointId, step: u64, value: usize) -> CheckpointRecord {
        CheckpointRecord::try_from_parts(CheckpointRecordParts {
            format_version: CheckpointFormatVersion::CURRENT,
            checkpoint_id,
            thread_id: ThreadId::from("cache-thread"),
            run_id: RunId::next(),
            parent_id: None,
            graph_version: Some(GraphVersion::from("test-v1")),
            superstep: step,
            step,
            snapshot: EncodedValue::new(
                crate::CodecDescriptor::new("group.test.usize", 1, "group.test.le-usize-v1"),
                value.to_le_bytes().to_vec(),
            ),
            next_frontier: vec![NodePath::from("next")],
            completed: false,
            interrupt: None,
        })
        .expect("test record should be valid")
    }

    #[tokio::test]
    async fn identical_checkpoint_request_replay_returns_original_arc() {
        let store = InMemoryCheckpointer::new(UsizeCodec);
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
            .save(request(checkpoint_id, None, run_id, 1, Arc::new(1)))
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
        let store = InMemoryCheckpointer::new(UsizeCodec);
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
        let store = InMemoryCheckpointer::new(UsizeCodec);
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

    #[test]
    fn occupied_cache_rejects_same_id_with_different_record_content() {
        let checkpoint_id = CheckpointId::next();
        let first_record = record(checkpoint_id, 1, 1);
        let second_record = record(checkpoint_id, 2, 2);
        let adapter = RecordCheckpointer::new(
            Arc::new(InMemoryCheckpointStore::new()),
            Arc::new(UsizeCodec),
        );
        let first =
            Checkpoint::from_record(&first_record, &UsizeCodec).expect("record should decode");
        adapter
            .cache(first_record, Arc::new(first))
            .expect("vacant cache entry should insert");
        let second =
            Checkpoint::from_record(&second_record, &UsizeCodec).expect("record should decode");
        let error = adapter
            .cache(second_record, Arc::new(second))
            .expect_err("occupied cache must compare complete record content");
        assert!(
            error
                .to_string()
                .contains("returned conflicting content for id")
        );
    }
}
