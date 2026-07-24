use std::fmt;

use thiserror::Error;

use crate::{CheckpointId, EncodedValue, GraphVersion, InterruptId, NodePath, RunId, ThreadId};

/// Version of the storage-neutral checkpoint record layout.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointFormatVersion(u32);

impl CheckpointFormatVersion {
    /// Record format emitted by this release.
    pub const CURRENT: Self = Self(1);

    /// Reconstructs a format version from storage.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric format version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for CheckpointFormatVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Storage-neutral metadata for one interrupted checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRecordInterrupt {
    id: InterruptId,
    node_path: NodePath,
    payload: EncodedValue,
}

impl CheckpointRecordInterrupt {
    /// Creates durable interrupt metadata.
    #[must_use]
    pub fn new(id: InterruptId, node_path: NodePath, payload: EncodedValue) -> Self {
        Self {
            id,
            node_path,
            payload,
        }
    }

    /// Returns the interrupt identifier.
    #[must_use]
    pub const fn id(&self) -> InterruptId {
        self.id
    }

    /// Returns the interrupted node path.
    #[must_use]
    pub const fn node_path(&self) -> &NodePath {
        &self.node_path
    }

    /// Returns the encoded interrupt payload.
    #[must_use]
    pub const fn payload(&self) -> &EncodedValue {
        &self.payload
    }
}

/// Public fields used to reconstruct one storage-neutral checkpoint record.
#[derive(Clone, Debug)]
pub struct CheckpointRecordParts {
    pub format_version: CheckpointFormatVersion,
    pub checkpoint_id: CheckpointId,
    pub thread_id: ThreadId,
    pub run_id: RunId,
    pub parent_id: Option<CheckpointId>,
    pub graph_version: Option<GraphVersion>,
    /// Fixed-width committed super-step count.
    pub superstep: u64,
    /// Fixed-width cumulative executed-node count.
    pub step: u64,
    pub snapshot: EncodedValue,
    pub next_frontier: Vec<NodePath>,
    pub completed: bool,
    pub interrupt: Option<CheckpointRecordInterrupt>,
}

/// Invalid checkpoint record structure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointRecordError {
    /// A completed record retained executable work.
    #[error("completed checkpoint record has a non-empty frontier")]
    CompletedWithFrontier,
    /// An incomplete record has no continuation.
    #[error("incomplete checkpoint record has an empty frontier")]
    IncompleteWithoutFrontier,
    /// A completed record also claims interruption.
    #[error("completed checkpoint record cannot contain interrupt metadata")]
    CompletedInterrupt,
    /// Interrupt metadata does not match its singleton frontier.
    #[error("interrupted checkpoint record frontier does not match `{interrupt_node}`")]
    InvalidInterruptFrontier { interrupt_node: NodePath },
}

/// Immutable storage-neutral checkpoint data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRecord {
    format_version: CheckpointFormatVersion,
    checkpoint_id: CheckpointId,
    thread_id: ThreadId,
    run_id: RunId,
    parent_id: Option<CheckpointId>,
    graph_version: Option<GraphVersion>,
    superstep: u64,
    step: u64,
    snapshot: EncodedValue,
    next_frontier: Vec<NodePath>,
    completed: bool,
    interrupt: Option<CheckpointRecordInterrupt>,
}

impl CheckpointRecord {
    /// Validates and reconstructs a record from storage fields.
    pub fn try_from_parts(parts: CheckpointRecordParts) -> Result<Self, CheckpointRecordError> {
        if parts.completed && !parts.next_frontier.is_empty() {
            return Err(CheckpointRecordError::CompletedWithFrontier);
        }
        if !parts.completed && parts.next_frontier.is_empty() {
            return Err(CheckpointRecordError::IncompleteWithoutFrontier);
        }
        if parts.completed && parts.interrupt.is_some() {
            return Err(CheckpointRecordError::CompletedInterrupt);
        }
        if let Some(interrupt) = &parts.interrupt {
            if parts.next_frontier.len() != 1
                || parts.next_frontier.first() != Some(interrupt.node_path())
            {
                return Err(CheckpointRecordError::InvalidInterruptFrontier {
                    interrupt_node: interrupt.node_path().clone(),
                });
            }
        }
        Ok(Self {
            format_version: parts.format_version,
            checkpoint_id: parts.checkpoint_id,
            thread_id: parts.thread_id,
            run_id: parts.run_id,
            parent_id: parts.parent_id,
            graph_version: parts.graph_version,
            superstep: parts.superstep,
            step: parts.step,
            snapshot: parts.snapshot,
            next_frontier: parts.next_frontier,
            completed: parts.completed,
            interrupt: parts.interrupt,
        })
    }

    #[must_use]
    pub const fn format_version(&self) -> CheckpointFormatVersion {
        self.format_version
    }

    #[must_use]
    pub const fn id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    #[must_use]
    pub const fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    #[must_use]
    pub const fn parent_id(&self) -> Option<CheckpointId> {
        self.parent_id
    }

    #[must_use]
    pub const fn graph_version(&self) -> Option<&GraphVersion> {
        self.graph_version.as_ref()
    }

    /// Returns the fixed-width committed super-step count.
    #[must_use]
    pub const fn superstep(&self) -> u64 {
        self.superstep
    }

    /// Returns the fixed-width cumulative executed-node count.
    #[must_use]
    pub const fn step(&self) -> u64 {
        self.step
    }

    #[must_use]
    pub const fn snapshot(&self) -> &EncodedValue {
        &self.snapshot
    }

    #[must_use]
    pub fn next_frontier(&self) -> &[NodePath] {
        &self.next_frontier
    }

    #[must_use]
    pub const fn completed(&self) -> bool {
        self.completed
    }

    #[must_use]
    pub const fn interrupt(&self) -> Option<&CheckpointRecordInterrupt> {
        self.interrupt.as_ref()
    }
}
