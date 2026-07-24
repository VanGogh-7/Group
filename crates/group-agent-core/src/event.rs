use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::{CheckpointId, NodeId, ThreadId};

static NEXT_RUN_ID: AtomicU64 = AtomicU64::new(1);

/// A lightweight identifier that distinguishes graph invocations.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunId(u64);

impl RunId {
    pub(crate) fn next() -> Self {
        Self(NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns the numeric run identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Controls whether emitted events are retained in a successful run report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum EventRetention {
    /// Retain every event in emission order.
    #[default]
    All,
    /// Do not retain events in the run report.
    None,
}

/// Receives lifecycle events synchronously as they occur.
///
/// Implementations must be thread-safe and should return quickly without
/// performing blocking work. A sink receives event metadata only, never the
/// complete graph state or a state update.
pub trait EventSink: Send + Sync {
    /// Observes one emitted event.
    fn on_event(&self, event: &GraphEvent);
}

impl<F> EventSink for F
where
    F: Fn(&GraphEvent) + Send + Sync,
{
    fn on_event(&self, event: &GraphEvent) {
        self(event);
    }
}

/// Event delivery and successful-report retention for one invocation.
#[derive(Clone)]
pub struct EventConfig {
    retention: EventRetention,
    sink: Option<Arc<dyn EventSink>>,
}

impl EventConfig {
    /// Creates an event configuration without a sink.
    #[must_use]
    pub const fn new(retention: EventRetention) -> Self {
        Self {
            retention,
            sink: None,
        }
    }

    /// Adds a sink while preserving the selected retention policy.
    #[must_use]
    pub fn with_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Returns the successful-report retention policy.
    #[must_use]
    pub const fn retention(&self) -> EventRetention {
        self.retention
    }

    pub(crate) fn sink(&self) -> Option<&dyn EventSink> {
        self.sink.as_deref()
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.retention == EventRetention::All || self.sink.is_some()
    }
}

impl Default for EventConfig {
    fn default() -> Self {
        Self::new(EventRetention::All)
    }
}

impl fmt::Debug for EventConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventConfig")
            .field("retention", &self.retention)
            .field("has_sink", &self.sink.is_some())
            .finish()
    }
}

/// A stable failure classification with execution context for observers.
///
/// Source errors remain available through [`crate::GraphRunError`] and are not
/// copied or stringified into lifecycle events.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RunFailure {
    /// Cancellation was requested for the invocation.
    Cancelled {
        node_id: Option<NodeId>,
        step: usize,
    },
    /// The configured run timeout elapsed.
    RunTimedOut {
        timeout: Duration,
        node_id: Option<NodeId>,
        step: usize,
    },
    /// The configured timeout for one node elapsed.
    NodeTimedOut {
        timeout: Duration,
        node_id: NodeId,
        step: usize,
    },
    /// The next node would exceed the configured step limit.
    MaxStepsExceeded {
        max_steps: usize,
        node_id: NodeId,
        step: usize,
    },
    /// A node returned an error.
    NodeFailed { node_id: NodeId, step: usize },
    /// Applying a node update failed.
    StateUpdateFailed { node_id: NodeId, step: usize },
    /// Applying a parallel state-update batch failed.
    StateBatchUpdateFailed { node_ids: Vec<NodeId>, step: usize },
    /// Creating a checkpoint snapshot failed.
    SnapshotFailed {
        thread_id: ThreadId,
        superstep: usize,
        step: usize,
    },
    /// Saving a checkpoint failed.
    CheckpointSaveFailed {
        thread_id: ThreadId,
        superstep: usize,
        step: usize,
    },
    /// A conditional router returned an error.
    RouteFailed { node_id: NodeId, step: usize },
    /// A router selected a target outside its declared whitelist.
    InvalidRouteTarget {
        node_id: NodeId,
        target: NodeId,
        step: usize,
    },
}

/// A lightweight lifecycle event recorded in a run report.
///
/// Events intentionally omit complete state values and state updates.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphEvent {
    /// A graph invocation began.
    RunStarted { run_id: RunId, max_steps: usize },
    /// A super-step began with a stable active frontier.
    SuperstepStarted {
        run_id: RunId,
        superstep: usize,
        node_ids: Vec<NodeId>,
    },
    /// A node began executing.
    NodeStarted {
        run_id: RunId,
        node_id: NodeId,
        step: usize,
    },
    /// A node returned an update successfully.
    NodeCompleted {
        run_id: RunId,
        node_id: NodeId,
        step: usize,
    },
    /// The runtime applied a node update.
    StateUpdated {
        run_id: RunId,
        node_id: NodeId,
        step: usize,
    },
    /// A conditional router selected an allowed target.
    RouteSelected {
        run_id: RunId,
        source: NodeId,
        target: NodeId,
        step: usize,
    },
    /// A super-step committed its updates and selected its successors.
    SuperstepCompleted { run_id: RunId, superstep: usize },
    /// A checkpoint was saved after a successful super-step.
    CheckpointSaved {
        run_id: RunId,
        checkpoint_id: CheckpointId,
        thread_id: ThreadId,
        superstep: usize,
        step: usize,
        completed: bool,
    },
    /// The invocation reached `END`.
    RunCompleted { run_id: RunId, steps: usize },
    /// The invocation failed and will not produce a run report.
    RunFailed { run_id: RunId, failure: RunFailure },
}

impl GraphEvent {
    /// Returns the invocation that emitted this event.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        match self {
            Self::RunStarted { run_id, .. }
            | Self::SuperstepStarted { run_id, .. }
            | Self::NodeStarted { run_id, .. }
            | Self::NodeCompleted { run_id, .. }
            | Self::StateUpdated { run_id, .. }
            | Self::RouteSelected { run_id, .. }
            | Self::SuperstepCompleted { run_id, .. }
            | Self::CheckpointSaved { run_id, .. }
            | Self::RunCompleted { run_id, .. }
            | Self::RunFailed { run_id, .. } => *run_id,
        }
    }
}
