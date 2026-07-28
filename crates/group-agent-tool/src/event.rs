use std::sync::Arc;
use std::time::Duration;

use crate::{ToolCallContext, ToolObserverError, ToolRuntimeErrorKind};

/// Payload-free lifecycle event for one tool execution.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolEvent {
    ExecutionStarted {
        context: ToolCallContext,
    },
    ExecutionCompleted {
        context: ToolCallContext,
        is_error: bool,
    },
    ExecutionFailed {
        context: ToolCallContext,
        kind: ToolRuntimeErrorKind,
    },
    ExecutionTimedOut {
        context: ToolCallContext,
        timeout: Duration,
    },
}

impl ToolEvent {
    /// Returns safe call identity and batch position.
    #[must_use]
    pub const fn context(&self) -> &ToolCallContext {
        match self {
            Self::ExecutionStarted { context }
            | Self::ExecutionCompleted { context, .. }
            | Self::ExecutionFailed { context, .. }
            | Self::ExecutionTimedOut { context, .. } => context,
        }
    }
}

/// Synchronous, fallible observer for tool lifecycle events.
///
/// Callbacks run inline, outside registry locks, and must remain lightweight.
/// Runtime catches both returned errors and panics. A failure while observing
/// `ExecutionStarted` prevents Tool execution. A terminal callback failure is
/// a secondary diagnostic and never replaces the already determined Tool
/// success, Tool failure, or timeout.
pub trait ToolEventSink: Send + Sync {
    fn on_event(&self, event: &ToolEvent) -> Result<(), ToolObserverError>;
}

impl<F> ToolEventSink for F
where
    F: Fn(&ToolEvent) -> Result<(), ToolObserverError> + Send + Sync,
{
    fn on_event(&self, event: &ToolEvent) -> Result<(), ToolObserverError> {
        self(event)
    }
}

pub(crate) type SharedToolEventSink = Arc<dyn ToolEventSink>;
