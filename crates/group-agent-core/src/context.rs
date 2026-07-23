use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::NodeId;

/// Per-node execution context.
#[derive(Clone, Debug)]
pub struct NodeContext {
    step: usize,
    node_id: NodeId,
    cancellation_token: CancellationToken,
    run_deadline: Option<Instant>,
}

impl PartialEq for NodeContext {
    fn eq(&self, other: &Self) -> bool {
        // Live control handles do not change the identity of a node position.
        self.step == other.step && self.node_id == other.node_id
    }
}

impl Eq for NodeContext {}

impl NodeContext {
    pub(crate) fn new(
        step: usize,
        node_id: NodeId,
        cancellation_token: CancellationToken,
        run_deadline: Option<Instant>,
    ) -> Self {
        Self {
            step,
            node_id,
            cancellation_token,
            run_deadline,
        }
    }

    /// Returns the one-based execution step.
    #[must_use]
    pub const fn step(&self) -> usize {
        self.step
    }

    /// Returns the node currently being executed.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns a clone of the cancellation token for this invocation.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Returns whether cancellation has been requested for this invocation.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Returns the absolute run deadline, when a run timeout is configured.
    #[must_use]
    pub const fn run_deadline(&self) -> Option<Instant> {
        self.run_deadline
    }

    /// Returns the remaining duration until the run deadline.
    ///
    /// The returned duration is zero when the deadline has already elapsed.
    #[must_use]
    pub fn remaining_run_time(&self) -> Option<Duration> {
        self.run_deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }
}

/// Configuration for one graph invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunConfig {
    /// Maximum number of nodes that may execute.
    pub max_steps: usize,
}

impl RunConfig {
    /// Creates a run configuration with the supplied step limit.
    #[must_use]
    pub const fn new(max_steps: usize) -> Self {
        Self { max_steps }
    }
}

impl Default for RunConfig {
    fn default() -> Self {
        Self { max_steps: 1_000 }
    }
}

/// Cooperative cancellation and timeout configuration for one invocation.
///
/// Cloned configurations share any explicitly supplied cancellation token,
/// which allows one cancellation request to affect multiple runs when callers
/// intentionally reuse a configuration or token.
#[derive(Clone, Debug)]
pub struct RunControl {
    cancellation_token: Option<CancellationToken>,
    run_timeout: Option<Duration>,
    node_timeout: Option<Duration>,
}

impl RunControl {
    /// Creates a control configuration with no external cancellation token, no
    /// run timeout, and no node timeout.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancellation_token: None,
            run_timeout: None,
            node_timeout: None,
        }
    }

    /// Uses an externally managed cancellation token.
    #[must_use]
    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = Some(cancellation_token);
        self
    }

    /// Sets the timeout measured from invocation start.
    #[must_use]
    pub const fn with_run_timeout(mut self, timeout: Duration) -> Self {
        self.run_timeout = Some(timeout);
        self
    }

    /// Sets the timeout measured separately from each `NodeStarted` event.
    #[must_use]
    pub const fn with_node_timeout(mut self, timeout: Duration) -> Self {
        self.node_timeout = Some(timeout);
        self
    }

    /// Returns the explicitly configured cancellation token.
    #[must_use]
    pub const fn cancellation_token(&self) -> Option<&CancellationToken> {
        self.cancellation_token.as_ref()
    }

    /// Returns the configured run timeout.
    #[must_use]
    pub const fn run_timeout(&self) -> Option<Duration> {
        self.run_timeout
    }

    /// Returns the configured per-node timeout.
    #[must_use]
    pub const fn node_timeout(&self) -> Option<Duration> {
        self.node_timeout
    }
}

impl Default for RunControl {
    fn default() -> Self {
        Self::new()
    }
}
