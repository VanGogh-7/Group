use crate::NodeId;

/// Per-node execution context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeContext {
    step: usize,
    node_id: NodeId,
}

impl NodeContext {
    pub(crate) fn new(step: usize, node_id: NodeId) -> Self {
        Self { step, node_id }
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
