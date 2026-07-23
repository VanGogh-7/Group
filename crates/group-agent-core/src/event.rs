use crate::NodeId;

/// A lightweight lifecycle event recorded in a run report.
///
/// Events intentionally omit complete state values and state updates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphEvent {
    /// A graph invocation began.
    RunStarted { max_steps: usize },
    /// A node began executing.
    NodeStarted { node_id: NodeId, step: usize },
    /// A node returned an update successfully.
    NodeCompleted { node_id: NodeId, step: usize },
    /// The runtime applied a node update.
    StateUpdated { node_id: NodeId, step: usize },
    /// The invocation reached `END`.
    RunCompleted { steps: usize },
}
