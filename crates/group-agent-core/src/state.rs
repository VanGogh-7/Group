use crate::StateError;

/// State carried through a graph execution.
///
/// Nodes inspect this state immutably and return an update. Only the runtime
/// applies updates.
pub trait GraphState: Clone + Send + Sync + 'static {
    /// The update produced by a node.
    type Update: Send + Sync + 'static;

    /// Applies a node update to this state.
    fn apply(&mut self, update: Self::Update) -> Result<(), StateError>;
}
