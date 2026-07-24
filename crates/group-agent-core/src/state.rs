use crate::{NodeId, NodePath, StateError};

/// One node's update in a deterministic parallel state-update batch.
///
/// The runtime orders batches by compiled node order, independently of the
/// order in which node futures complete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeUpdate<U> {
    node_path: NodePath,
    update: U,
}

impl<U> NodeUpdate<U> {
    pub(crate) fn new(node_path: NodePath, update: U) -> Self {
        Self { node_path, update }
    }

    /// Returns the node that produced this update.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        self.node_path.leaf()
    }

    /// Returns the complete structured source path.
    #[must_use]
    pub const fn node_path(&self) -> &NodePath {
        &self.node_path
    }

    /// Returns the update without consuming this batch entry.
    #[must_use]
    pub const fn update(&self) -> &U {
        &self.update
    }

    /// Consumes the entry and returns its source and update.
    #[must_use]
    pub fn into_parts(self) -> (NodePath, U) {
        (self.node_path, self.update)
    }
}

/// State carried through a graph execution.
///
/// Nodes inspect this state immutably and return an update. Only the runtime
/// applies updates.
pub trait GraphState: Send + Sync + 'static {
    /// The update produced by a node.
    type Update: Send + Sync + 'static;

    /// Applies a node update to this state.
    fn apply(&mut self, update: Self::Update) -> Result<(), StateError>;

    /// Atomically validates and applies a parallel super-step update batch.
    ///
    /// The default implementation preserves sequential behavior for one
    /// update and rejects multiple updates before modifying state. States that
    /// participate in static fan-out must override this method, validate the
    /// complete batch first, and then commit it without requiring a complete
    /// state clone.
    fn apply_batch(
        &mut self,
        mut updates: Vec<NodeUpdate<Self::Update>>,
    ) -> Result<(), StateError> {
        if updates.len() != 1 {
            return Err(StateError::message(format!(
                "parallel super-step produced {} updates, but this state does not define \
                 apply_batch",
                updates.len()
            )));
        }

        let (_, update) = updates
            .pop()
            .expect("a one-entry batch contains one update")
            .into_parts();
        self.apply(update)
    }
}
