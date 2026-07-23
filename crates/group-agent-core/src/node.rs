use async_trait::async_trait;

use crate::{GraphState, NodeContext, NodeError};

/// An asynchronous graph node.
#[async_trait]
pub trait Node<S>: Send + Sync
where
    S: GraphState,
{
    /// Inspects state and produces one update.
    async fn run(&self, state: &S, context: &NodeContext) -> Result<S::Update, NodeError>;
}
