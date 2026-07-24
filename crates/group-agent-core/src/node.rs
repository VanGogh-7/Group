use std::sync::Arc;

use async_trait::async_trait;

use crate::{GraphState, NodeContext, NodeError, NodeOutcome};

/// An asynchronous graph node.
#[async_trait]
pub trait Node<S>: Send + Sync
where
    S: GraphState,
{
    /// Inspects state and produces one update.
    async fn run(&self, state: &S, context: &NodeContext) -> Result<S::Update, NodeError>;
}

/// An asynchronous node that may update state or suspend execution.
///
/// Ordinary update-only nodes should continue implementing [`Node`].
#[async_trait]
pub trait InterruptibleNode<S>: Send + Sync
where
    S: GraphState,
{
    /// Inspects state and either produces an update or requests an interrupt.
    async fn run(
        &self,
        state: &S,
        context: &NodeContext,
    ) -> Result<NodeOutcome<S::Update>, NodeError>;
}

pub(crate) enum NodeKind<S>
where
    S: GraphState,
{
    Normal(Arc<dyn Node<S>>),
    Interruptible(Arc<dyn InterruptibleNode<S>>),
}

impl<S> Clone for NodeKind<S>
where
    S: GraphState,
{
    fn clone(&self) -> Self {
        match self {
            Self::Normal(node) => Self::Normal(Arc::clone(node)),
            Self::Interruptible(node) => Self::Interruptible(Arc::clone(node)),
        }
    }
}
