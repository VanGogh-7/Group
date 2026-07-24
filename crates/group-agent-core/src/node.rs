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

#[async_trait]
pub(crate) trait RuntimeNode<S>: Send + Sync
where
    S: GraphState,
{
    async fn run(
        &self,
        state: &S,
        context: &NodeContext,
    ) -> Result<NodeOutcome<S::Update>, NodeError>;
}

pub(crate) struct UpdateNode<N>(pub(crate) N);

#[async_trait]
impl<S, N> RuntimeNode<S> for UpdateNode<N>
where
    S: GraphState,
    N: Node<S>,
{
    async fn run(
        &self,
        state: &S,
        context: &NodeContext,
    ) -> Result<NodeOutcome<S::Update>, NodeError> {
        self.0.run(state, context).await.map(NodeOutcome::Update)
    }
}

pub(crate) struct SuspendingNode<N>(pub(crate) N);

#[async_trait]
impl<S, N> RuntimeNode<S> for SuspendingNode<N>
where
    S: GraphState,
    N: InterruptibleNode<S>,
{
    async fn run(
        &self,
        state: &S,
        context: &NodeContext,
    ) -> Result<NodeOutcome<S::Update>, NodeError> {
        self.0.run(state, context).await
    }
}
