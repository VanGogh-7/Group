use std::fmt;
use std::sync::{Arc, LazyLock};

use crate::{GraphState, RouteError};

/// The reserved textual identifier for the graph entry point.
pub const START: &str = "__start__";

/// The reserved textual identifier for the graph exit point.
pub const END: &str = "__end__";

/// A stable, public node identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NodeId(Arc<str>);

static START_NODE_ID: LazyLock<NodeId> = LazyLock::new(|| NodeId(Arc::from(START)));
static END_NODE_ID: LazyLock<NodeId> = LazyLock::new(|| NodeId(Arc::from(END)));

impl NodeId {
    /// Creates a node identifier.
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// Returns the reserved graph entry identifier.
    #[must_use]
    pub fn start() -> Self {
        (*START_NODE_ID).clone()
    }

    /// Returns the reserved graph exit identifier.
    #[must_use]
    pub fn end() -> Self {
        (*END_NODE_ID).clone()
    }

    /// Returns this identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn is_start(&self) -> bool {
        self.as_str() == START
    }

    pub(crate) fn is_end(&self) -> bool {
        self.as_str() == END
    }

    pub(crate) fn is_reserved(&self) -> bool {
        self.is_start() || self.is_end()
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FixedEdge {
    pub(crate) from: NodeId,
    pub(crate) to: NodeId,
}

impl FixedEdge {
    pub(crate) fn new(from: NodeId, to: NodeId) -> Self {
        Self { from, to }
    }
}

pub(crate) type Router<S> = Arc<dyn Fn(&S) -> Result<NodeId, RouteError> + Send + Sync + 'static>;

pub(crate) struct ConditionalEdge<S>
where
    S: GraphState,
{
    pub(crate) source: NodeId,
    pub(crate) allowed_targets: Vec<NodeId>,
    pub(crate) router: Router<S>,
}

impl<S> ConditionalEdge<S>
where
    S: GraphState,
{
    pub(crate) fn new(source: NodeId, allowed_targets: Vec<NodeId>, router: Router<S>) -> Self {
        Self {
            source,
            allowed_targets,
            router,
        }
    }
}
