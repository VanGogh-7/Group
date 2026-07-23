use std::fmt;

/// The reserved textual identifier for the graph entry point.
pub const START: &str = "__start__";

/// The reserved textual identifier for the graph exit point.
pub const END: &str = "__end__";

/// A stable, public node identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NodeId(String);

impl NodeId {
    /// Creates a node identifier.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the reserved graph entry identifier.
    #[must_use]
    pub fn start() -> Self {
        Self::from(START)
    }

    /// Returns the reserved graph exit identifier.
    #[must_use]
    pub fn end() -> Self {
        Self::from(END)
    }

    /// Returns this identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn is_reserved(&self) -> bool {
        self.0 == START || self.0 == END
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(value)
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
