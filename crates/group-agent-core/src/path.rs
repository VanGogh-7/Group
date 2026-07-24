use std::fmt;
use std::sync::Arc;

use crate::NodeId;

fn write_segments(
    formatter: &mut fmt::Formatter<'_>,
    segments: &[NodeId],
    root_label: bool,
) -> fmt::Result {
    if segments.is_empty() {
        return if root_label {
            formatter.write_str("<root>")
        } else {
            Ok(())
        };
    }
    for segment in segments {
        formatter.write_str("/")?;
        for character in segment.as_str().chars() {
            match character {
                '%' => formatter.write_str("%25")?,
                '/' => formatter.write_str("%2F")?,
                character => fmt::Display::fmt(&character, formatter)?,
            }
        }
    }
    Ok(())
}

/// A structured namespace of nested subgraph mount identifiers.
///
/// Display uses slash-prefixed segments, percent-escaping `%` and `/` inside
/// identifiers. The root namespace displays as `<root>`.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct GraphPath(Arc<[NodeId]>);

impl GraphPath {
    /// Returns the root graph namespace.
    #[must_use]
    pub fn root() -> Self {
        Self(Arc::from([]))
    }

    /// Creates a graph namespace from structured mount identifiers.
    #[must_use]
    pub fn new<I, T>(segments: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<NodeId>,
    {
        Self(segments.into_iter().map(Into::into).collect())
    }

    /// Returns the structured mount identifiers.
    #[must_use]
    pub fn segments(&self) -> &[NodeId] {
        &self.0
    }

    /// Returns whether this is the root namespace.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn child(&self, segment: NodeId) -> Self {
        let mut segments = Vec::with_capacity(self.0.len() + 1);
        segments.extend(self.0.iter().cloned());
        segments.push(segment);
        Self(segments.into())
    }

    pub(crate) fn prefixed(&self, prefix: &Self) -> Self {
        let mut segments = Vec::with_capacity(prefix.0.len() + self.0.len());
        segments.extend(prefix.0.iter().cloned());
        segments.extend(self.0.iter().cloned());
        Self(segments.into())
    }

    pub(crate) fn prefixes(&self) -> impl Iterator<Item = Self> + '_ {
        (1..=self.0.len()).map(|length| Self(Arc::from(&self.0[..length])))
    }

    pub(crate) fn mount_path(&self) -> NodePath {
        assert!(!self.is_root(), "root graph has no mount node");
        NodePath(Arc::clone(&self.0))
    }
}

impl Default for GraphPath {
    fn default() -> Self {
        Self::root()
    }
}

impl fmt::Display for GraphPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_segments(formatter, &self.0, true)
    }
}

impl fmt::Debug for GraphPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GraphPath")
            .field(&self.to_string())
            .finish()
    }
}

/// A structured path to one real node or structural subgraph mount.
///
/// Display uses slash-prefixed segments, percent-escaping `%` and `/` inside
/// identifiers. Runtime lookup uses the structured segments, never this text.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct NodePath(Arc<[NodeId]>);

impl NodePath {
    /// Creates a root-node path.
    #[must_use]
    pub fn root(node_id: impl Into<NodeId>) -> Self {
        Self(Arc::from([node_id.into()]))
    }

    /// Creates a node path from its namespace and leaf identifier.
    #[must_use]
    pub fn new(graph_path: &GraphPath, node_id: impl Into<NodeId>) -> Self {
        let mut segments = Vec::with_capacity(graph_path.0.len() + 1);
        segments.extend(graph_path.0.iter().cloned());
        segments.push(node_id.into());
        Self(segments.into())
    }

    /// Returns every structured segment, including the leaf node.
    #[must_use]
    pub fn segments(&self) -> &[NodeId] {
        &self.0
    }

    /// Returns the leaf node identifier.
    #[must_use]
    pub fn leaf(&self) -> &NodeId {
        self.0
            .last()
            .expect("a NodePath always contains a leaf node")
    }

    /// Returns the leaf identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.leaf().as_str()
    }

    /// Returns the containing graph namespace.
    #[must_use]
    pub fn graph_path(&self) -> GraphPath {
        GraphPath(Arc::from(&self.0[..self.0.len() - 1]))
    }

    pub(crate) fn prefixed(&self, prefix: &GraphPath) -> Self {
        let mut segments = Vec::with_capacity(prefix.0.len() + self.0.len());
        segments.extend(prefix.0.iter().cloned());
        segments.extend(self.0.iter().cloned());
        Self(segments.into())
    }
}

impl From<NodeId> for NodePath {
    fn from(node_id: NodeId) -> Self {
        Self::root(node_id)
    }
}

impl From<&str> for NodePath {
    fn from(node_id: &str) -> Self {
        Self::root(node_id)
    }
}

impl PartialEq<NodeId> for NodePath {
    fn eq(&self, other: &NodeId) -> bool {
        self.0.len() == 1 && self.leaf() == other
    }
}

impl PartialEq<NodePath> for NodeId {
    fn eq(&self, other: &NodePath) -> bool {
        other == self
    }
}

impl fmt::Display for NodePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_segments(formatter, &self.0, false)
    }
}

impl fmt::Debug for NodePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("NodePath")
            .field(&self.to_string())
            .finish()
    }
}
