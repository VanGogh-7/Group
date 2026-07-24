use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::Dfs;

use crate::edge::{ConditionalEdge, FanOutEdge, FixedEdge, Router};
use crate::{
    GraphBuildError, GraphCompileError, GraphState, GraphVersion, Node, NodeId, RouteError,
};

#[derive(Clone, Copy, Debug)]
enum TopologyEdge {
    Fixed,
    FanOut,
    Conditional,
}

struct FixedOutgoing {
    count: usize,
    successor: NodeId,
}

struct EdgeAggregation<'a, S>
where
    S: GraphState,
{
    fixed_by_source: HashMap<NodeId, FixedOutgoing>,
    fan_out_by_source: HashMap<NodeId, &'a FanOutEdge>,
    conditional_by_source: HashMap<NodeId, &'a ConditionalEdge<S>>,
}

/// A mutable builder for a state graph.
pub struct StateGraph<S>
where
    S: GraphState,
{
    nodes: IndexMap<NodeId, Arc<dyn Node<S>>>,
    fixed_edges: Vec<FixedEdge>,
    fan_out_edges: Vec<FanOutEdge>,
    conditional_edges: Vec<ConditionalEdge<S>>,
    version: Option<GraphVersion>,
}

impl<S> StateGraph<S>
where
    S: GraphState,
{
    /// Creates an empty graph builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: IndexMap::new(),
            fixed_edges: Vec::new(),
            fan_out_edges: Vec::new(),
            conditional_edges: Vec::new(),
            version: None,
        }
    }

    /// Assigns an explicit compatibility version to this graph.
    pub fn set_version(&mut self, version: impl Into<GraphVersion>) -> &mut Self {
        self.version = Some(version.into());
        self
    }

    /// Assigns an explicit compatibility version and returns this builder.
    #[must_use]
    pub fn with_version(mut self, version: impl Into<GraphVersion>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Registers a normal graph node.
    pub fn add_node<N>(
        &mut self,
        node_id: impl Into<NodeId>,
        node: N,
    ) -> Result<&mut Self, GraphBuildError>
    where
        N: Node<S> + 'static,
    {
        let node_id = node_id.into();
        if node_id.is_reserved() {
            return Err(GraphBuildError::ReservedNodeId { node_id });
        }
        if self.nodes.contains_key(&node_id) {
            return Err(GraphBuildError::DuplicateNode { node_id });
        }

        self.nodes.insert(node_id, Arc::new(node));
        Ok(self)
    }

    /// Registers a directed fixed edge.
    ///
    /// Endpoint validation is deferred to [`Self::compile`], so callers may add
    /// edges before registering all nodes.
    pub fn add_edge(&mut self, from: impl Into<NodeId>, to: impl Into<NodeId>) -> &mut Self {
        self.fixed_edges
            .push(FixedEdge::new(from.into(), to.into()));
        self
    }

    /// Registers one static fan-out transition.
    ///
    /// All target nodes become members of the next active frontier and execute
    /// concurrently against the same immutable state snapshot.
    pub fn add_fan_out<I, T>(
        &mut self,
        source: impl Into<NodeId>,
        targets: I,
    ) -> Result<&mut Self, GraphBuildError>
    where
        I: IntoIterator<Item = T>,
        T: Into<NodeId>,
    {
        let source = source.into();
        if self.fan_out_edges.iter().any(|edge| edge.source == source) {
            return Err(GraphBuildError::MultipleFanOutTransitions {
                source_node: source,
            });
        }

        let targets: Vec<NodeId> = targets.into_iter().map(Into::into).collect();
        if targets.is_empty() {
            return Err(GraphBuildError::EmptyFanOutTargets {
                source_node: source,
            });
        }

        let mut unique_targets = HashSet::new();
        for target in &targets {
            if !unique_targets.insert(target.clone()) {
                return Err(GraphBuildError::DuplicateFanOutTarget {
                    source_node: source,
                    target: target.clone(),
                });
            }
        }

        self.fan_out_edges.push(FanOutEdge::new(source, targets));
        Ok(self)
    }

    /// Registers one synchronous conditional router and its target whitelist.
    ///
    /// The router runs after the source node's update has been applied. It may
    /// only select one of `allowed_targets`.
    pub fn add_conditional_edges<I, T, F>(
        &mut self,
        source: impl Into<NodeId>,
        allowed_targets: I,
        router: F,
    ) -> Result<&mut Self, GraphBuildError>
    where
        I: IntoIterator<Item = T>,
        T: Into<NodeId>,
        F: Fn(&S) -> Result<NodeId, RouteError> + Send + Sync + 'static,
    {
        let source = source.into();
        if self
            .conditional_edges
            .iter()
            .any(|edge| edge.source == source)
        {
            return Err(GraphBuildError::MultipleConditionalRouters {
                source_node: source,
            });
        }

        let allowed_targets: Vec<NodeId> = allowed_targets.into_iter().map(Into::into).collect();
        if allowed_targets.is_empty() {
            return Err(GraphBuildError::EmptyConditionalTargets {
                source_node: source,
            });
        }

        let mut unique_targets = HashSet::new();
        for target in &allowed_targets {
            if !unique_targets.insert(target.clone()) {
                return Err(GraphBuildError::DuplicateConditionalTarget {
                    source_node: source,
                    target: target.clone(),
                });
            }
        }

        self.conditional_edges.push(ConditionalEdge::new(
            source,
            allowed_targets,
            Arc::new(router),
        ));
        Ok(self)
    }

    /// Validates and freezes the graph for repeated execution.
    pub fn compile(&self) -> Result<CompiledGraph<S>, GraphCompileError> {
        let edges = self.aggregate_edges()?;
        self.validate_edge_shapes(&edges)?;

        let mut topology = StableDiGraph::<NodeId, TopologyEdge>::new();
        let mut indices = IndexMap::new();

        let start_id = NodeId::start();
        let start_index = topology.add_node(start_id.clone());
        indices.insert(start_id, start_index);

        for node_id in self.nodes.keys() {
            let index = topology.add_node(node_id.clone());
            indices.insert(node_id.clone(), index);
        }

        let end_id = NodeId::end();
        let end_index = topology.add_node(end_id.clone());
        indices.insert(end_id, end_index);

        for edge in &self.fixed_edges {
            topology.add_edge(indices[&edge.from], indices[&edge.to], TopologyEdge::Fixed);
        }
        for edge in &self.fan_out_edges {
            for target in &edge.targets {
                topology.add_edge(indices[&edge.source], indices[target], TopologyEdge::FanOut);
            }
        }
        for edge in &self.conditional_edges {
            for target in &edge.allowed_targets {
                topology.add_edge(
                    indices[&edge.source],
                    indices[target],
                    TopologyEdge::Conditional,
                );
            }
        }

        self.validate_reachability(&topology, &indices, start_index, end_index, &edges)?;

        let entry_target = &edges
            .fixed_by_source
            .get(&NodeId::start())
            .expect("validated graph has one START successor")
            .successor;
        let entry_index = indices[entry_target];

        let mut compiled_nodes = std::iter::repeat_with(|| None)
            .take(topology.node_count())
            .collect::<Vec<_>>();

        for (node_id, node) in &self.nodes {
            let transition = if let Some(outgoing) = edges.fixed_by_source.get(node_id) {
                CompiledTransition::Fixed(indices[&outgoing.successor])
            } else if let Some(edge) = edges.fan_out_by_source.get(node_id) {
                CompiledTransition::FanOut(
                    edge.targets.iter().map(|target| indices[target]).collect(),
                )
            } else {
                let edge = edges
                    .conditional_by_source
                    .get(node_id)
                    .expect("validated executable node has one transition");
                let allowed_targets = edge
                    .allowed_targets
                    .iter()
                    .map(|target| (target.clone(), indices[target]))
                    .collect();
                CompiledTransition::Conditional {
                    router: Arc::clone(&edge.router),
                    allowed_targets,
                }
            };
            let index = indices[node_id];
            compiled_nodes[index.index()] = Some(CompiledNode {
                id: node_id.clone(),
                node: Arc::clone(node),
                transition,
            });
        }

        Ok(CompiledGraph {
            _topology: topology,
            nodes: compiled_nodes,
            node_indices: indices,
            entry_index,
            end_index,
            version: self.version.clone(),
        })
    }

    fn aggregate_edges(&self) -> Result<EdgeAggregation<'_, S>, GraphCompileError> {
        let mut fixed_by_source = HashMap::with_capacity(self.fixed_edges.len());

        for edge in &self.fixed_edges {
            for endpoint in [&edge.from, &edge.to] {
                if !endpoint.is_reserved() && !self.nodes.contains_key(endpoint) {
                    return Err(GraphCompileError::UnknownNode {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        node_id: endpoint.clone(),
                    });
                }
            }

            if edge.to.is_start() {
                return Err(GraphCompileError::StartHasIncoming {
                    from: edge.from.clone(),
                });
            }
            if edge.from.is_end() {
                return Err(GraphCompileError::EndHasOutgoing {
                    to: edge.to.clone(),
                });
            }

            fixed_by_source
                .entry(edge.from.clone())
                .and_modify(|outgoing: &mut FixedOutgoing| outgoing.count += 1)
                .or_insert_with(|| FixedOutgoing {
                    count: 1,
                    successor: edge.to.clone(),
                });
        }

        let mut conditional_by_source = HashMap::with_capacity(self.conditional_edges.len());
        for edge in &self.conditional_edges {
            if edge.source.is_start() {
                return Err(GraphCompileError::StartHasConditionalEdge);
            }
            if edge.source.is_end() {
                return Err(GraphCompileError::EndHasConditionalEdge);
            }
            if !self.nodes.contains_key(&edge.source) {
                return Err(GraphCompileError::UnknownConditionalSource {
                    source_node: edge.source.clone(),
                });
            }

            for target in &edge.allowed_targets {
                if target.is_start() {
                    return Err(GraphCompileError::StartHasIncoming {
                        from: edge.source.clone(),
                    });
                }
                if !target.is_end() && !self.nodes.contains_key(target) {
                    return Err(GraphCompileError::UnknownConditionalTarget {
                        source_node: edge.source.clone(),
                        target: target.clone(),
                    });
                }
            }

            conditional_by_source.insert(edge.source.clone(), edge);
        }

        let mut fan_out_by_source = HashMap::with_capacity(self.fan_out_edges.len());
        for edge in &self.fan_out_edges {
            if edge.source.is_start() {
                return Err(GraphCompileError::StartHasFanOut);
            }
            if edge.source.is_end() {
                return Err(GraphCompileError::EndHasFanOut);
            }
            if !self.nodes.contains_key(&edge.source) {
                return Err(GraphCompileError::UnknownFanOutSource {
                    source_node: edge.source.clone(),
                });
            }

            for target in &edge.targets {
                if target.is_start() {
                    return Err(GraphCompileError::StartHasIncoming {
                        from: edge.source.clone(),
                    });
                }
                if !target.is_end() && !self.nodes.contains_key(target) {
                    return Err(GraphCompileError::UnknownFanOutTarget {
                        source_node: edge.source.clone(),
                        target: target.clone(),
                    });
                }
            }

            fan_out_by_source.insert(edge.source.clone(), edge);
        }

        Ok(EdgeAggregation {
            fixed_by_source,
            fan_out_by_source,
            conditional_by_source,
        })
    }

    fn validate_edge_shapes(
        &self,
        edges: &EdgeAggregation<'_, S>,
    ) -> Result<(), GraphCompileError> {
        let start_count = edges
            .fixed_by_source
            .get(&NodeId::start())
            .map(|outgoing| outgoing.count)
            .unwrap_or(0);
        match start_count {
            0 => return Err(GraphCompileError::MissingStartEdge),
            1 => {}
            count => return Err(GraphCompileError::MultipleStartEdges { count }),
        }

        for node_id in self.nodes.keys() {
            let fixed_count = edges
                .fixed_by_source
                .get(node_id)
                .map(|outgoing| outgoing.count)
                .unwrap_or(0);
            if fixed_count > 1 {
                return Err(GraphCompileError::MultipleOutgoingEdges {
                    node_id: node_id.clone(),
                    count: fixed_count,
                });
            }

            let transition_kind_count = usize::from(fixed_count == 1)
                + usize::from(edges.fan_out_by_source.contains_key(node_id))
                + usize::from(edges.conditional_by_source.contains_key(node_id));
            if transition_kind_count > 1 {
                return Err(GraphCompileError::MixedOutgoingEdgeKinds {
                    node_id: node_id.clone(),
                });
            }
        }

        Ok(())
    }

    fn validate_reachability(
        &self,
        topology: &StableDiGraph<NodeId, TopologyEdge>,
        indices: &IndexMap<NodeId, NodeIndex>,
        start_index: NodeIndex,
        end_index: NodeIndex,
        edges: &EdgeAggregation<'_, S>,
    ) -> Result<(), GraphCompileError> {
        let mut depth_first = Dfs::new(topology, start_index);
        let mut reachable = HashSet::new();
        while let Some(index) = depth_first.next(topology) {
            reachable.insert(index);
        }

        for node_id in self.nodes.keys() {
            if !reachable.contains(&indices[node_id]) {
                return Err(GraphCompileError::UnreachableNode {
                    node_id: node_id.clone(),
                });
            }
        }

        self.validate_outgoing_completeness(edges)?;

        if !reachable.contains(&end_index) {
            return Err(GraphCompileError::NoReachableEnd);
        }

        Ok(())
    }

    fn validate_outgoing_completeness(
        &self,
        edges: &EdgeAggregation<'_, S>,
    ) -> Result<(), GraphCompileError> {
        for node_id in self.nodes.keys() {
            let has_fixed = edges.fixed_by_source.contains_key(node_id);
            let has_fan_out = edges.fan_out_by_source.contains_key(node_id);
            let has_conditional = edges.conditional_by_source.contains_key(node_id);
            if !has_fixed && !has_fan_out && !has_conditional {
                return Err(GraphCompileError::MissingOutgoingEdge {
                    node_id: node_id.clone(),
                });
            }
        }
        Ok(())
    }
}

impl<S> Default for StateGraph<S>
where
    S: GraphState,
{
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) enum CompiledTransition<S>
where
    S: GraphState,
{
    Fixed(NodeIndex),
    FanOut(Vec<NodeIndex>),
    Conditional {
        router: Router<S>,
        allowed_targets: IndexMap<NodeId, NodeIndex>,
    },
}

pub(crate) struct CompiledNode<S>
where
    S: GraphState,
{
    pub(crate) id: NodeId,
    pub(crate) node: Arc<dyn Node<S>>,
    pub(crate) transition: CompiledTransition<S>,
}

/// An immutable graph that can be invoked repeatedly.
pub struct CompiledGraph<S>
where
    S: GraphState,
{
    _topology: StableDiGraph<NodeId, TopologyEdge>,
    pub(crate) nodes: Vec<Option<CompiledNode<S>>>,
    pub(crate) node_indices: IndexMap<NodeId, NodeIndex>,
    pub(crate) entry_index: NodeIndex,
    pub(crate) end_index: NodeIndex,
    pub(crate) version: Option<GraphVersion>,
}

impl<S> CompiledGraph<S>
where
    S: GraphState,
{
    pub(crate) fn node_at(&self, index: NodeIndex) -> &CompiledNode<S> {
        self.nodes[index.index()]
            .as_ref()
            .expect("compiled executable index contains a node")
    }

    /// Returns the explicit compatibility version, if one was assigned.
    #[must_use]
    pub const fn version(&self) -> Option<&GraphVersion> {
        self.version.as_ref()
    }
}
