use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use indexmap::IndexMap;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::Dfs;

use crate::edge::{ConditionalEdge, ConditionalFanOutEdge, FanOutEdge, FixedEdge};
use crate::node::NodeKind;
use crate::transition::CompiledTransition;
use crate::{
    GraphBuildError, GraphCompileError, GraphPath, GraphState, GraphVersion, InterruptibleNode,
    Node, NodeId, NodePath, RouteError,
};

#[derive(Clone, Copy, Debug)]
enum TopologyEdge {
    Fixed,
    FanOut,
    Conditional,
    ConditionalFanOut,
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
    conditional_fan_out_by_source: HashMap<NodeId, &'a ConditionalFanOutEdge<S>>,
}

enum DeclaredItem<S>
where
    S: GraphState,
{
    Node(NodeKind<S>),
    Subgraph(Box<CompiledGraph<S>>),
}

impl<S> DeclaredItem<S>
where
    S: GraphState,
{
    const fn is_subgraph(&self) -> bool {
        matches!(self, Self::Subgraph(_))
    }
}

/// A mutable builder for a state graph.
pub struct StateGraph<S>
where
    S: GraphState,
{
    items: IndexMap<NodeId, DeclaredItem<S>>,
    fixed_edges: Vec<FixedEdge>,
    fan_out_edges: Vec<FanOutEdge>,
    conditional_edges: Vec<ConditionalEdge<S>>,
    conditional_fan_out_edges: Vec<ConditionalFanOutEdge<S>>,
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
            items: IndexMap::new(),
            fixed_edges: Vec::new(),
            fan_out_edges: Vec::new(),
            conditional_edges: Vec::new(),
            conditional_fan_out_edges: Vec::new(),
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

    /// Registers a normal update-only graph node.
    pub fn add_node<N>(
        &mut self,
        node_id: impl Into<NodeId>,
        node: N,
    ) -> Result<&mut Self, GraphBuildError>
    where
        N: Node<S> + 'static,
    {
        self.insert_node(node_id.into(), NodeKind::Normal(Arc::new(node)))
    }

    /// Registers a graph node that may return an update or request suspension.
    pub fn add_interruptible_node<N>(
        &mut self,
        node_id: impl Into<NodeId>,
        node: N,
    ) -> Result<&mut Self, GraphBuildError>
    where
        N: InterruptibleNode<S> + 'static,
    {
        self.insert_node(node_id.into(), NodeKind::Interruptible(Arc::new(node)))
    }

    /// Mounts an immutable shared-state subgraph as a structural graph item.
    ///
    /// The mount does not execute a node or consume a step. Its outgoing
    /// transition is followed only after the child graph reaches its END.
    pub fn add_subgraph(
        &mut self,
        node_id: impl Into<NodeId>,
        subgraph: CompiledGraph<S>,
    ) -> Result<&mut Self, GraphBuildError> {
        let node_id = node_id.into();
        if node_id.is_reserved() {
            return Err(GraphBuildError::ReservedNodeId { node_id });
        }
        if let Some(existing) = self.items.get(&node_id) {
            return Err(if existing.is_subgraph() {
                GraphBuildError::DuplicateSubgraphMount { node_id }
            } else {
                GraphBuildError::DuplicateNode { node_id }
            });
        }
        self.items
            .insert(node_id, DeclaredItem::Subgraph(Box::new(subgraph)));
        Ok(self)
    }

    fn insert_node(
        &mut self,
        node_id: NodeId,
        node: NodeKind<S>,
    ) -> Result<&mut Self, GraphBuildError> {
        if node_id.is_reserved() {
            return Err(GraphBuildError::ReservedNodeId { node_id });
        }
        if self.items.contains_key(&node_id) {
            return Err(GraphBuildError::DuplicateNode { node_id });
        }
        self.items.insert(node_id, DeclaredItem::Node(node));
        Ok(self)
    }

    /// Registers a directed fixed edge.
    ///
    /// Endpoint validation is deferred to [`Self::compile`], so declarations
    /// may be added before every item is registered.
    pub fn add_edge(&mut self, from: impl Into<NodeId>, to: impl Into<NodeId>) -> &mut Self {
        self.fixed_edges
            .push(FixedEdge::new(from.into(), to.into()));
        self
    }

    /// Registers one static fan-out transition.
    ///
    /// All targets become members of one next frontier and inspect the same
    /// immutable state snapshot.
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
    /// The router runs after the source update commits and may select only one
    /// declared target.
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

    /// Registers one synchronous conditional fan-out router and its target whitelist.
    ///
    /// The router runs after the source update commits and must select one or
    /// more distinct declared targets. `END` may be selected beside executable
    /// targets and exits only the source branch.
    pub fn add_conditional_fan_out<I, T, F>(
        &mut self,
        source: impl Into<NodeId>,
        allowed_targets: I,
        router: F,
    ) -> Result<&mut Self, GraphBuildError>
    where
        I: IntoIterator<Item = T>,
        T: Into<NodeId>,
        F: Fn(&S) -> Result<Vec<NodeId>, RouteError> + Send + Sync + 'static,
    {
        let source = source.into();
        if self
            .conditional_fan_out_edges
            .iter()
            .any(|edge| edge.source == source)
        {
            return Err(GraphBuildError::MultipleConditionalFanOutRouters {
                source_node: source,
            });
        }
        let allowed_targets: Vec<NodeId> = allowed_targets.into_iter().map(Into::into).collect();
        if allowed_targets.is_empty() {
            return Err(GraphBuildError::EmptyConditionalFanOutTargets {
                source_node: source,
            });
        }
        let mut unique_targets = HashSet::new();
        for target in &allowed_targets {
            if !unique_targets.insert(target.clone()) {
                return Err(GraphBuildError::DuplicateConditionalFanOutTarget {
                    source_node: source,
                    target: target.clone(),
                });
            }
        }
        self.conditional_fan_out_edges
            .push(ConditionalFanOutEdge::new(
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
        let mut topology_indices = IndexMap::new();
        let start_id = NodeId::start();
        let start_index = topology.add_node(start_id.clone());
        topology_indices.insert(start_id, start_index);
        for item_id in self.items.keys() {
            topology_indices.insert(item_id.clone(), topology.add_node(item_id.clone()));
        }
        let end_id = NodeId::end();
        let end_index = topology.add_node(end_id.clone());
        topology_indices.insert(end_id, end_index);

        for edge in &self.fixed_edges {
            topology.add_edge(
                topology_indices[&edge.from],
                topology_indices[&edge.to],
                TopologyEdge::Fixed,
            );
        }
        for edge in &self.fan_out_edges {
            for target in &edge.targets {
                topology.add_edge(
                    topology_indices[&edge.source],
                    topology_indices[target],
                    TopologyEdge::FanOut,
                );
            }
        }
        for edge in &self.conditional_edges {
            for target in &edge.allowed_targets {
                topology.add_edge(
                    topology_indices[&edge.source],
                    topology_indices[target],
                    TopologyEdge::Conditional,
                );
            }
        }
        for edge in &self.conditional_fan_out_edges {
            for target in &edge.allowed_targets {
                topology.add_edge(
                    topology_indices[&edge.source],
                    topology_indices[target],
                    TopologyEdge::ConditionalFanOut,
                );
            }
        }
        self.validate_reachability(&topology, &topology_indices, start_index, end_index, &edges)?;
        self.validate_subgraph_frontiers(&edges)?;

        let mut builder = FlatBuilder::new();
        let root_path = GraphPath::root();
        let mut item_entries = IndexMap::new();
        let mut mount_exits = HashMap::new();

        for (item_id, item) in &self.items {
            match item {
                DeclaredItem::Node(_) => {
                    item_entries.insert(item_id.clone(), builder.allocate());
                }
                DeclaredItem::Subgraph(_) => {
                    let entry = builder.allocate();
                    let exit = builder.allocate();
                    item_entries.insert(item_id.clone(), entry);
                    mount_exits.insert(item_id.clone(), exit);
                }
            }
        }

        for (item_id, item) in &self.items {
            let transition = self.compile_transition(item_id, &edges, &item_entries)?;
            match item {
                DeclaredItem::Node(kind) => {
                    let path = NodePath::new(&root_path, item_id.clone());
                    builder.insert_node(
                        item_entries[item_id],
                        CompiledNode {
                            path,
                            graph_path: root_path.clone(),
                            kind: kind.clone(),
                            transition,
                        },
                    )?;
                }
                DeclaredItem::Subgraph(child) => {
                    let graph_path = root_path.child(item_id.clone());
                    let exit = mount_exits[item_id];
                    let child_entry = builder.append_graph(child, &graph_path, exit)?;
                    builder.set(
                        item_entries[item_id],
                        CompiledItem::EnterSubgraph {
                            graph_path: graph_path.clone(),
                            transition: CompiledTransition::Fixed(child_entry.or(Some(exit))),
                        },
                    );
                    builder.set(
                        exit,
                        CompiledItem::ExitSubgraph {
                            graph_path: graph_path.clone(),
                            mount_path: NodePath::new(&root_path, item_id.clone()),
                            transition,
                        },
                    );
                    builder.scope_exits.insert(graph_path, exit);
                }
            }
        }

        let entry_target = &edges
            .fixed_by_source
            .get(&NodeId::start())
            .expect("validated graph has one START successor")
            .successor;
        let entry_index = (!entry_target.is_end()).then(|| item_entries[entry_target]);

        let (items, node_paths, scope_exits) = builder.finish();
        Ok(CompiledGraph {
            items,
            node_paths,
            scope_exits,
            entry_index,
            version: self.version.clone(),
        })
    }

    fn compile_transition(
        &self,
        item_id: &NodeId,
        edges: &EdgeAggregation<'_, S>,
        item_entries: &IndexMap<NodeId, NodeIndex>,
    ) -> Result<CompiledTransition<S>, GraphCompileError> {
        let target = |node_id: &NodeId| (!node_id.is_end()).then(|| item_entries[node_id]);
        let transition = if let Some(outgoing) = edges.fixed_by_source.get(item_id) {
            CompiledTransition::Fixed(target(&outgoing.successor))
        } else if let Some(edge) = edges.fan_out_by_source.get(item_id) {
            let mut targets = edge.targets.iter().filter_map(target).collect::<Vec<_>>();
            targets.sort_unstable_by_key(|index| index.index());
            CompiledTransition::StaticFanOut(targets)
        } else if let Some(edge) = edges.conditional_by_source.get(item_id) {
            CompiledTransition::Conditional {
                router: Arc::clone(&edge.router),
                allowed_targets: edge
                    .allowed_targets
                    .iter()
                    .map(|node_id| (node_id.clone(), target(node_id)))
                    .collect(),
            }
        } else if let Some(edge) = edges.conditional_fan_out_by_source.get(item_id) {
            CompiledTransition::ConditionalFanOut {
                router: Arc::clone(&edge.router),
                allowed_targets: edge
                    .allowed_targets
                    .iter()
                    .map(|node_id| (node_id.clone(), target(node_id)))
                    .collect(),
            }
        } else {
            return Err(GraphCompileError::MissingOutgoingEdge {
                node_id: item_id.clone(),
            });
        };
        Ok(transition)
    }

    fn aggregate_edges(&self) -> Result<EdgeAggregation<'_, S>, GraphCompileError> {
        let mut fixed_by_source = HashMap::with_capacity(self.fixed_edges.len());
        for edge in &self.fixed_edges {
            for endpoint in [&edge.from, &edge.to] {
                if !endpoint.is_reserved() && !self.items.contains_key(endpoint) {
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
            if !self.items.contains_key(&edge.source) {
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
                if !target.is_end() && !self.items.contains_key(target) {
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
            if !self.items.contains_key(&edge.source) {
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
                if !target.is_end() && !self.items.contains_key(target) {
                    return Err(GraphCompileError::UnknownFanOutTarget {
                        source_node: edge.source.clone(),
                        target: target.clone(),
                    });
                }
            }
            fan_out_by_source.insert(edge.source.clone(), edge);
        }

        let mut conditional_fan_out_by_source =
            HashMap::with_capacity(self.conditional_fan_out_edges.len());
        for edge in &self.conditional_fan_out_edges {
            if edge.source.is_start() {
                return Err(GraphCompileError::StartHasConditionalFanOut);
            }
            if edge.source.is_end() {
                return Err(GraphCompileError::EndHasConditionalFanOut);
            }
            if !self.items.contains_key(&edge.source) {
                return Err(GraphCompileError::UnknownConditionalFanOutSource {
                    source_node: edge.source.clone(),
                });
            }
            for target in &edge.allowed_targets {
                if target.is_start() {
                    return Err(GraphCompileError::StartHasIncoming {
                        from: edge.source.clone(),
                    });
                }
                if target.is_end() {
                    continue;
                }
                let Some(item) = self.items.get(target) else {
                    return Err(GraphCompileError::UnknownConditionalFanOutTarget {
                        source_node: edge.source.clone(),
                        target: target.clone(),
                    });
                };
                if item.is_subgraph() {
                    return Err(GraphCompileError::ConditionalFanOutTargetsSubgraph {
                        source_node: edge.source.clone(),
                        target: target.clone(),
                    });
                }
            }
            conditional_fan_out_by_source.insert(edge.source.clone(), edge);
        }

        Ok(EdgeAggregation {
            fixed_by_source,
            fan_out_by_source,
            conditional_by_source,
            conditional_fan_out_by_source,
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
        for item_id in self.items.keys() {
            let fixed_count = edges
                .fixed_by_source
                .get(item_id)
                .map(|outgoing| outgoing.count)
                .unwrap_or(0);
            if fixed_count > 1 {
                return Err(GraphCompileError::MultipleOutgoingEdges {
                    node_id: item_id.clone(),
                    count: fixed_count,
                });
            }
            let kind_count = usize::from(fixed_count == 1)
                + usize::from(edges.fan_out_by_source.contains_key(item_id))
                + usize::from(edges.conditional_by_source.contains_key(item_id))
                + usize::from(edges.conditional_fan_out_by_source.contains_key(item_id));
            if kind_count > 1 {
                return Err(GraphCompileError::MixedOutgoingEdgeKinds {
                    node_id: item_id.clone(),
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
        for item_id in self.items.keys() {
            if !reachable.contains(&indices[item_id]) {
                return Err(GraphCompileError::UnreachableNode {
                    node_id: item_id.clone(),
                });
            }
        }
        for item_id in self.items.keys() {
            if !edges.fixed_by_source.contains_key(item_id)
                && !edges.fan_out_by_source.contains_key(item_id)
                && !edges.conditional_by_source.contains_key(item_id)
                && !edges.conditional_fan_out_by_source.contains_key(item_id)
            {
                return Err(GraphCompileError::MissingOutgoingEdge {
                    node_id: item_id.clone(),
                });
            }
        }
        if !reachable.contains(&end_index) {
            return Err(GraphCompileError::NoReachableEnd);
        }
        Ok(())
    }

    fn validate_subgraph_frontiers(
        &self,
        edges: &EdgeAggregation<'_, S>,
    ) -> Result<(), GraphCompileError> {
        if !self.items.values().any(DeclaredItem::is_subgraph)
            || (edges.fan_out_by_source.is_empty()
                && edges.conditional_fan_out_by_source.is_empty())
        {
            return Ok(());
        }

        let canonical_pair = |left: NodeId, right: NodeId| {
            if left.as_str() <= right.as_str() {
                (left, right)
            } else {
                (right, left)
            }
        };
        let mut queued = HashSet::new();
        let mut work = VecDeque::new();
        let enqueue_frontier = |frontier: Vec<NodeId>,
                                queued: &mut HashSet<(NodeId, NodeId)>,
                                work: &mut VecDeque<(NodeId, NodeId)>|
         -> Result<(), GraphCompileError> {
            let mut seen = HashSet::with_capacity(frontier.len());
            let frontier = frontier
                .into_iter()
                .filter(|node_id| !node_id.is_end())
                .filter(|node_id| seen.insert(node_id.clone()))
                .collect::<Vec<_>>();
            for (offset, left) in frontier.iter().enumerate() {
                for right in frontier.iter().skip(offset + 1) {
                    for candidate in [left, right] {
                        if self
                            .items
                            .get(candidate)
                            .is_some_and(DeclaredItem::is_subgraph)
                        {
                            return Err(GraphCompileError::SubgraphInParallelFrontier {
                                node_id: candidate.clone(),
                            });
                        }
                    }
                    let pair = canonical_pair(left.clone(), right.clone());
                    if queued.insert(pair.clone()) {
                        work.push_back(pair);
                    }
                }
            }
            Ok(())
        };

        for fan_out in &self.fan_out_edges {
            enqueue_frontier(fan_out.targets.clone(), &mut queued, &mut work)?;
        }
        for fan_out in &self.conditional_fan_out_edges {
            enqueue_frontier(fan_out.allowed_targets.clone(), &mut queued, &mut work)?;
        }

        while let Some((left, right)) = work.pop_front() {
            let left_alternatives = self.transition_alternatives(&left, edges);
            let right_alternatives = self.transition_alternatives(&right, edges);
            for left_targets in &left_alternatives {
                for right_targets in &right_alternatives {
                    let mut frontier =
                        Vec::with_capacity(left_targets.len().saturating_add(right_targets.len()));
                    let mut seen = HashSet::new();
                    for target in left_targets.iter().chain(right_targets) {
                        if seen.insert(target.clone()) {
                            frontier.push(target.clone());
                        }
                    }
                    enqueue_frontier(frontier, &mut queued, &mut work)?;
                }
            }
        }
        Ok(())
    }

    fn transition_alternatives(
        &self,
        source: &NodeId,
        edges: &EdgeAggregation<'_, S>,
    ) -> Vec<Vec<NodeId>> {
        if source.is_end() {
            return vec![Vec::new()];
        }
        let executable_target = |target: &NodeId| {
            if target.is_end() {
                Vec::new()
            } else {
                vec![target.clone()]
            }
        };
        if let Some(fixed) = edges.fixed_by_source.get(source) {
            vec![executable_target(&fixed.successor)]
        } else if let Some(fan_out) = edges.fan_out_by_source.get(source) {
            vec![
                fan_out
                    .targets
                    .iter()
                    .filter(|target| !target.is_end())
                    .cloned()
                    .collect(),
            ]
        } else if let Some(fan_out) = edges.conditional_fan_out_by_source.get(source) {
            vec![
                fan_out
                    .allowed_targets
                    .iter()
                    .filter(|target| !target.is_end())
                    .cloned()
                    .collect(),
            ]
        } else {
            edges.conditional_by_source.get(source).map_or_else(
                || vec![Vec::new()],
                |conditional| {
                    conditional
                        .allowed_targets
                        .iter()
                        .map(executable_target)
                        .collect()
                },
            )
        }
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

pub(crate) struct CompiledNode<S>
where
    S: GraphState,
{
    pub(crate) path: NodePath,
    pub(crate) graph_path: GraphPath,
    pub(crate) kind: NodeKind<S>,
    pub(crate) transition: CompiledTransition<S>,
}

pub(crate) enum CompiledItem<S>
where
    S: GraphState,
{
    Node(CompiledNode<S>),
    EnterSubgraph {
        graph_path: GraphPath,
        transition: CompiledTransition<S>,
    },
    ExitSubgraph {
        graph_path: GraphPath,
        mount_path: NodePath,
        transition: CompiledTransition<S>,
    },
}

struct FlatBuilder<S>
where
    S: GraphState,
{
    items: Vec<Option<CompiledItem<S>>>,
    node_paths: IndexMap<NodePath, NodeIndex>,
    scope_exits: HashMap<GraphPath, NodeIndex>,
}

type FlatGraphParts<S> = (
    Vec<CompiledItem<S>>,
    IndexMap<NodePath, NodeIndex>,
    HashMap<GraphPath, NodeIndex>,
);

impl<S> FlatBuilder<S>
where
    S: GraphState,
{
    fn new() -> Self {
        Self {
            items: Vec::new(),
            node_paths: IndexMap::new(),
            scope_exits: HashMap::new(),
        }
    }

    fn allocate(&mut self) -> NodeIndex {
        let index = NodeIndex::new(self.items.len());
        self.items.push(None);
        index
    }

    fn set(&mut self, index: NodeIndex, item: CompiledItem<S>) {
        self.items[index.index()] = Some(item);
    }

    fn insert_node(
        &mut self,
        index: NodeIndex,
        node: CompiledNode<S>,
    ) -> Result<(), GraphCompileError> {
        if self.node_paths.contains_key(&node.path) {
            return Err(GraphCompileError::DuplicateNodePath {
                node_path: node.path,
            });
        }
        self.node_paths.insert(node.path.clone(), index);
        self.set(index, CompiledItem::Node(node));
        Ok(())
    }

    fn append_graph(
        &mut self,
        graph: &CompiledGraph<S>,
        prefix: &GraphPath,
        _outer_exit: NodeIndex,
    ) -> Result<Option<NodeIndex>, GraphCompileError> {
        let mapping = graph
            .items
            .iter()
            .map(|_| self.allocate())
            .collect::<Vec<_>>();
        for (old_index, item) in graph.items.iter().enumerate() {
            let item = match item {
                CompiledItem::Node(node) => {
                    let path = node.path.prefixed(prefix);
                    let graph_path = node.graph_path.prefixed(prefix);
                    let compiled = CompiledNode {
                        path,
                        graph_path,
                        kind: node.kind.clone(),
                        transition: node.transition.remap(&mapping),
                    };
                    self.insert_node(mapping[old_index], compiled)?;
                    continue;
                }
                CompiledItem::EnterSubgraph {
                    graph_path,
                    transition,
                } => CompiledItem::EnterSubgraph {
                    graph_path: graph_path.prefixed(prefix),
                    transition: transition.remap(&mapping),
                },
                CompiledItem::ExitSubgraph {
                    graph_path,
                    mount_path,
                    transition,
                } => CompiledItem::ExitSubgraph {
                    graph_path: graph_path.prefixed(prefix),
                    mount_path: mount_path.prefixed(prefix),
                    transition: transition.remap(&mapping),
                },
            };
            self.set(mapping[old_index], item);
        }
        for (path, exit) in &graph.scope_exits {
            self.scope_exits
                .insert(path.prefixed(prefix), mapping[exit.index()]);
        }
        Ok(graph.entry_index.map(|index| mapping[index.index()]))
    }

    fn finish(mut self) -> FlatGraphParts<S> {
        let items = self
            .items
            .drain(..)
            .map(|item| item.expect("compiled item placeholder was filled"))
            .collect();
        (items, self.node_paths, self.scope_exits)
    }
}

/// An immutable graph that can be invoked repeatedly or mounted as a subgraph.
pub struct CompiledGraph<S>
where
    S: GraphState,
{
    pub(crate) items: Vec<CompiledItem<S>>,
    pub(crate) node_paths: IndexMap<NodePath, NodeIndex>,
    pub(crate) scope_exits: HashMap<GraphPath, NodeIndex>,
    pub(crate) entry_index: Option<NodeIndex>,
    pub(crate) version: Option<GraphVersion>,
}

impl<S> CompiledGraph<S>
where
    S: GraphState,
{
    pub(crate) fn item_at(&self, index: NodeIndex) -> &CompiledItem<S> {
        &self.items[index.index()]
    }

    pub(crate) fn node_at(&self, index: NodeIndex) -> &CompiledNode<S> {
        match self.item_at(index) {
            CompiledItem::Node(node) => node,
            CompiledItem::EnterSubgraph { .. } | CompiledItem::ExitSubgraph { .. } => {
                panic!("structural subgraph index is not a real node")
            }
        }
    }

    /// Returns the explicit root compatibility version.
    #[must_use]
    pub const fn version(&self) -> Option<&GraphVersion> {
        self.version.as_ref()
    }
}
