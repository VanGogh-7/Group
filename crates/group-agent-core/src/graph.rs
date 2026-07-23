use std::collections::HashSet;
use std::sync::Arc;

use indexmap::IndexMap;
use petgraph::Direction;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::Dfs;

use crate::edge::FixedEdge;
use crate::{GraphBuildError, GraphCompileError, GraphState, Node, NodeId};

/// A mutable builder for a state graph.
pub struct StateGraph<S>
where
    S: GraphState,
{
    nodes: IndexMap<NodeId, Arc<dyn Node<S>>>,
    edges: Vec<FixedEdge>,
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
            edges: Vec::new(),
        }
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
        self.edges.push(FixedEdge::new(from.into(), to.into()));
        self
    }

    /// Validates and freezes the graph for repeated execution.
    pub fn compile(&self) -> Result<CompiledGraph<S>, GraphCompileError> {
        self.validate_edge_endpoints()?;
        self.validate_edge_shapes()?;

        let mut topology = StableDiGraph::<NodeId, ()>::new();
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

        for edge in &self.edges {
            let from_index = indices[&edge.from];
            let to_index = indices[&edge.to];
            topology.add_edge(from_index, to_index, ());
        }

        Self::validate_reachability(&topology, &indices, start_index, end_index, &self.nodes)?;

        Ok(CompiledGraph {
            topology,
            indices,
            nodes: self.nodes.clone(),
            start_index,
        })
    }

    fn validate_edge_endpoints(&self) -> Result<(), GraphCompileError> {
        for edge in &self.edges {
            for endpoint in [&edge.from, &edge.to] {
                if !endpoint.is_reserved() && !self.nodes.contains_key(endpoint) {
                    return Err(GraphCompileError::UnknownNode {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        node_id: endpoint.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_edge_shapes(&self) -> Result<(), GraphCompileError> {
        let mut outgoing_counts: IndexMap<NodeId, usize> = IndexMap::new();

        for edge in &self.edges {
            if edge.to == NodeId::start() {
                return Err(GraphCompileError::StartHasIncoming {
                    from: edge.from.clone(),
                });
            }
            if edge.from == NodeId::end() {
                return Err(GraphCompileError::EndHasOutgoing {
                    to: edge.to.clone(),
                });
            }
            *outgoing_counts.entry(edge.from.clone()).or_default() += 1;
        }

        let start_count = outgoing_counts.get(&NodeId::start()).copied().unwrap_or(0);
        match start_count {
            0 => return Err(GraphCompileError::MissingStartEdge),
            1 => {}
            count => return Err(GraphCompileError::MultipleStartEdges { count }),
        }

        for node_id in self.nodes.keys() {
            let count = outgoing_counts.get(node_id).copied().unwrap_or(0);
            if count > 1 {
                return Err(GraphCompileError::MultipleOutgoingEdges {
                    node_id: node_id.clone(),
                    count,
                });
            }
        }

        Ok(())
    }

    fn validate_reachability(
        topology: &StableDiGraph<NodeId, ()>,
        indices: &IndexMap<NodeId, NodeIndex>,
        start_index: NodeIndex,
        end_index: NodeIndex,
        nodes: &IndexMap<NodeId, Arc<dyn Node<S>>>,
    ) -> Result<(), GraphCompileError> {
        let mut depth_first = Dfs::new(topology, start_index);
        let mut reachable = HashSet::new();
        while let Some(index) = depth_first.next(topology) {
            reachable.insert(index);
        }

        for node_id in nodes.keys() {
            if !reachable.contains(&indices[node_id]) {
                return Err(GraphCompileError::UnreachableNode {
                    node_id: node_id.clone(),
                });
            }
        }

        if !reachable.contains(&end_index) {
            return Err(GraphCompileError::NoReachableEnd);
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

/// An immutable graph that can be invoked repeatedly.
pub struct CompiledGraph<S>
where
    S: GraphState,
{
    pub(crate) topology: StableDiGraph<NodeId, ()>,
    pub(crate) indices: IndexMap<NodeId, NodeIndex>,
    pub(crate) nodes: IndexMap<NodeId, Arc<dyn Node<S>>>,
    pub(crate) start_index: NodeIndex,
}

impl<S> CompiledGraph<S>
where
    S: GraphState,
{
    pub(crate) fn first_node_id(&self) -> NodeId {
        self.topology
            .neighbors_directed(self.start_index, Direction::Outgoing)
            .next()
            .and_then(|index| self.topology.node_weight(index))
            .cloned()
            .expect("compiled graph always has one START successor")
    }

    pub(crate) fn successor_id(&self, node_id: &NodeId) -> NodeId {
        let index = self.indices[node_id];
        self.topology
            .neighbors_directed(index, Direction::Outgoing)
            .next()
            .and_then(|successor| self.topology.node_weight(successor))
            .cloned()
            .expect("every executable node in a compiled graph has one successor")
    }

    pub(crate) fn node(&self, node_id: &NodeId) -> &Arc<dyn Node<S>> {
        self.nodes
            .get(node_id)
            .expect("compiled graph contains every executable node")
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::{GraphRunError, NodeContext, NodeError, RunConfig, StateError};

    #[derive(Clone)]
    struct LoopState;

    impl GraphState for LoopState {
        type Update = ();

        fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
            Ok(())
        }
    }

    struct LoopNode;

    #[async_trait]
    impl Node<LoopState> for LoopNode {
        async fn run(&self, _state: &LoopState, _context: &NodeContext) -> Result<(), NodeError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn runtime_step_limit_stops_an_infinite_cycle() {
        let start_id = NodeId::start();
        let loop_id = NodeId::from("loop");
        let end_id = NodeId::end();
        let mut topology = StableDiGraph::new();
        let start_index = topology.add_node(start_id.clone());
        let loop_index = topology.add_node(loop_id.clone());
        let end_index = topology.add_node(end_id.clone());
        topology.add_edge(start_index, loop_index, ());
        topology.add_edge(loop_index, loop_index, ());

        let indices = IndexMap::from([
            (start_id, start_index),
            (loop_id.clone(), loop_index),
            (end_id, end_index),
        ]);
        let loop_node: Arc<dyn Node<LoopState>> = Arc::new(LoopNode);
        let nodes = IndexMap::from([(loop_id.clone(), loop_node)]);
        let compiled = CompiledGraph {
            topology,
            indices,
            nodes,
            start_index,
        };

        let result = compiled
            .invoke_with_config(LoopState, RunConfig::new(3))
            .await;

        assert!(matches!(
            result,
            Err(GraphRunError::MaxStepsExceeded {
                max_steps: 3,
                node_id,
                step: 4,
            }) if node_id == loop_id
        ));
    }
}
