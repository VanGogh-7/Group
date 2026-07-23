use async_trait::async_trait;
use group_agent_core::{
    END, GraphBuildError, GraphCompileError, GraphState, Node, NodeContext, NodeError, NodeId,
    START, StateError, StateGraph,
};

#[derive(Clone, Debug, Default)]
struct TestState;

impl GraphState for TestState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        Ok(())
    }
}

struct Noop;

#[async_trait]
impl Node<TestState> for Noop {
    async fn run(&self, _state: &TestState, _context: &NodeContext) -> Result<(), NodeError> {
        Ok(())
    }
}

fn graph_with_nodes(node_ids: &[&str]) -> StateGraph<TestState> {
    let mut graph = StateGraph::new();
    for node_id in node_ids {
        graph
            .add_node(*node_id, Noop)
            .expect("unique normal node should register");
    }
    graph
}

#[test]
fn duplicate_node_is_rejected() {
    let mut graph = graph_with_nodes(&["node"]);
    let result = graph.add_node("node", Noop);

    assert!(matches!(
        result,
        Err(GraphBuildError::DuplicateNode { node_id }) if node_id == NodeId::from("node")
    ));
}

#[test]
fn reserved_node_identifier_is_rejected() {
    let mut graph = StateGraph::<TestState>::new();
    let result = graph.add_node(START, Noop);

    assert!(matches!(
        result,
        Err(GraphBuildError::ReservedNodeId { node_id }) if node_id == NodeId::start()
    ));
}

#[test]
fn edge_with_unknown_node_is_rejected() {
    let mut graph = graph_with_nodes(&["known"]);
    graph.add_edge(START, "missing").add_edge("known", END);

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::UnknownNode {
            from,
            to,
            node_id,
        }) if from == NodeId::start()
            && to == NodeId::from("missing")
            && node_id == NodeId::from("missing")
    ));
}

#[test]
fn missing_start_edge_is_rejected() {
    let mut graph = graph_with_nodes(&["node"]);
    graph.add_edge("node", END);

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::MissingStartEdge)
    ));
}

#[test]
fn incoming_start_edge_is_rejected() {
    let mut graph = graph_with_nodes(&["node"]);
    graph.add_edge(START, "node").add_edge("node", START);

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::StartHasIncoming { from })
            if from == NodeId::from("node")
    ));
}

#[test]
fn outgoing_end_edge_is_rejected() {
    let mut graph = graph_with_nodes(&["node"]);
    graph
        .add_edge(START, "node")
        .add_edge("node", END)
        .add_edge(END, "node");

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::EndHasOutgoing { to })
            if to == NodeId::from("node")
    ));
}

#[test]
fn multiple_start_edges_are_rejected() {
    let mut graph = graph_with_nodes(&["left", "right"]);
    graph
        .add_edge(START, "left")
        .add_edge(START, "right")
        .add_edge("left", END)
        .add_edge("right", END);

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::MultipleStartEdges { count: 2 })
    ));
}

#[test]
fn multiple_fixed_successors_are_rejected() {
    let mut graph = graph_with_nodes(&["branch", "left"]);
    graph
        .add_edge(START, "branch")
        .add_edge("branch", "left")
        .add_edge("branch", END)
        .add_edge("left", END);

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::MultipleOutgoingEdges { node_id, count: 2 })
            if node_id == NodeId::from("branch")
    ));
}

#[test]
fn unreachable_node_is_rejected() {
    let mut graph = graph_with_nodes(&["reachable", "orphan"]);
    graph
        .add_edge(START, "reachable")
        .add_edge("reachable", END);

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::UnreachableNode { node_id })
            if node_id == NodeId::from("orphan")
    ));
}

#[test]
fn graph_without_reachable_end_is_rejected() {
    let mut graph = graph_with_nodes(&["loop"]);
    graph.add_edge(START, "loop").add_edge("loop", "loop");

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::NoReachableEnd)
    ));
}
