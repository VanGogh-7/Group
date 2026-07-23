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
    let start_result = graph.add_node(START, Noop);

    assert!(matches!(
        start_result,
        Err(GraphBuildError::ReservedNodeId { node_id }) if node_id == NodeId::start()
    ));

    let end_result = graph.add_node(END, Noop);
    assert!(matches!(
        end_result,
        Err(GraphBuildError::ReservedNodeId { node_id }) if node_id == NodeId::end()
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

#[test]
fn empty_conditional_target_list_is_rejected() {
    let mut graph = graph_with_nodes(&["router"]);
    let result = graph.add_conditional_edges("router", Vec::<NodeId>::new(), |_| Ok(NodeId::end()));

    assert!(matches!(
        result,
        Err(GraphBuildError::EmptyConditionalTargets { source_node })
            if source_node == NodeId::from("router")
    ));
}

#[test]
fn duplicate_conditional_target_is_rejected() {
    let mut graph = graph_with_nodes(&["router", "answer"]);
    let result = graph.add_conditional_edges("router", ["answer", "answer"], |_| {
        Ok(NodeId::from("answer"))
    });

    assert!(matches!(
        result,
        Err(GraphBuildError::DuplicateConditionalTarget {
            source_node,
            target,
        }) if source_node == NodeId::from("router") && target == NodeId::from("answer")
    ));
}

#[test]
fn unknown_conditional_target_is_rejected_at_compile_time() {
    let mut graph = graph_with_nodes(&["router"]);
    graph.add_edge(START, "router");
    graph
        .add_conditional_edges("router", ["missing", END], |_| Ok(NodeId::end()))
        .expect("conditional edge declaration should be accepted");

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::UnknownConditionalTarget {
            source_node,
            target,
        }) if source_node == NodeId::from("router") && target == NodeId::from("missing")
    ));
}

#[test]
fn fixed_and_conditional_edges_on_same_node_are_rejected() {
    let mut graph = graph_with_nodes(&["router", "answer"]);
    graph
        .add_edge(START, "router")
        .add_edge("router", "answer")
        .add_edge("answer", END);
    graph
        .add_conditional_edges("router", ["answer"], |_| Ok(NodeId::from("answer")))
        .expect("conditional edge declaration should be accepted");

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::MixedOutgoingEdgeKinds { node_id })
            if node_id == NodeId::from("router")
    ));
}

#[test]
fn multiple_conditional_routers_on_same_node_are_rejected() {
    let mut graph = graph_with_nodes(&["router", "answer"]);
    graph
        .add_conditional_edges("router", ["answer"], |_| Ok(NodeId::from("answer")))
        .expect("first router should register");
    let result = graph.add_conditional_edges("router", [END], |_| Ok(NodeId::from("answer")));

    assert!(matches!(
        result,
        Err(GraphBuildError::MultipleConditionalRouters { source_node })
            if source_node == NodeId::from("router")
    ));
}

#[test]
fn start_cannot_use_conditional_routing() {
    let mut graph = graph_with_nodes(&["node"]);
    graph
        .add_conditional_edges(START, ["node"], |_| Ok(NodeId::from("node")))
        .expect("declaration should be validated during compile");

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::StartHasConditionalEdge)
    ));
}

#[test]
fn end_cannot_use_conditional_routing() {
    let mut graph = graph_with_nodes(&["node"]);
    graph.add_edge(START, "node").add_edge("node", END);
    graph
        .add_conditional_edges(END, ["node"], |_| Ok(NodeId::from("node")))
        .expect("declaration should be validated during compile");

    assert!(matches!(
        graph.compile(),
        Err(GraphCompileError::EndHasConditionalEdge)
    ));
}

#[test]
fn unknown_conditional_source_is_rejected() {
    let mut graph = graph_with_nodes(&["node"]);
    graph.add_edge(START, "node").add_edge("node", END);
    graph
        .add_conditional_edges("missing", [END], |_| Ok(NodeId::end()))
        .expect("declaration should be validated during compile");

    assert_eq!(
        graph.compile().err().expect("source should be rejected"),
        GraphCompileError::UnknownConditionalSource {
            source_node: NodeId::from("missing"),
        }
    );
}

#[test]
fn conditional_target_cannot_point_to_start() {
    let mut graph = graph_with_nodes(&["router"]);
    graph.add_edge(START, "router");
    graph
        .add_conditional_edges("router", [START, END], |_| Ok(NodeId::end()))
        .expect("declaration should be validated during compile");

    assert_eq!(
        graph
            .compile()
            .err()
            .expect("START target should be rejected"),
        GraphCompileError::StartHasIncoming {
            from: NodeId::from("router"),
        }
    );
}

#[test]
fn reachable_node_without_outgoing_edge_is_rejected() {
    let mut graph = graph_with_nodes(&["unfinished"]);
    graph.add_edge(START, "unfinished");

    assert_eq!(
        graph
            .compile()
            .err()
            .expect("missing transition should fail"),
        GraphCompileError::MissingOutgoingEdge {
            node_id: NodeId::from("unfinished"),
        }
    );
}

fn assert_clone_eq<T: Clone + Eq + PartialEq>() {}

#[test]
fn build_and_compile_errors_restore_clone_and_equality_traits() {
    assert_clone_eq::<GraphBuildError>();
    assert_clone_eq::<GraphCompileError>();

    let error = GraphBuildError::DuplicateNode {
        node_id: NodeId::from("duplicate"),
    };
    assert_eq!(error.clone(), error);
}
