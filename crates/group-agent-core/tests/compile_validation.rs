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
fn empty_and_duplicate_conditional_fan_out_whitelists_are_rejected() {
    let mut graph = graph_with_nodes(&["router", "answer"]);
    assert_eq!(
        graph
            .add_conditional_fan_out("router", Vec::<NodeId>::new(), |_| Ok(Vec::new()))
            .err()
            .expect("empty whitelist should fail"),
        GraphBuildError::EmptyConditionalFanOutTargets {
            source_node: NodeId::from("router"),
        }
    );
    assert_eq!(
        graph
            .add_conditional_fan_out("router", ["answer", "answer"], |_| {
                Ok(vec![NodeId::from("answer")])
            })
            .err()
            .expect("duplicate whitelist should fail"),
        GraphBuildError::DuplicateConditionalFanOutTarget {
            source_node: NodeId::from("router"),
            target: NodeId::from("answer"),
        }
    );
}

#[test]
fn conditional_fan_out_endpoints_and_transition_conflicts_are_structured() {
    let mut unknown_source = graph_with_nodes(&["node"]);
    unknown_source.add_edge(START, "node").add_edge("node", END);
    unknown_source
        .add_conditional_fan_out("missing", [END], |_| Ok(vec![NodeId::end()]))
        .expect("declaration should register");
    assert_eq!(
        unknown_source.compile().err().expect("source should fail"),
        GraphCompileError::UnknownConditionalFanOutSource {
            source_node: NodeId::from("missing"),
        }
    );

    let mut unknown_target = graph_with_nodes(&["router"]);
    unknown_target.add_edge(START, "router");
    unknown_target
        .add_conditional_fan_out("router", ["first-missing", "second-missing"], |_| {
            Ok(vec![NodeId::from("first-missing")])
        })
        .expect("declaration should register");
    assert_eq!(
        unknown_target.compile().err().expect("target should fail"),
        GraphCompileError::UnknownConditionalFanOutTarget {
            source_node: NodeId::from("router"),
            target: NodeId::from("first-missing"),
        }
    );

    let mut mixed = graph_with_nodes(&["router", "answer"]);
    mixed
        .add_edge(START, "router")
        .add_edge("router", "answer")
        .add_edge("answer", END);
    mixed
        .add_conditional_fan_out("router", ["answer"], |_| Ok(vec![NodeId::from("answer")]))
        .expect("fan-out should register");
    assert_eq!(
        mixed.compile().err().expect("mixed transition should fail"),
        GraphCompileError::MixedOutgoingEdgeKinds {
            node_id: NodeId::from("router"),
        }
    );

    let mut conditional_mixed = graph_with_nodes(&["router", "answer"]);
    conditional_mixed
        .add_edge(START, "router")
        .add_edge("answer", END);
    conditional_mixed
        .add_conditional_edges("router", ["answer"], |_| Ok(NodeId::from("answer")))
        .expect("single router should register");
    conditional_mixed
        .add_conditional_fan_out("router", ["answer"], |_| Ok(vec![NodeId::from("answer")]))
        .expect("fan-out router should register");
    assert_eq!(
        conditional_mixed
            .compile()
            .err()
            .expect("mixed conditional transition should fail"),
        GraphCompileError::MixedOutgoingEdgeKinds {
            node_id: NodeId::from("router"),
        }
    );

    let mut static_mixed = graph_with_nodes(&["router", "answer"]);
    static_mixed
        .add_edge(START, "router")
        .add_edge("answer", END);
    static_mixed
        .add_fan_out("router", ["answer"])
        .expect("static fan-out should register");
    static_mixed
        .add_conditional_fan_out("router", ["answer"], |_| Ok(vec![NodeId::from("answer")]))
        .expect("conditional fan-out should register");
    assert_eq!(
        static_mixed
            .compile()
            .err()
            .expect("mixed fan-out transition should fail"),
        GraphCompileError::MixedOutgoingEdgeKinds {
            node_id: NodeId::from("router"),
        }
    );
}

#[test]
fn multiple_conditional_fan_out_routers_are_rejected_by_the_builder() {
    let mut graph = graph_with_nodes(&["router", "answer"]);
    graph
        .add_conditional_fan_out("router", ["answer"], |_| Ok(vec![NodeId::from("answer")]))
        .expect("first router should register");
    assert_eq!(
        graph
            .add_conditional_fan_out("router", [END], |_| Ok(vec![NodeId::end()]))
            .err()
            .expect("second router should fail"),
        GraphBuildError::MultipleConditionalFanOutRouters {
            source_node: NodeId::from("router"),
        }
    );
}

#[test]
fn conditional_fan_out_cannot_target_a_subgraph_mount() {
    let mut child = graph_with_nodes(&["child-node"]);
    child
        .add_edge(START, "child-node")
        .add_edge("child-node", END);
    let child = child.compile().expect("child should compile");

    let mut parent = graph_with_nodes(&["router"]);
    parent
        .add_subgraph("child", child)
        .expect("child should mount");
    parent.add_edge(START, "router").add_edge("child", END);
    parent
        .add_conditional_fan_out("router", ["child", END], |_| {
            Ok(vec![NodeId::from("child")])
        })
        .expect("declaration should register");

    assert_eq!(
        parent.compile().err().expect("mount target should fail"),
        GraphCompileError::ConditionalFanOutTargetsSubgraph {
            source_node: NodeId::from("router"),
            target: NodeId::from("child"),
        }
    );
}

#[test]
fn conditional_fan_out_reserved_sources_and_start_target_are_rejected() {
    let mut start = graph_with_nodes(&["node"]);
    start
        .add_conditional_fan_out(START, ["node"], |_| Ok(vec![NodeId::from("node")]))
        .expect("declaration should register");
    assert_eq!(
        start.compile().err().expect("START source should fail"),
        GraphCompileError::StartHasConditionalFanOut
    );

    let mut end = graph_with_nodes(&["node"]);
    end.add_edge(START, "node").add_edge("node", END);
    end.add_conditional_fan_out(END, ["node"], |_| Ok(vec![NodeId::from("node")]))
        .expect("declaration should register");
    assert_eq!(
        end.compile().err().expect("END source should fail"),
        GraphCompileError::EndHasConditionalFanOut
    );

    let mut target = graph_with_nodes(&["node"]);
    target.add_edge(START, "node");
    target
        .add_conditional_fan_out("node", [START, END], |_| Ok(vec![NodeId::end()]))
        .expect("declaration should register");
    assert_eq!(
        target.compile().err().expect("START target should fail"),
        GraphCompileError::StartHasIncoming {
            from: NodeId::from("node"),
        }
    );
}

#[test]
fn conditional_fan_out_compile_validation_never_panics_for_invalid_inputs() {
    let mut graphs = Vec::new();

    let mut missing_source = graph_with_nodes(&["node"]);
    missing_source.add_edge(START, "node").add_edge("node", END);
    missing_source
        .add_conditional_fan_out("missing", [END], |_| Ok(vec![NodeId::end()]))
        .expect("declaration");
    graphs.push(missing_source);

    let mut missing_target = graph_with_nodes(&["router"]);
    missing_target.add_edge(START, "router");
    missing_target
        .add_conditional_fan_out("router", ["missing"], |_| Ok(vec![NodeId::from("missing")]))
        .expect("declaration");
    graphs.push(missing_target);

    let mut mixed = graph_with_nodes(&["router", "answer"]);
    mixed
        .add_edge(START, "router")
        .add_edge("router", "answer")
        .add_edge("answer", END);
    mixed
        .add_conditional_fan_out("router", ["answer"], |_| Ok(vec![NodeId::from("answer")]))
        .expect("declaration");
    graphs.push(mixed);

    for graph in graphs {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| graph.compile()));
        assert!(result.is_ok(), "compile must not panic");
        assert!(
            result.expect("compile should return").is_err(),
            "invalid graph must return a structured error"
        );
    }
}

#[test]
fn empty_and_duplicate_fan_out_targets_are_rejected() {
    let mut graph = graph_with_nodes(&["source", "target"]);
    assert_eq!(
        graph
            .add_fan_out("source", Vec::<NodeId>::new())
            .err()
            .expect("empty fan-out should fail"),
        GraphBuildError::EmptyFanOutTargets {
            source_node: NodeId::from("source"),
        }
    );
    assert_eq!(
        graph
            .add_fan_out("source", ["target", "target"])
            .err()
            .expect("duplicate target should fail"),
        GraphBuildError::DuplicateFanOutTarget {
            source_node: NodeId::from("source"),
            target: NodeId::from("target"),
        }
    );
}

#[test]
fn fan_out_endpoints_and_transition_kind_are_validated() {
    let mut unknown_source = graph_with_nodes(&["node"]);
    unknown_source.add_edge(START, "node").add_edge("node", END);
    unknown_source
        .add_fan_out("missing", [END])
        .expect("declaration should be accepted");
    assert_eq!(
        unknown_source
            .compile()
            .err()
            .expect("unknown source should fail"),
        GraphCompileError::UnknownFanOutSource {
            source_node: NodeId::from("missing"),
        }
    );

    let mut unknown_target = graph_with_nodes(&["source"]);
    unknown_target.add_edge(START, "source");
    unknown_target
        .add_fan_out("source", ["missing", END])
        .expect("declaration should be accepted");
    assert_eq!(
        unknown_target
            .compile()
            .err()
            .expect("unknown target should fail"),
        GraphCompileError::UnknownFanOutTarget {
            source_node: NodeId::from("source"),
            target: NodeId::from("missing"),
        }
    );

    let mut mixed = graph_with_nodes(&["source", "target"]);
    mixed
        .add_edge(START, "source")
        .add_edge("source", "target")
        .add_edge("target", END);
    mixed
        .add_fan_out("source", ["target"])
        .expect("fan-out should register");
    assert_eq!(
        mixed.compile().err().expect("mixed transition should fail"),
        GraphCompileError::MixedOutgoingEdgeKinds {
            node_id: NodeId::from("source"),
        }
    );
}

#[test]
fn start_end_and_start_target_cannot_participate_in_fan_out() {
    let mut start = graph_with_nodes(&["node"]);
    start
        .add_fan_out(START, ["node"])
        .expect("declaration should be accepted");
    assert_eq!(
        start.compile().err().expect("START fan-out should fail"),
        GraphCompileError::StartHasFanOut
    );

    let mut end = graph_with_nodes(&["node"]);
    end.add_edge(START, "node").add_edge("node", END);
    end.add_fan_out(END, ["node"])
        .expect("declaration should be accepted");
    assert_eq!(
        end.compile().err().expect("END fan-out should fail"),
        GraphCompileError::EndHasFanOut
    );

    let mut incoming_start = graph_with_nodes(&["node"]);
    incoming_start.add_edge(START, "node");
    incoming_start
        .add_fan_out("node", [START, END])
        .expect("declaration should be accepted");
    assert_eq!(
        incoming_start
            .compile()
            .err()
            .expect("START target should fail"),
        GraphCompileError::StartHasIncoming {
            from: NodeId::from("node"),
        }
    );
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
