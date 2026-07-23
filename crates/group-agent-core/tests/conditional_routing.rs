use std::error::Error as _;
use std::fmt;

use async_trait::async_trait;
use group_agent_core::{
    CompiledGraph, END, GraphEvent, GraphRunError, GraphState, Node, NodeContext, NodeError,
    NodeId, RouteError, RunConfig, START, StateError, StateGraph,
};

#[derive(Debug, Default, Eq, PartialEq)]
struct RouteState {
    value: usize,
    limit: usize,
}

#[derive(Clone, Copy, Debug)]
struct Add(usize);

impl GraphState for RouteState {
    type Update = Add;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update.0;
        Ok(())
    }
}

struct AddNode(usize);

#[async_trait]
impl Node<RouteState> for AddNode {
    async fn run(&self, _state: &RouteState, _context: &NodeContext) -> Result<Add, NodeError> {
        Ok(Add(self.0))
    }
}

fn branching_graph() -> CompiledGraph<RouteState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("router", AddNode(1))
        .expect("router should register");
    graph
        .add_node("answer", AddNode(0))
        .expect("answer should register");
    graph
        .add_node("revise", AddNode(1))
        .expect("revise should register");
    graph.add_edge(START, "router");
    graph
        .add_conditional_edges("router", ["answer", "revise"], |state| {
            if state.value >= state.limit {
                Ok(NodeId::from("answer"))
            } else {
                Ok(NodeId::from("revise"))
            }
        })
        .expect("conditional edge should register");
    graph.add_edge("answer", END).add_edge("revise", "router");
    graph.compile().expect("branching graph should compile")
}

#[tokio::test]
async fn router_selects_first_branch_after_observing_applied_update() {
    let report = branching_graph()
        .invoke(RouteState { value: 0, limit: 1 })
        .await
        .expect("run should succeed");

    assert_eq!(report.final_state().value, 1);
    assert_eq!(
        report.visited_nodes(),
        [NodeId::from("router"), NodeId::from("answer")]
    );
    let run_id = report.run_id();
    assert_eq!(
        report.events(),
        [
            GraphEvent::RunStarted {
                run_id,
                max_steps: 1_000,
            },
            GraphEvent::NodeStarted {
                run_id,
                node_id: NodeId::from("router"),
                step: 1,
            },
            GraphEvent::NodeCompleted {
                run_id,
                node_id: NodeId::from("router"),
                step: 1,
            },
            GraphEvent::StateUpdated {
                run_id,
                node_id: NodeId::from("router"),
                step: 1,
            },
            GraphEvent::RouteSelected {
                run_id,
                source: NodeId::from("router"),
                target: NodeId::from("answer"),
                step: 1,
            },
            GraphEvent::NodeStarted {
                run_id,
                node_id: NodeId::from("answer"),
                step: 2,
            },
            GraphEvent::NodeCompleted {
                run_id,
                node_id: NodeId::from("answer"),
                step: 2,
            },
            GraphEvent::StateUpdated {
                run_id,
                node_id: NodeId::from("answer"),
                step: 2,
            },
            GraphEvent::RunCompleted { run_id, steps: 2 },
        ]
    );
}

#[tokio::test]
async fn router_selects_second_branch_and_loop_eventually_exits() {
    let report = branching_graph()
        .invoke(RouteState { value: 0, limit: 3 })
        .await
        .expect("run should succeed");

    assert_eq!(report.final_state().value, 3);
    assert_eq!(
        report.visited_nodes(),
        [
            NodeId::from("router"),
            NodeId::from("revise"),
            NodeId::from("router"),
            NodeId::from("answer"),
        ]
    );
    assert_eq!(report.steps(), 4);

    let selected_routes = report
        .events()
        .iter()
        .filter_map(|event| match event {
            GraphEvent::RouteSelected {
                source,
                target,
                step,
                ..
            } => Some((source.clone(), target.clone(), *step)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        selected_routes,
        [
            (NodeId::from("router"), NodeId::from("revise"), 1),
            (NodeId::from("router"), NodeId::from("answer"), 3),
        ]
    );
}

#[tokio::test]
async fn undeclared_router_target_is_a_structured_run_error() {
    let mut graph = StateGraph::new();
    graph
        .add_node("router", AddNode(0))
        .expect("router should register");
    graph
        .add_node("answer", AddNode(0))
        .expect("answer should register");
    graph.add_edge(START, "router").add_edge("answer", END);
    graph
        .add_conditional_edges("router", ["answer"], |_| Ok(NodeId::from("undeclared")))
        .expect("conditional edge should register");
    let compiled = graph.compile().expect("graph should compile");

    let result = compiled.invoke(RouteState::default()).await;

    assert!(matches!(
        result,
        Err(GraphRunError::InvalidRouteTarget {
            node_id,
            target,
            step: 1,
        }) if node_id == NodeId::from("router") && target == NodeId::from("undeclared")
    ));
}

#[tokio::test]
async fn non_terminating_conditional_loop_is_stopped_by_max_steps() {
    let mut graph = StateGraph::new();
    graph
        .add_node("loop", AddNode(1))
        .expect("loop should register");
    graph.add_edge(START, "loop");
    graph
        .add_conditional_edges("loop", ["loop", END], |_| Ok(NodeId::from("loop")))
        .expect("conditional edge should register");
    let compiled = graph.compile().expect("graph should compile");

    let result = compiled
        .invoke_with_config(RouteState::default(), RunConfig::new(3))
        .await;

    assert!(matches!(
        result,
        Err(GraphRunError::MaxStepsExceeded {
            max_steps: 3,
            node_id,
            step: 4,
        }) if node_id == NodeId::from("loop")
    ));
}

#[tokio::test]
async fn identical_inputs_produce_identical_routes_and_visit_order() {
    let compiled = branching_graph();
    let first = compiled
        .invoke(RouteState { value: 0, limit: 5 })
        .await
        .expect("first run should succeed");
    let second = compiled
        .invoke(RouteState { value: 0, limit: 5 })
        .await
        .expect("second run should succeed");

    assert_eq!(first.final_state(), second.final_state());
    assert_eq!(first.visited_nodes(), second.visited_nodes());
    assert_eq!(
        first
            .events()
            .iter()
            .map(std::mem::discriminant)
            .collect::<Vec<_>>(),
        second
            .events()
            .iter()
            .map(std::mem::discriminant)
            .collect::<Vec<_>>()
    );
    assert_ne!(first.run_id(), second.run_id());
    assert_eq!(first.steps(), second.steps());
}

#[derive(Debug)]
struct RouterCause;

impl fmt::Display for RouterCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("router cause")
    }
}

impl std::error::Error for RouterCause {}

#[tokio::test]
async fn conditional_router_error_preserves_its_source_chain() {
    let mut graph = StateGraph::new();
    graph
        .add_node("router", AddNode(0))
        .expect("router should register");
    graph.add_edge(START, "router");
    graph
        .add_conditional_edges("router", [END], |_| {
            Err(RouteError::with_source("route failed", RouterCause))
        })
        .expect("conditional edge should register");
    let compiled = graph.compile().expect("graph should compile");

    let result = compiled.invoke(RouteState::default()).await;
    let error = result.expect_err("router should fail");

    match &error {
        GraphRunError::RouteFailed {
            node_id,
            step,
            source,
        } => {
            assert_eq!(node_id, &NodeId::from("router"));
            assert_eq!(*step, 1);
            assert_eq!(source.as_message(), "route failed");
            assert_eq!(
                source
                    .source()
                    .expect("route source should exist")
                    .to_string(),
                "router cause"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
    assert_eq!(
        error
            .source()
            .expect("run error should expose route error")
            .source()
            .expect("route error should expose root cause")
            .to_string(),
        "router cause"
    );
}
