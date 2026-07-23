use std::error::Error as _;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    CompiledGraph, END, GraphEvent, GraphRunError, GraphState, Node, NodeContext, NodeError,
    NodeId, RunConfig, RunReport, START, StateError, StateGraph,
};

#[derive(Debug, Default, Eq, PartialEq)]
struct RunState {
    value: usize,
    fail_node: bool,
    fail_apply: bool,
}

#[derive(Clone, Copy)]
struct Increment;

impl GraphState for RunState {
    type Update = Increment;

    fn apply(&mut self, Increment: Self::Update) -> Result<(), StateError> {
        if self.fail_apply {
            return Err(StateError::with_source(
                "apply failed",
                TestCause("state cause"),
            ));
        }
        self.value += 1;
        Ok(())
    }
}

struct ControlledNode;

#[async_trait]
impl Node<RunState> for ControlledNode {
    async fn run(&self, state: &RunState, _context: &NodeContext) -> Result<Increment, NodeError> {
        if state.fail_node {
            return Err(NodeError::with_source(
                "node failed",
                TestCause("node cause"),
            ));
        }
        Ok(Increment)
    }
}

fn single_node_graph() -> CompiledGraph<RunState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("node", ControlledNode)
        .expect("node should register");
    graph.add_edge(START, "node").add_edge("node", END);
    graph.compile().expect("graph should compile")
}

#[derive(Debug)]
struct TestCause(&'static str);

impl fmt::Display for TestCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for TestCause {}

fn assert_send_sync<T: Send + Sync>() {}
fn assert_clone<T: Clone>() {}

#[test]
fn compiled_graph_is_send_and_sync() {
    assert_send_sync::<CompiledGraph<RunState>>();
}

#[test]
fn run_report_is_clone_when_state_is_clone() {
    #[derive(Clone)]
    struct CloneState;

    impl GraphState for CloneState {
        type Update = ();

        fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
            Ok(())
        }
    }

    assert_clone::<RunReport<CloneState>>();
}

#[test]
fn node_and_state_errors_expose_original_sources() {
    let node_error = NodeError::with_source("node wrapper", TestCause("node root"));
    assert_eq!(
        node_error
            .source()
            .expect("node source should exist")
            .to_string(),
        "node root"
    );

    let state_error = StateError::with_source("state wrapper", TestCause("state root"));
    assert_eq!(
        state_error
            .source()
            .expect("state source should exist")
            .to_string(),
        "state root"
    );

    let boxed_source: Box<dyn std::error::Error + Send + Sync + 'static> =
        Box::new(TestCause("boxed root"));
    let boxed_error = NodeError::with_source("boxed wrapper", boxed_source);
    assert_eq!(
        boxed_error
            .source()
            .expect("boxed source should exist")
            .to_string(),
        "boxed root"
    );
}

#[tokio::test]
async fn shared_compiled_graph_supports_concurrent_isolated_invocations() {
    let compiled = Arc::new(single_node_graph());

    let (first, second) = tokio::join!(
        compiled.invoke(RunState {
            value: 0,
            fail_node: false,
            fail_apply: false,
        }),
        compiled.invoke(RunState {
            value: 40,
            fail_node: false,
            fail_apply: false,
        })
    );
    let first = first.expect("first run should succeed");
    let second = second.expect("second run should succeed");

    assert_eq!(first.final_state().value, 1);
    assert_eq!(second.final_state().value, 41);
    assert_eq!(first.steps(), 1);
    assert_eq!(second.steps(), 1);
    assert_eq!(first.visited_nodes(), [NodeId::from("node")]);
    assert_eq!(second.visited_nodes(), [NodeId::from("node")]);
    assert_ne!(first.run_id(), second.run_id());
    assert_eq!(first.events().len(), second.events().len());
}

#[tokio::test]
async fn state_apply_failure_preserves_node_step_and_source_chain() {
    let compiled = single_node_graph();
    let result = compiled
        .invoke(RunState {
            value: 0,
            fail_node: false,
            fail_apply: true,
        })
        .await;
    let error = result.expect_err("state apply should fail");

    match &error {
        GraphRunError::StateUpdateFailed {
            node_id,
            step,
            source,
        } => {
            assert_eq!(node_id, &NodeId::from("node"));
            assert_eq!(*step, 1);
            assert_eq!(source.as_message(), "apply failed");
            assert_eq!(
                source
                    .source()
                    .expect("state source should exist")
                    .to_string(),
                "state cause"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
    assert_eq!(
        error
            .source()
            .expect("run error should expose state error")
            .source()
            .expect("state error should expose root cause")
            .to_string(),
        "state cause"
    );
}

#[tokio::test]
async fn failed_invocation_does_not_poison_compiled_graph() {
    let compiled = single_node_graph();
    let failed = compiled
        .invoke(RunState {
            value: 0,
            fail_node: true,
            fail_apply: false,
        })
        .await;
    assert!(matches!(
        failed,
        Err(GraphRunError::NodeFailed {
            node_id,
            step: 1,
            ..
        }) if node_id == NodeId::from("node")
    ));

    let recovered = compiled
        .invoke(RunState {
            value: 10,
            fail_node: false,
            fail_apply: false,
        })
        .await
        .expect("later run should succeed");
    assert_eq!(recovered.final_state().value, 11);
    assert_eq!(recovered.steps(), 1);
}

#[tokio::test]
async fn max_steps_zero_prevents_first_node_execution() {
    let compiled = single_node_graph();
    let result = compiled
        .invoke_with_config(RunState::default(), RunConfig::new(0))
        .await;

    assert!(matches!(
        result,
        Err(GraphRunError::MaxStepsExceeded {
            max_steps: 0,
            node_id,
            step: 1,
        }) if node_id == NodeId::from("node")
    ));
}

#[tokio::test]
async fn max_steps_one_allows_single_node_to_reach_end() {
    let report = single_node_graph()
        .invoke_with_config(RunState::default(), RunConfig::new(1))
        .await
        .expect("single node should fit within one step");

    assert_eq!(report.steps(), 1);
    assert_eq!(report.final_state().value, 1);
    assert_eq!(report.visited_nodes(), [NodeId::from("node")]);
}

#[tokio::test]
async fn successful_event_order_and_steps_are_consistent() {
    let report = single_node_graph()
        .invoke(RunState::default())
        .await
        .expect("run should succeed");

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
                node_id: NodeId::from("node"),
                step: 1,
            },
            GraphEvent::NodeCompleted {
                run_id,
                node_id: NodeId::from("node"),
                step: 1,
            },
            GraphEvent::StateUpdated {
                run_id,
                node_id: NodeId::from("node"),
                step: 1,
            },
            GraphEvent::RunCompleted { run_id, steps: 1 },
        ]
    );
    assert_eq!(report.steps(), report.visited_nodes().len());
}
