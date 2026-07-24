use async_trait::async_trait;
use group_agent_core::{
    END, GraphEvent, GraphRunError, GraphState, Node, NodeContext, NodeError, NodeId, RunConfig,
    START, StateError, StateGraph,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TestState {
    value: i32,
    applied: Vec<&'static str>,
}

#[derive(Clone, Copy, Debug)]
struct TestUpdate {
    amount: i32,
    label: &'static str,
}

impl GraphState for TestState {
    type Update = TestUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update.amount;
        self.applied.push(update.label);
        Ok(())
    }
}

struct Increment {
    amount: i32,
    label: &'static str,
}

#[async_trait]
impl Node<TestState> for Increment {
    async fn run(
        &self,
        _state: &TestState,
        _context: &NodeContext,
    ) -> Result<TestUpdate, NodeError> {
        Ok(TestUpdate {
            amount: self.amount,
            label: self.label,
        })
    }
}

struct FailingNode;

#[async_trait]
impl Node<TestState> for FailingNode {
    async fn run(
        &self,
        _state: &TestState,
        _context: &NodeContext,
    ) -> Result<TestUpdate, NodeError> {
        Err(NodeError::new("intentional failure"))
    }
}

fn linear_graph() -> StateGraph<TestState> {
    let mut graph = StateGraph::new();
    graph
        .add_node(
            "first",
            Increment {
                amount: 2,
                label: "first",
            },
        )
        .expect("first node should register");
    graph
        .add_node(
            "second",
            Increment {
                amount: 3,
                label: "second",
            },
        )
        .expect("second node should register");
    graph
        .add_edge(START, "first")
        .add_edge("first", "second")
        .add_edge("second", END);
    graph
}

#[tokio::test]
async fn linear_graph_executes_in_edge_order_and_applies_updates() {
    let compiled = linear_graph().compile().expect("graph should compile");
    let report = compiled
        .invoke(TestState::default())
        .await
        .expect("run should succeed");

    assert_eq!(report.final_state().value, 5);
    assert_eq!(report.final_state().applied, ["first", "second"]);
    assert_eq!(report.steps(), 2);
    assert_eq!(
        report.visited_nodes(),
        [NodeId::from("first"), NodeId::from("second")]
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
                node_id: NodeId::from("first").into(),
                step: 1,
            },
            GraphEvent::NodeCompleted {
                run_id,
                node_id: NodeId::from("first").into(),
                step: 1,
            },
            GraphEvent::StateUpdated {
                run_id,
                node_id: NodeId::from("first").into(),
                step: 1,
            },
            GraphEvent::NodeStarted {
                run_id,
                node_id: NodeId::from("second").into(),
                step: 2,
            },
            GraphEvent::NodeCompleted {
                run_id,
                node_id: NodeId::from("second").into(),
                step: 2,
            },
            GraphEvent::StateUpdated {
                run_id,
                node_id: NodeId::from("second").into(),
                step: 2,
            },
            GraphEvent::RunCompleted { run_id, steps: 2 },
        ]
    );
}

#[tokio::test]
async fn compiled_graph_is_reusable_and_runs_are_isolated() {
    let compiled = linear_graph().compile().expect("graph should compile");

    let first = compiled
        .invoke(TestState::default())
        .await
        .expect("first run should succeed");
    let second = compiled
        .invoke(TestState {
            value: 10,
            applied: Vec::new(),
        })
        .await
        .expect("second run should succeed");

    assert_eq!(first.final_state().value, 5);
    assert_eq!(first.final_state().applied, ["first", "second"]);
    assert_eq!(second.final_state().value, 15);
    assert_eq!(second.final_state().applied, ["first", "second"]);
}

#[tokio::test]
async fn step_limit_reports_the_next_node_and_step() {
    let compiled = linear_graph().compile().expect("graph should compile");
    let error = compiled
        .invoke_with_config(TestState::default(), RunConfig::new(1))
        .await;

    assert!(matches!(
        error,
        Err(GraphRunError::MaxStepsExceeded {
            max_steps: 1,
            node_id,
            step: 2,
        }) if node_id == NodeId::from("second")
    ));
}

#[tokio::test]
async fn node_error_preserves_node_id_and_step() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fails", FailingNode)
        .expect("node should register");
    graph.add_edge(START, "fails").add_edge("fails", END);
    let compiled = graph.compile().expect("graph should compile");

    let error = compiled.invoke(TestState::default()).await;

    assert!(matches!(
        error,
        Err(GraphRunError::NodeFailed {
            node_id,
            step: 1,
            source,
        }) if node_id == NodeId::from("fails") && source.as_message() == "intentional failure"
    ));
}
