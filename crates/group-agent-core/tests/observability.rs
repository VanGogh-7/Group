use std::error::Error as _;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use group_agent_core::{
    CompiledGraph, END, EventConfig, EventRetention, EventSink, GraphEvent, GraphRunError,
    GraphState, Node, NodeContext, NodeError, NodeId, RouteError, RunConfig, RunFailure, START,
    StateError, StateGraph,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum FailureMode {
    #[default]
    None,
    Node,
    Apply,
    Route,
    InvalidTarget,
}

#[derive(Debug, Default)]
struct ObservedState {
    value: usize,
    failure: FailureMode,
}

struct Increment;

impl GraphState for ObservedState {
    type Update = Increment;

    fn apply(&mut self, Increment: Self::Update) -> Result<(), StateError> {
        if self.failure == FailureMode::Apply {
            return Err(StateError::message("apply failed"));
        }
        self.value += 1;
        Ok(())
    }
}

struct ControlledNode;

#[async_trait]
impl Node<ObservedState> for ControlledNode {
    async fn run(
        &self,
        state: &ObservedState,
        _context: &NodeContext,
    ) -> Result<Increment, NodeError> {
        if state.failure == FailureMode::Node {
            return Err(NodeError::with_source("node failed", MiddleCause));
        }
        Ok(Increment)
    }
}

fn observed_graph() -> CompiledGraph<ObservedState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("observed", ControlledNode)
        .expect("node should register");
    graph.add_edge(START, "observed");
    graph
        .add_conditional_edges("observed", [END], |state: &ObservedState| {
            match state.failure {
                FailureMode::Route => Err(RouteError::message("route failed")),
                FailureMode::InvalidTarget => Ok(NodeId::from("undeclared")),
                _ => Ok(NodeId::end()),
            }
        })
        .expect("router should register");
    graph.compile().expect("graph should compile")
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<GraphEvent>>,
}

impl RecordingSink {
    fn snapshot(&self) -> Vec<GraphEvent> {
        self.events
            .lock()
            .expect("recording sink lock should not be poisoned")
            .clone()
    }
}

impl EventSink for RecordingSink {
    fn on_event(&self, event: &GraphEvent) {
        self.events
            .lock()
            .expect("recording sink lock should not be poisoned")
            .push(event.clone());
    }
}

fn event_config(retention: EventRetention, sink: &Arc<RecordingSink>) -> EventConfig {
    let sink: Arc<dyn EventSink> = Arc::clone(sink) as Arc<dyn EventSink>;
    EventConfig::new(retention).with_sink(sink)
}

fn event_kinds(events: &[GraphEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|event| match event {
            GraphEvent::RunStarted { .. } => "run_started",
            GraphEvent::NodeStarted { .. } => "node_started",
            GraphEvent::NodeCompleted { .. } => "node_completed",
            GraphEvent::StateUpdated { .. } => "state_updated",
            GraphEvent::RouteSelected { .. } => "route_selected",
            GraphEvent::RunCompleted { .. } => "run_completed",
            GraphEvent::RunFailed { .. } => "run_failed",
            _ => "unknown",
        })
        .collect()
}

#[tokio::test]
async fn sink_and_retained_report_receive_the_same_complete_success_sequence() {
    let compiled = observed_graph();
    let sink = Arc::new(RecordingSink::default());
    let report = compiled
        .invoke_with_events(
            ObservedState::default(),
            RunConfig::default(),
            event_config(EventRetention::All, &sink),
        )
        .await
        .expect("run should succeed");

    let sink_events = sink.snapshot();
    assert_eq!(sink_events, report.events());
    assert_eq!(
        event_kinds(&sink_events),
        [
            "run_started",
            "node_started",
            "node_completed",
            "state_updated",
            "route_selected",
            "run_completed",
        ]
    );
    assert!(
        sink_events
            .iter()
            .all(|event| event.run_id() == report.run_id())
    );
}

#[tokio::test]
async fn disabling_retention_leaves_report_empty_while_sink_receives_events() {
    let compiled = observed_graph();
    let sink = Arc::new(RecordingSink::default());
    let report = compiled
        .invoke_with_events(
            ObservedState::default(),
            RunConfig::default(),
            event_config(EventRetention::None, &sink),
        )
        .await
        .expect("run should succeed");

    assert!(report.events().is_empty());
    assert_eq!(
        event_kinds(&sink.snapshot()),
        [
            "run_started",
            "node_started",
            "node_completed",
            "state_updated",
            "route_selected",
            "run_completed",
        ]
    );
}

#[tokio::test]
async fn every_runtime_failure_emits_partial_events_then_run_failed() {
    let compiled = observed_graph();
    let cases = [
        (
            FailureMode::Node,
            RunConfig::default(),
            vec!["run_started", "node_started", "run_failed"],
            RunFailure::NodeFailed {
                node_id: NodeId::from("observed"),
                step: 1,
            },
        ),
        (
            FailureMode::Apply,
            RunConfig::default(),
            vec![
                "run_started",
                "node_started",
                "node_completed",
                "run_failed",
            ],
            RunFailure::StateUpdateFailed {
                node_id: NodeId::from("observed"),
                step: 1,
            },
        ),
        (
            FailureMode::Route,
            RunConfig::default(),
            vec![
                "run_started",
                "node_started",
                "node_completed",
                "state_updated",
                "run_failed",
            ],
            RunFailure::RouteFailed {
                node_id: NodeId::from("observed"),
                step: 1,
            },
        ),
        (
            FailureMode::InvalidTarget,
            RunConfig::default(),
            vec![
                "run_started",
                "node_started",
                "node_completed",
                "state_updated",
                "run_failed",
            ],
            RunFailure::InvalidRouteTarget {
                node_id: NodeId::from("observed"),
                target: NodeId::from("undeclared"),
                step: 1,
            },
        ),
        (
            FailureMode::None,
            RunConfig::new(0),
            vec!["run_started", "run_failed"],
            RunFailure::MaxStepsExceeded {
                max_steps: 0,
                node_id: NodeId::from("observed"),
                step: 1,
            },
        ),
    ];

    for (failure, run_config, expected_kinds, expected_failure) in cases {
        let sink = Arc::new(RecordingSink::default());
        let result = compiled
            .invoke_with_events(
                ObservedState { value: 0, failure },
                run_config,
                event_config(EventRetention::None, &sink),
            )
            .await;
        assert!(result.is_err());

        let events = sink.snapshot();
        assert_eq!(event_kinds(&events), expected_kinds);
        let run_id = events[0].run_id();
        assert!(events.iter().all(|event| event.run_id() == run_id));
        assert_eq!(
            events.last(),
            Some(&GraphEvent::RunFailed {
                run_id,
                failure: expected_failure,
            })
        );
    }
}

#[tokio::test]
async fn concurrent_runs_using_one_sink_remain_distinguishable() {
    let compiled = Arc::new(observed_graph());
    let sink = Arc::new(RecordingSink::default());
    let config = event_config(EventRetention::None, &sink);

    let (first, second) = tokio::join!(
        compiled.invoke_with_events(
            ObservedState::default(),
            RunConfig::default(),
            config.clone(),
        ),
        compiled.invoke_with_events(
            ObservedState {
                value: 40,
                failure: FailureMode::None,
            },
            RunConfig::default(),
            config,
        )
    );
    let first = first.expect("first run should succeed");
    let second = second.expect("second run should succeed");

    assert_ne!(first.run_id(), second.run_id());
    let events = sink.snapshot();
    for run_id in [first.run_id(), second.run_id()] {
        let run_events = events
            .iter()
            .filter(|event| event.run_id() == run_id)
            .collect::<Vec<_>>();
        assert_eq!(run_events.len(), 6);
        assert!(matches!(
            run_events.first(),
            Some(GraphEvent::RunStarted { .. })
        ));
        assert!(matches!(
            run_events.last(),
            Some(GraphEvent::RunCompleted { .. })
        ));
    }
    assert_eq!(events.len(), 12);
}

#[tokio::test]
async fn apply_and_route_failures_do_not_poison_the_compiled_graph() {
    let compiled = observed_graph();

    let apply_failure = compiled
        .invoke(ObservedState {
            value: 0,
            failure: FailureMode::Apply,
        })
        .await;
    assert!(matches!(
        apply_failure,
        Err(GraphRunError::StateUpdateFailed { .. })
    ));
    let after_apply = compiled
        .invoke(ObservedState::default())
        .await
        .expect("run after apply failure should succeed");
    assert_eq!(after_apply.final_state().value, 1);

    let route_failure = compiled
        .invoke(ObservedState {
            value: 0,
            failure: FailureMode::Route,
        })
        .await;
    assert!(matches!(
        route_failure,
        Err(GraphRunError::RouteFailed { .. })
    ));
    let after_route = compiled
        .invoke(ObservedState::default())
        .await
        .expect("run after route failure should succeed");
    assert_eq!(after_route.final_state().value, 1);
}

#[derive(Debug)]
struct RootCause;

impl fmt::Display for RootCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("root cause")
    }
}

impl std::error::Error for RootCause {}

#[derive(Debug)]
struct MiddleCause;

impl fmt::Display for MiddleCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("middle cause")
    }
}

impl std::error::Error for MiddleCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&RootCause)
    }
}

#[tokio::test]
async fn node_failure_preserves_the_complete_error_source_chain() {
    let error = observed_graph()
        .invoke(ObservedState {
            value: 0,
            failure: FailureMode::Node,
        })
        .await
        .expect_err("node should fail");

    let node_error = error.source().expect("run error should expose node error");
    assert_eq!(node_error.to_string(), "node failed");
    let middle = node_error
        .source()
        .expect("node error should expose middle cause");
    assert_eq!(middle.to_string(), "middle cause");
    assert_eq!(
        middle
            .source()
            .expect("middle cause should expose root cause")
            .to_string(),
        "root cause"
    );
}
