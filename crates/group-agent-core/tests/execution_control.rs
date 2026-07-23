use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    CompiledGraph, END, EventConfig, EventRetention, EventSink, GraphEvent, GraphRunError,
    GraphState, Node, NodeContext, NodeError, NodeId, RunConfig, RunControl, RunFailure, START,
    StateError, StateGraph,
};
use tokio::sync::{Barrier, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct ControlState {
    value: usize,
    gate: Option<Arc<Barrier>>,
    release: Option<Arc<Notify>>,
    dropped: Option<Arc<AtomicBool>>,
}

struct Increment;

impl GraphState for ControlState {
    type Update = Increment;

    fn apply(&mut self, Increment: Self::Update) -> Result<(), StateError> {
        self.value += 1;
        Ok(())
    }
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct CoordinatedNode;

#[async_trait]
impl Node<ControlState> for CoordinatedNode {
    async fn run(
        &self,
        state: &ControlState,
        _context: &NodeContext,
    ) -> Result<Increment, NodeError> {
        let _drop_flag = state
            .dropped
            .as_ref()
            .map(|flag| DropFlag(Arc::clone(flag)));
        if let Some(gate) = &state.gate {
            gate.wait().await;
            if let Some(release) = &state.release {
                release.notified().await;
            } else {
                pending::<()>().await;
            }
        }
        Ok(Increment)
    }
}

fn controlled_graph() -> CompiledGraph<ControlState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("controlled", CoordinatedNode)
        .expect("node should register");
    graph
        .add_edge(START, "controlled")
        .add_edge("controlled", END);
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

fn sink_config(sink: &Arc<RecordingSink>) -> EventConfig {
    let sink: Arc<dyn EventSink> = sink.clone();
    EventConfig::new(EventRetention::None).with_sink(sink)
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

fn assert_one_terminal_failure(events: &[GraphEvent]) {
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, GraphEvent::RunFailed { .. }))
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, GraphEvent::RunCompleted { .. }))
    );
}

#[tokio::test]
async fn cancellation_requested_before_first_node_fails_before_node_started() {
    let token = CancellationToken::new();
    token.cancel();
    let sink = Arc::new(RecordingSink::default());
    let error = controlled_graph()
        .invoke_with_control(
            ControlState::default(),
            RunConfig::default(),
            sink_config(&sink),
            RunControl::new().with_cancellation_token(token),
        )
        .await
        .expect_err("pre-cancelled run should fail");

    let events = sink.snapshot();
    assert_eq!(event_kinds(&events), ["run_started", "run_failed"]);
    assert_one_terminal_failure(&events);
    let run_id = events[0].run_id();
    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            run_id: error_run_id,
            node_id: Some(node_id),
            step: 1,
        } if error_run_id == run_id && node_id == NodeId::from("controlled")
    ));
    assert_eq!(
        events.last(),
        Some(&GraphEvent::RunFailed {
            run_id,
            failure: RunFailure::Cancelled {
                node_id: Some(NodeId::from("controlled")),
                step: 1,
            },
        })
    );
}

#[tokio::test]
async fn cancellation_during_node_drops_future_and_preserves_partial_events() {
    let compiled = Arc::new(controlled_graph());
    let gate = Arc::new(Barrier::new(2));
    let dropped = Arc::new(AtomicBool::new(false));
    let token = CancellationToken::new();
    let sink = Arc::new(RecordingSink::default());

    let task = {
        let compiled = Arc::clone(&compiled);
        let token = token.clone();
        let gate = Arc::clone(&gate);
        let dropped = Arc::clone(&dropped);
        let event_config = sink_config(&sink);
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 0,
                        gate: Some(gate),
                        release: None,
                        dropped: Some(dropped),
                    },
                    RunConfig::default(),
                    event_config,
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };

    gate.wait().await;
    token.cancel();
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("cancelled run should fail");

    assert!(matches!(error, GraphRunError::Cancelled { step: 1, .. }));
    assert!(dropped.load(Ordering::SeqCst));
    let events = sink.snapshot();
    assert_eq!(
        event_kinds(&events),
        ["run_started", "node_started", "run_failed"]
    );
    assert_one_terminal_failure(&events);
}

#[derive(Debug)]
struct ApplyTrackingState {
    applied: Arc<AtomicUsize>,
}

impl GraphState for ApplyTrackingState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct ImmediateTrackingNode;

#[async_trait]
impl Node<ApplyTrackingState> for ImmediateTrackingNode {
    async fn run(
        &self,
        _state: &ApplyTrackingState,
        _context: &NodeContext,
    ) -> Result<(), NodeError> {
        Ok(())
    }
}

struct CancelOnNodeCompletedSink {
    token: CancellationToken,
    events: Mutex<Vec<GraphEvent>>,
}

impl EventSink for CancelOnNodeCompletedSink {
    fn on_event(&self, event: &GraphEvent) {
        self.events
            .lock()
            .expect("cancel sink lock should not be poisoned")
            .push(event.clone());
        if matches!(event, GraphEvent::NodeCompleted { .. }) {
            self.token.cancel();
        }
    }
}

#[tokio::test]
async fn cancellation_after_node_result_prevents_update_application() {
    let applied = Arc::new(AtomicUsize::new(0));
    let token = CancellationToken::new();
    let sink = Arc::new(CancelOnNodeCompletedSink {
        token: token.clone(),
        events: Mutex::new(Vec::new()),
    });
    let event_sink: Arc<dyn EventSink> = sink.clone();
    let mut graph = StateGraph::new();
    graph
        .add_node("tracked", ImmediateTrackingNode)
        .expect("node should register");
    graph.add_edge(START, "tracked").add_edge("tracked", END);

    let error = graph
        .compile()
        .expect("graph should compile")
        .invoke_with_control(
            ApplyTrackingState {
                applied: Arc::clone(&applied),
            },
            RunConfig::default(),
            EventConfig::new(EventRetention::None).with_sink(event_sink),
            RunControl::new().with_cancellation_token(token),
        )
        .await
        .expect_err("sink-triggered cancellation should fail the run");

    assert!(matches!(error, GraphRunError::Cancelled { step: 1, .. }));
    assert_eq!(applied.load(Ordering::SeqCst), 0);
    let events = sink
        .events
        .lock()
        .expect("cancel sink lock should not be poisoned")
        .clone();
    assert_eq!(
        event_kinds(&events),
        [
            "run_started",
            "node_started",
            "node_completed",
            "run_failed",
        ]
    );
    assert_one_terminal_failure(&events);
}

#[tokio::test(start_paused = true)]
async fn run_timeout_uses_invocation_deadline_and_drops_node_future() {
    let compiled = Arc::new(controlled_graph());
    let gate = Arc::new(Barrier::new(2));
    let dropped = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(RecordingSink::default());
    let task = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let dropped = Arc::clone(&dropped);
        let event_config = sink_config(&sink);
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 0,
                        gate: Some(gate),
                        release: None,
                        dropped: Some(dropped),
                    },
                    RunConfig::default(),
                    event_config,
                    RunControl::new().with_run_timeout(Duration::from_secs(5)),
                )
                .await
        })
    };

    gate.wait().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("run should time out");

    assert!(matches!(
        error,
        GraphRunError::RunTimedOut {
            timeout,
            step: 1,
            ..
        } if timeout == Duration::from_secs(5)
    ));
    assert!(dropped.load(Ordering::SeqCst));
    let events = sink.snapshot();
    assert_eq!(
        event_kinds(&events),
        ["run_started", "node_started", "run_failed"]
    );
    assert_one_terminal_failure(&events);
    let run_id = events[0].run_id();
    assert_eq!(
        events.last(),
        Some(&GraphEvent::RunFailed {
            run_id,
            failure: RunFailure::RunTimedOut {
                timeout: Duration::from_secs(5),
                node_id: Some(NodeId::from("controlled")),
                step: 1,
            },
        })
    );
}

#[tokio::test(start_paused = true)]
async fn node_timeout_is_measured_from_node_started() {
    let compiled = Arc::new(controlled_graph());
    let gate = Arc::new(Barrier::new(2));
    let sink = Arc::new(RecordingSink::default());
    let task = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let event_config = sink_config(&sink);
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 0,
                        gate: Some(gate),
                        release: None,
                        dropped: None,
                    },
                    RunConfig::default(),
                    event_config,
                    RunControl::new().with_node_timeout(Duration::from_secs(3)),
                )
                .await
        })
    };

    gate.wait().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("node should time out");

    assert!(matches!(
        error,
        GraphRunError::NodeTimedOut {
            timeout,
            node_id,
            step: 1,
            ..
        } if timeout == Duration::from_secs(3) && node_id == NodeId::from("controlled")
    ));
    let events = sink.snapshot();
    assert_eq!(
        event_kinds(&events),
        ["run_started", "node_started", "run_failed"]
    );
    assert_one_terminal_failure(&events);
    let run_id = events[0].run_id();
    assert_eq!(
        events.last(),
        Some(&GraphEvent::RunFailed {
            run_id,
            failure: RunFailure::NodeTimedOut {
                timeout: Duration::from_secs(3),
                node_id: NodeId::from("controlled"),
                step: 1,
            },
        })
    );
}

#[tokio::test(start_paused = true)]
async fn expired_deadlines_are_classified_by_earliest_instant_with_run_winning_ties() {
    let cases = [
        (Duration::from_secs(2), Duration::from_secs(5), true),
        (Duration::from_secs(5), Duration::from_secs(2), false),
        (Duration::from_secs(2), Duration::from_secs(2), true),
    ];

    for (run_timeout, node_timeout, expect_run_timeout) in cases {
        let compiled = Arc::new(controlled_graph());
        let gate = Arc::new(Barrier::new(2));
        let sink = Arc::new(RecordingSink::default());
        let task = {
            let compiled = Arc::clone(&compiled);
            let gate = Arc::clone(&gate);
            let event_config = sink_config(&sink);
            tokio::spawn(async move {
                compiled
                    .invoke_with_control(
                        ControlState {
                            value: 0,
                            gate: Some(gate),
                            release: None,
                            dropped: None,
                        },
                        RunConfig::default(),
                        event_config,
                        RunControl::new()
                            .with_run_timeout(run_timeout)
                            .with_node_timeout(node_timeout),
                    )
                    .await
            })
        };

        gate.wait().await;
        tokio::time::advance(run_timeout.max(node_timeout)).await;
        let error = task
            .await
            .expect("run task should not panic")
            .expect_err("expired deadline should fail the run");
        let events = sink.snapshot();
        assert_eq!(
            event_kinds(&events),
            ["run_started", "node_started", "run_failed"]
        );
        assert_one_terminal_failure(&events);
        let run_id = events[0].run_id();
        let expected_failure = if expect_run_timeout {
            assert!(matches!(
                error,
                GraphRunError::RunTimedOut {
                    run_id: error_run_id,
                    timeout,
                    node_id: Some(node_id),
                    step: 1,
                } if error_run_id == run_id
                    && timeout == run_timeout
                    && node_id == NodeId::from("controlled")
            ));
            RunFailure::RunTimedOut {
                timeout: run_timeout,
                node_id: Some(NodeId::from("controlled")),
                step: 1,
            }
        } else {
            assert!(matches!(
                error,
                GraphRunError::NodeTimedOut {
                    run_id: error_run_id,
                    timeout,
                    node_id,
                    step: 1,
                } if error_run_id == run_id
                    && timeout == node_timeout
                    && node_id == NodeId::from("controlled")
            ));
            RunFailure::NodeTimedOut {
                timeout: node_timeout,
                node_id: NodeId::from("controlled"),
                step: 1,
            }
        };
        assert_eq!(
            events.last(),
            Some(&GraphEvent::RunFailed {
                run_id,
                failure: expected_failure,
            })
        );

        let recovered = compiled
            .invoke(ControlState::default())
            .await
            .expect("compiled graph should recover after timeout");
        assert_eq!(recovered.final_state().value, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_wins_when_both_deadlines_are_also_ready() {
    let compiled = Arc::new(controlled_graph());
    let gate = Arc::new(Barrier::new(2));
    let token = CancellationToken::new();
    let sink = Arc::new(RecordingSink::default());
    let task = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let token = token.clone();
        let event_config = sink_config(&sink);
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 0,
                        gate: Some(gate),
                        release: None,
                        dropped: None,
                    },
                    RunConfig::default(),
                    event_config,
                    RunControl::new()
                        .with_cancellation_token(token)
                        .with_run_timeout(Duration::from_secs(5))
                        .with_node_timeout(Duration::from_secs(2)),
                )
                .await
        })
    };

    gate.wait().await;
    token.cancel();
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("cancellation should win");

    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            node_id: Some(node_id),
            step: 1,
            ..
        } if node_id == NodeId::from("controlled")
    ));
    let events = sink.snapshot();
    assert_one_terminal_failure(&events);
    assert!(matches!(
        events.last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::Cancelled {
                node_id: Some(node_id),
                step: 1,
            },
            ..
        }) if node_id == &NodeId::from("controlled")
    ));
}

struct DeadlineResultNode {
    started: Arc<Notify>,
    delay: Duration,
}

#[async_trait]
impl Node<ControlState> for DeadlineResultNode {
    async fn run(
        &self,
        _state: &ControlState,
        _context: &NodeContext,
    ) -> Result<Increment, NodeError> {
        self.started.notify_one();
        tokio::time::sleep(self.delay).await;
        Ok(Increment)
    }
}

#[tokio::test(start_paused = true)]
async fn selected_timeout_wins_when_node_result_is_ready_at_the_same_poll() {
    let started = Arc::new(Notify::new());
    let mut graph = StateGraph::new();
    graph
        .add_node(
            "timed-result",
            DeadlineResultNode {
                started: Arc::clone(&started),
                delay: Duration::from_secs(2),
            },
        )
        .expect("node should register");
    graph
        .add_edge(START, "timed-result")
        .add_edge("timed-result", END);
    let compiled = Arc::new(graph.compile().expect("graph should compile"));
    let task = {
        let compiled = Arc::clone(&compiled);
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState::default(),
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new()
                        .with_run_timeout(Duration::from_secs(5))
                        .with_node_timeout(Duration::from_secs(2)),
                )
                .await
        })
    };

    started.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("node timeout should beat the ready node result");

    assert!(matches!(
        error,
        GraphRunError::NodeTimedOut {
            timeout,
            node_id,
            step: 1,
            ..
        } if timeout == Duration::from_secs(2)
            && node_id == NodeId::from("timed-result")
    ));
}

#[tokio::test]
async fn cancelled_graph_can_be_invoked_successfully_again() {
    let compiled = Arc::new(controlled_graph());
    let gate = Arc::new(Barrier::new(2));
    let token = CancellationToken::new();
    let failed = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let token = token.clone();
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 0,
                        gate: Some(gate),
                        release: None,
                        dropped: None,
                    },
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };

    gate.wait().await;
    token.cancel();
    assert!(matches!(
        failed.await.expect("run task should not panic"),
        Err(GraphRunError::Cancelled { .. })
    ));

    let recovered = compiled
        .invoke(ControlState::default())
        .await
        .expect("later run should succeed");
    assert_eq!(recovered.final_state().value, 1);
}

#[tokio::test]
async fn independent_tokens_do_not_cross_cancel_concurrent_runs() {
    let compiled = Arc::new(controlled_graph());
    let gate = Arc::new(Barrier::new(3));
    let second_release = Arc::new(Notify::new());
    let first_token = CancellationToken::new();
    let second_token = CancellationToken::new();

    let first = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let token = first_token.clone();
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 0,
                        gate: Some(gate),
                        release: None,
                        dropped: None,
                    },
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };
    let second = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let release = Arc::clone(&second_release);
        let token = second_token.clone();
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 40,
                        gate: Some(gate),
                        release: Some(release),
                        dropped: None,
                    },
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };

    gate.wait().await;
    first_token.cancel();
    second_release.notify_one();

    assert!(matches!(
        first.await.expect("first task should not panic"),
        Err(GraphRunError::Cancelled { .. })
    ));
    let second = second
        .await
        .expect("second task should not panic")
        .expect("second run should not be cancelled");
    assert_eq!(second.final_state().value, 41);
    assert!(!second_token.is_cancelled());
}

#[tokio::test]
async fn shared_token_cancels_multiple_runs() {
    let compiled = Arc::new(controlled_graph());
    let gate = Arc::new(Barrier::new(3));
    let token = CancellationToken::new();

    let first = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let token = token.clone();
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 0,
                        gate: Some(gate),
                        release: None,
                        dropped: None,
                    },
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };
    let second = {
        let compiled = Arc::clone(&compiled);
        let gate = Arc::clone(&gate);
        let token = token.clone();
        tokio::spawn(async move {
            compiled
                .invoke_with_control(
                    ControlState {
                        value: 10,
                        gate: Some(gate),
                        release: None,
                        dropped: None,
                    },
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };

    gate.wait().await;
    token.cancel();
    let (first, second) = tokio::join!(first, second);
    assert!(matches!(
        first.expect("first task should not panic"),
        Err(GraphRunError::Cancelled { .. })
    ));
    assert!(matches!(
        second.expect("second task should not panic"),
        Err(GraphRunError::Cancelled { .. })
    ));
}

#[derive(Default)]
struct ContextState;

impl GraphState for ContextState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        Ok(())
    }
}

type ContextObservation =
    Arc<Mutex<Option<(CancellationToken, Option<Instant>, Option<Duration>)>>>;

struct ObserveContextNode {
    observation: ContextObservation,
}

#[async_trait]
impl Node<ContextState> for ObserveContextNode {
    async fn run(&self, _state: &ContextState, context: &NodeContext) -> Result<(), NodeError> {
        let mut observation = self
            .observation
            .lock()
            .expect("context observation lock should not be poisoned");
        *observation = Some((
            context.cancellation_token(),
            context.run_deadline(),
            context.remaining_run_time(),
        ));
        assert!(!context.is_cancelled());
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn node_context_exposes_run_token_and_deadline() {
    let observation: ContextObservation = Arc::new(Mutex::new(None));
    let mut graph = StateGraph::new();
    graph
        .add_node(
            "observe",
            ObserveContextNode {
                observation: Arc::clone(&observation),
            },
        )
        .expect("node should register");
    graph.add_edge(START, "observe").add_edge("observe", END);
    let token = CancellationToken::new();
    graph
        .compile()
        .expect("graph should compile")
        .invoke_with_control(
            ContextState,
            RunConfig::default(),
            EventConfig::default(),
            RunControl::new()
                .with_cancellation_token(token.clone())
                .with_run_timeout(Duration::from_secs(30)),
        )
        .await
        .expect("run should succeed");

    let (context_token, deadline, remaining) = observation
        .lock()
        .expect("context observation lock should not be poisoned")
        .take()
        .expect("node should record its context");
    assert!(deadline.is_some());
    assert_eq!(remaining, Some(Duration::from_secs(30)));
    assert!(!context_token.is_cancelled());
    token.cancel();
    assert!(context_token.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_run_timeout_precede_max_steps_without_off_by_one() {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_error = controlled_graph()
        .invoke_with_control(
            ControlState::default(),
            RunConfig::new(0),
            EventConfig::default(),
            RunControl::new().with_cancellation_token(cancelled),
        )
        .await
        .expect_err("cancellation should win");
    assert!(matches!(
        cancelled_error,
        GraphRunError::Cancelled { step: 1, .. }
    ));

    let timeout_error = controlled_graph()
        .invoke_with_control(
            ControlState::default(),
            RunConfig::new(0),
            EventConfig::default(),
            RunControl::new().with_run_timeout(Duration::ZERO),
        )
        .await
        .expect_err("run timeout should win");
    assert!(matches!(
        timeout_error,
        GraphRunError::RunTimedOut { step: 1, .. }
    ));

    let max_steps_error = controlled_graph()
        .invoke_with_config(ControlState::default(), RunConfig::new(0))
        .await
        .expect_err("step limit should fail");
    assert!(matches!(
        max_steps_error,
        GraphRunError::MaxStepsExceeded { step: 1, .. }
    ));
}
