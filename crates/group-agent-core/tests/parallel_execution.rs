use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    CompiledGraph, END, EventConfig, EventRetention, EventSink, GraphEvent, GraphRunError,
    GraphState, Node, NodeContext, NodeError, NodeId, NodeUpdate, RunConfig, RunControl,
    RunFailure, START, StateError, StateGraph,
};
use tokio::sync::{Barrier, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct MergeState {
    value: i32,
    observations: Arc<Mutex<Vec<(NodeId, i32)>>>,
    batch_order: Vec<NodeId>,
    synthesis_runs: Arc<AtomicUsize>,
}

#[derive(Clone, Copy, Debug)]
struct Add(i32);

impl GraphState for MergeState {
    type Update = Add;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update.0;
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        let validated = updates
            .iter()
            .map(|update| (update.node_id().clone(), update.update().0))
            .collect::<Vec<_>>();
        let total = validated.iter().map(|(_, amount)| amount).sum::<i32>();

        self.batch_order = validated
            .iter()
            .map(|(node_id, _)| node_id.clone())
            .collect();
        self.value += total;
        Ok(())
    }
}

struct AddNode(i32);

#[async_trait]
impl Node<MergeState> for AddNode {
    async fn run(&self, _state: &MergeState, _context: &NodeContext) -> Result<Add, NodeError> {
        Ok(Add(self.0))
    }
}

struct ObserveNode(i32);

#[async_trait]
impl Node<MergeState> for ObserveNode {
    async fn run(&self, state: &MergeState, context: &NodeContext) -> Result<Add, NodeError> {
        state
            .observations
            .lock()
            .expect("observation lock should not be poisoned")
            .push((context.node_id().clone(), state.value));
        Ok(Add(self.0))
    }
}

struct SynthesisNode;

#[async_trait]
impl Node<MergeState> for SynthesisNode {
    async fn run(&self, state: &MergeState, _context: &NodeContext) -> Result<Add, NodeError> {
        state.synthesis_runs.fetch_add(1, Ordering::SeqCst);
        assert_eq!(state.value, 13);
        Ok(Add(100))
    }
}

fn merge_graph() -> CompiledGraph<MergeState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("prepare", AddNode(10))
        .expect("prepare should register");
    graph
        .add_node("local_search", ObserveNode(1))
        .expect("local search should register");
    graph
        .add_node("web_search", ObserveNode(2))
        .expect("web search should register");
    graph
        .add_node("synthesis", SynthesisNode)
        .expect("synthesis should register");
    graph.add_edge(START, "prepare");
    graph
        .add_fan_out("prepare", ["local_search", "web_search"])
        .expect("fan-out should register");
    graph
        .add_edge("local_search", "synthesis")
        .add_edge("web_search", "synthesis")
        .add_edge("synthesis", END);
    graph.compile().expect("parallel graph should compile")
}

fn merge_state() -> MergeState {
    MergeState {
        value: 0,
        observations: Arc::new(Mutex::new(Vec::new())),
        batch_order: Vec::new(),
        synthesis_runs: Arc::new(AtomicUsize::new(0)),
    }
}

#[tokio::test]
async fn parallel_nodes_share_snapshot_merge_stably_and_fan_in_once() {
    let report = merge_graph()
        .invoke(merge_state())
        .await
        .expect("parallel run should succeed");

    let mut observations = report
        .final_state()
        .observations
        .lock()
        .expect("observation lock should not be poisoned")
        .clone();
    observations.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    assert_eq!(
        observations,
        [
            (NodeId::from("local_search"), 10),
            (NodeId::from("web_search"), 10),
        ]
    );
    assert_eq!(
        report.final_state().batch_order,
        [NodeId::from("local_search"), NodeId::from("web_search"),]
    );
    assert_eq!(report.final_state().value, 113);
    assert_eq!(
        report.final_state().synthesis_runs.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        report.visited_nodes(),
        [
            NodeId::from("prepare"),
            NodeId::from("local_search"),
            NodeId::from("web_search"),
            NodeId::from("synthesis"),
        ]
    );

    let run_id = report.run_id();
    let parallel_events = report
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event,
                GraphEvent::SuperstepStarted { .. }
                    | GraphEvent::SuperstepCompleted { .. }
                    | GraphEvent::NodeStarted { step: 2 | 3, .. }
                    | GraphEvent::NodeCompleted { step: 2 | 3, .. }
                    | GraphEvent::StateUpdated { step: 2 | 3, .. }
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(matches!(
        parallel_events.first(),
        Some(GraphEvent::SuperstepStarted {
            run_id: event_run_id,
            superstep: 2,
            node_ids,
        }) if *event_run_id == run_id
            && node_ids == &[NodeId::from("local_search"), NodeId::from("web_search")]
    ));
    let started = parallel_events
        .iter()
        .filter_map(|event| match event {
            GraphEvent::NodeStarted { node_id, .. } => Some(node_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let updated = parallel_events
        .iter()
        .filter_map(|event| match event {
            GraphEvent::StateUpdated { node_id, .. } => Some(node_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        started,
        [NodeId::from("local_search"), NodeId::from("web_search")]
    );
    assert_eq!(updated, started);
    assert!(matches!(
        parallel_events.last(),
        Some(GraphEvent::SuperstepCompleted {
            run_id: event_run_id,
            superstep: 2,
        }) if *event_run_id == run_id
    ));
}

#[derive(Debug)]
struct BarrierState {
    barrier: Arc<Barrier>,
    applied: usize,
}

impl GraphState for BarrierState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        self.applied += 1;
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        self.applied += updates.len();
        Ok(())
    }
}

struct BarrierNode;

#[async_trait]
impl Node<BarrierState> for BarrierNode {
    async fn run(&self, state: &BarrierState, _context: &NodeContext) -> Result<(), NodeError> {
        state.barrier.wait().await;
        Ok(())
    }
}

struct BarrierFork;

#[async_trait]
impl Node<BarrierState> for BarrierFork {
    async fn run(&self, _state: &BarrierState, _context: &NodeContext) -> Result<(), NodeError> {
        Ok(())
    }
}

#[tokio::test]
async fn fan_out_nodes_are_polled_concurrently_without_spawn_per_node() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", BarrierFork)
        .expect("fork should register");
    graph
        .add_node("left", BarrierNode)
        .expect("left should register");
    graph
        .add_node("right", BarrierNode)
        .expect("right should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    graph.add_edge("left", END).add_edge("right", END);
    let compiled = Arc::new(graph.compile().expect("graph should compile"));
    let barrier = Arc::new(Barrier::new(2));

    let run = {
        let compiled = Arc::clone(&compiled);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            compiled
                .invoke(BarrierState {
                    barrier,
                    applied: 0,
                })
                .await
        })
    };

    let report = tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("both parallel nodes must reach the barrier")
        .expect("run task should not panic")
        .expect("run should succeed");
    assert_eq!(report.final_state().applied, 3);
}

#[tokio::test]
async fn concurrent_parallel_runs_isolate_frontiers_state_events_and_run_ids() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", BarrierFork)
        .expect("fork should register");
    graph
        .add_node("left", BarrierNode)
        .expect("left should register");
    graph
        .add_node("right", BarrierNode)
        .expect("right should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    graph.add_edge("left", END).add_edge("right", END);
    let compiled = Arc::new(graph.compile().expect("graph should compile"));
    let sink = Arc::new(RecordingSink::default());
    let event_config =
        EventConfig::new(EventRetention::None).with_sink(Arc::clone(&sink) as Arc<dyn EventSink>);

    let (first, second) = tokio::join!(
        compiled.invoke_with_events(
            BarrierState {
                barrier: Arc::new(Barrier::new(2)),
                applied: 0,
            },
            RunConfig::default(),
            event_config.clone(),
        ),
        compiled.invoke_with_events(
            BarrierState {
                barrier: Arc::new(Barrier::new(2)),
                applied: 10,
            },
            RunConfig::default(),
            event_config,
        ),
    );
    let first = first.expect("first run should succeed");
    let second = second.expect("second run should succeed");
    assert_eq!(first.final_state().applied, 3);
    assert_eq!(second.final_state().applied, 13);
    assert_ne!(first.run_id(), second.run_id());

    let events = sink.0.lock().expect("sink lock should not be poisoned");
    for run_id in [first.run_id(), second.run_id()] {
        let run_events = events
            .iter()
            .filter(|event| event.run_id() == run_id)
            .collect::<Vec<_>>();
        assert!(matches!(
            run_events.first(),
            Some(GraphEvent::RunStarted { .. })
        ));
        assert!(matches!(
            run_events.last(),
            Some(GraphEvent::RunCompleted { .. })
        ));
        assert_eq!(
            run_events
                .iter()
                .filter(|event| matches!(event, GraphEvent::SuperstepStarted { .. }))
                .count(),
            1
        );
    }
}

#[derive(Debug)]
struct DefaultBatchState {
    applies: Arc<AtomicUsize>,
}

impl GraphState for DefaultBatchState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct NoopNode;

#[async_trait]
impl Node<DefaultBatchState> for NoopNode {
    async fn run(
        &self,
        _state: &DefaultBatchState,
        _context: &NodeContext,
    ) -> Result<(), NodeError> {
        Ok(())
    }
}

#[tokio::test]
async fn default_state_rejects_parallel_updates_before_applying_any_of_them() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", NoopNode)
        .expect("fork should register");
    graph
        .add_node("left", NoopNode)
        .expect("left should register");
    graph
        .add_node("right", NoopNode)
        .expect("right should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    graph.add_edge("left", END).add_edge("right", END);
    let compiled = graph.compile().expect("graph should compile");
    let applies = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(RecordingSink::default());

    let error = compiled
        .invoke_with_events(
            DefaultBatchState {
                applies: Arc::clone(&applies),
            },
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
        )
        .await
        .expect_err("default batch implementation should reject two updates");

    assert!(matches!(
        error,
        GraphRunError::StateBatchUpdateFailed {
            node_ids,
            step: 2,
            ..
        } if node_ids == [NodeId::from("left"), NodeId::from("right")]
    ));
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert!(matches!(
        events.last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::StateBatchUpdateFailed {
                node_ids,
                step: 2,
            },
            ..
        }) if node_ids == &[NodeId::from("left"), NodeId::from("right")]
    ));
    assert!(!events.iter().any(|event| {
        matches!(
            event,
            GraphEvent::StateUpdated { step: 2 | 3, .. }
                | GraphEvent::SuperstepCompleted { superstep: 2, .. }
        )
    }));
}

#[tokio::test]
async fn an_end_branch_does_not_stop_another_branch() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", AddNode(1))
        .expect("fork should register");
    graph
        .add_node("continues", AddNode(2))
        .expect("continuation should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", [END, "continues"])
        .expect("fan-out should register");
    graph.add_edge("continues", END);

    let report = graph
        .compile()
        .expect("graph should compile")
        .invoke(merge_state())
        .await
        .expect("remaining branch should run");
    assert_eq!(report.final_state().value, 3);
    assert_eq!(
        report.visited_nodes(),
        [NodeId::from("fork"), NodeId::from("continues")]
    );
}

#[derive(Debug)]
struct FailureState {
    fail: bool,
    applied: Arc<AtomicUsize>,
}

impl GraphState for FailureState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        self.applied.fetch_add(updates.len(), Ordering::SeqCst);
        Ok(())
    }
}

struct MaybeFail;

#[async_trait]
impl Node<FailureState> for MaybeFail {
    async fn run(&self, state: &FailureState, _context: &NodeContext) -> Result<(), NodeError> {
        if state.fail {
            Err(NodeError::message("parallel failure"))
        } else {
            Ok(())
        }
    }
}

struct Success;

#[async_trait]
impl Node<FailureState> for Success {
    async fn run(&self, _state: &FailureState, _context: &NodeContext) -> Result<(), NodeError> {
        Ok(())
    }
}

fn failure_graph() -> CompiledGraph<FailureState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", Success)
        .expect("fork should register");
    graph
        .add_node("success", Success)
        .expect("success should register");
    graph
        .add_node("maybe_fail", MaybeFail)
        .expect("failure node should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["success", "maybe_fail"])
        .expect("fan-out should register");
    graph.add_edge("success", END).add_edge("maybe_fail", END);
    graph.compile().expect("graph should compile")
}

#[tokio::test]
async fn parallel_node_failure_commits_no_batch_and_graph_remains_reusable() {
    let compiled = failure_graph();
    let failed_applies = Arc::new(AtomicUsize::new(0));
    let error = compiled
        .invoke(FailureState {
            fail: true,
            applied: Arc::clone(&failed_applies),
        })
        .await
        .expect_err("one parallel node should fail");
    assert!(matches!(
        error,
        GraphRunError::NodeFailed {
            node_id,
            step: 3,
            ..
        } if node_id == NodeId::from("maybe_fail")
    ));
    assert_eq!(failed_applies.load(Ordering::SeqCst), 1);

    let recovered_applies = Arc::new(AtomicUsize::new(0));
    let report = compiled
        .invoke(FailureState {
            fail: false,
            applied: Arc::clone(&recovered_applies),
        })
        .await
        .expect("later run should succeed");
    assert_eq!(report.steps(), 3);
    assert_eq!(recovered_applies.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn max_steps_never_executes_a_partial_parallel_frontier() {
    let sink = Arc::new(RecordingSink::default());
    let error = merge_graph()
        .invoke_with_events(
            merge_state(),
            RunConfig::new(2),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
        )
        .await
        .expect_err("the complete two-node frontier cannot fit");
    assert!(matches!(
        error,
        GraphRunError::MaxStepsExceeded {
            max_steps: 2,
            node_id,
            step: 3,
        } if node_id == NodeId::from("web_search")
    ));
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert!(!events.iter().any(|event| {
        matches!(
            event,
            GraphEvent::NodeStarted { node_id, .. }
                if node_id == &NodeId::from("local_search")
                    || node_id == &NodeId::from("web_search")
        )
    }));
}

#[derive(Debug)]
struct ControlState {
    wait: bool,
    started: Arc<Notify>,
    applied: Arc<AtomicUsize>,
}

impl GraphState for ControlState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        self.applied.fetch_add(updates.len(), Ordering::SeqCst);
        Ok(())
    }
}

struct SignalAndWait;

#[async_trait]
impl Node<ControlState> for SignalAndWait {
    async fn run(&self, state: &ControlState, _context: &NodeContext) -> Result<(), NodeError> {
        state.started.notify_one();
        if state.wait { pending().await } else { Ok(()) }
    }
}

struct ControlSuccess;

#[async_trait]
impl Node<ControlState> for ControlSuccess {
    async fn run(&self, _state: &ControlState, _context: &NodeContext) -> Result<(), NodeError> {
        Ok(())
    }
}

fn control_graph() -> Arc<CompiledGraph<ControlState>> {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", ControlSuccess)
        .expect("fork should register");
    graph
        .add_node("wait", SignalAndWait)
        .expect("wait node should register");
    graph
        .add_node("fast", ControlSuccess)
        .expect("fast node should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["wait", "fast"])
        .expect("fan-out should register");
    graph.add_edge("wait", END).add_edge("fast", END);
    Arc::new(graph.compile().expect("graph should compile"))
}

#[derive(Default)]
struct RecordingSink(Mutex<Vec<GraphEvent>>);

impl EventSink for RecordingSink {
    fn on_event(&self, event: &GraphEvent) {
        self.0
            .lock()
            .expect("sink lock should not be poisoned")
            .push(event.clone());
    }
}

async fn start_controlled_run(
    compiled: Arc<CompiledGraph<ControlState>>,
    control: RunControl,
    sink: Arc<RecordingSink>,
    started: Arc<Notify>,
    applied: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<Result<group_agent_core::RunReport<ControlState>, GraphRunError>> {
    tokio::spawn(async move {
        compiled
            .invoke_with_control(
                ControlState {
                    wait: true,
                    started,
                    applied,
                },
                RunConfig::default(),
                EventConfig::new(EventRetention::None).with_sink(sink),
                control,
            )
            .await
    })
}

fn assert_parallel_control_failure(events: &[GraphEvent], expected: impl Fn(&RunFailure) -> bool) {
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
    assert!(matches!(
        events.last(),
        Some(GraphEvent::RunFailed { failure, .. }) if expected(failure)
    ));
}

#[tokio::test(start_paused = true)]
async fn parallel_node_timeout_discards_the_superstep_batch() {
    let compiled = control_graph();
    let started = Arc::new(Notify::new());
    let applied = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(RecordingSink::default());
    let run = start_controlled_run(
        compiled,
        RunControl::new().with_node_timeout(Duration::from_secs(2)),
        Arc::clone(&sink),
        Arc::clone(&started),
        Arc::clone(&applied),
    )
    .await;
    started.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;

    let error = run
        .await
        .expect("run task should not panic")
        .expect_err("waiting node should time out");
    assert!(matches!(
        error,
        GraphRunError::NodeTimedOut {
            node_id,
            step: 2,
            timeout,
            ..
        } if node_id == NodeId::from("wait") && timeout == Duration::from_secs(2)
    ));
    assert_eq!(applied.load(Ordering::SeqCst), 1);
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                GraphEvent::NodeCompleted {
                    node_id, step: 3, ..
                } => Some(node_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        [NodeId::from("fast")]
    );
    assert!(!events.iter().any(|event| {
        matches!(
            event,
            GraphEvent::StateUpdated { step: 2 | 3, .. }
                | GraphEvent::SuperstepCompleted { superstep: 2, .. }
        )
    }));
    assert_parallel_control_failure(&events, |failure| {
        matches!(
            failure,
            RunFailure::NodeTimedOut {
                node_id,
                step: 2,
                ..
            } if node_id == &NodeId::from("wait")
        )
    });
}

#[tokio::test(start_paused = true)]
async fn parallel_run_timeout_discards_the_superstep_batch() {
    let compiled = control_graph();
    let started = Arc::new(Notify::new());
    let applied = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(RecordingSink::default());
    let run = start_controlled_run(
        compiled,
        RunControl::new().with_run_timeout(Duration::from_secs(2)),
        Arc::clone(&sink),
        Arc::clone(&started),
        Arc::clone(&applied),
    )
    .await;
    started.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;

    let error = run
        .await
        .expect("run task should not panic")
        .expect_err("run should time out");
    assert!(matches!(
        error,
        GraphRunError::RunTimedOut {
            node_id: Some(node_id),
            timeout,
            ..
        } if node_id == NodeId::from("wait") && timeout == Duration::from_secs(2)
    ));
    assert_eq!(applied.load(Ordering::SeqCst), 1);
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert_parallel_control_failure(&events, |failure| {
        matches!(failure, RunFailure::RunTimedOut { .. })
    });
}

#[tokio::test]
async fn parallel_cancellation_discards_the_superstep_batch() {
    let compiled = control_graph();
    let started = Arc::new(Notify::new());
    let applied = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(RecordingSink::default());
    let token = CancellationToken::new();
    let run = start_controlled_run(
        Arc::clone(&compiled),
        RunControl::new().with_cancellation_token(token.clone()),
        Arc::clone(&sink),
        Arc::clone(&started),
        Arc::clone(&applied),
    )
    .await;
    started.notified().await;
    token.cancel();

    let error = run
        .await
        .expect("run task should not panic")
        .expect_err("run should be cancelled");
    assert!(matches!(error, GraphRunError::Cancelled { .. }));
    assert_eq!(applied.load(Ordering::SeqCst), 1);
    {
        let events = sink.0.lock().expect("sink lock should not be poisoned");
        assert_parallel_control_failure(&events, |failure| {
            matches!(failure, RunFailure::Cancelled { .. })
        });
    }

    let recovered_applies = Arc::new(AtomicUsize::new(0));
    let report = compiled
        .invoke(ControlState {
            wait: false,
            started: Arc::new(Notify::new()),
            applied: Arc::clone(&recovered_applies),
        })
        .await
        .expect("compiled graph should remain reusable after cancellation");
    assert_eq!(report.steps(), 3);
    assert_eq!(recovered_applies.load(Ordering::SeqCst), 3);
}

#[derive(Debug)]
struct CompletionOrderState {
    left_started: Arc<Notify>,
    right_started: Arc<Notify>,
    release_left: Arc<Notify>,
    release_right: Arc<Notify>,
    batch_order: Arc<Mutex<Vec<NodeId>>>,
}

impl GraphState for CompletionOrderState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        *self
            .batch_order
            .lock()
            .expect("batch order lock should not be poisoned") = updates
            .into_iter()
            .map(|update| update.into_parts().0.leaf().clone())
            .collect();
        Ok(())
    }
}

struct CompletionFork;

#[async_trait]
impl Node<CompletionOrderState> for CompletionFork {
    async fn run(
        &self,
        _state: &CompletionOrderState,
        _context: &NodeContext,
    ) -> Result<(), NodeError> {
        Ok(())
    }
}

struct OrderedCompletionNode {
    right: bool,
}

#[async_trait]
impl Node<CompletionOrderState> for OrderedCompletionNode {
    async fn run(
        &self,
        state: &CompletionOrderState,
        _context: &NodeContext,
    ) -> Result<(), NodeError> {
        if self.right {
            state.right_started.notify_one();
            state.release_right.notified().await;
        } else {
            state.left_started.notify_one();
            state.release_left.notified().await;
        }
        Ok(())
    }
}

struct CompletionSink {
    events: Mutex<Vec<GraphEvent>>,
    right_observed: Notify,
}

impl EventSink for CompletionSink {
    fn on_event(&self, event: &GraphEvent) {
        self.events
            .lock()
            .expect("completion sink lock should not be poisoned")
            .push(event.clone());
        if matches!(
            event,
            GraphEvent::NodeCompleted { node_id, .. }
                if node_id == &NodeId::from("right")
        ) {
            self.right_observed.notify_one();
        }
    }
}

#[tokio::test]
async fn higher_index_node_can_complete_first_while_batch_order_stays_stable() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", CompletionFork)
        .expect("fork should register");
    graph
        .add_node("left", OrderedCompletionNode { right: false })
        .expect("left should register");
    graph
        .add_node("right", OrderedCompletionNode { right: true })
        .expect("right should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    graph.add_edge("left", END).add_edge("right", END);
    let compiled = Arc::new(graph.compile().expect("graph should compile"));

    let left_started = Arc::new(Notify::new());
    let right_started = Arc::new(Notify::new());
    let release_left = Arc::new(Notify::new());
    let release_right = Arc::new(Notify::new());
    let batch_order = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::new(CompletionSink {
        events: Mutex::new(Vec::new()),
        right_observed: Notify::new(),
    });
    let run = {
        let compiled = Arc::clone(&compiled);
        let state = CompletionOrderState {
            left_started: Arc::clone(&left_started),
            right_started: Arc::clone(&right_started),
            release_left: Arc::clone(&release_left),
            release_right: Arc::clone(&release_right),
            batch_order: Arc::clone(&batch_order),
        };
        let event_config = EventConfig::new(EventRetention::None)
            .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>);
        tokio::spawn(async move {
            compiled
                .invoke_with_events(state, RunConfig::default(), event_config)
                .await
        })
    };

    left_started.notified().await;
    right_started.notified().await;
    release_right.notify_one();
    sink.right_observed.notified().await;
    release_left.notify_one();
    run.await
        .expect("run task should not panic")
        .expect("run should succeed");

    let completed = sink
        .events
        .lock()
        .expect("completion sink lock should not be poisoned")
        .iter()
        .filter_map(|event| match event {
            GraphEvent::NodeCompleted {
                node_id,
                step: 2 | 3,
                ..
            } => Some(node_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(completed, [NodeId::from("right"), NodeId::from("left")]);
    assert_eq!(
        *batch_order
            .lock()
            .expect("batch order lock should not be poisoned"),
        [NodeId::from("left"), NodeId::from("right")]
    );
}

#[derive(Debug)]
struct DropState {
    sibling_started: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
    applied: Arc<AtomicUsize>,
}

impl GraphState for DropState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct DropFork;

#[async_trait]
impl Node<DropState> for DropFork {
    async fn run(&self, _state: &DropState, _context: &NodeContext) -> Result<(), NodeError> {
        Ok(())
    }
}

struct FutureDropGuard(Arc<AtomicUsize>);

impl Drop for FutureDropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingSibling;

#[async_trait]
impl Node<DropState> for PendingSibling {
    async fn run(&self, state: &DropState, _context: &NodeContext) -> Result<(), NodeError> {
        let _guard = FutureDropGuard(Arc::clone(&state.dropped));
        state.sibling_started.notify_one();
        pending().await
    }
}

struct FailAfterSiblingStarts;

#[async_trait]
impl Node<DropState> for FailAfterSiblingStarts {
    async fn run(&self, state: &DropState, _context: &NodeContext) -> Result<(), NodeError> {
        state.sibling_started.notified().await;
        Err(NodeError::message("parallel failure"))
    }
}

#[tokio::test]
async fn parallel_failure_drops_pending_sibling_future() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", DropFork)
        .expect("fork should register");
    graph
        .add_node("pending", PendingSibling)
        .expect("pending sibling should register");
    graph
        .add_node("fails", FailAfterSiblingStarts)
        .expect("failing node should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["pending", "fails"])
        .expect("fan-out should register");
    graph.add_edge("pending", END).add_edge("fails", END);
    let compiled = graph.compile().expect("graph should compile");
    let dropped = Arc::new(AtomicUsize::new(0));
    let applied = Arc::new(AtomicUsize::new(0));

    let error = compiled
        .invoke(DropState {
            sibling_started: Arc::new(Notify::new()),
            dropped: Arc::clone(&dropped),
            applied: Arc::clone(&applied),
        })
        .await
        .expect_err("parallel node should fail");
    assert!(matches!(
        error,
        GraphRunError::NodeFailed {
            node_id,
            step: 3,
            ..
        } if node_id == NodeId::from("fails")
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(applied.load(Ordering::SeqCst), 1);
}
