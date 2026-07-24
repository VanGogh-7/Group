use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy, CheckpointState,
    Checkpointer, CodecDescriptor, END, EventConfig, EventRetention, EventSink, GraphEvent,
    GraphRunError, GraphState, GraphVersion, InMemoryCheckpointer, InterruptibleNode, Node,
    NodeContext, NodeError, NodeId, NodeOutcome, NodePath, NodeUpdate, ResumeConfig, RouteError,
    RunConfig, RunControl, RunFailure, START, SnapshotError, StateError, StateGraph, ThreadId,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default, Eq, PartialEq)]
struct FanOutState {
    value: u64,
    observations: Vec<(&'static str, u64)>,
    applied: Vec<&'static str>,
}

enum Update {
    Add(u64),
    Branch(&'static str, u64),
    Join,
}

impl GraphState for FanOutState {
    type Update = Update;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        match update {
            Update::Add(value) => self.value += value,
            Update::Branch(source, observed) => {
                self.observations.push((source, observed));
                self.applied.push(source);
            }
            Update::Join => self.applied.push("join"),
        }
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        for update in updates {
            let (_, update) = update.into_parts();
            self.apply(update)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct FanOutSnapshot {
    value: u64,
}

impl CheckpointState for FanOutState {
    type Snapshot = FanOutSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(FanOutSnapshot { value: self.value })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            value: snapshot.value,
            ..Self::default()
        })
    }
}

struct SnapshotCodec;

impl CheckpointCodec<FanOutSnapshot> for SnapshotCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.tests.conditional-fan-out", 1, "raw-u64-le")
    }

    fn encode_snapshot(&self, snapshot: &FanOutSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.value.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<FanOutSnapshot, CheckpointCodecError> {
        let bytes = <[u8; 8]>::try_from(bytes)
            .map_err(|_| CheckpointCodecError::message("invalid fan-out snapshot"))?;
        Ok(FanOutSnapshot {
            value: u64::from_le_bytes(bytes),
        })
    }
}

struct Add(u64);

#[async_trait]
impl Node<FanOutState> for Add {
    async fn run(&self, _state: &FanOutState, _context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::Add(self.0))
    }
}

struct Observe(&'static str);

#[async_trait]
impl Node<FanOutState> for Observe {
    async fn run(&self, state: &FanOutState, _context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::Branch(self.0, state.value))
    }
}

struct Join;

#[async_trait]
impl Node<FanOutState> for Join {
    async fn run(&self, _state: &FanOutState, _context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::Join)
    }
}

fn fan_out_graph() -> group_agent_core::CompiledGraph<FanOutState> {
    let mut graph = StateGraph::new();
    graph.set_version("conditional-fan-out-v1");
    graph
        .add_node("router", Add(1))
        .expect("router should register");
    graph
        .add_node("alpha", Observe("alpha"))
        .expect("alpha should register");
    graph
        .add_node("beta", Observe("beta"))
        .expect("beta should register");
    graph.add_node("join", Join).expect("join should register");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["alpha", "beta", END], |state: &FanOutState| {
            assert_eq!(state.value, 1, "router must observe the committed update");
            Ok(vec![
                NodeId::end(),
                NodeId::from("beta"),
                NodeId::from("alpha"),
            ])
        })
        .expect("conditional fan-out should register");
    graph
        .add_edge("alpha", "join")
        .add_edge("beta", "join")
        .add_edge("join", END);
    graph.compile().expect("fan-out graph should compile")
}

#[tokio::test]
async fn conditional_fan_out_is_stable_reads_new_state_and_fans_in_once() {
    let report = fan_out_graph()
        .invoke(FanOutState::default())
        .await
        .expect("conditional fan-out should succeed");

    assert_eq!(
        report.visited_nodes(),
        [
            NodePath::from("router"),
            NodePath::from("alpha"),
            NodePath::from("beta"),
            NodePath::from("join"),
        ]
    );
    assert_eq!(
        report.final_state().observations,
        [("alpha", 1), ("beta", 1)]
    );
    assert_eq!(report.final_state().applied, ["alpha", "beta", "join"]);
    let selected = report
        .events()
        .iter()
        .find_map(|event| match event {
            GraphEvent::RoutesSelected {
                source, targets, ..
            } => Some((source, targets)),
            _ => None,
        })
        .expect("fan-out route event should be emitted");
    assert_eq!(selected.0, &NodePath::from("router"));
    assert_eq!(
        selected.1,
        &[
            NodePath::from("alpha"),
            NodePath::from("beta"),
            NodePath::from(END),
        ]
    );
}

#[tokio::test]
async fn one_selected_target_behaves_as_a_singleton_frontier() {
    let mut graph = StateGraph::new();
    graph.add_node("router", Add(1)).expect("router");
    graph.add_node("only", Join).expect("only");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["only", END], |_| Ok(vec![NodeId::from("only")]))
        .expect("fan-out");
    graph.add_edge("only", END);

    let report = graph
        .compile()
        .expect("compile")
        .invoke(FanOutState::default())
        .await
        .expect("invoke");
    assert_eq!(
        report.visited_nodes(),
        [NodePath::from("router"), NodePath::from("only")]
    );
    assert!(
        report
            .events()
            .iter()
            .any(|event| matches!(event, GraphEvent::RoutesSelected { targets, .. } if targets == &[NodePath::from("only")]))
    );
    assert!(
        !report
            .events()
            .iter()
            .any(|event| matches!(event, GraphEvent::SuperstepStarted { .. }))
    );
}

#[derive(Default)]
struct RecordingSink(Mutex<Vec<GraphEvent>>);

impl EventSink for RecordingSink {
    fn on_event(&self, event: &GraphEvent) {
        self.0.lock().expect("sink lock").push(event.clone());
    }
}

async fn invoke_invalid_selection(selected: Vec<NodeId>) -> (GraphRunError, Arc<RecordingSink>) {
    let mut graph = StateGraph::new();
    graph.add_node("router", Add(0)).expect("router");
    graph.add_node("allowed", Join).expect("allowed");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["allowed", END], move |_| Ok(selected.clone()))
        .expect("fan-out");
    graph.add_edge("allowed", END);
    let sink = Arc::new(RecordingSink::default());
    let event_sink: Arc<dyn EventSink> = sink.clone();
    let error = graph
        .compile()
        .expect("compile")
        .invoke_with_control(
            FanOutState::default(),
            RunConfig::default(),
            EventConfig::new(EventRetention::None).with_sink(event_sink),
            RunControl::default(),
        )
        .await
        .expect_err("selection should fail");
    (error, sink)
}

#[tokio::test]
async fn empty_duplicate_and_undeclared_results_are_structured_and_emit_no_route_event() {
    let (empty, empty_sink) = invoke_invalid_selection(Vec::new()).await;
    assert!(matches!(
        empty,
        GraphRunError::EmptyRouteTargets { node_id, step: 1 }
            if node_id == NodePath::from("router")
    ));

    let (duplicate, duplicate_sink) =
        invoke_invalid_selection(vec![NodeId::from("allowed"), NodeId::from("allowed")]).await;
    assert!(matches!(
        duplicate,
        GraphRunError::DuplicateRouteTarget {
            node_id,
            target,
            step: 1,
        } if node_id == NodePath::from("router") && target == NodePath::from("allowed")
    ));

    let (invalid, invalid_sink) = invoke_invalid_selection(vec![NodeId::from("undeclared")]).await;
    assert!(matches!(
        invalid,
        GraphRunError::InvalidRouteTarget {
            node_id,
            target,
            step: 1,
        } if node_id == NodePath::from("router") && target == NodePath::from("undeclared")
    ));

    assert!(matches!(
        empty_sink.0.lock().expect("sink lock").last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::EmptyRouteTargets { step: 1, .. },
            ..
        })
    ));
    assert!(matches!(
        duplicate_sink.0.lock().expect("sink lock").last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::DuplicateRouteTarget { step: 1, .. },
            ..
        })
    ));
    assert!(matches!(
        invalid_sink.0.lock().expect("sink lock").last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::InvalidRouteTarget { step: 1, .. },
            ..
        })
    ));

    for sink in [empty_sink, duplicate_sink, invalid_sink] {
        let events = sink.0.lock().expect("sink lock");
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, GraphEvent::RoutesSelected { .. }))
        );
        assert!(matches!(events.last(), Some(GraphEvent::RunFailed { .. })));
    }
}

#[tokio::test]
async fn router_error_preserves_source_and_emits_no_routes_selected() {
    let mut graph = StateGraph::new();
    graph.add_node("router", Add(0)).expect("router");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", [END], |_| {
            Err(RouteError::message("fan-out routing failed"))
        })
        .expect("fan-out");
    let sink = Arc::new(RecordingSink::default());
    let event_sink: Arc<dyn EventSink> = sink.clone();
    let error = graph
        .compile()
        .expect("compile")
        .invoke_with_events(
            FanOutState::default(),
            RunConfig::default(),
            EventConfig::new(EventRetention::None).with_sink(event_sink),
        )
        .await
        .expect_err("router should fail");
    assert!(matches!(
        error,
        GraphRunError::RouteFailed { source, .. }
            if source.as_message() == "fan-out routing failed"
    ));
    assert!(
        !sink
            .0
            .lock()
            .expect("sink lock")
            .iter()
            .any(|event| matches!(event, GraphEvent::RoutesSelected { .. }))
    );
    assert!(matches!(
        sink.0.lock().expect("sink lock").last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::RouteFailed { step: 1, .. },
            ..
        })
    ));
}

#[tokio::test]
async fn router_error_does_not_poison_a_reusable_compiled_graph() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut graph = StateGraph::new();
    graph.add_node("router", Add(1)).expect("router");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", [END], {
            let calls = Arc::clone(&calls);
            move |_| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(RouteError::message("first routing attempt fails"))
                } else {
                    Ok(vec![NodeId::end()])
                }
            }
        })
        .expect("fan-out");
    let compiled = graph.compile().expect("compile");

    assert!(matches!(
        compiled.invoke(FanOutState::default()).await,
        Err(GraphRunError::RouteFailed { .. })
    ));
    let report = compiled
        .invoke(FanOutState::default())
        .await
        .expect("same compiled graph should remain reusable");
    assert_eq!(report.final_state().value, 1);
}

#[tokio::test]
async fn max_steps_never_executes_a_partial_selected_frontier() {
    let error = fan_out_graph()
        .invoke_with_config(FanOutState::default(), RunConfig::new(2))
        .await
        .expect_err("two-node frontier does not fit remaining budget");
    assert!(matches!(
        error,
        GraphRunError::MaxStepsExceeded {
            max_steps: 2,
            node_id,
            step: 3,
        } if node_id == NodePath::from("beta")
    ));
}

#[tokio::test]
async fn checkpoint_and_resume_preserve_a_conditional_multi_node_frontier() {
    let graph = fan_out_graph();
    let store = Arc::new(InMemoryCheckpointer::new(SnapshotCodec));
    let checkpointer: Arc<dyn Checkpointer<FanOutSnapshot>> = store.clone();
    let setup_error = graph
        .invoke_with_checkpoint(
            FanOutState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "conditional-fan-out-resume",
                Arc::clone(&checkpointer),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("setup should stop before the selected frontier");
    assert!(matches!(
        setup_error,
        GraphRunError::MaxStepsExceeded { step: 2, .. }
    ));

    let checkpoint = store
        .latest(&ThreadId::from("conditional-fan-out-resume"))
        .await
        .expect("latest query")
        .expect("frontier checkpoint");
    assert_eq!(
        checkpoint.next_frontier(),
        [NodePath::from("alpha"), NodePath::from("beta")]
    );
    assert_eq!(
        checkpoint.graph_version(),
        Some(&GraphVersion::from("conditional-fan-out-v1"))
    );

    let outcome = graph
        .resume(
            ResumeConfig::new("conditional-fan-out-resume", checkpointer)
                .with_checkpoint_id(checkpoint.id())
                .with_run_config(RunConfig::new(3)),
        )
        .await
        .expect("resume should complete");
    assert_eq!(
        outcome.visited_nodes(),
        [
            NodePath::from("alpha"),
            NodePath::from("beta"),
            NodePath::from("join"),
        ]
    );
    assert_eq!(
        outcome.final_state().observations,
        [("alpha", 1), ("beta", 1)]
    );
}

#[derive(Debug)]
struct ControlState {
    batch_commits: Arc<AtomicUsize>,
}

impl GraphState for ControlState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        Ok(())
    }

    fn apply_batch(&mut self, _updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        self.batch_commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Immediate;

#[async_trait]
impl Node<ControlState> for Immediate {
    async fn run(&self, _state: &ControlState, _context: &NodeContext) -> Result<(), NodeError> {
        Ok(())
    }
}

struct Delayed;

#[async_trait]
impl Node<ControlState> for Delayed {
    async fn run(&self, _state: &ControlState, _context: &NodeContext) -> Result<(), NodeError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}

fn controlled_fan_out_graph() -> group_agent_core::CompiledGraph<ControlState> {
    let mut graph = StateGraph::new();
    graph.add_node("router", Immediate).expect("router");
    graph.add_node("left", Delayed).expect("left");
    graph.add_node("right", Delayed).expect("right");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["left", "right"], |_| {
            Ok(vec![NodeId::from("right"), NodeId::from("left")])
        })
        .expect("fan-out");
    graph.add_edge("left", END).add_edge("right", END);
    graph.compile().expect("controlled graph should compile")
}

#[tokio::test]
async fn run_and_node_timeouts_discard_the_selected_parallel_batch() {
    for control in [
        RunControl::new().with_run_timeout(Duration::from_millis(10)),
        RunControl::new().with_node_timeout(Duration::from_millis(10)),
    ] {
        let batch_commits = Arc::new(AtomicUsize::new(0));
        let error = controlled_fan_out_graph()
            .invoke_with_control(
                ControlState {
                    batch_commits: Arc::clone(&batch_commits),
                },
                RunConfig::default(),
                EventConfig::default(),
                control,
            )
            .await
            .expect_err("timeout should stop the selected frontier");
        assert!(matches!(
            error,
            GraphRunError::RunTimedOut { .. } | GraphRunError::NodeTimedOut { .. }
        ));
        assert_eq!(batch_commits.load(Ordering::SeqCst), 0);
    }
}

struct CancelOnRoutesSelected {
    token: CancellationToken,
}

impl EventSink for CancelOnRoutesSelected {
    fn on_event(&self, event: &GraphEvent) {
        if matches!(event, GraphEvent::RoutesSelected { .. }) {
            self.token.cancel();
        }
    }
}

#[tokio::test]
async fn cancellation_after_route_selection_prevents_the_parallel_frontier() {
    let token = CancellationToken::new();
    let sink: Arc<dyn EventSink> = Arc::new(CancelOnRoutesSelected {
        token: token.clone(),
    });
    let batch_commits = Arc::new(AtomicUsize::new(0));
    let error = controlled_fan_out_graph()
        .invoke_with_control(
            ControlState {
                batch_commits: Arc::clone(&batch_commits),
            },
            RunConfig::default(),
            EventConfig::new(EventRetention::None).with_sink(sink),
            RunControl::new().with_cancellation_token(token),
        )
        .await
        .expect_err("cancellation should stop before the selected frontier");
    assert!(matches!(error, GraphRunError::Cancelled { step: 1, .. }));
    assert_eq!(batch_commits.load(Ordering::SeqCst), 0);
}

struct Interrupt;

#[async_trait]
impl InterruptibleNode<ControlState> for Interrupt {
    async fn run(
        &self,
        _state: &ControlState,
        _context: &NodeContext,
    ) -> Result<NodeOutcome<()>, NodeError> {
        Ok(NodeOutcome::interrupt("approval required"))
    }
}

#[tokio::test]
async fn interrupt_in_a_conditionally_selected_parallel_frontier_remains_unsupported() {
    let mut graph = StateGraph::new();
    graph.add_node("router", Immediate).expect("router");
    graph
        .add_interruptible_node("interrupt", Interrupt)
        .expect("interrupt");
    graph.add_node("sibling", Delayed).expect("sibling");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["interrupt", "sibling"], |_| {
            Ok(vec![NodeId::from("sibling"), NodeId::from("interrupt")])
        })
        .expect("fan-out");
    graph.add_edge("interrupt", END).add_edge("sibling", END);

    let batch_commits = Arc::new(AtomicUsize::new(0));
    let error = graph
        .compile()
        .expect("compile")
        .invoke(ControlState {
            batch_commits: Arc::clone(&batch_commits),
        })
        .await
        .expect_err("parallel interrupt should fail");
    assert!(matches!(
        error,
        GraphRunError::UnsupportedParallelInterrupt { node_id, step: 2, .. }
            if node_id == NodePath::from("interrupt")
    ));
    assert_eq!(batch_commits.load(Ordering::SeqCst), 0);
}
