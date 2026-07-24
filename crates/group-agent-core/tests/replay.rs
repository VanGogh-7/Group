use std::error::Error as _;
use std::fmt;
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointId,
    CheckpointPolicy, CheckpointRequest, CheckpointState, CheckpointWriteError, Checkpointer,
    CheckpointerError, CodecDescriptor, CompiledGraph, END, EncodedValue, EventConfig,
    EventRetention, EventSink, GraphEvent, GraphRunError, GraphState, InMemoryCheckpointer,
    InterruptPayload, InterruptibleNode, Node, NodeContext, NodeError, NodeId, NodeOutcome,
    NodePath, NodeUpdate, ReplayConfig, RunConfig, RunControl, RunFailure, START, SnapshotError,
    StateError, StateGraph, ThreadId,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct ReplayState {
    value: usize,
    observations: Vec<(&'static str, usize)>,
    applied: Vec<&'static str>,
    fail_restore: bool,
    restore_calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct ReplaySnapshot {
    value: usize,
    fail_restore: bool,
    restore_calls: Arc<AtomicUsize>,
}

enum Update {
    Add(usize),
    Observe(&'static str, usize),
    Join,
}

impl GraphState for ReplayState {
    type Update = Update;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        match update {
            Update::Add(value) => self.value += value,
            Update::Observe(source, value) => {
                self.observations.push((source, value));
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

impl CheckpointState for ReplayState {
    type Snapshot = ReplaySnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(ReplaySnapshot {
            value: self.value,
            fail_restore: self.fail_restore,
            restore_calls: Arc::clone(&self.restore_calls),
        })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        snapshot.restore_calls.fetch_add(1, Ordering::SeqCst);
        if snapshot.fail_restore {
            return Err(SnapshotError::with_source(
                "replay restore failed",
                LayerError {
                    message: "restore layer",
                    source: RootError("restore root"),
                },
            ));
        }
        Ok(Self {
            value: snapshot.value,
            restore_calls: Arc::clone(&snapshot.restore_calls),
            ..Self::default()
        })
    }
}

struct Codec;

impl CheckpointCodec<ReplaySnapshot> for Codec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.tests.replay", 1, "raw-usize-bool-v1")
    }

    fn encode_snapshot(&self, snapshot: &ReplaySnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        let mut bytes = snapshot.value.to_le_bytes().to_vec();
        bytes.push(u8::from(snapshot.fail_restore));
        Ok(bytes)
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<ReplaySnapshot, CheckpointCodecError> {
        let (fail_restore, value) = bytes
            .split_last()
            .ok_or_else(|| CheckpointCodecError::message("empty replay snapshot"))?;
        let value = value
            .try_into()
            .map(usize::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid replay snapshot"))?;
        Ok(ReplaySnapshot {
            value,
            fail_restore: *fail_restore != 0,
            restore_calls: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let message = payload
            .downcast_ref::<&'static str>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        Ok(EncodedValue::new(
            CodecDescriptor::new("group.tests.replay.interrupt", 1, "raw-usize-bool-v1"),
            message.as_bytes(),
        ))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        let expected = CodecDescriptor::new("group.tests.replay.interrupt", 1, "raw-usize-bool-v1");
        if value.descriptor() != &expected {
            return Err(CheckpointCodecError::message(
                "invalid replay interrupt descriptor",
            ));
        }
        let message = std::str::from_utf8(value.bytes())
            .map_err(|source| CheckpointCodecError::with_source("invalid interrupt", source))?;
        Ok(InterruptPayload::new(message.to_owned()))
    }
}

fn store() -> Arc<InMemoryCheckpointer<ReplaySnapshot>> {
    Arc::new(InMemoryCheckpointer::new(Codec))
}

fn checkpoint_config(
    thread: &str,
    store: &Arc<InMemoryCheckpointer<ReplaySnapshot>>,
) -> CheckpointConfig<ReplaySnapshot> {
    CheckpointConfig::new(
        thread,
        Arc::clone(store) as Arc<dyn Checkpointer<ReplaySnapshot>>,
        CheckpointPolicy::EverySuperstep,
    )
}

fn replay_config(
    thread: &str,
    checkpoint_id: CheckpointId,
    store: &Arc<InMemoryCheckpointer<ReplaySnapshot>>,
) -> ReplayConfig<ReplaySnapshot> {
    ReplayConfig::new(
        thread,
        checkpoint_id,
        Arc::clone(store) as Arc<dyn Checkpointer<ReplaySnapshot>>,
    )
}

struct Add(usize);

#[async_trait]
impl Node<ReplayState> for Add {
    async fn run(&self, _state: &ReplayState, _context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::Add(self.0))
    }
}

struct Observe(&'static str);

#[async_trait]
impl Node<ReplayState> for Observe {
    async fn run(&self, state: &ReplayState, _context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::Observe(self.0, state.value))
    }
}

struct Join;

#[async_trait]
impl Node<ReplayState> for Join {
    async fn run(&self, _state: &ReplayState, _context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::Join)
    }
}

fn linear_graph() -> CompiledGraph<ReplayState> {
    let mut graph = StateGraph::new();
    graph.set_version("replay-linear-v1");
    graph.add_node("one", Add(1)).expect("one");
    graph.add_node("two", Add(2)).expect("two");
    graph.add_node("three", Add(3)).expect("three");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    graph.compile().expect("linear graph")
}

fn fan_out_graph() -> CompiledGraph<ReplayState> {
    let mut graph = StateGraph::new();
    graph.set_version("replay-fan-out-v1");
    graph.add_node("router", Add(1)).expect("router");
    graph.add_node("alpha", Observe("alpha")).expect("alpha");
    graph.add_node("beta", Observe("beta")).expect("beta");
    graph.add_node("join", Join).expect("join");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["alpha", "beta"], |_| {
            Ok(vec![NodeId::from("beta"), NodeId::from("alpha")])
        })
        .expect("conditional fan-out");
    graph
        .add_edge("alpha", "join")
        .add_edge("beta", "join")
        .add_edge("join", END);
    graph.compile().expect("fan-out graph")
}

fn nested_graph() -> CompiledGraph<ReplayState> {
    let mut child = StateGraph::new();
    child.add_node("one", Add(1)).expect("one");
    child.add_node("two", Add(2)).expect("two");
    child
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);

    let mut root = StateGraph::new();
    root.set_version("replay-nested-v1");
    root.add_subgraph("child", child.compile().expect("child"))
        .expect("mount");
    root.add_edge(START, "child").add_edge("child", END);
    root.compile().expect("nested graph")
}

async fn complete_history(
    graph: &CompiledGraph<ReplayState>,
    thread: &str,
    store: &Arc<InMemoryCheckpointer<ReplaySnapshot>>,
) -> Vec<Arc<Checkpoint<ReplaySnapshot>>> {
    graph
        .invoke_with_checkpoint(
            ReplayState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config(thread, store),
        )
        .await
        .expect("checkpoint setup should complete");
    store
        .history(&ThreadId::from(thread))
        .await
        .expect("history")
}

fn history_shape(
    checkpoints: &[Arc<Checkpoint<ReplaySnapshot>>],
) -> Vec<(CheckpointId, Option<CheckpointId>, usize, usize, bool)> {
    checkpoints
        .iter()
        .map(|checkpoint| {
            (
                checkpoint.id(),
                checkpoint.parent_id(),
                checkpoint.step(),
                checkpoint.superstep(),
                checkpoint.completed(),
            )
        })
        .collect()
}

#[tokio::test]
async fn non_latest_replay_succeeds_and_leaves_latest_history_and_lineage_unchanged() {
    let graph = linear_graph();
    let store = store();
    let before = complete_history(&graph, "historical", &store).await;
    let source = Arc::clone(&before[0]);
    let latest_id = before.last().expect("latest").id();
    let before_shape = history_shape(&before);

    let report = graph
        .replay(replay_config("historical", source.id(), &store).with_run_config(RunConfig::new(2)))
        .await
        .expect("non-latest replay");

    assert_ne!(report.run_id(), source.run_id());
    assert_eq!(report.source_thread_id(), &ThreadId::from("historical"));
    assert_eq!(report.source_checkpoint_id(), source.id());
    assert_eq!((report.source_step(), report.source_superstep()), (1, 1));
    assert_eq!(report.final_state().value, 6);
    assert_eq!(report.steps(), 3);
    assert_eq!(
        report.visited_nodes(),
        [NodePath::from("two"), NodePath::from("three")]
    );
    assert!(matches!(
        report.events(),
        [
            GraphEvent::RunStarted { run_id, .. },
            GraphEvent::ReplayStarted {
                run_id: replay_id,
                source_checkpoint_id,
                step: 1,
                superstep: 1,
                ..
            },
            ..
        ] if run_id == replay_id
            && *run_id == report.run_id()
            && *source_checkpoint_id == source.id()
    ));
    assert!(matches!(
        report.events().last(),
        Some(GraphEvent::RunCompleted { run_id, steps: 3 })
            if *run_id == report.run_id()
    ));
    assert!(
        report
            .events()
            .iter()
            .all(|event| event.run_id() == report.run_id())
    );

    let after = store
        .history(&ThreadId::from("historical"))
        .await
        .expect("history after replay");
    assert_eq!(history_shape(&after), before_shape);
    assert_eq!(
        store
            .latest(&ThreadId::from("historical"))
            .await
            .expect("latest")
            .expect("latest checkpoint")
            .id(),
        latest_id
    );
}

struct BlockingAdd {
    amount: usize,
    block_once: Arc<AtomicBool>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl Node<ReplayState> for BlockingAdd {
    async fn run(&self, _state: &ReplayState, _context: &NodeContext) -> Result<Update, NodeError> {
        if self.block_once.swap(false, Ordering::SeqCst) {
            self.started.notify_waiters();
            self.release.notified().await;
        }
        Ok(Update::Add(self.amount))
    }
}

fn blocking_graph(
    block_once: Arc<AtomicBool>,
    started: Arc<Notify>,
    release: Arc<Notify>,
) -> CompiledGraph<ReplayState> {
    let mut graph = StateGraph::new();
    graph.set_version("replay-concurrent-v1");
    graph.add_node("one", Add(1)).expect("one");
    graph
        .add_node(
            "two",
            BlockingAdd {
                amount: 2,
                block_once,
                started,
                release,
            },
        )
        .expect("two");
    graph.add_node("three", Add(3)).expect("three");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    graph.compile().expect("blocking graph")
}

#[tokio::test]
async fn source_thread_can_advance_while_replay_is_running_without_cas_or_writes() {
    let block_once = Arc::new(AtomicBool::new(false));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let graph = Arc::new(blocking_graph(
        Arc::clone(&block_once),
        Arc::clone(&started),
        Arc::clone(&release),
    ));
    let store = store();
    let initial = complete_history(&graph, "concurrent", &store).await;
    let source_id = initial[0].id();
    let old_latest = initial.last().expect("latest").id();
    block_once.store(true, Ordering::SeqCst);
    let node_started = started.notified();

    let replay_task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let store = Arc::clone(&store);
        async move {
            graph
                .replay(
                    replay_config("concurrent", source_id, &store)
                        .with_run_config(RunConfig::new(2)),
                )
                .await
        }
    });
    node_started.await;

    let advancing = graph
        .invoke_with_checkpoint(
            ReplayState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("concurrent", &store).with_expected_parent(Some(old_latest)),
        )
        .await
        .expect("source lineage should advance");
    let advanced_run_id = advancing.run_id();
    let advanced_history = store
        .history(&ThreadId::from("concurrent"))
        .await
        .expect("advanced history");
    assert_eq!(advanced_history.len(), 6);

    release.notify_waiters();
    let replay = replay_task
        .await
        .expect("replay task")
        .expect("replay should ignore source head advancement");
    assert_eq!(replay.final_state().value, 6);
    let after = store
        .history(&ThreadId::from("concurrent"))
        .await
        .expect("history after replay");
    assert_eq!(history_shape(&after), history_shape(&advanced_history));
    assert_eq!(after.last().expect("latest").run_id(), advanced_run_id);
}

#[tokio::test]
async fn completed_checkpoint_replay_is_a_read_only_noop() {
    let graph = linear_graph();
    let store = store();
    let before = complete_history(&graph, "completed", &store).await;
    let completed = before.last().expect("completed");

    let report = graph
        .replay(replay_config("completed", completed.id(), &store))
        .await
        .expect("completed replay");
    assert_eq!(report.final_state().value, 6);
    assert_eq!(report.steps(), 3);
    assert!(report.visited_nodes().is_empty());
    assert_eq!(
        report.events(),
        [
            GraphEvent::RunStarted {
                run_id: report.run_id(),
                max_steps: RunConfig::default().max_steps,
            },
            GraphEvent::ReplayStarted {
                run_id: report.run_id(),
                source_thread_id: ThreadId::from("completed"),
                source_checkpoint_id: completed.id(),
                step: 3,
                superstep: 3,
            },
            GraphEvent::RunCompleted {
                run_id: report.run_id(),
                steps: 3,
            },
        ]
    );
    assert_eq!(
        history_shape(
            &store
                .history(&ThreadId::from("completed"))
                .await
                .expect("history")
        ),
        history_shape(&before)
    );
}

#[tokio::test]
async fn replay_uses_cumulative_positions_and_an_additional_step_budget() {
    let graph = linear_graph();
    let store = store();
    let history = complete_history(&graph, "budget", &store).await;
    let before = history_shape(&history);

    let error = graph
        .replay(replay_config("budget", history[0].id(), &store).with_run_config(RunConfig::new(1)))
        .await
        .expect_err("one additional node should stop before node three");
    assert!(matches!(
        error,
        GraphRunError::MaxStepsExceeded {
            max_steps: 1,
            ref node_id,
            step: 3,
        } if node_id == &NodePath::from("three")
    ));
    assert_eq!(
        history_shape(
            &store
                .history(&ThreadId::from("budget"))
                .await
                .expect("history")
        ),
        before
    );
}

#[tokio::test]
async fn replay_restores_conditional_fan_out_and_nested_subgraph_frontiers() {
    let fan_out = fan_out_graph();
    let fan_store = store();
    fan_out
        .invoke_with_checkpoint(
            ReplayState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("fan-out", &fan_store),
        )
        .await
        .expect_err("setup stops before parallel frontier");
    let fan_source = fan_store
        .latest(&ThreadId::from("fan-out"))
        .await
        .expect("latest")
        .expect("source");
    assert_eq!(
        fan_source.next_frontier(),
        [NodePath::from("alpha"), NodePath::from("beta")]
    );
    let fan_report = fan_out
        .replay(
            replay_config("fan-out", fan_source.id(), &fan_store)
                .with_run_config(RunConfig::new(3)),
        )
        .await
        .expect("fan-out replay");
    assert_eq!(
        fan_report.final_state().observations,
        [("alpha", 1), ("beta", 1)]
    );
    assert_eq!(fan_report.final_state().applied, ["alpha", "beta", "join"]);
    assert_eq!(fan_report.steps(), 4);
    assert_eq!(
        fan_report
            .visited_nodes()
            .iter()
            .filter(|node| node.as_str() == "join")
            .count(),
        1
    );
    assert_eq!(
        fan_store
            .history(&ThreadId::from("fan-out"))
            .await
            .expect("history")
            .len(),
        1
    );

    let nested = nested_graph();
    let nested_store = store();
    nested
        .invoke_with_checkpoint(
            ReplayState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("nested", &nested_store),
        )
        .await
        .expect_err("setup stops inside child");
    let nested_source = nested_store
        .latest(&ThreadId::from("nested"))
        .await
        .expect("latest")
        .expect("source");
    let report = nested
        .replay(
            replay_config("nested", nested_source.id(), &nested_store)
                .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("nested replay");
    assert_eq!(report.final_state().value, 3);
    assert!(matches!(
        report.events(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::ReplayStarted { .. },
            GraphEvent::SubgraphStarted { graph_path, .. },
            GraphEvent::NodeStarted { node_id, step: 2, .. },
            ..
        ] if graph_path == &group_agent_core::GraphPath::new(["child"])
            && node_id == &NodePath::new(
                &group_agent_core::GraphPath::new(["child"]),
                "two"
            )
    ));
    assert!(
        report
            .events()
            .iter()
            .any(|event| matches!(event, GraphEvent::SubgraphCompleted { .. }))
    );
    assert_eq!(
        nested_store
            .history(&ThreadId::from("nested"))
            .await
            .expect("history")
            .len(),
        1
    );
}

struct Approval;

#[async_trait]
impl InterruptibleNode<ReplayState> for Approval {
    async fn run(
        &self,
        _state: &ReplayState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<Update>, NodeError> {
        if context.has_resume_value() {
            let value = context
                .require_resume_value::<usize>()
                .map_err(|source| NodeError::with_source("invalid replay approval", source))?;
            Ok(NodeOutcome::update(Update::Add(*value)))
        } else {
            Ok(NodeOutcome::interrupt("approval required"))
        }
    }
}

struct AlwaysInterrupt;

#[async_trait]
impl InterruptibleNode<ReplayState> for AlwaysInterrupt {
    async fn run(
        &self,
        _state: &ReplayState,
        _context: &NodeContext,
    ) -> Result<NodeOutcome<Update>, NodeError> {
        Ok(NodeOutcome::interrupt("still interrupted"))
    }
}

fn interrupt_graph(always: bool) -> CompiledGraph<ReplayState> {
    let mut graph = StateGraph::new();
    graph.set_version(if always {
        "replay-repeat-interrupt-v1"
    } else {
        "replay-interrupt-v1"
    });
    if always {
        graph
            .add_interruptible_node("approval", AlwaysInterrupt)
            .expect("approval");
    } else {
        graph
            .add_interruptible_node("approval", Approval)
            .expect("approval");
    }
    graph.add_edge(START, "approval").add_edge("approval", END);
    graph.compile().expect("interrupt graph")
}

async fn interrupted_source(
    graph: &CompiledGraph<ReplayState>,
    thread: &str,
    store: &Arc<InMemoryCheckpointer<ReplaySnapshot>>,
) -> Arc<Checkpoint<ReplaySnapshot>> {
    graph
        .invoke_with_checkpoint(
            ReplayState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config(thread, store),
        )
        .await
        .expect("interrupt should save");
    store
        .latest(&ThreadId::from(thread))
        .await
        .expect("latest")
        .expect("interrupted checkpoint")
}

#[tokio::test]
async fn interrupted_replay_requires_and_consumes_a_typed_resume_value_without_writing() {
    let graph = interrupt_graph(false);
    let approval_store = store();
    let source = interrupted_source(&graph, "approval", &approval_store).await;

    let missing = graph
        .replay(replay_config("approval", source.id(), &approval_store))
        .await
        .expect_err("missing value");
    assert!(matches!(
        missing,
        GraphRunError::MissingResumeValue {
            checkpoint_id,
            step: 0,
            ..
        } if checkpoint_id == source.id()
    ));

    let wrong = graph
        .replay(
            replay_config("approval", source.id(), &approval_store).with_resume_value("approve"),
        )
        .await
        .expect_err("wrong typed value");
    assert!(matches!(wrong, GraphRunError::NodeFailed { step: 1, .. }));
    let node_error = wrong.source().expect("node source");
    let value_error = node_error.source().expect("typed value source");
    assert!(value_error.to_string().contains("type mismatch"));

    let report = graph
        .replay(replay_config("approval", source.id(), &approval_store).with_resume_value(7_usize))
        .await
        .expect("typed replay");
    assert_eq!(report.final_state().value, 7);
    assert_eq!(report.steps(), 1);
    assert!(report.events().iter().all(|event| !matches!(
        event,
        GraphEvent::CheckpointSaved { .. } | GraphEvent::RunInterrupted { .. }
    )));
    assert_eq!(
        approval_store
            .history(&ThreadId::from("approval"))
            .await
            .expect("history")
            .len(),
        1
    );

    let ordinary_store = store();
    let ordinary = complete_history(&linear_graph(), "unexpected", &ordinary_store).await;
    let unexpected = linear_graph()
        .replay(
            replay_config("unexpected", ordinary[0].id(), &ordinary_store)
                .with_resume_value(1_usize),
        )
        .await
        .expect_err("ordinary checkpoint rejects a value");
    assert!(matches!(
        unexpected,
        GraphRunError::UnexpectedResumeValue { step: 1, .. }
    ));
}

#[derive(Default)]
struct RecordingSink(Mutex<Vec<GraphEvent>>);

impl EventSink for RecordingSink {
    fn on_event(&self, event: &GraphEvent) {
        self.0.lock().expect("event lock").push(event.clone());
    }
}

#[tokio::test]
async fn a_second_interrupt_is_rejected_as_read_only_without_run_interrupted_or_save() {
    let graph = interrupt_graph(true);
    let store = store();
    let source = interrupted_source(&graph, "repeat", &store).await;
    let sink = Arc::new(RecordingSink::default());

    let error = graph
        .replay(
            replay_config("repeat", source.id(), &store)
                .with_resume_value(())
                .with_event_config(
                    EventConfig::new(EventRetention::None)
                        .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
                ),
        )
        .await
        .expect_err("read-only replay cannot persist another interrupt");
    assert!(matches!(
        error,
        GraphRunError::ReplayInterruptUnsupported {
            source_checkpoint_id,
            ref source_thread_id,
            ref node_id,
            step: 1,
            ..
        } if source_checkpoint_id == source.id()
            && source_thread_id == &ThreadId::from("repeat")
            && node_id == &NodePath::from("approval")
    ));
    {
        let events = sink.0.lock().expect("event lock");
        assert!(
            events
                .iter()
                .any(|event| matches!(event, GraphEvent::NodeInterrupted { step: 1, .. }))
        );
        assert!(matches!(
            events.last(),
            Some(GraphEvent::RunFailed {
                failure: RunFailure::ReplayInterruptUnsupported {
                    source_checkpoint_id,
                    step: 1,
                    ..
                },
                ..
            }) if *source_checkpoint_id == source.id()
        ));
        assert!(events.iter().all(|event| !matches!(
            event,
            GraphEvent::CheckpointSaved { .. } | GraphEvent::RunInterrupted { .. }
        )));
    }
    assert_eq!(
        store
            .history(&ThreadId::from("repeat"))
            .await
            .expect("history")
            .len(),
        1
    );
}

struct PendingNode;

#[async_trait]
impl Node<ReplayState> for PendingNode {
    async fn run(&self, _state: &ReplayState, _context: &NodeContext) -> Result<Update, NodeError> {
        pending().await
    }
}

fn pending_graph() -> CompiledGraph<ReplayState> {
    let mut graph = StateGraph::new();
    graph.set_version("replay-control-v1");
    graph.add_node("one", Add(1)).expect("one");
    graph.add_node("pending", PendingNode).expect("pending");
    graph
        .add_edge(START, "one")
        .add_edge("one", "pending")
        .add_edge("pending", END);
    graph.compile().expect("pending graph")
}

struct BlockingGet {
    inner: Arc<InMemoryCheckpointer<ReplaySnapshot>>,
    started: Notify,
    release: Notify,
}

#[async_trait]
impl Checkpointer<ReplaySnapshot> for BlockingGet {
    async fn save(
        &self,
        request: CheckpointRequest<ReplaySnapshot>,
    ) -> Result<Arc<Checkpoint<ReplaySnapshot>>, CheckpointWriteError> {
        self.inner.save(request).await
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<ReplaySnapshot>>>, CheckpointerError> {
        self.inner.latest(thread_id).await
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<ReplaySnapshot>>>, CheckpointerError> {
        self.started.notify_waiters();
        self.release.notified().await;
        self.inner.get(thread_id, checkpoint_id).await
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<ReplaySnapshot>>>, CheckpointerError> {
        self.inner.history(thread_id).await
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_run_timeout_and_node_timeout_cover_replay() {
    let graph = Arc::new(linear_graph());
    let inner = store();
    let source = complete_history(&graph, "load-control", &inner).await[0].id();

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = graph
        .replay(
            replay_config("load-control", source, &inner)
                .with_control(RunControl::new().with_cancellation_token(cancellation)),
        )
        .await
        .expect_err("pre-cancelled replay");
    assert!(matches!(
        cancelled,
        GraphRunError::Cancelled {
            node_id: None,
            step: 0,
            ..
        }
    ));

    let blocking = Arc::new(BlockingGet {
        inner,
        started: Notify::new(),
        release: Notify::new(),
    });
    let load_started = blocking.started.notified();
    let timeout_task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let blocking = Arc::clone(&blocking);
        async move {
            graph
                .replay(
                    ReplayConfig::new(
                        "load-control",
                        source,
                        blocking as Arc<dyn Checkpointer<ReplaySnapshot>>,
                    )
                    .with_control(RunControl::new().with_run_timeout(Duration::from_secs(5))),
                )
                .await
        }
    });
    load_started.await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let timeout = timeout_task
        .await
        .expect("timeout task")
        .expect_err("load timeout");
    assert!(matches!(
        timeout,
        GraphRunError::RunTimedOut {
            node_id: None,
            step: 0,
            timeout,
            ..
        } if timeout == Duration::from_secs(5)
    ));

    let pending = Arc::new(pending_graph());
    let pending_store = store();
    pending
        .invoke_with_checkpoint(
            ReplayState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("node-timeout", &pending_store),
        )
        .await
        .expect_err("source setup");
    let pending_source = pending_store
        .latest(&ThreadId::from("node-timeout"))
        .await
        .expect("latest")
        .expect("source")
        .id();
    let node_task = tokio::spawn({
        let pending = Arc::clone(&pending);
        let pending_store = Arc::clone(&pending_store);
        async move {
            pending
                .replay(
                    replay_config("node-timeout", pending_source, &pending_store)
                        .with_control(RunControl::new().with_node_timeout(Duration::from_secs(3))),
                )
                .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    let node_timeout = node_task
        .await
        .expect("node task")
        .expect_err("node timeout");
    assert!(matches!(
        node_timeout,
        GraphRunError::NodeTimedOut {
            ref node_id,
            step: 2,
            timeout,
            ..
        } if node_id == &NodePath::from("pending")
            && timeout == Duration::from_secs(3)
    ));
}

#[derive(Debug)]
struct RootError(&'static str);

impl fmt::Display for RootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for RootError {}

#[derive(Debug)]
struct LayerError {
    message: &'static str,
    source: RootError,
}

impl fmt::Display for LayerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for LayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

struct FailingGet;

#[async_trait]
impl Checkpointer<ReplaySnapshot> for FailingGet {
    async fn save(
        &self,
        request: CheckpointRequest<ReplaySnapshot>,
    ) -> Result<Arc<Checkpoint<ReplaySnapshot>>, CheckpointWriteError> {
        Ok(Arc::new(request.into_checkpoint()))
    }

    async fn latest(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<ReplaySnapshot>>>, CheckpointerError> {
        Ok(None)
    }

    async fn get(
        &self,
        _thread_id: &ThreadId,
        _checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<ReplaySnapshot>>>, CheckpointerError> {
        Err(CheckpointerError::with_source(
            "replay load failed",
            LayerError {
                message: "load layer",
                source: RootError("load root"),
            },
        ))
    }

    async fn history(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<ReplaySnapshot>>>, CheckpointerError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn replay_load_and_restore_failures_preserve_the_complete_source_chain() {
    let checkpoint_id = CheckpointId::new();
    let load = linear_graph()
        .replay(ReplayConfig::new(
            "load-source",
            checkpoint_id,
            Arc::new(FailingGet) as Arc<dyn Checkpointer<ReplaySnapshot>>,
        ))
        .await
        .expect_err("load should fail");
    assert!(matches!(
        load,
        GraphRunError::CheckpointLoadFailed {
            checkpoint_id: Some(actual),
            ..
        } if actual == checkpoint_id
    ));
    let checkpointer = load.source().expect("checkpointer error");
    assert_eq!(checkpointer.to_string(), "replay load failed");
    let layer = checkpointer.source().expect("load layer");
    assert_eq!(layer.to_string(), "load layer");
    assert_eq!(layer.source().expect("load root").to_string(), "load root");

    let graph = linear_graph();
    let store = store();
    graph
        .invoke_with_checkpoint(
            ReplayState {
                fail_restore: true,
                ..ReplayState::default()
            },
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("restore-source", &store),
        )
        .await
        .expect_err("source setup");
    let source = store
        .latest(&ThreadId::from("restore-source"))
        .await
        .expect("latest")
        .expect("source");
    let restore_calls = Arc::clone(&source.snapshot().restore_calls);
    let restore = graph
        .replay(replay_config("restore-source", source.id(), &store))
        .await
        .expect_err("restore should fail");
    assert!(matches!(
        restore,
        GraphRunError::RestoreFailed {
            step: 1,
            superstep: 1,
            ..
        }
    ));
    let snapshot = restore.source().expect("snapshot error");
    assert_eq!(snapshot.to_string(), "replay restore failed");
    let layer = snapshot.source().expect("restore layer");
    assert_eq!(layer.to_string(), "restore layer");
    assert_eq!(
        layer.source().expect("restore root").to_string(),
        "restore root"
    );
    assert_eq!(restore_calls.load(Ordering::SeqCst), 1);
}
