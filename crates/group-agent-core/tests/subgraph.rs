use std::error::Error as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy,
    CheckpointRequest, CheckpointState, CheckpointWriteError, Checkpointer, CheckpointerError,
    CodecDescriptor, CompiledGraph, END, EncodedValue, EventConfig, GraphBuildError,
    GraphCompileError, GraphEvent, GraphPath, GraphRunError, GraphState, InMemoryCheckpointer,
    InterruptPayload, InterruptibleNode, Node, NodeContext, NodeError, NodeId, NodeOutcome,
    NodePath, NodeUpdate, ResumeConfig, ResumeValueError, RunConfig, RunControl, RunFailure, START,
    SnapshotError, StateError, StateGraph, ThreadId,
};
use tokio::sync::{Barrier, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct SharedState {
    value: i32,
    observed: Arc<Mutex<Vec<(NodePath, i32)>>>,
    resume_leaked: bool,
}

#[derive(Debug)]
struct SharedSnapshot {
    value: i32,
    observed: Arc<Mutex<Vec<(NodePath, i32)>>>,
    resume_leaked: bool,
}

struct SharedCodec;

impl CheckpointCodec<SharedSnapshot> for SharedCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new(
            "group.tests.subgraph.snapshot",
            1,
            "group.tests.subgraph.raw-v1",
        )
    }

    fn encode_snapshot(&self, snapshot: &SharedSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        let mut bytes = snapshot.value.to_le_bytes().to_vec();
        bytes.push(u8::from(snapshot.resume_leaked));
        Ok(bytes)
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<SharedSnapshot, CheckpointCodecError> {
        let (resume_leaked, value_bytes) = bytes
            .split_last()
            .ok_or_else(|| CheckpointCodecError::message("empty SharedSnapshot"))?;
        let value = value_bytes
            .try_into()
            .map(i32::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid SharedSnapshot value"))?;
        Ok(SharedSnapshot {
            value,
            observed: Arc::new(Mutex::new(Vec::new())),
            resume_leaked: *resume_leaked != 0,
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
            CodecDescriptor::new(
                "group.tests.subgraph.static-str",
                1,
                "group.tests.subgraph.raw-v1",
            ),
            message.as_bytes(),
        ))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        if value.descriptor()
            != &CodecDescriptor::new(
                "group.tests.subgraph.static-str",
                1,
                "group.tests.subgraph.raw-v1",
            )
            || value.bytes() != b"approval required"
        {
            return Err(CheckpointCodecError::message(
                "unsupported subgraph interrupt payload",
            ));
        }
        Ok(InterruptPayload::new("approval required"))
    }
}

fn new_store() -> Arc<InMemoryCheckpointer<SharedSnapshot>> {
    Arc::new(InMemoryCheckpointer::new(SharedCodec))
}

#[derive(Debug)]
enum Update {
    Add(i32),
    ObserveResume(bool),
}

impl GraphState for SharedState {
    type Update = Update;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        match update {
            Update::Add(amount) => self.value += amount,
            Update::ObserveResume(value) => self.resume_leaked = value,
        }
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        for update in updates {
            self.apply(update.into_parts().1)?;
        }
        Ok(())
    }
}

impl CheckpointState for SharedState {
    type Snapshot = SharedSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(SharedSnapshot {
            value: self.value,
            observed: Arc::clone(&self.observed),
            resume_leaked: self.resume_leaked,
        })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            value: snapshot.value,
            observed: Arc::clone(&snapshot.observed),
            resume_leaked: snapshot.resume_leaked,
        })
    }
}

fn state() -> SharedState {
    SharedState {
        value: 0,
        observed: Arc::new(Mutex::new(Vec::new())),
        resume_leaked: false,
    }
}

struct Add(i32);

#[async_trait]
impl Node<SharedState> for Add {
    async fn run(&self, state: &SharedState, context: &NodeContext) -> Result<Update, NodeError> {
        state
            .observed
            .lock()
            .expect("observation lock should not be poisoned")
            .push((context.node_path().clone(), state.value));
        Ok(Update::Add(self.0))
    }
}

struct FailingNode;

#[async_trait]
impl Node<SharedState> for FailingNode {
    async fn run(&self, _state: &SharedState, _context: &NodeContext) -> Result<Update, NodeError> {
        Err(NodeError::message("child failed"))
    }
}

struct PendingNode {
    started: Arc<Notify>,
}

struct BarrierAdd {
    barrier: Arc<Barrier>,
}

#[async_trait]
impl Node<SharedState> for BarrierAdd {
    async fn run(&self, _state: &SharedState, _context: &NodeContext) -> Result<Update, NodeError> {
        self.barrier.wait().await;
        Ok(Update::Add(5))
    }
}

#[async_trait]
impl Node<SharedState> for PendingNode {
    async fn run(&self, _state: &SharedState, _context: &NodeContext) -> Result<Update, NodeError> {
        self.started.notify_one();
        std::future::pending::<()>().await;
        unreachable!("execution control drops the pending child future")
    }
}

struct Approval;

#[async_trait]
impl InterruptibleNode<SharedState> for Approval {
    async fn run(
        &self,
        _state: &SharedState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<Update>, NodeError> {
        if context.has_resume_value() {
            let value = context
                .require_resume_value::<String>()
                .map_err(|source| NodeError::with_source("invalid approval value", source))?;
            Ok(NodeOutcome::update(Update::Add(
                i32::try_from(value.len()).expect("test value length fits i32"),
            )))
        } else {
            Ok(NodeOutcome::interrupt("approval required"))
        }
    }
}

struct ObserveResume;

#[async_trait]
impl Node<SharedState> for ObserveResume {
    async fn run(&self, _state: &SharedState, context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::ObserveResume(context.has_resume_value()))
    }
}

fn linear_child() -> CompiledGraph<SharedState> {
    let mut child = StateGraph::new();
    child.add_node("one", Add(2)).expect("one should register");
    child.add_node("two", Add(3)).expect("two should register");
    child
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    child.compile().expect("child should compile")
}

fn parent_with_linear_child() -> CompiledGraph<SharedState> {
    let mut parent = StateGraph::new();
    parent.set_version("composed-v1");
    parent
        .add_node("prepare", Add(1))
        .expect("prepare should register");
    parent
        .add_subgraph("research", linear_child())
        .expect("subgraph should mount");
    parent
        .add_node("answer", Add(4))
        .expect("answer should register");
    parent
        .add_edge(START, "prepare")
        .add_edge("prepare", "research")
        .add_edge("research", "answer")
        .add_edge("answer", END);
    parent.compile().expect("parent should compile")
}

fn checkpoint_config(
    thread_id: &str,
    store: &Arc<InMemoryCheckpointer<SharedSnapshot>>,
) -> CheckpointConfig<SharedSnapshot> {
    CheckpointConfig::new(
        thread_id,
        Arc::clone(store) as Arc<dyn Checkpointer<SharedSnapshot>>,
        CheckpointPolicy::EverySuperstep,
    )
}

fn pending_child_graph(started: Arc<Notify>) -> CompiledGraph<SharedState> {
    let mut child = StateGraph::new();
    child
        .add_node("wait", PendingNode { started })
        .expect("wait should register");
    child.add_edge(START, "wait").add_edge("wait", END);
    let mut parent = StateGraph::new();
    parent
        .add_subgraph("child", child.compile().expect("child should compile"))
        .expect("child should mount");
    parent.add_edge(START, "child").add_edge("child", END);
    parent.compile().expect("parent should compile")
}

struct FailingSave;

#[async_trait]
impl Checkpointer<SharedSnapshot> for FailingSave {
    async fn save(
        &self,
        _request: CheckpointRequest<SharedSnapshot>,
    ) -> Result<Arc<Checkpoint<SharedSnapshot>>, CheckpointWriteError> {
        Err(CheckpointWriteError::from(CheckpointerError::message(
            "save failed",
        )))
    }

    async fn latest(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<SharedSnapshot>>>, CheckpointerError> {
        Ok(None)
    }

    async fn history(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<SharedSnapshot>>>, CheckpointerError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn subgraph_shares_state_and_parent_continues_with_global_counts() {
    let report = parent_with_linear_child()
        .invoke(state())
        .await
        .expect("composed graph should complete");

    assert_eq!(report.final_state().value, 10);
    assert_eq!(report.steps(), 4);
    assert_eq!(
        report.visited_nodes(),
        [
            NodePath::from("prepare"),
            NodePath::new(&GraphPath::new(["research"]), "one"),
            NodePath::new(&GraphPath::new(["research"]), "two"),
            NodePath::from("answer"),
        ]
    );
    let observations = report
        .final_state()
        .observed
        .lock()
        .expect("observation lock should not be poisoned");
    assert_eq!(
        observations.as_slice(),
        [
            (NodePath::from("prepare"), 0),
            (NodePath::new(&GraphPath::new(["research"]), "one"), 1,),
            (NodePath::new(&GraphPath::new(["research"]), "two"), 3,),
            (NodePath::from("answer"), 6),
        ]
    );

    let run_id = report.run_id();
    assert!(report.events().iter().all(|event| event.run_id() == run_id));
    let boundaries = report
        .events()
        .iter()
        .filter_map(|event| match event {
            GraphEvent::SubgraphStarted { graph_path, .. } => Some(("started", graph_path.clone())),
            GraphEvent::SubgraphCompleted { graph_path, .. } => {
                Some(("completed", graph_path.clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        boundaries,
        [
            ("started", GraphPath::new(["research"])),
            ("completed", GraphPath::new(["research"])),
        ]
    );
}

#[tokio::test]
async fn empty_and_two_level_nested_subgraphs_preserve_structured_paths() {
    let mut empty = StateGraph::new();
    empty.add_edge(START, END);
    let empty = empty.compile().expect("empty child should compile");

    let mut empty_parent = StateGraph::new();
    empty_parent
        .add_subgraph("empty", empty)
        .expect("empty child should mount");
    empty_parent
        .add_node("after", Add(1))
        .expect("after should register");
    empty_parent
        .add_edge(START, "empty")
        .add_edge("empty", "after")
        .add_edge("after", END);
    let report = empty_parent
        .compile()
        .expect("empty parent should compile")
        .invoke(state())
        .await
        .expect("empty child should return immediately");
    assert_eq!(report.steps(), 1);
    assert_eq!(report.visited_nodes(), [NodePath::from("after")]);

    let mut leaf = StateGraph::new();
    leaf.add_node("work", Add(7)).expect("work should register");
    leaf.add_edge(START, "work").add_edge("work", END);
    let leaf = leaf.compile().expect("leaf graph should compile");
    let mut middle = StateGraph::new();
    middle
        .add_subgraph("inner", leaf)
        .expect("inner should mount");
    middle.add_edge(START, "inner").add_edge("inner", END);
    let middle = middle.compile().expect("middle graph should compile");
    let mut root = StateGraph::new();
    root.add_subgraph("outer", middle)
        .expect("outer should mount");
    root.add_edge(START, "outer").add_edge("outer", END);
    let report = root
        .compile()
        .expect("root graph should compile")
        .invoke(state())
        .await
        .expect("nested graph should complete");
    let work = NodePath::new(&GraphPath::new(["outer", "inner"]), "work");
    assert_eq!(report.visited_nodes(), [work]);
    assert_eq!(report.final_state().value, 7);
    let boundaries = report
        .events()
        .iter()
        .filter_map(|event| match event {
            GraphEvent::SubgraphStarted { graph_path, .. } => Some(format!("+{graph_path}")),
            GraphEvent::SubgraphCompleted { graph_path, .. } => Some(format!("-{graph_path}")),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        boundaries,
        ["+/outer", "+/outer/inner", "-/outer/inner", "-/outer"]
    );
}

#[tokio::test]
async fn child_conditional_fan_out_and_fan_in_use_the_shared_runtime() {
    let mut child = StateGraph::new();
    child
        .add_node("choose", Add(1))
        .expect("choose should register");
    child
        .add_node("left", Add(2))
        .expect("left should register");
    child
        .add_node("right", Add(3))
        .expect("right should register");
    child.add_edge(START, "choose");
    child
        .add_conditional_fan_out("choose", ["left", "right", END], |state: &SharedState| {
            Ok(if state.value == 1 {
                vec![NodeId::from("right"), NodeId::from("left")]
            } else {
                vec![NodeId::end()]
            })
        })
        .expect("router should register");
    child.add_edge("left", END).add_edge("right", END);

    let mut parent = StateGraph::new();
    parent.set_version("parallel-child-v1");
    parent
        .add_subgraph("research", child.compile().expect("child should compile"))
        .expect("child should mount");
    parent.add_edge(START, "research").add_edge("research", END);
    let store = new_store();
    let outcome = parent
        .compile()
        .expect("parent should compile")
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("parallel-child", &store),
        )
        .await
        .expect("parallel child should complete");
    let report = outcome.as_completed().expect("run should complete");
    assert_eq!(report.final_state().value, 6);
    assert_eq!(report.steps(), 3);
    let namespace = GraphPath::new(["research"]);
    assert_eq!(
        report.visited_nodes(),
        [
            NodePath::new(&namespace, "choose"),
            NodePath::new(&namespace, "left"),
            NodePath::new(&namespace, "right"),
        ]
    );
    let final_checkpoint = store
        .latest(&ThreadId::from("parallel-child"))
        .await
        .expect("latest should load")
        .expect("final checkpoint should exist");
    assert_eq!(final_checkpoint.step(), 3);
    assert_eq!(final_checkpoint.superstep(), 2);
    assert!(final_checkpoint.completed());
}

#[tokio::test]
async fn checkpoint_frontier_and_resume_reenter_the_child_namespace() {
    let graph = parent_with_linear_child();
    let store = new_store();
    let error = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("subgraph-resume", &store),
        )
        .await
        .expect_err("budget should stop at first child node");
    assert!(matches!(
        error,
        GraphRunError::MaxStepsExceeded {
            node_id,
            step: 2,
            ..
        } if node_id == NodePath::new(&GraphPath::new(["research"]), "one")
    ));

    let first = store
        .latest(&ThreadId::from("subgraph-resume"))
        .await
        .expect("latest should load")
        .expect("checkpoint should exist");
    assert_eq!(
        first.next_frontier(),
        [NodePath::new(&GraphPath::new(["research"]), "one")]
    );
    assert_eq!(first.step(), 1);
    assert_eq!(first.superstep(), 1);

    let outcome = graph
        .resume(
            ResumeConfig::new(
                "subgraph-resume",
                Arc::clone(&store) as Arc<dyn Checkpointer<SharedSnapshot>>,
            )
            .with_run_config(RunConfig::new(3)),
        )
        .await
        .expect("resume should complete");
    let report = outcome
        .as_completed()
        .expect("resume should return completion");
    assert_eq!(report.final_state().value, 10);
    assert_eq!(report.steps(), 4);
    assert!(matches!(
        report.events(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::RunResumed {
                step: 1,
                superstep: 1,
                ..
            },
            GraphEvent::SubgraphStarted { graph_path, .. },
            ..
        ] if graph_path == &GraphPath::new(["research"])
    ));

    let history = store
        .history(&ThreadId::from("subgraph-resume"))
        .await
        .expect("history should load");
    assert_eq!(history[1].parent_id(), Some(first.id()));
    assert!(history.last().expect("final checkpoint").completed());
}

#[tokio::test]
async fn child_interrupt_uses_full_path_and_typed_value_only_for_reexecution() {
    let mut child = StateGraph::new();
    child
        .add_interruptible_node("approval", Approval)
        .expect("approval should register");
    child
        .add_node("child_after", ObserveResume)
        .expect("child after should register");
    child
        .add_edge(START, "approval")
        .add_edge("approval", "child_after")
        .add_edge("child_after", END);
    let child = child.compile().expect("interrupt child should compile");
    let mut parent = StateGraph::new();
    parent.set_version("subgraph-interrupt-v1");
    parent
        .add_subgraph("review", child)
        .expect("review should mount");
    parent
        .add_node("parent_after", ObserveResume)
        .expect("parent after should register");
    parent
        .add_edge(START, "review")
        .add_edge("review", "parent_after")
        .add_edge("parent_after", END);
    let graph = parent.compile().expect("interrupt parent should compile");
    let store = new_store();

    let outcome = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("child-interrupt", &store),
        )
        .await
        .expect("interrupt should save");
    let interrupted = outcome.as_interrupted().expect("child should interrupt");
    let approval_path = NodePath::new(&GraphPath::new(["review"]), "approval");
    assert_eq!(interrupted.interrupt().node_path(), &approval_path);
    assert!(matches!(
        interrupted.events(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::SubgraphStarted { graph_path, .. },
            GraphEvent::NodeStarted { node_id, .. },
            GraphEvent::NodeInterrupted {
                node_id: interrupted_node,
                ..
            },
            GraphEvent::CheckpointSaved { .. },
            GraphEvent::RunInterrupted {
                node_id: run_node,
                ..
            },
        ] if graph_path == &GraphPath::new(["review"])
            && node_id == &approval_path
            && interrupted_node == &approval_path
            && run_node == &approval_path
    ));
    assert_eq!(
        store
            .latest(&ThreadId::from("child-interrupt"))
            .await
            .expect("latest should load")
            .expect("checkpoint should exist")
            .next_frontier(),
        std::slice::from_ref(&approval_path)
    );

    let wrong_type = graph
        .resume(
            ResumeConfig::new(
                "child-interrupt",
                Arc::clone(&store) as Arc<dyn Checkpointer<SharedSnapshot>>,
            )
            .with_resume_value(42_u32),
        )
        .await
        .expect_err("wrong resume type should fail in the node");
    match wrong_type {
        GraphRunError::NodeFailed {
            node_id, source, ..
        } => {
            assert_eq!(node_id, approval_path);
            let typed = source
                .source()
                .and_then(|source| source.downcast_ref::<ResumeValueError>())
                .expect("typed source should be preserved");
            assert!(matches!(
                typed,
                ResumeValueError::TypeMismatch { expected, actual }
                    if expected.contains("String") && actual == &"u32"
            ));
        }
        other => panic!("unexpected wrong-type error: {other}"),
    }

    let outcome = graph
        .resume(
            ResumeConfig::new(
                "child-interrupt",
                Arc::clone(&store) as Arc<dyn Checkpointer<SharedSnapshot>>,
            )
            .with_resume_value(String::from("yes")),
        )
        .await
        .expect("typed resume should complete");
    let report = outcome.as_completed().expect("resume should complete");
    assert_eq!(report.final_state().value, 3);
    assert!(!report.final_state().resume_leaked);
    assert_eq!(
        report.visited_nodes(),
        [
            approval_path,
            NodePath::new(&GraphPath::new(["review"]), "child_after"),
            NodePath::from("parent_after"),
        ]
    );
}

#[tokio::test]
async fn child_failure_carries_full_path_and_never_completes_the_subgraph() {
    let mut child = StateGraph::new();
    child
        .add_node("broken", FailingNode)
        .expect("broken should register");
    child.add_edge(START, "broken").add_edge("broken", END);
    let mut parent = StateGraph::new();
    parent
        .add_subgraph("research", child.compile().expect("child should compile"))
        .expect("child should mount");
    parent.add_edge(START, "research").add_edge("research", END);
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&events);
    let error = parent
        .compile()
        .expect("parent should compile")
        .invoke_with_events(
            state(),
            RunConfig::default(),
            EventConfig::default().with_sink(Arc::new(move |event: &GraphEvent| {
                captured
                    .lock()
                    .expect("event lock should not be poisoned")
                    .push(event.clone());
            })),
        )
        .await
        .expect_err("child should fail");
    assert!(matches!(
        error,
        GraphRunError::NodeFailed { node_id, .. }
            if node_id == NodePath::new(&GraphPath::new(["research"]), "broken")
    ));
    assert!(
        !events
            .lock()
            .expect("event lock should not be poisoned")
            .iter()
            .any(|event| matches!(event, GraphEvent::SubgraphCompleted { .. }))
    );
}

#[tokio::test]
async fn child_checkpoint_failure_does_not_claim_subgraph_completion() {
    let mut child = StateGraph::new();
    child
        .add_node("work", Add(1))
        .expect("work should register");
    child.add_edge(START, "work").add_edge("work", END);
    let mut parent = StateGraph::new();
    parent.set_version("child-save-failure-v1");
    parent
        .add_subgraph("child", child.compile().expect("child should compile"))
        .expect("child should mount");
    parent.add_edge(START, "child").add_edge("child", END);
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink_events = Arc::clone(&captured);
    let error = parent
        .compile()
        .expect("parent should compile")
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::default().with_sink(Arc::new(move |event: &GraphEvent| {
                sink_events
                    .lock()
                    .expect("event lock should not be poisoned")
                    .push(event.clone());
            })),
            RunControl::default(),
            CheckpointConfig::new(
                "child-save-failure",
                Arc::new(FailingSave),
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("checkpoint save should fail");
    assert!(matches!(
        error,
        GraphRunError::CheckpointSaveFailed {
            step: 1,
            superstep: 1,
            ..
        }
    ));
    let events = captured.lock().expect("event lock should not be poisoned");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, GraphEvent::SubgraphStarted { .. }))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, GraphEvent::SubgraphCompleted { .. }))
    );
    assert!(matches!(events.last(), Some(GraphEvent::RunFailed { .. })));
}

#[tokio::test]
async fn child_inherits_cancellation() {
    let started = Arc::new(Notify::new());
    let graph = Arc::new(pending_child_graph(Arc::clone(&started)));
    let token = CancellationToken::new();
    let cancelled = {
        let graph = Arc::clone(&graph);
        let token = token.clone();
        tokio::spawn(async move {
            graph
                .invoke_with_control(
                    state(),
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                )
                .await
        })
    };
    started.notified().await;
    token.cancel();
    let error = cancelled
        .await
        .expect("cancel task should join")
        .expect_err("child should observe cancellation");
    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            node_id: Some(node_id),
            step: 1,
            ..
        } if node_id == NodePath::new(&GraphPath::new(["child"]), "wait")
    ));
}

#[tokio::test(start_paused = true)]
async fn child_node_timeout_is_classified_independently() {
    let started = Arc::new(Notify::new());
    let graph = Arc::new(pending_child_graph(Arc::clone(&started)));
    let captured = Arc::new(Mutex::new(Vec::new()));
    let timed_out = {
        let graph = Arc::clone(&graph);
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            graph
                .invoke_with_control(
                    state(),
                    RunConfig::default(),
                    EventConfig::default().with_sink(Arc::new(move |event: &GraphEvent| {
                        captured
                            .lock()
                            .expect("event lock should not be poisoned")
                            .push(event.clone());
                    })),
                    RunControl::new().with_node_timeout(Duration::from_secs(2)),
                )
                .await
        })
    };
    started.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = timed_out
        .await
        .expect("timeout task should join")
        .expect_err("child node deadline should fire");
    assert!(matches!(
        error,
        GraphRunError::NodeTimedOut {
            node_id,
            step: 1,
            timeout,
            ..
        } if node_id == NodePath::new(&GraphPath::new(["child"]), "wait")
            && timeout == Duration::from_secs(2)
    ));
    assert!(matches!(
        captured
            .lock()
            .expect("event lock should not be poisoned")
            .last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::NodeTimedOut {
                node_id,
                step: 1,
                ..
            },
            ..
        }) if node_id == &NodePath::new(&GraphPath::new(["child"]), "wait")
    ));
}

#[tokio::test(start_paused = true)]
async fn child_run_timeout_is_classified_independently() {
    let started = Arc::new(Notify::new());
    let graph = Arc::new(pending_child_graph(Arc::clone(&started)));
    let captured = Arc::new(Mutex::new(Vec::new()));
    let timed_out = {
        let graph = Arc::clone(&graph);
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            graph
                .invoke_with_control(
                    state(),
                    RunConfig::default(),
                    EventConfig::default().with_sink(Arc::new(move |event: &GraphEvent| {
                        captured
                            .lock()
                            .expect("event lock should not be poisoned")
                            .push(event.clone());
                    })),
                    RunControl::new().with_run_timeout(Duration::from_secs(2)),
                )
                .await
        })
    };
    started.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = timed_out
        .await
        .expect("timeout task should join")
        .expect_err("child run deadline should fire");
    assert!(matches!(
        error,
        GraphRunError::RunTimedOut {
            node_id: Some(node_id),
            step: 1,
            timeout,
            ..
        } if node_id == NodePath::new(&GraphPath::new(["child"]), "wait")
            && timeout == Duration::from_secs(2)
    ));
    assert!(matches!(
        captured
            .lock()
            .expect("event lock should not be poisoned")
            .last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::RunTimedOut {
                node_id: Some(node_id),
                step: 1,
                ..
            },
            ..
        }) if node_id == &NodePath::new(&GraphPath::new(["child"]), "wait")
    ));
}

#[tokio::test]
async fn concurrent_resume_inside_a_subgraph_conflicts_without_a_fork() {
    let barrier = Arc::new(Barrier::new(3));
    let mut child = StateGraph::new();
    child
        .add_node(
            "work",
            BarrierAdd {
                barrier: Arc::clone(&barrier),
            },
        )
        .expect("work should register");
    child.add_edge(START, "work").add_edge("work", END);
    let mut parent = StateGraph::new();
    parent.set_version("subgraph-cas-v1");
    parent
        .add_node("prepare", Add(1))
        .expect("prepare should register");
    parent
        .add_subgraph("child", child.compile().expect("child should compile"))
        .expect("child should mount");
    parent
        .add_edge(START, "prepare")
        .add_edge("prepare", "child")
        .add_edge("child", END);
    let graph = Arc::new(parent.compile().expect("parent should compile"));
    let store = new_store();
    graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("subgraph-cas", &store),
        )
        .await
        .expect_err("seed run should stop before child work");
    let base = store
        .latest(&ThreadId::from("subgraph-cas"))
        .await
        .expect("latest should load")
        .expect("base checkpoint should exist");
    let base_id = base.id();

    let launch = |graph: Arc<CompiledGraph<SharedState>>,
                  store: Arc<InMemoryCheckpointer<SharedSnapshot>>| {
        tokio::spawn(async move {
            graph
                .resume(
                    ResumeConfig::new(
                        "subgraph-cas",
                        store as Arc<dyn Checkpointer<SharedSnapshot>>,
                    )
                    .with_checkpoint_id(base_id),
                )
                .await
        })
    };
    let first = launch(Arc::clone(&graph), Arc::clone(&store));
    let second = launch(Arc::clone(&graph), Arc::clone(&store));
    barrier.wait().await;
    let results = [
        first.await.expect("first task should join"),
        second.await.expect("second task should join"),
    ];
    assert_eq!(
        results
            .iter()
            .filter(|result| result
                .as_ref()
                .is_ok_and(|outcome| outcome.as_completed().is_some()))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(GraphRunError::CheckpointConflict { .. })))
            .count(),
        1
    );
    let history = store
        .history(&ThreadId::from("subgraph-cas"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].parent_id(), Some(base_id));
    assert!(
        history[1].next_frontier().is_empty(),
        "the conflict must not create a fork frontier"
    );

    let reused = graph
        .resume(ResumeConfig::new(
            "subgraph-cas",
            Arc::clone(&store) as Arc<dyn Checkpointer<SharedSnapshot>>,
        ))
        .await
        .expect("completed latest should remain reusable");
    assert!(reused.as_completed().is_some());
}

#[test]
fn graph_and_node_paths_are_structured_values() {
    let graph_path = GraphPath::new(["research", "verification"]);
    let node_path = NodePath::new(&graph_path, "check");
    assert_eq!(graph_path.to_string(), "/research/verification");
    assert_eq!(node_path.to_string(), "/research/verification/check");
    assert_eq!(node_path.leaf(), &NodeId::from("check"));
    assert_eq!(node_path.graph_path(), graph_path);
    let cloned = node_path.clone();
    assert_eq!(cloned, node_path);
    let mut set = std::collections::HashSet::new();
    set.insert(node_path);
    assert!(set.contains(&cloned));
}

#[test]
fn path_display_distinguishes_dots_empty_segments_and_escaped_delimiters() {
    let dotted_segment = GraphPath::new(["research.verify"]);
    let two_segments = GraphPath::new(["research", "verify"]);
    assert_ne!(dotted_segment, two_segments);
    assert_eq!(dotted_segment.to_string(), "/research.verify");
    assert_eq!(two_segments.to_string(), "/research/verify");
    assert_ne!(dotted_segment.to_string(), two_segments.to_string());

    let empty_segment = GraphPath::new([""]);
    assert_ne!(empty_segment, GraphPath::root());
    assert_eq!(empty_segment.to_string(), "/");
    assert_eq!(GraphPath::root().to_string(), "<root>");

    let slash_segment = GraphPath::new(["a/b"]);
    let percent_text = GraphPath::new(["a%2Fb"]);
    assert_eq!(slash_segment.to_string(), "/a%2Fb");
    assert_eq!(percent_text.to_string(), "/a%252Fb");
    assert_ne!(slash_segment.to_string(), percent_text.to_string());

    let dotted_node = NodePath::from("research.verify");
    let nested_node = NodePath::new(&GraphPath::new(["research"]), "verify");
    assert_ne!(dotted_node, nested_node);
    assert_eq!(dotted_node.to_string(), "/research.verify");
    assert_eq!(nested_node.to_string(), "/research/verify");
    assert_ne!(dotted_node.to_string(), nested_node.to_string());
}

#[tokio::test]
async fn direct_end_branch_leaves_one_branch_that_can_enter_a_subgraph() {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", Add(0))
        .expect("fork should register");
    graph
        .add_node("continues", Add(1))
        .expect("continues should register");
    graph
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", [END, "continues"])
        .expect("fan-out should register");
    graph.add_edge("continues", "child").add_edge("child", END);

    let report = graph
        .compile()
        .expect("END must leave only the continuing branch")
        .invoke(state())
        .await
        .expect("valid graph should run");
    assert_eq!(report.final_state().value, 6);
    assert_eq!(
        report.visited_nodes(),
        [
            NodePath::from("fork"),
            NodePath::from("continues"),
            NodePath::new(&GraphPath::new(["child"]), "one"),
            NodePath::new(&GraphPath::new(["child"]), "two"),
        ]
    );
}

#[tokio::test]
async fn indirect_end_branch_exits_before_the_remaining_branch_enters_a_subgraph() {
    let mut graph = StateGraph::new();
    for node_id in ["fork", "ending", "continues", "bridge"] {
        graph
            .add_node(node_id, Add(1))
            .expect("node should register");
    }
    graph
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["ending", "continues"])
        .expect("fan-out should register");
    graph
        .add_edge("ending", END)
        .add_edge("continues", "bridge")
        .add_edge("bridge", "child")
        .add_edge("child", END);

    let report = graph
        .compile()
        .expect("ended branch must not remain active")
        .invoke(state())
        .await
        .expect("valid graph should run");
    assert_eq!(report.steps(), 6);
    assert_eq!(report.final_state().value, 9);
}

#[tokio::test]
async fn multiple_end_branches_can_drain_before_one_subgraph_branch() {
    let mut graph = StateGraph::new();
    for node_id in [
        "fork",
        "ends_one",
        "ends_two",
        "end_tail",
        "continues",
        "delay",
    ] {
        graph
            .add_node(node_id, Add(1))
            .expect("node should register");
    }
    graph
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", [END, "ends_one", "ends_two", "continues"])
        .expect("fan-out should register");
    graph
        .add_edge("ends_one", END)
        .add_edge("ends_two", "end_tail")
        .add_edge("end_tail", END)
        .add_edge("continues", "delay")
        .add_edge("delay", "child")
        .add_edge("child", END);

    let report = graph
        .compile()
        .expect("all ended branches must be removed")
        .invoke(state())
        .await
        .expect("valid graph should run");
    assert_eq!(report.steps(), 8);
    assert_eq!(report.final_state().value, 11);
}

#[tokio::test]
async fn conditional_fan_in_and_loop_combinations_with_end_do_not_panic() {
    let mut conditional = StateGraph::new();
    conditional
        .add_node("choice", Add(0))
        .expect("choice should register");
    conditional
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    conditional.add_edge(START, "choice");
    conditional
        .add_conditional_edges("choice", [END, "child"], |_| Ok(NodeId::from("child")))
        .expect("conditional should register");
    conditional.add_edge("child", END);
    assert_eq!(
        conditional
            .compile()
            .expect("single-target conditional execution is valid")
            .invoke(state())
            .await
            .expect("conditional graph should run")
            .final_state()
            .value,
        5
    );

    let mut fan_in = StateGraph::new();
    for node_id in ["fork", "left", "right", "join"] {
        fan_in
            .add_node(node_id, Add(1))
            .expect("fan-in node should register");
    }
    fan_in
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    fan_in.add_edge(START, "fork");
    fan_in
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    fan_in
        .add_edge("left", "join")
        .add_edge("right", "join")
        .add_edge("join", "child")
        .add_edge("child", END);
    assert!(
        fan_in
            .compile()
            .expect("fan-in leaves one active child branch")
            .invoke(state())
            .await
            .is_ok()
    );

    let mut looping = StateGraph::new();
    looping
        .add_node("fork", Add(0))
        .expect("fork should register");
    looping
        .add_node("loop", Add(1))
        .expect("loop should register");
    looping
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    looping.add_edge(START, "fork");
    looping
        .add_fan_out("fork", [END, "loop"])
        .expect("fan-out should register");
    looping
        .add_conditional_edges("loop", [END, "loop", "child"], |state: &SharedState| {
            Ok(if state.value >= 2 {
                NodeId::from("child")
            } else {
                NodeId::from("loop")
            })
        })
        .expect("loop router should register");
    looping.add_edge("child", END);
    let report = looping
        .compile()
        .expect("END plus a single looping branch is valid")
        .invoke_with_config(state(), RunConfig::new(10))
        .await
        .expect("loop should reach child");
    assert_eq!(report.final_state().value, 7);
}

#[test]
fn small_end_frontier_table_always_returns_a_compile_result() {
    for bridge_count in 0..=3 {
        let mut graph = StateGraph::new();
        graph
            .add_node("fork", Add(0))
            .expect("fork should register");
        graph
            .add_node("continues", Add(0))
            .expect("continues should register");
        graph
            .add_subgraph("child", linear_child())
            .expect("child should mount");
        graph.add_edge(START, "fork");
        graph
            .add_fan_out("fork", [END, "continues"])
            .expect("fan-out should register");

        let mut previous = NodeId::from("continues");
        for index in 0..bridge_count {
            let bridge = NodeId::from(format!("bridge_{index}"));
            graph
                .add_node(bridge.clone(), Add(0))
                .expect("bridge should register");
            graph.add_edge(previous, bridge.clone());
            previous = bridge;
        }
        graph.add_edge(previous, "child").add_edge("child", END);
        assert!(
            graph.compile().is_ok(),
            "bridge_count={bridge_count} must return Ok without querying END transitions"
        );
    }

    let mut mixed = StateGraph::new();
    mixed
        .add_node("fork", Add(0))
        .expect("fork should register");
    mixed
        .add_node("ordinary", Add(0))
        .expect("ordinary should register");
    mixed
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    mixed.add_edge(START, "fork");
    mixed
        .add_fan_out("fork", [END, "ordinary", "child"])
        .expect("fan-out should register");
    mixed.add_edge("ordinary", END).add_edge("child", END);
    assert!(matches!(
        mixed.compile(),
        Err(GraphCompileError::SubgraphInParallelFrontier { .. })
    ));
}

#[test]
fn subgraph_frontier_validation_uses_fan_out_declaration_order() {
    for _ in 0..16 {
        let mut graph = StateGraph::new();
        for node_id in [
            "fork",
            "left_source",
            "right_source",
            "left_ordinary",
            "right_ordinary",
        ] {
            graph
                .add_node(node_id, Add(0))
                .expect("node should register");
        }
        graph
            .add_subgraph("left_child", linear_child())
            .expect("left child should mount");
        graph
            .add_subgraph("right_child", linear_child())
            .expect("right child should mount");
        graph.add_edge(START, "fork");
        graph
            .add_fan_out("fork", ["left_source", "right_source"])
            .expect("root fan-out should register");
        graph
            .add_fan_out("right_source", ["right_ordinary", "right_child"])
            .expect("first invalid fan-out should register");
        graph
            .add_fan_out("left_source", ["left_ordinary", "left_child"])
            .expect("second invalid fan-out should register");
        graph
            .add_edge("left_ordinary", END)
            .add_edge("right_ordinary", END)
            .add_edge("left_child", END)
            .add_edge("right_child", END);

        assert!(matches!(
            graph.compile(),
            Err(GraphCompileError::SubgraphInParallelFrontier { node_id })
                if node_id == NodeId::from("right_child")
        ));
    }
}

#[test]
fn parent_parallel_frontier_cannot_contain_a_subgraph_mount() {
    let mut parent = StateGraph::new();
    parent
        .add_node("fork", Add(0))
        .expect("fork should register");
    parent
        .add_node("ordinary", Add(0))
        .expect("ordinary should register");
    parent
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    parent.add_edge(START, "fork");
    parent
        .add_fan_out("fork", ["ordinary", "child"])
        .expect("fan-out should register");
    parent.add_edge("ordinary", END).add_edge("child", END);
    assert!(matches!(
        parent.compile(),
        Err(GraphCompileError::SubgraphInParallelFrontier { node_id })
            if node_id == NodeId::from("child")
    ));

    let mut indirect = StateGraph::new();
    indirect
        .add_node("fork", Add(0))
        .expect("fork should register");
    indirect
        .add_node("left", Add(0))
        .expect("left should register");
    indirect
        .add_node("right", Add(0))
        .expect("right should register");
    indirect
        .add_node("right_next", Add(0))
        .expect("right next should register");
    indirect
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    indirect.add_edge(START, "fork");
    indirect
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    indirect
        .add_edge("left", "child")
        .add_edge("right", "right_next")
        .add_edge("right_next", END)
        .add_edge("child", END);
    assert!(matches!(
        indirect.compile(),
        Err(GraphCompileError::SubgraphInParallelFrontier { node_id })
            if node_id == NodeId::from("child")
    ));

    let mut after_barrier = StateGraph::new();
    for node_id in ["fork", "left", "right", "join"] {
        after_barrier
            .add_node(node_id, Add(0))
            .expect("barrier node should register");
    }
    after_barrier
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    after_barrier.add_edge(START, "fork");
    after_barrier
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    after_barrier
        .add_edge("left", "join")
        .add_edge("right", "join")
        .add_edge("join", "child")
        .add_edge("child", END);
    after_barrier
        .compile()
        .expect("a child after a deduplicating fan-in barrier is valid");
}

#[test]
fn subgraph_mount_still_obeys_transition_shape_validation() {
    let mut missing = StateGraph::new();
    missing
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    missing.add_edge(START, "child");
    assert!(matches!(
        missing.compile(),
        Err(GraphCompileError::MissingOutgoingEdge { node_id })
            if node_id == NodeId::from("child")
    ));

    let mut mixed = StateGraph::new();
    mixed
        .add_subgraph("child", linear_child())
        .expect("child should mount");
    mixed.add_edge(START, "child").add_edge("child", END);
    mixed
        .add_conditional_edges("child", [END], |_| Ok(NodeId::end()))
        .expect("conditional should register");
    assert!(matches!(
        mixed.compile(),
        Err(GraphCompileError::MixedOutgoingEdgeKinds { node_id })
            if node_id == NodeId::from("child")
    ));
}

#[test]
fn duplicate_and_reserved_subgraph_mount_names_are_rejected() {
    let mut graph = StateGraph::new();
    graph
        .add_subgraph("child", linear_child())
        .expect("first child should mount");
    let duplicate = match graph.add_subgraph("child", linear_child()) {
        Err(error) => error,
        Ok(_) => panic!("duplicate child mount should fail"),
    };
    assert_eq!(
        duplicate,
        GraphBuildError::DuplicateSubgraphMount {
            node_id: NodeId::from("child"),
        }
    );

    let mut reserved = StateGraph::new();
    let reserved_error = match reserved.add_subgraph(START, linear_child()) {
        Err(error) => error,
        Ok(_) => panic!("START cannot be a mount"),
    };
    assert_eq!(
        reserved_error,
        GraphBuildError::ReservedNodeId {
            node_id: NodeId::start(),
        }
    );
}

#[test]
fn graph_path_clone_and_node_execution_types_remain_send_sync() {
    fn assert_traits<T: Clone + Eq + std::hash::Hash + Send + Sync>() {}
    fn assert_graph<T: Send + Sync>() {}
    assert_traits::<GraphPath>();
    assert_traits::<NodePath>();
    assert_graph::<CompiledGraph<SharedState>>();
    let counter = Arc::new(AtomicUsize::new(0));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}
