use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointId,
    CheckpointPolicy, CheckpointRequest, CheckpointState, CheckpointWriteError, Checkpointer,
    CheckpointerError, CodecDescriptor, END, EncodedValue, EventConfig, EventRetention, EventSink,
    ExecutionOutcome, GraphEvent, GraphRunError, GraphState, InMemoryCheckpointer,
    InterruptPayload, InterruptibleNode, Node, NodeContext, NodeError, NodeOutcome, NodeUpdate,
    ResumeConfig, RunConfig, RunControl, SnapshotError, StateError, StateGraph, ThreadId,
};
use tokio::sync::{Barrier, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct TestState {
    value: i32,
    resume_leaked: bool,
    apply_calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct TestSnapshot {
    value: i32,
    resume_leaked: bool,
    apply_calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
enum TestUpdate {
    Add(i32),
    ObserveResume(bool),
    Noop,
}

impl GraphState for TestState {
    type Update = TestUpdate;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.apply_calls.fetch_add(1, Ordering::SeqCst);
        match update {
            TestUpdate::Add(value) => self.value += value,
            TestUpdate::ObserveResume(observed) => self.resume_leaked = observed,
            TestUpdate::Noop => {}
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

impl CheckpointState for TestState {
    type Snapshot = TestSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(TestSnapshot {
            value: self.value,
            resume_leaked: self.resume_leaked,
            apply_calls: Arc::clone(&self.apply_calls),
        })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            value: snapshot.value,
            resume_leaked: snapshot.resume_leaked,
            apply_calls: Arc::clone(&snapshot.apply_calls),
        })
    }
}

fn state() -> TestState {
    TestState {
        value: 0,
        resume_leaked: false,
        apply_calls: Arc::new(AtomicUsize::new(0)),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ApprovalPrompt {
    message: &'static str,
}

struct TestCodec;

impl CheckpointCodec<TestSnapshot> for TestCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new(
            "group.tests.interrupt.snapshot",
            1,
            "group.tests.interrupt.raw-v1",
        )
    }

    fn encode_snapshot(&self, snapshot: &TestSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        let mut bytes = snapshot.value.to_le_bytes().to_vec();
        bytes.push(u8::from(snapshot.resume_leaked));
        Ok(bytes)
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<TestSnapshot, CheckpointCodecError> {
        let (resume_leaked, value_bytes) = bytes
            .split_last()
            .ok_or_else(|| CheckpointCodecError::message("empty TestSnapshot"))?;
        let value = value_bytes
            .try_into()
            .map(i32::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid TestSnapshot value"))?;
        Ok(TestSnapshot {
            value,
            resume_leaked: *resume_leaked != 0,
            apply_calls: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        if let Some(prompt) = payload.downcast_ref::<ApprovalPrompt>() {
            return Ok(EncodedValue::new(
                CodecDescriptor::new(
                    "group.tests.interrupt.approval",
                    1,
                    "group.tests.interrupt.raw-v1",
                ),
                prompt.message.as_bytes(),
            ));
        }
        if let Some(message) = payload.downcast_ref::<&'static str>() {
            return Ok(EncodedValue::new(
                CodecDescriptor::new(
                    "group.tests.interrupt.static-str",
                    1,
                    "group.tests.interrupt.raw-v1",
                ),
                message.as_bytes(),
            ));
        }
        Err(CheckpointCodecError::unsupported_interrupt(payload))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        let message = std::str::from_utf8(value.bytes())
            .map_err(|source| CheckpointCodecError::with_source("invalid UTF-8 payload", source))?;
        match value.descriptor().schema() {
            "group.tests.interrupt.approval" if message == "approve" => {
                Ok(InterruptPayload::new(ApprovalPrompt { message: "approve" }))
            }
            "group.tests.interrupt.static-str" => {
                let message = match message {
                    "again" => "again",
                    "race" => "race",
                    "parallel" => "parallel",
                    other => {
                        return Err(CheckpointCodecError::message(format!(
                            "unknown static payload `{other}`"
                        )));
                    }
                };
                Ok(InterruptPayload::new(message))
            }
            schema => Err(CheckpointCodecError::message(format!(
                "unsupported interrupt schema `{schema}`"
            ))),
        }
    }
}

fn new_store() -> Arc<InMemoryCheckpointer<TestSnapshot>> {
    Arc::new(InMemoryCheckpointer::new(TestCodec))
}

struct ApprovalNode;

#[async_trait]
impl InterruptibleNode<TestState> for ApprovalNode {
    async fn run(
        &self,
        _state: &TestState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<TestUpdate>, NodeError> {
        if let Some(value) = context.resume_value::<i32>() {
            Ok(NodeOutcome::update(TestUpdate::Add(*value)))
        } else {
            Ok(NodeOutcome::interrupt(ApprovalPrompt {
                message: "approve",
            }))
        }
    }
}

struct ObserveResumeNode;

#[async_trait]
impl Node<TestState> for ObserveResumeNode {
    async fn run(
        &self,
        _state: &TestState,
        context: &NodeContext,
    ) -> Result<TestUpdate, NodeError> {
        Ok(TestUpdate::ObserveResume(context.has_resume_value()))
    }
}

fn approval_graph() -> group_agent_core::CompiledGraph<TestState> {
    let mut graph = StateGraph::new();
    graph.set_version("interrupt-v1");
    graph
        .add_interruptible_node("approval", ApprovalNode)
        .expect("approval node should register");
    graph
        .add_node("after", ObserveResumeNode)
        .expect("after node should register");
    graph
        .add_edge(group_agent_core::START, "approval")
        .add_edge("approval", "after")
        .add_edge("after", END);
    graph.compile().expect("interrupt graph should compile")
}

fn checkpoint_config(
    thread_id: &str,
    store: &Arc<InMemoryCheckpointer<TestSnapshot>>,
) -> CheckpointConfig<TestSnapshot> {
    CheckpointConfig::new(
        thread_id,
        Arc::clone(store) as Arc<dyn Checkpointer<TestSnapshot>>,
        CheckpointPolicy::EverySuperstep,
    )
}

async fn interrupt_once(
    graph: &group_agent_core::CompiledGraph<TestState>,
    thread_id: &str,
    store: &Arc<InMemoryCheckpointer<TestSnapshot>>,
    run_config: RunConfig,
) -> group_agent_core::InterruptReport<TestState> {
    let outcome = graph
        .invoke_with_checkpoint(
            state(),
            run_config,
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config(thread_id, store),
        )
        .await
        .expect("interrupt checkpoint save should succeed");
    match outcome {
        ExecutionOutcome::Interrupted(report) => report,
        ExecutionOutcome::Completed(_) => panic!("approval node should interrupt"),
        _ => panic!("unexpected execution outcome"),
    }
}

#[tokio::test]
async fn interrupt_preserves_state_saves_current_frontier_and_emits_success_outcome() {
    let graph = approval_graph();
    let store = new_store();
    let report = interrupt_once(&graph, "interrupt", &store, RunConfig::default()).await;

    assert_eq!(report.state().value, 0);
    assert_eq!(report.state().apply_calls.load(Ordering::SeqCst), 0);
    assert_eq!(report.steps(), 0);
    assert_eq!(report.superstep(), 0);
    assert_eq!(report.interrupt().node_id().as_str(), "approval");
    assert_eq!(
        report
            .interrupt()
            .payload()
            .downcast_ref::<ApprovalPrompt>(),
        Some(&ApprovalPrompt { message: "approve" })
    );

    let checkpoint = store
        .latest(&ThreadId::from("interrupt"))
        .await
        .expect("latest should load")
        .expect("interrupted checkpoint should exist");
    assert_eq!(checkpoint.id(), report.checkpoint_id());
    assert!(checkpoint.interrupted());
    assert!(!checkpoint.completed());
    assert_eq!(checkpoint.step(), 0);
    assert_eq!(checkpoint.superstep(), 0);
    assert_eq!(
        checkpoint.next_frontier(),
        std::slice::from_ref(report.interrupt().node_id())
    );
    assert_eq!(
        checkpoint.interrupt().expect("interrupt metadata").id(),
        report.interrupt().id()
    );

    assert!(matches!(
        report.events(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::NodeStarted {
                node_id,
                step: 1,
                ..
            },
            GraphEvent::NodeInterrupted {
                node_id: interrupted_node,
                step: 1,
                ..
            },
            GraphEvent::CheckpointSaved {
                checkpoint_id,
                superstep: 0,
                step: 0,
                completed: false,
                ..
            },
            GraphEvent::RunInterrupted {
                checkpoint_id: interrupted_checkpoint,
                node_id: run_node,
                superstep: 0,
                step: 0,
                ..
            }
        ] if node_id.as_str() == "approval"
            && interrupted_node == node_id
            && run_node == node_id
            && checkpoint_id == interrupted_checkpoint
    ));
}

#[tokio::test]
async fn resume_value_reexecutes_interrupted_node_then_is_cleared() {
    let graph = approval_graph();
    let store = new_store();
    let interrupted = interrupt_once(&graph, "resume-value", &store, RunConfig::new(1)).await;
    assert_eq!(interrupted.steps(), 0);

    let outcome = graph
        .resume(
            ResumeConfig::new(
                "resume-value",
                Arc::clone(&store) as Arc<dyn Checkpointer<TestSnapshot>>,
            )
            .with_resume_value(7_i32)
            .with_run_config(RunConfig::new(2)),
        )
        .await
        .expect("resume should complete");
    let report = outcome
        .as_completed()
        .expect("resume value should let the graph complete");
    assert_eq!(report.final_state().value, 7);
    assert!(!report.final_state().resume_leaked);
    assert_eq!(report.steps(), 2);
    assert_eq!(
        report
            .visited_nodes()
            .iter()
            .map(|node| node.as_str())
            .collect::<Vec<_>>(),
        ["approval", "after"]
    );

    let history = store
        .history(&ThreadId::from("resume-value"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 3);
    assert_eq!(history[1].parent_id(), Some(interrupted.checkpoint_id()));
    assert_eq!(history[2].parent_id(), Some(history[1].id()));
    assert!(history[2].completed());
}

#[tokio::test]
async fn missing_and_unexpected_resume_values_are_rejected() {
    let graph = approval_graph();
    let store = new_store();
    let interrupted = interrupt_once(&graph, "resume-errors", &store, RunConfig::default()).await;

    let missing = graph
        .resume(ResumeConfig::new(
            "resume-errors",
            Arc::clone(&store) as Arc<dyn Checkpointer<TestSnapshot>>,
        ))
        .await
        .expect_err("interrupted checkpoint requires a value");
    assert!(matches!(
        missing,
        GraphRunError::MissingResumeValue {
            checkpoint_id,
            node_id,
            step: 0,
            ..
        } if checkpoint_id == interrupted.checkpoint_id() && node_id.as_str() == "approval"
    ));

    graph
        .resume(
            ResumeConfig::new(
                "resume-errors",
                Arc::clone(&store) as Arc<dyn Checkpointer<TestSnapshot>>,
            )
            .with_resume_value(1_i32),
        )
        .await
        .expect("valid resume should complete");
    let completed = store
        .latest(&ThreadId::from("resume-errors"))
        .await
        .expect("latest should load")
        .expect("completed checkpoint should exist");
    assert!(completed.completed());

    let unexpected = graph
        .resume(
            ResumeConfig::new(
                "resume-errors",
                store as Arc<dyn Checkpointer<TestSnapshot>>,
            )
            .with_resume_value(2_i32),
        )
        .await
        .expect_err("ordinary checkpoint must reject a resume value");
    assert!(matches!(
        unexpected,
        GraphRunError::UnexpectedResumeValue {
            checkpoint_id,
            step: 2,
            ..
        } if checkpoint_id == completed.id()
    ));
}

struct AlwaysInterrupt;

#[async_trait]
impl InterruptibleNode<TestState> for AlwaysInterrupt {
    async fn run(
        &self,
        _state: &TestState,
        _context: &NodeContext,
    ) -> Result<NodeOutcome<TestUpdate>, NodeError> {
        Ok(NodeOutcome::interrupt("again"))
    }
}

fn repeated_interrupt_graph<N>(node: N) -> group_agent_core::CompiledGraph<TestState>
where
    N: InterruptibleNode<TestState> + 'static,
{
    let mut graph = StateGraph::new();
    graph.set_version("repeat-interrupt-v1");
    graph
        .add_interruptible_node("pause", node)
        .expect("pause node should register");
    graph
        .add_edge(group_agent_core::START, "pause")
        .add_edge("pause", END);
    graph.compile().expect("graph should compile")
}

#[tokio::test]
async fn repeated_interrupts_create_new_ids_and_continuous_lineage() {
    let graph = repeated_interrupt_graph(AlwaysInterrupt);
    let store = new_store();
    let first = interrupt_once_for_graph(&graph, "repeat", &store).await;
    let second = graph
        .resume(
            ResumeConfig::new(
                "repeat",
                Arc::clone(&store) as Arc<dyn Checkpointer<TestSnapshot>>,
            )
            .with_resume_value(()),
        )
        .await
        .expect("second interrupt should save");
    let second = second
        .as_interrupted()
        .expect("node should interrupt again");
    assert_ne!(first.interrupt().id(), second.interrupt().id());
    assert_eq!(first.steps(), 0);
    assert_eq!(second.steps(), 0);

    let history = store
        .history(&ThreadId::from("repeat"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].parent_id(), Some(history[0].id()));
}

async fn interrupt_once_for_graph(
    graph: &group_agent_core::CompiledGraph<TestState>,
    thread_id: &str,
    store: &Arc<InMemoryCheckpointer<TestSnapshot>>,
) -> group_agent_core::InterruptReport<TestState> {
    let outcome = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config(thread_id, store),
        )
        .await
        .expect("interrupt should save");
    match outcome {
        ExecutionOutcome::Interrupted(report) => report,
        ExecutionOutcome::Completed(_) => panic!("node should interrupt"),
        _ => panic!("unexpected execution outcome"),
    }
}

struct BarrierInterrupt {
    resumed: Arc<Barrier>,
}

#[async_trait]
impl InterruptibleNode<TestState> for BarrierInterrupt {
    async fn run(
        &self,
        _state: &TestState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<TestUpdate>, NodeError> {
        if context.has_resume_value() {
            self.resumed.wait().await;
        }
        Ok(NodeOutcome::interrupt("race"))
    }
}

#[tokio::test]
async fn concurrent_interrupt_resume_conflicts_without_forming_a_fork() {
    let resumed = Arc::new(Barrier::new(3));
    let graph = Arc::new(repeated_interrupt_graph(BarrierInterrupt {
        resumed: Arc::clone(&resumed),
    }));
    let store = new_store();
    let base = interrupt_once_for_graph(&graph, "race", &store).await;
    let base_id = base.checkpoint_id();

    let first = {
        let graph = Arc::clone(&graph);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            graph
                .resume(
                    ResumeConfig::new("race", store as Arc<dyn Checkpointer<TestSnapshot>>)
                        .with_checkpoint_id(base_id)
                        .with_resume_value("first"),
                )
                .await
        })
    };
    let second = {
        let graph = Arc::clone(&graph);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            graph
                .resume(
                    ResumeConfig::new("race", store as Arc<dyn Checkpointer<TestSnapshot>>)
                        .with_checkpoint_id(base_id)
                        .with_resume_value("second"),
                )
                .await
        })
    };
    resumed.wait().await;

    let results = [
        first.await.expect("first task should join"),
        second.await.expect("second task should join"),
    ];
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Ok(ExecutionOutcome::Interrupted(_))))
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
        .history(&ThreadId::from("race"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].parent_id(), Some(base_id));
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

struct BlockingSaveCheckpointer {
    inner: InMemoryCheckpointer<TestSnapshot>,
    save_started: Notify,
    release_save: Notify,
}

impl BlockingSaveCheckpointer {
    fn new() -> Self {
        Self {
            inner: InMemoryCheckpointer::new(TestCodec),
            save_started: Notify::new(),
            release_save: Notify::new(),
        }
    }
}

#[async_trait]
impl Checkpointer<TestSnapshot> for BlockingSaveCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointWriteError> {
        self.save_started.notify_one();
        self.release_save.notified().await;
        self.inner.save(request).await
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<TestSnapshot>>>, CheckpointerError> {
        self.inner.latest(thread_id).await
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<TestSnapshot>>>, CheckpointerError> {
        self.inner.get(thread_id, checkpoint_id).await
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<TestSnapshot>>>, CheckpointerError> {
        self.inner.history(thread_id).await
    }
}

#[tokio::test]
async fn cancellation_during_interrupt_save_emits_failure_not_interrupted_outcome() {
    let graph = Arc::new(repeated_interrupt_graph(AlwaysInterrupt));
    let store = Arc::new(BlockingSaveCheckpointer::new());
    let sink = Arc::new(RecordingSink::default());
    let token = CancellationToken::new();
    let task = {
        let graph = Arc::clone(&graph);
        let store = Arc::clone(&store);
        let sink = Arc::clone(&sink);
        let token = token.clone();
        tokio::spawn(async move {
            graph
                .invoke_with_checkpoint(
                    state(),
                    RunConfig::default(),
                    EventConfig::new(EventRetention::None).with_sink(sink as Arc<dyn EventSink>),
                    RunControl::new().with_cancellation_token(token),
                    CheckpointConfig::new(
                        "cancel-save",
                        store as Arc<dyn Checkpointer<TestSnapshot>>,
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
                .await
        })
    };
    store.save_started.notified().await;
    token.cancel();
    let error = task
        .await
        .expect("task should join")
        .expect_err("cancellation should win");
    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            node_id: None,
            step: 0,
            ..
        }
    ));
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, GraphEvent::NodeInterrupted { .. }))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, GraphEvent::CheckpointSaved { .. }))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, GraphEvent::RunInterrupted { .. }))
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, GraphEvent::RunFailed { .. }))
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn run_timeout_remains_active_during_interrupt_save() {
    let graph = Arc::new(repeated_interrupt_graph(AlwaysInterrupt));
    let store = Arc::new(BlockingSaveCheckpointer::new());
    let task = {
        let graph = Arc::clone(&graph);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            graph
                .invoke_with_checkpoint(
                    state(),
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_run_timeout(Duration::from_secs(5)),
                    CheckpointConfig::new(
                        "timeout-save",
                        store as Arc<dyn Checkpointer<TestSnapshot>>,
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
                .await
        })
    };
    store.save_started.notified().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = task
        .await
        .expect("task should join")
        .expect_err("run timeout should stop save");
    assert!(matches!(
        error,
        GraphRunError::RunTimedOut {
            node_id: None,
            step: 0,
            timeout,
            ..
        } if timeout == Duration::from_secs(5)
    ));
}

struct FailingSaveCheckpointer;

#[async_trait]
impl Checkpointer<TestSnapshot> for FailingSaveCheckpointer {
    async fn save(
        &self,
        _request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointWriteError> {
        Err(CheckpointWriteError::Failed(CheckpointerError::message(
            "interrupt store unavailable",
        )))
    }

    async fn latest(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<TestSnapshot>>>, CheckpointerError> {
        Ok(None)
    }

    async fn history(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<TestSnapshot>>>, CheckpointerError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn interrupt_save_failure_is_an_error_not_an_interrupted_outcome() {
    let graph = repeated_interrupt_graph(AlwaysInterrupt);
    let sink = Arc::new(RecordingSink::default());
    let error = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
            RunControl::default(),
            CheckpointConfig::new(
                "save-failure",
                Arc::new(FailingSaveCheckpointer),
                CheckpointPolicy::FinalOnly,
            ),
        )
        .await
        .expect_err("failed interrupt save must fail the run");
    assert!(matches!(
        error,
        GraphRunError::CheckpointSaveFailed {
            superstep: 0,
            step: 0,
            ..
        }
    ));
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, GraphEvent::NodeInterrupted { .. }))
    );
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
            .any(|event| matches!(event, GraphEvent::RunInterrupted { .. }))
    );
}

#[tokio::test]
async fn interrupt_without_checkpoint_is_a_structured_failure() {
    let graph = repeated_interrupt_graph(AlwaysInterrupt);
    let error = graph
        .invoke(state())
        .await
        .expect_err("checkpoint-disabled interrupt must fail");
    assert!(matches!(
        error,
        GraphRunError::InterruptRequiresCheckpoint {
            node_id,
            step: 1,
            ..
        } if node_id.as_str() == "pause"
    ));
}

struct NoopNode;

#[async_trait]
impl Node<TestState> for NoopNode {
    async fn run(
        &self,
        _state: &TestState,
        _context: &NodeContext,
    ) -> Result<TestUpdate, NodeError> {
        Ok(TestUpdate::Noop)
    }
}

struct PendingSibling {
    entered: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    entered_notify: Arc<Notify>,
}

struct PendingGuard(Arc<AtomicUsize>);

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Node<TestState> for PendingSibling {
    async fn run(
        &self,
        _state: &TestState,
        _context: &NodeContext,
    ) -> Result<TestUpdate, NodeError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let _guard = PendingGuard(Arc::clone(&self.dropped));
        self.entered_notify.notify_one();
        std::future::pending::<()>().await;
        unreachable!("pending sibling is cancelled with the failed super-step")
    }
}

struct CoordinatedInterrupt {
    sibling_entered: Arc<Notify>,
}

#[async_trait]
impl InterruptibleNode<TestState> for CoordinatedInterrupt {
    async fn run(
        &self,
        _state: &TestState,
        _context: &NodeContext,
    ) -> Result<NodeOutcome<TestUpdate>, NodeError> {
        self.sibling_entered.notified().await;
        Ok(NodeOutcome::interrupt("parallel"))
    }
}

#[tokio::test]
async fn parallel_interrupt_discards_the_complete_superstep() {
    let state = state();
    let apply_calls = Arc::clone(&state.apply_calls);
    let sibling_entered = Arc::new(AtomicUsize::new(0));
    let sibling_dropped = Arc::new(AtomicUsize::new(0));
    let sibling_entered_notify = Arc::new(Notify::new());
    let mut graph = StateGraph::new();
    graph
        .add_node("prepare", NoopNode)
        .expect("prepare should register");
    graph
        .add_interruptible_node(
            "pause",
            CoordinatedInterrupt {
                sibling_entered: Arc::clone(&sibling_entered_notify),
            },
        )
        .expect("pause should register");
    graph
        .add_node(
            "sibling",
            PendingSibling {
                entered: Arc::clone(&sibling_entered),
                dropped: Arc::clone(&sibling_dropped),
                entered_notify: Arc::clone(&sibling_entered_notify),
            },
        )
        .expect("sibling should register");
    graph
        .add_edge(group_agent_core::START, "prepare")
        .add_edge("pause", END)
        .add_edge("sibling", END);
    graph
        .add_fan_out("prepare", ["pause", "sibling"])
        .expect("fan-out should register");
    let graph = graph.compile().expect("parallel graph should compile");

    let error = graph
        .invoke(state)
        .await
        .expect_err("parallel interrupt is unsupported");
    assert!(matches!(
        error,
        GraphRunError::UnsupportedParallelInterrupt {
            node_id,
            step: 2,
            ..
        } if node_id.as_str() == "pause"
    ));
    assert_eq!(
        apply_calls.load(Ordering::SeqCst),
        1,
        "only the earlier prepare super-step may commit"
    );
    assert_eq!(sibling_entered.load(Ordering::SeqCst), 1);
    assert_eq!(
        sibling_dropped.load(Ordering::SeqCst),
        1,
        "the pending sibling future must be destroyed"
    );
}

#[tokio::test]
async fn interrupt_does_not_consume_the_resume_calls_additional_step_budget() {
    let mut graph = StateGraph::new();
    graph.set_version("budget-v1");
    graph
        .add_interruptible_node("approval", ApprovalNode)
        .expect("approval should register");
    graph
        .add_edge(group_agent_core::START, "approval")
        .add_edge("approval", END);
    let graph = graph.compile().expect("graph should compile");
    let store = new_store();
    let interrupted = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("budget", &store),
        )
        .await
        .expect("interrupt should fit the initial one-step budget");
    let interrupted = interrupted
        .as_interrupted()
        .expect("approval should interrupt");
    assert_eq!(interrupted.steps(), 0);

    let completed = graph
        .resume(
            ResumeConfig::new("budget", store as Arc<dyn Checkpointer<TestSnapshot>>)
                .with_resume_value(3_i32)
                .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("one additional committed step should be allowed");
    assert_eq!(completed.steps(), 1);
    assert_eq!(completed.state().value, 3);
}
