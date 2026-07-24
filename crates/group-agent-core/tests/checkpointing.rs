use std::error::Error as _;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy,
    CheckpointRequest, CheckpointState, CheckpointWriteError, Checkpointer, CheckpointerError,
    CodecDescriptor, CompiledGraph, END, EventConfig, EventRetention, EventSink, GraphEvent,
    GraphRunError, GraphState, InMemoryCheckpointer, Node, NodeContext, NodeError, NodeId,
    NodeUpdate, RunConfig, RunControl, RunFailure, START, SnapshotError, StateError, StateGraph,
    ThreadId,
};
use tokio::sync::{Barrier, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct TestState {
    value: usize,
    fail_node: bool,
    fail_batch: bool,
    snapshot_fail_at: Option<usize>,
    snapshot_calls: Arc<AtomicUsize>,
}

#[derive(Debug, Eq, PartialEq)]
struct TestSnapshot {
    value: usize,
}

struct TestCodec;

impl CheckpointCodec<TestSnapshot> for TestCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new(
            "group.tests.checkpointing",
            1,
            "group.tests.checkpointing.le-usize-v1",
        )
    }

    fn encode_snapshot(&self, snapshot: &TestSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.value.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<TestSnapshot, CheckpointCodecError> {
        let value = bytes
            .try_into()
            .map(usize::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid TestSnapshot bytes"))?;
        Ok(TestSnapshot { value })
    }
}

fn new_store() -> Arc<InMemoryCheckpointer<TestSnapshot>> {
    Arc::new(InMemoryCheckpointer::new(TestCodec))
}

#[derive(Clone, Copy)]
struct Add(usize);

impl GraphState for TestState {
    type Update = Add;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update.0;
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        if self.fail_batch {
            return Err(StateError::message("batch merge failed"));
        }
        let total = updates
            .iter()
            .map(|update| update.update().0)
            .sum::<usize>();
        self.value += total;
        Ok(())
    }
}

impl CheckpointState for TestState {
    type Snapshot = TestSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
        if self.snapshot_fail_at == Some(self.value) {
            return Err(SnapshotError::message("snapshot failed"));
        }
        Ok(TestSnapshot { value: self.value })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(TestState {
            value: snapshot.value,
            fail_node: false,
            fail_batch: false,
            snapshot_fail_at: None,
            snapshot_calls: Arc::new(AtomicUsize::new(0)),
        })
    }
}

struct AddNode(usize);

#[async_trait]
impl Node<TestState> for AddNode {
    async fn run(&self, _state: &TestState, _context: &NodeContext) -> Result<Add, NodeError> {
        Ok(Add(self.0))
    }
}

struct MaybeFailNode;

#[async_trait]
impl Node<TestState> for MaybeFailNode {
    async fn run(&self, state: &TestState, _context: &NodeContext) -> Result<Add, NodeError> {
        if state.fail_node {
            Err(NodeError::message("node failed"))
        } else {
            Ok(Add(1))
        }
    }
}

fn state() -> TestState {
    TestState {
        value: 0,
        fail_node: false,
        fail_batch: false,
        snapshot_fail_at: None,
        snapshot_calls: Arc::new(AtomicUsize::new(0)),
    }
}

fn linear_graph() -> CompiledGraph<TestState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("one", AddNode(1))
        .expect("one should register");
    graph
        .add_node("two", AddNode(2))
        .expect("two should register");
    graph
        .add_node("three", AddNode(3))
        .expect("three should register");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    graph.compile().expect("linear graph should compile")
}

fn parallel_graph() -> CompiledGraph<TestState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("fork", AddNode(1))
        .expect("fork should register");
    graph
        .add_node("left", AddNode(2))
        .expect("left should register");
    graph
        .add_node("right", MaybeFailNode)
        .expect("right should register");
    graph.add_edge(START, "fork");
    graph
        .add_fan_out("fork", ["left", "right"])
        .expect("fan-out should register");
    graph.add_edge("left", END).add_edge("right", END);
    graph.compile().expect("parallel graph should compile")
}

fn zero_node_graph() -> CompiledGraph<TestState> {
    let mut graph = StateGraph::new();
    graph.add_edge(START, END);
    graph.compile().expect("zero-node graph should compile")
}

fn checkpoint_config(
    thread_id: &str,
    checkpointer: &Arc<InMemoryCheckpointer<TestSnapshot>>,
    policy: CheckpointPolicy,
) -> CheckpointConfig<TestSnapshot> {
    CheckpointConfig::new(
        thread_id,
        Arc::clone(checkpointer) as Arc<dyn Checkpointer<TestSnapshot>>,
        policy,
    )
}

async fn invoke_checkpointed(
    graph: &CompiledGraph<TestState>,
    initial_state: TestState,
    config: CheckpointConfig<TestSnapshot>,
) -> Result<group_agent_core::ExecutionOutcome<TestState>, GraphRunError> {
    graph
        .invoke_with_checkpoint(
            initial_state,
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            config,
        )
        .await
}

#[tokio::test]
async fn every_successful_superstep_saves_with_parent_frontier_and_completed_metadata() {
    let checkpointer = new_store();
    let report = invoke_checkpointed(
        &linear_graph(),
        state(),
        checkpoint_config("thread-a", &checkpointer, CheckpointPolicy::EverySuperstep),
    )
    .await
    .expect("run should succeed");

    let history = checkpointer
        .history(&ThreadId::from("thread-a"))
        .await
        .expect("history query should succeed");
    assert_eq!(history.len(), 3);
    assert_eq!(
        history
            .iter()
            .map(|checkpoint| checkpoint.superstep())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        history
            .iter()
            .map(|checkpoint| checkpoint.step())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(history[0].next_frontier(), [NodeId::from("two")]);
    assert_eq!(history[1].next_frontier(), [NodeId::from("three")]);
    assert!(history[2].next_frontier().is_empty());
    assert!(!history[0].completed());
    assert!(!history[1].completed());
    assert!(history[2].completed());
    assert_eq!(history[0].parent_id(), None);
    assert_eq!(history[1].parent_id(), Some(history[0].id()));
    assert_eq!(history[2].parent_id(), Some(history[1].id()));
    assert_eq!(history[0].snapshot().value, 1);
    assert_eq!(history[1].snapshot().value, 3);
    assert_eq!(history[2].snapshot().value, 6);
    let restored =
        TestState::restore(history[2].snapshot()).expect("snapshot should reconstruct state");
    assert_eq!(restored.value, 6);
    assert!(
        history
            .iter()
            .all(|checkpoint| checkpoint.run_id() == report.run_id())
    );

    let latest = checkpointer
        .latest(&ThreadId::from("thread-a"))
        .await
        .expect("latest query should succeed")
        .expect("latest checkpoint should exist");
    assert!(Arc::ptr_eq(&latest, &history[2]));
    assert!(Arc::ptr_eq(latest.snapshot(), history[2].snapshot()));
    let checkpoint_events = report
        .events()
        .iter()
        .filter(|event| matches!(event, GraphEvent::CheckpointSaved { .. }))
        .count();
    assert_eq!(checkpoint_events, 3);
}

#[tokio::test]
async fn final_only_policy_saves_one_completed_checkpoint() {
    let checkpointer = new_store();
    invoke_checkpointed(
        &linear_graph(),
        state(),
        checkpoint_config("final-only", &checkpointer, CheckpointPolicy::FinalOnly),
    )
    .await
    .expect("run should succeed");

    let history = checkpointer
        .history(&ThreadId::from("final-only"))
        .await
        .expect("history query should succeed");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].superstep(), 3);
    assert_eq!(history[0].step(), 3);
    assert!(history[0].completed());
    assert!(history[0].next_frontier().is_empty());
}

#[tokio::test]
async fn zero_node_graph_saves_one_completed_checkpoint_for_both_policies() {
    for (thread, policy) in [
        ("zero-every", CheckpointPolicy::EverySuperstep),
        ("zero-final", CheckpointPolicy::FinalOnly),
    ] {
        let checkpointer = new_store();
        let report = invoke_checkpointed(
            &zero_node_graph(),
            state(),
            checkpoint_config(thread, &checkpointer, policy),
        )
        .await
        .expect("zero-node run should succeed");

        let history = checkpointer
            .history(&ThreadId::from(thread))
            .await
            .expect("history query should succeed");
        assert_eq!(history.len(), 1);
        let checkpoint = &history[0];
        assert_eq!(checkpoint.parent_id(), None);
        assert_eq!(checkpoint.superstep(), 0);
        assert_eq!(checkpoint.step(), 0);
        assert!(checkpoint.next_frontier().is_empty());
        assert!(checkpoint.completed());
        assert_eq!(report.steps(), 0);
        assert_eq!(
            report.events(),
            [
                GraphEvent::RunStarted {
                    run_id: report.run_id(),
                    max_steps: RunConfig::default().max_steps,
                },
                GraphEvent::CheckpointSaved {
                    run_id: report.run_id(),
                    checkpoint_id: checkpoint.id(),
                    thread_id: ThreadId::from(thread),
                    superstep: 0,
                    step: 0,
                    completed: true,
                },
                GraphEvent::RunCompleted {
                    run_id: report.run_id(),
                    steps: 0,
                },
            ]
        );
    }
}

#[tokio::test]
async fn successive_runs_on_one_thread_extend_one_parent_chain() {
    let checkpointer = new_store();
    let graph = linear_graph();
    let first = invoke_checkpointed(
        &graph,
        state(),
        checkpoint_config("continued", &checkpointer, CheckpointPolicy::EverySuperstep),
    )
    .await
    .expect("first run should succeed");
    let base = checkpointer
        .latest(&ThreadId::from("continued"))
        .await
        .expect("latest query should succeed")
        .expect("first run should create a checkpoint")
        .id();
    let second = invoke_checkpointed(
        &graph,
        state(),
        checkpoint_config("continued", &checkpointer, CheckpointPolicy::EverySuperstep)
            .with_expected_parent(Some(base)),
    )
    .await
    .expect("second run should succeed");

    let history = checkpointer
        .history(&ThreadId::from("continued"))
        .await
        .expect("history query should succeed");
    assert_eq!(history.len(), 6);
    assert_eq!(history[3].parent_id(), Some(history[2].id()));
    assert_eq!(history[0].run_id(), first.run_id());
    assert_eq!(history[3].run_id(), second.run_id());
    assert_ne!(first.run_id(), second.run_id());
}

#[tokio::test]
async fn explicit_no_parent_does_not_attach_a_new_run_to_existing_latest() {
    let checkpointer = new_store();
    let graph = zero_node_graph();
    invoke_checkpointed(
        &graph,
        state(),
        checkpoint_config(
            "explicit-root",
            &checkpointer,
            CheckpointPolicy::EverySuperstep,
        ),
    )
    .await
    .expect("first root run should succeed");

    let error = invoke_checkpointed(
        &graph,
        state(),
        checkpoint_config(
            "explicit-root",
            &checkpointer,
            CheckpointPolicy::EverySuperstep,
        ),
    )
    .await
    .expect_err("a second explicit root must conflict");
    let history = checkpointer
        .history(&ThreadId::from("explicit-root"))
        .await
        .expect("history query should succeed");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].parent_id(), None);
    assert!(matches!(
        error,
        GraphRunError::CheckpointConflict {
            ref thread_id,
            superstep: 0,
            step: 0,
            expected_parent: None,
            actual_parent: Some(actual),
            ..
        } if thread_id == &ThreadId::from("explicit-root") && actual == history[0].id()
    ));
}

#[tokio::test]
async fn thread_histories_are_isolated_including_concurrent_runs() {
    let checkpointer = new_store();
    let graph = linear_graph();
    let (first, second) = tokio::join!(
        invoke_checkpointed(
            &graph,
            state(),
            checkpoint_config(
                "concurrent-a",
                &checkpointer,
                CheckpointPolicy::EverySuperstep,
            ),
        ),
        invoke_checkpointed(
            &graph,
            state(),
            checkpoint_config(
                "concurrent-b",
                &checkpointer,
                CheckpointPolicy::EverySuperstep,
            ),
        ),
    );
    let first = first.expect("first run should succeed");
    let second = second.expect("second run should succeed");
    assert_ne!(first.run_id(), second.run_id());

    let first_history = checkpointer
        .history(&ThreadId::from("concurrent-a"))
        .await
        .expect("first history should load");
    let second_history = checkpointer
        .history(&ThreadId::from("concurrent-b"))
        .await
        .expect("second history should load");
    assert_eq!(first_history.len(), 3);
    assert_eq!(second_history.len(), 3);
    assert!(
        first_history
            .iter()
            .all(|checkpoint| checkpoint.thread_id() == &ThreadId::from("concurrent-a"))
    );
    assert!(
        second_history
            .iter()
            .all(|checkpoint| checkpoint.thread_id() == &ThreadId::from("concurrent-b"))
    );
}

#[tokio::test]
async fn failed_parallel_superstep_and_batch_merge_create_no_new_checkpoint() {
    let checkpointer = new_store();
    let graph = parallel_graph();

    let mut node_failure = state();
    node_failure.fail_node = true;
    let node_error = invoke_checkpointed(
        &graph,
        node_failure,
        checkpoint_config(
            "node-failure",
            &checkpointer,
            CheckpointPolicy::EverySuperstep,
        ),
    )
    .await;
    assert!(matches!(node_error, Err(GraphRunError::NodeFailed { .. })));
    let node_history = checkpointer
        .history(&ThreadId::from("node-failure"))
        .await
        .expect("history should load");
    assert_eq!(node_history.len(), 1);
    assert_eq!(node_history[0].superstep(), 1);

    let mut batch_failure = state();
    batch_failure.fail_batch = true;
    let batch_error = invoke_checkpointed(
        &graph,
        batch_failure,
        checkpoint_config(
            "batch-failure",
            &checkpointer,
            CheckpointPolicy::EverySuperstep,
        ),
    )
    .await
    .expect_err("batch merge should fail");
    assert!(matches!(
        batch_error,
        GraphRunError::StateBatchUpdateFailed { .. }
    ));
    let batch_history = checkpointer
        .history(&ThreadId::from("batch-failure"))
        .await
        .expect("history should load");
    assert_eq!(batch_history.len(), 1);
    assert_eq!(batch_history[0].snapshot().value, 1);
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

struct OrderedConflictCheckpointer {
    inner: Arc<InMemoryCheckpointer<TestSnapshot>>,
    arrived: Barrier,
    first_committed: Notify,
}

#[async_trait]
impl Checkpointer<TestSnapshot> for OrderedConflictCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointWriteError> {
        let first_committed = self.first_committed.notified();
        let is_first = request.snapshot().value == 1;
        self.arrived.wait().await;
        if is_first {
            let result = self.inner.save(request).await;
            self.first_committed.notify_waiters();
            result
        } else {
            first_committed.await;
            self.inner.save(request).await
        }
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<TestSnapshot>>>, CheckpointerError> {
        self.inner.latest(thread_id).await
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<TestSnapshot>>>, CheckpointerError> {
        self.inner.history(thread_id).await
    }
}

#[tokio::test]
async fn concurrent_runs_on_one_thread_conflict_without_crossing_parent_chains() {
    let inner = new_store();
    let checkpointer = Arc::new(OrderedConflictCheckpointer {
        inner: Arc::clone(&inner),
        arrived: Barrier::new(2),
        first_committed: Notify::new(),
    });
    let graph = zero_node_graph();
    let sink = Arc::new(RecordingSink::default());
    let config = || {
        CheckpointConfig::new(
            "shared-thread",
            Arc::clone(&checkpointer) as Arc<dyn Checkpointer<TestSnapshot>>,
            CheckpointPolicy::EverySuperstep,
        )
    };
    let mut first_state = state();
    first_state.value = 1;
    let mut second_state = state();
    second_state.value = 2;

    let (first, second) = tokio::join!(
        graph.invoke_with_checkpoint(
            first_state,
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            config(),
        ),
        graph.invoke_with_checkpoint(
            second_state,
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
            RunControl::default(),
            config(),
        ),
    );
    let first = first.expect("designated first save should succeed");
    let error = second.expect_err("later save should conflict");

    let history = inner
        .history(&ThreadId::from("shared-thread"))
        .await
        .expect("history query should succeed");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].parent_id(), None);
    assert_eq!(history[0].run_id(), first.run_id());
    assert_eq!(history[0].snapshot().value, 1);
    assert!(matches!(
        error,
        GraphRunError::CheckpointConflict {
            ref thread_id,
            superstep: 0,
            step: 0,
            expected_parent: None,
            actual_parent: Some(actual),
            ..
        } if thread_id == &ThreadId::from("shared-thread") && actual == history[0].id()
    ));
    {
        let events = sink.0.lock().expect("sink lock should not be poisoned");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GraphEvent::RunFailed { .. }))
                .count(),
            1
        );
        assert!(matches!(
            events.last(),
            Some(GraphEvent::RunFailed {
                failure: RunFailure::CheckpointConflict {
                    expected_parent: None,
                    actual_parent: Some(actual),
                    ..
                },
                ..
            }) if *actual == history[0].id()
        ));
    }
    let report = graph
        .invoke(state())
        .await
        .expect("compiled graph should remain reusable after conflict");
    assert_eq!(report.steps(), 0);
}

struct FailingCheckpointer {
    saves: AtomicUsize,
}

#[async_trait]
impl Checkpointer<TestSnapshot> for FailingCheckpointer {
    async fn save(
        &self,
        _request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointWriteError> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        Err(CheckpointerError::message("storage unavailable").into())
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

struct PendingSaveCheckpointer {
    started: Notify,
    release: Notify,
    dropped: AtomicUsize,
}

struct SaveDropGuard<'a> {
    dropped: &'a AtomicUsize,
    completed: bool,
}

impl Drop for SaveDropGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl Checkpointer<TestSnapshot> for PendingSaveCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointWriteError> {
        let mut guard = SaveDropGuard {
            dropped: &self.dropped,
            completed: false,
        };
        self.started.notify_waiters();
        self.release.notified().await;
        guard.completed = true;
        Ok(Arc::new(request.into_checkpoint()))
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

struct TimedSaveCheckpointer {
    delay: Duration,
}

#[async_trait]
impl Checkpointer<TestSnapshot> for TimedSaveCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointWriteError> {
        tokio::time::sleep(self.delay).await;
        Ok(Arc::new(request.into_checkpoint()))
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

#[derive(Debug)]
struct RootStorageError;

impl fmt::Display for RootStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("root storage error")
    }
}

impl std::error::Error for RootStorageError {}

#[derive(Debug)]
struct StorageLayerError {
    source: RootStorageError,
}

impl fmt::Display for StorageLayerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("storage layer error")
    }
}

impl std::error::Error for StorageLayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

struct SourceFailingCheckpointer;

#[async_trait]
impl Checkpointer<TestSnapshot> for SourceFailingCheckpointer {
    async fn save(
        &self,
        _request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointWriteError> {
        Err(CheckpointerError::with_source(
            "checkpoint adapter failed",
            StorageLayerError {
                source: RootStorageError,
            },
        )
        .into())
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
async fn save_failure_returns_structured_error_and_one_terminal_failure_event() {
    let checkpointer = Arc::new(FailingCheckpointer {
        saves: AtomicUsize::new(0),
    });
    let sink = Arc::new(RecordingSink::default());
    let config = CheckpointConfig::new(
        "save-failure",
        Arc::clone(&checkpointer) as Arc<dyn Checkpointer<TestSnapshot>>,
        CheckpointPolicy::EverySuperstep,
    );
    let graph = linear_graph();
    let error = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
            RunControl::default(),
            config,
        )
        .await
        .expect_err("save should fail");

    match &error {
        GraphRunError::CheckpointSaveFailed {
            thread_id,
            superstep: 1,
            step: 1,
            source,
            ..
        } => {
            assert_eq!(thread_id, &ThreadId::from("save-failure"));
            assert_eq!(source.as_message(), "storage unavailable");
        }
        other => panic!("unexpected error: {other}"),
    }
    assert_eq!(
        error
            .source()
            .expect("run error should expose checkpointer error")
            .to_string(),
        "storage unavailable"
    );
    assert_eq!(checkpointer.saves.load(Ordering::SeqCst), 1);
    {
        let events = sink.0.lock().expect("sink lock should not be poisoned");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GraphEvent::RunFailed { .. }))
                .count(),
            1
        );
        assert!(!events.iter().any(|event| {
            matches!(
                event,
                GraphEvent::CheckpointSaved { .. } | GraphEvent::RunCompleted { .. }
            )
        }));
        assert!(matches!(
            events.last(),
            Some(GraphEvent::RunFailed {
                failure: RunFailure::CheckpointSaveFailed {
                    thread_id,
                    superstep: 1,
                    step: 1,
                },
                ..
            }) if thread_id == &ThreadId::from("save-failure")
        ));
    }
    let report = graph
        .invoke(state())
        .await
        .expect("compiled graph should remain reusable after save failure");
    assert_eq!(report.final_state().value, 6);
}

#[tokio::test]
async fn checkpoint_save_failure_preserves_the_complete_source_chain() {
    let error = zero_node_graph()
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "source-chain",
                Arc::new(SourceFailingCheckpointer) as Arc<dyn Checkpointer<TestSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("storage should fail");
    let checkpointer = error
        .source()
        .expect("run error should expose checkpointer error");
    assert_eq!(checkpointer.to_string(), "checkpoint adapter failed");
    let layer = checkpointer
        .source()
        .expect("checkpointer error should expose storage layer");
    assert_eq!(layer.to_string(), "storage layer error");
    assert_eq!(
        layer
            .source()
            .expect("storage layer should expose root source")
            .to_string(),
        "root storage error"
    );
}

#[tokio::test]
async fn zero_node_snapshot_and_save_failures_have_terminal_events_without_completion() {
    let graph = zero_node_graph();

    let snapshot_sink = Arc::new(RecordingSink::default());
    let mut snapshot_state = state();
    snapshot_state.snapshot_fail_at = Some(0);
    let snapshot_store = new_store();
    let snapshot_error = graph
        .invoke_with_checkpoint(
            snapshot_state,
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&snapshot_sink) as Arc<dyn EventSink>),
            RunControl::default(),
            checkpoint_config(
                "zero-snapshot-failure",
                &snapshot_store,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("zero-node snapshot should fail");
    assert!(matches!(
        snapshot_error,
        GraphRunError::SnapshotFailed {
            superstep: 0,
            step: 0,
            ..
        }
    ));
    {
        let snapshot_events = snapshot_sink
            .0
            .lock()
            .expect("sink lock should not be poisoned");
        assert!(matches!(
            snapshot_events.as_slice(),
            [
                GraphEvent::RunStarted { .. },
                GraphEvent::RunFailed {
                    failure: RunFailure::SnapshotFailed {
                        superstep: 0,
                        step: 0,
                        ..
                    },
                    ..
                },
            ]
        ));
    }

    let save_sink = Arc::new(RecordingSink::default());
    let failing = Arc::new(FailingCheckpointer {
        saves: AtomicUsize::new(0),
    });
    let save_error = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&save_sink) as Arc<dyn EventSink>),
            RunControl::default(),
            CheckpointConfig::new(
                "zero-save-failure",
                failing as Arc<dyn Checkpointer<TestSnapshot>>,
                CheckpointPolicy::FinalOnly,
            ),
        )
        .await
        .expect_err("zero-node save should fail");
    assert!(matches!(
        save_error,
        GraphRunError::CheckpointSaveFailed {
            superstep: 0,
            step: 0,
            ..
        }
    ));
    let save_events = save_sink
        .0
        .lock()
        .expect("sink lock should not be poisoned");
    assert!(matches!(
        save_events.as_slice(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::RunFailed {
                failure: RunFailure::CheckpointSaveFailed {
                    superstep: 0,
                    step: 0,
                    ..
                },
                ..
            },
        ]
    ));
}

#[tokio::test]
async fn zero_node_cancel_and_timeout_fail_before_snapshot_and_completion() {
    let graph = zero_node_graph();
    let checkpointer = new_store();
    let cancelled_sink = Arc::new(RecordingSink::default());
    let token = CancellationToken::new();
    token.cancel();
    let cancelled = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&cancelled_sink) as Arc<dyn EventSink>),
            RunControl::new().with_cancellation_token(token),
            checkpoint_config(
                "zero-cancelled",
                &checkpointer,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("pre-cancelled zero-node run should fail");
    assert!(matches!(
        cancelled,
        GraphRunError::Cancelled {
            node_id: None,
            step: 0,
            ..
        }
    ));
    {
        let cancelled_events = cancelled_sink
            .0
            .lock()
            .expect("sink lock should not be poisoned");
        assert!(matches!(
            cancelled_events.as_slice(),
            [
                GraphEvent::RunStarted { .. },
                GraphEvent::RunFailed {
                    failure: RunFailure::Cancelled {
                        node_id: None,
                        step: 0,
                    },
                    ..
                },
            ]
        ));
    }

    let timeout_sink = Arc::new(RecordingSink::default());
    let timed_out = graph
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&timeout_sink) as Arc<dyn EventSink>),
            RunControl::new().with_run_timeout(Duration::ZERO),
            checkpoint_config("zero-timeout", &checkpointer, CheckpointPolicy::FinalOnly),
        )
        .await
        .expect_err("zero-duration run should time out");
    assert!(matches!(
        timed_out,
        GraphRunError::RunTimedOut {
            node_id: None,
            step: 0,
            timeout: Duration::ZERO,
            ..
        }
    ));
    let timeout_events = timeout_sink
        .0
        .lock()
        .expect("sink lock should not be poisoned");
    assert!(matches!(
        timeout_events.as_slice(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::RunFailed {
                failure: RunFailure::RunTimedOut {
                    node_id: None,
                    step: 0,
                    timeout: Duration::ZERO,
                },
                ..
            },
        ]
    ));
}

#[tokio::test]
async fn cancellation_during_save_uses_checkpoint_boundary_and_drops_save_future() {
    let graph = Arc::new(linear_graph());
    let checkpointer = Arc::new(PendingSaveCheckpointer {
        started: Notify::new(),
        release: Notify::new(),
        dropped: AtomicUsize::new(0),
    });
    let started = checkpointer.started.notified();
    let sink = Arc::new(RecordingSink::default());
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let checkpointer = Arc::clone(&checkpointer);
        let sink = Arc::clone(&sink);
        let token = token.clone();
        async move {
            graph
                .invoke_with_checkpoint(
                    state(),
                    RunConfig::default(),
                    EventConfig::new(EventRetention::None).with_sink(sink as Arc<dyn EventSink>),
                    RunControl::new().with_cancellation_token(token),
                    CheckpointConfig::new(
                        "cancel-during-save",
                        checkpointer as Arc<dyn Checkpointer<TestSnapshot>>,
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
                .await
        }
    });
    started.await;
    token.cancel();
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("cancellation should win");
    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            node_id: None,
            step: 1,
            ..
        }
    ));
    assert_eq!(checkpointer.dropped.load(Ordering::SeqCst), 1);
    {
        let events = sink.0.lock().expect("sink lock should not be poisoned");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GraphEvent::RunFailed { .. }))
                .count(),
            1
        );
        assert!(matches!(
            events.last(),
            Some(GraphEvent::RunFailed {
                failure: RunFailure::Cancelled {
                    node_id: None,
                    step: 1,
                },
                ..
            })
        ));
        assert!(!events.iter().any(|event| {
            matches!(
                event,
                GraphEvent::CheckpointSaved { .. } | GraphEvent::RunCompleted { .. }
            )
        }));
    }

    let report = graph
        .invoke(state())
        .await
        .expect("compiled graph should remain reusable");
    assert_eq!(report.final_state().value, 6);
}

#[tokio::test(start_paused = true)]
async fn run_timeout_during_save_uses_checkpoint_boundary_context() {
    let graph = Arc::new(linear_graph());
    let checkpointer = Arc::new(PendingSaveCheckpointer {
        started: Notify::new(),
        release: Notify::new(),
        dropped: AtomicUsize::new(0),
    });
    let started = checkpointer.started.notified();
    let sink = Arc::new(RecordingSink::default());
    let task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let checkpointer = Arc::clone(&checkpointer);
        let sink = Arc::clone(&sink);
        async move {
            graph
                .invoke_with_checkpoint(
                    state(),
                    RunConfig::default(),
                    EventConfig::new(EventRetention::None).with_sink(sink as Arc<dyn EventSink>),
                    RunControl::new().with_run_timeout(Duration::from_secs(5)),
                    CheckpointConfig::new(
                        "timeout-during-save",
                        checkpointer as Arc<dyn Checkpointer<TestSnapshot>>,
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
                .await
        }
    });
    started.await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("run timeout should win");
    assert!(matches!(
        error,
        GraphRunError::RunTimedOut {
            node_id: None,
            step: 1,
            timeout,
            ..
        } if timeout == Duration::from_secs(5)
    ));
    assert_eq!(checkpointer.dropped.load(Ordering::SeqCst), 1);
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, GraphEvent::RunFailed { .. }))
            .count(),
        1
    );
    assert!(!events.iter().any(|event| {
        matches!(
            event,
            GraphEvent::CheckpointSaved { .. } | GraphEvent::RunCompleted { .. }
        )
    }));
}

#[tokio::test]
async fn cancellation_wins_when_save_and_cancellation_are_ready_together() {
    let graph = Arc::new(zero_node_graph());
    let checkpointer = Arc::new(PendingSaveCheckpointer {
        started: Notify::new(),
        release: Notify::new(),
        dropped: AtomicUsize::new(0),
    });
    let started = checkpointer.started.notified();
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let checkpointer = Arc::clone(&checkpointer);
        let token = token.clone();
        async move {
            graph
                .invoke_with_checkpoint(
                    state(),
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(token),
                    CheckpointConfig::new(
                        "simultaneous-cancel-save",
                        checkpointer as Arc<dyn Checkpointer<TestSnapshot>>,
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
                .await
        }
    });
    started.await;
    token.cancel();
    checkpointer.release.notify_waiters();
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("cancellation should beat a ready save");
    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            node_id: None,
            step: 0,
            ..
        }
    ));
    assert_eq!(checkpointer.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn cancellation_wins_when_save_run_timeout_and_cancellation_are_all_ready() {
    let graph = Arc::new(zero_node_graph());
    let checkpointer = Arc::new(PendingSaveCheckpointer {
        started: Notify::new(),
        release: Notify::new(),
        dropped: AtomicUsize::new(0),
    });
    let started = checkpointer.started.notified();
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let checkpointer = Arc::clone(&checkpointer);
        let token = token.clone();
        async move {
            graph
                .invoke_with_checkpoint(
                    state(),
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new()
                        .with_cancellation_token(token)
                        .with_run_timeout(Duration::from_secs(5)),
                    CheckpointConfig::new(
                        "triple-ready",
                        checkpointer as Arc<dyn Checkpointer<TestSnapshot>>,
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
                .await
        }
    });
    started.await;
    checkpointer.release.notify_waiters();
    token.cancel();
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = task
        .await
        .expect("run task should not panic")
        .expect_err("cancellation should have highest priority");
    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            node_id: None,
            step: 0,
            ..
        }
    ));
    assert_eq!(checkpointer.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn run_timeout_wins_when_save_result_has_the_same_deadline() {
    let checkpointer: Arc<dyn Checkpointer<TestSnapshot>> = Arc::new(TimedSaveCheckpointer {
        delay: Duration::from_secs(5),
    });
    let error = zero_node_graph()
        .invoke_with_checkpoint(
            state(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::new().with_run_timeout(Duration::from_secs(5)),
            CheckpointConfig::new(
                "simultaneous-timeout-save",
                checkpointer,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("run timeout should beat a simultaneously ready save");
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

#[tokio::test]
async fn snapshot_failure_preserves_prior_checkpoint_and_source_context() {
    let checkpointer = new_store();
    let sink = Arc::new(RecordingSink::default());
    let mut initial_state = state();
    initial_state.snapshot_fail_at = Some(3);
    let error = linear_graph()
        .invoke_with_checkpoint(
            initial_state,
            RunConfig::default(),
            EventConfig::new(EventRetention::None)
                .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
            RunControl::default(),
            checkpoint_config(
                "snapshot-failure",
                &checkpointer,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("second snapshot should fail");
    assert!(matches!(
        error,
        GraphRunError::SnapshotFailed {
            ref thread_id,
            superstep: 2,
            step: 2,
            ..
        } if thread_id == &ThreadId::from("snapshot-failure")
    ));
    assert_eq!(
        error
            .source()
            .expect("run error should expose snapshot error")
            .to_string(),
        "snapshot failed"
    );
    let history = checkpointer
        .history(&ThreadId::from("snapshot-failure"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].snapshot().value, 1);
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert!(matches!(
        events.last(),
        Some(GraphEvent::RunFailed {
            failure: RunFailure::SnapshotFailed {
                superstep: 2,
                step: 2,
                ..
            },
            ..
        })
    ));
}

#[tokio::test]
async fn ordinary_invoke_never_creates_a_snapshot_or_enters_storage() {
    let snapshot_calls = Arc::new(AtomicUsize::new(0));
    let report = linear_graph()
        .invoke(TestState {
            value: 0,
            fail_node: false,
            fail_batch: false,
            snapshot_fail_at: Some(1),
            snapshot_calls: Arc::clone(&snapshot_calls),
        })
        .await
        .expect("disabled checkpoint path should ignore snapshot capability");
    assert_eq!(report.final_state().value, 6);
    assert_eq!(snapshot_calls.load(Ordering::SeqCst), 0);
    assert!(
        !report
            .events()
            .iter()
            .any(|event| matches!(event, GraphEvent::CheckpointSaved { .. }))
    );
}
