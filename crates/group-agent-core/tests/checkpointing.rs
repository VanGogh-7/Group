use std::error::Error as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointConfig, CheckpointPolicy, CheckpointRequest, CheckpointState,
    Checkpointer, CheckpointerError, CompiledGraph, END, EventConfig, EventRetention, EventSink,
    GraphEvent, GraphRunError, GraphState, InMemoryCheckpointer, Node, NodeContext, NodeError,
    NodeId, NodeUpdate, RunConfig, RunControl, RunFailure, START, SnapshotError, StateError,
    StateGraph, ThreadId,
};

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
) -> Result<group_agent_core::RunReport<TestState>, GraphRunError> {
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
    let checkpointer = Arc::new(InMemoryCheckpointer::new());
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
    let checkpointer = Arc::new(InMemoryCheckpointer::new());
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
async fn successive_runs_on_one_thread_extend_one_parent_chain() {
    let checkpointer = Arc::new(InMemoryCheckpointer::new());
    let graph = linear_graph();
    let first = invoke_checkpointed(
        &graph,
        state(),
        checkpoint_config("continued", &checkpointer, CheckpointPolicy::EverySuperstep),
    )
    .await
    .expect("first run should succeed");
    let second = invoke_checkpointed(
        &graph,
        state(),
        checkpoint_config("continued", &checkpointer, CheckpointPolicy::EverySuperstep),
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
async fn thread_histories_are_isolated_including_concurrent_runs() {
    let checkpointer = Arc::new(InMemoryCheckpointer::new());
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
    let checkpointer = Arc::new(InMemoryCheckpointer::new());
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

struct FailingCheckpointer {
    saves: AtomicUsize,
}

#[async_trait]
impl Checkpointer<TestSnapshot> for FailingCheckpointer {
    async fn save(
        &self,
        _request: CheckpointRequest<TestSnapshot>,
    ) -> Result<Arc<Checkpoint<TestSnapshot>>, CheckpointerError> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        Err(CheckpointerError::message("storage unavailable"))
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
    let error = linear_graph()
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

#[tokio::test]
async fn snapshot_failure_preserves_prior_checkpoint_and_source_context() {
    let checkpointer = Arc::new(InMemoryCheckpointer::new());
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
