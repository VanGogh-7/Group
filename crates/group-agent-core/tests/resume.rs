use std::error::Error as _;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use group_agent_core::{
    Checkpoint, CheckpointConfig, CheckpointId, CheckpointIncompatibility, CheckpointPolicy,
    CheckpointRequest, CheckpointState, CheckpointWriteError, Checkpointer, CheckpointerError,
    CompiledGraph, END, EventConfig, EventRetention, EventSink, GraphEvent, GraphRunError,
    GraphState, GraphVersion, InMemoryCheckpointer, Node, NodeContext, NodeError, NodeId,
    ResumeConfig, RunConfig, RunControl, RunFailure, START, SnapshotError, StateError, StateGraph,
    ThreadId,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct ResumeState {
    value: usize,
    fail_restore: bool,
    restore_calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct ResumeSnapshot {
    value: usize,
    fail_restore: bool,
    restore_calls: Arc<AtomicUsize>,
}

impl GraphState for ResumeState {
    type Update = usize;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }
}

impl CheckpointState for ResumeState {
    type Snapshot = ResumeSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(ResumeSnapshot {
            value: self.value,
            fail_restore: self.fail_restore,
            restore_calls: Arc::clone(&self.restore_calls),
        })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        snapshot.restore_calls.fetch_add(1, Ordering::SeqCst);
        if snapshot.fail_restore {
            return Err(SnapshotError::with_source(
                "restore adapter failed",
                RestoreLayerError {
                    source: RestoreRootError,
                },
            ));
        }
        Ok(Self {
            value: snapshot.value,
            fail_restore: false,
            restore_calls: Arc::clone(&snapshot.restore_calls),
        })
    }
}

struct Add(usize);

#[async_trait]
impl Node<ResumeState> for Add {
    async fn run(&self, _state: &ResumeState, _context: &NodeContext) -> Result<usize, NodeError> {
        Ok(self.0)
    }
}

struct BlockingAdd {
    amount: usize,
    block_once: Arc<AtomicBool>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl Node<ResumeState> for BlockingAdd {
    async fn run(&self, _state: &ResumeState, _context: &NodeContext) -> Result<usize, NodeError> {
        if self.block_once.swap(false, Ordering::SeqCst) {
            self.started.notify_waiters();
            self.release.notified().await;
        }
        Ok(self.amount)
    }
}

fn linear_graph(version: &str) -> CompiledGraph<ResumeState> {
    let mut graph = StateGraph::new();
    graph.set_version(version);
    graph.add_node("one", Add(1)).expect("one should register");
    graph.add_node("two", Add(2)).expect("two should register");
    graph
        .add_node("three", Add(3))
        .expect("three should register");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    graph.compile().expect("linear graph should compile")
}

fn two_node_graph(version: &str, second: &str) -> CompiledGraph<ResumeState> {
    let mut graph = StateGraph::new();
    graph.set_version(version);
    graph.add_node("one", Add(1)).expect("one should register");
    graph
        .add_node(second, Add(2))
        .expect("second should register");
    graph
        .add_edge(START, "one")
        .add_edge("one", second)
        .add_edge(second, END);
    graph.compile().expect("two-node graph should compile")
}

fn unversioned_linear_graph() -> CompiledGraph<ResumeState> {
    let mut graph = StateGraph::new();
    graph.add_node("one", Add(1)).expect("one should register");
    graph.add_node("two", Add(2)).expect("two should register");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    graph.compile().expect("unversioned graph should compile")
}

fn blocking_linear_graph(
    block_once: Arc<AtomicBool>,
    started: Arc<Notify>,
    release: Arc<Notify>,
) -> CompiledGraph<ResumeState> {
    let mut graph = StateGraph::new();
    graph.set_version("cas-race-v1");
    graph.add_node("one", Add(1)).expect("one should register");
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
        .expect("two should register");
    graph
        .add_node("three", Add(3))
        .expect("three should register");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", "three")
        .add_edge("three", END);
    graph.compile().expect("blocking graph should compile")
}

fn versioned_zero_node_graph() -> CompiledGraph<ResumeState> {
    let mut graph = StateGraph::new();
    graph.set_version("cas-race-v1");
    graph.add_edge(START, END);
    graph.compile().expect("zero-node graph should compile")
}

fn checkpoint_config(
    thread_id: &str,
    store: &Arc<InMemoryCheckpointer<ResumeSnapshot>>,
) -> CheckpointConfig<ResumeSnapshot> {
    CheckpointConfig::new(
        thread_id,
        Arc::clone(store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
        CheckpointPolicy::EverySuperstep,
    )
}

async fn create_middle_checkpoint(
    graph: &CompiledGraph<ResumeState>,
    thread_id: &str,
    store: &Arc<InMemoryCheckpointer<ResumeSnapshot>>,
    fail_restore: bool,
) -> Arc<Checkpoint<ResumeSnapshot>> {
    create_middle_checkpoint_with_counter(graph, thread_id, store, fail_restore)
        .await
        .0
}

async fn create_middle_checkpoint_with_counter(
    graph: &CompiledGraph<ResumeState>,
    thread_id: &str,
    store: &Arc<InMemoryCheckpointer<ResumeSnapshot>>,
    fail_restore: bool,
) -> (Arc<Checkpoint<ResumeSnapshot>>, Arc<AtomicUsize>) {
    let restore_calls = Arc::new(AtomicUsize::new(0));
    let error = graph
        .invoke_with_checkpoint(
            ResumeState {
                value: 0,
                fail_restore,
                restore_calls: Arc::clone(&restore_calls),
            },
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config(thread_id, store),
        )
        .await
        .expect_err("one-step budget should stop before the second node");
    assert!(matches!(
        error,
        GraphRunError::MaxStepsExceeded { step: 2, .. }
    ));
    let checkpoint = store
        .latest(&ThreadId::from(thread_id))
        .await
        .expect("latest should load")
        .expect("middle checkpoint should exist");
    (checkpoint, restore_calls)
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

#[tokio::test]
async fn resume_from_middle_matches_uninterrupted_result_and_continues_lineage() {
    let graph = linear_graph("linear-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    let middle = create_middle_checkpoint(&graph, "resume-middle", &store, false).await;
    assert_eq!(
        middle.graph_version(),
        Some(&GraphVersion::from("linear-v1"))
    );
    assert_eq!(middle.step(), 1);
    assert_eq!(middle.superstep(), 1);
    assert_eq!(middle.next_frontier(), [NodeId::from("two")]);

    let report = graph
        .resume(
            ResumeConfig::new(
                "resume-middle",
                Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
            )
            .with_checkpoint_id(middle.id())
            .with_run_config(RunConfig::new(2)),
        )
        .await
        .expect("resume should finish the graph");
    let uninterrupted = graph
        .invoke(ResumeState::default())
        .await
        .expect("uninterrupted run should succeed");
    assert_eq!(
        report.final_state().value,
        uninterrupted.final_state().value
    );
    assert_eq!(report.steps(), 3);
    assert_eq!(
        report.visited_nodes(),
        [NodeId::from("two"), NodeId::from("three")]
    );

    let history = store
        .history(&ThreadId::from("resume-middle"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 3);
    assert_eq!(history[1].parent_id(), Some(middle.id()));
    assert_eq!(history[2].parent_id(), Some(history[1].id()));
    assert_eq!(
        history
            .iter()
            .map(|checkpoint| (checkpoint.step(), checkpoint.superstep()))
            .collect::<Vec<_>>(),
        [(1, 1), (2, 2), (3, 3)]
    );
    assert!(matches!(
        report.events(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::RunResumed {
                checkpoint_id,
                step: 1,
                superstep: 1,
                ..
            },
            GraphEvent::NodeStarted { step: 2, .. },
            ..
        ] if *checkpoint_id == middle.id()
    ));
    assert!(matches!(
        report.events().last(),
        Some(GraphEvent::RunCompleted { steps: 3, .. })
    ));
}

#[tokio::test]
async fn resume_max_steps_is_an_additional_budget() {
    let graph = linear_graph("budget-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    create_middle_checkpoint(&graph, "resume-budget", &store, false).await;

    let error = graph
        .resume(
            ResumeConfig::new(
                "resume-budget",
                Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
            )
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect_err("one additional step should stop before node three");
    assert!(matches!(
        error,
        GraphRunError::MaxStepsExceeded {
            max_steps: 1,
            ref node_id,
            step: 3,
        } if node_id == &NodeId::from("three")
    ));
    let history = store
        .history(&ThreadId::from("resume-budget"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].step(), 2);
    assert_eq!(history[1].superstep(), 2);
}

#[tokio::test]
async fn completed_checkpoint_resume_is_a_no_op_without_duplicate_save() {
    let graph = linear_graph("completed-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    graph
        .invoke_with_checkpoint(
            ResumeState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("completed", &store),
        )
        .await
        .expect("initial run should complete");
    let before = store
        .history(&ThreadId::from("completed"))
        .await
        .expect("history should load");
    let completed = before.last().expect("completed checkpoint should exist");

    let report = graph
        .resume(ResumeConfig::new(
            "completed",
            Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
        ))
        .await
        .expect("completed resume should succeed");
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
            GraphEvent::RunResumed {
                run_id: report.run_id(),
                thread_id: ThreadId::from("completed"),
                checkpoint_id: completed.id(),
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
        store
            .history(&ThreadId::from("completed"))
            .await
            .expect("history should load")
            .len(),
        before.len()
    );
}

#[tokio::test]
async fn explicit_non_latest_checkpoint_is_rejected_without_forking() {
    let graph = linear_graph("conflict-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    graph
        .invoke_with_checkpoint(
            ResumeState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("non-latest", &store),
        )
        .await
        .expect("initial run should complete");
    let history = store
        .history(&ThreadId::from("non-latest"))
        .await
        .expect("history should load");

    let error = graph
        .resume(
            ResumeConfig::new(
                "non-latest",
                Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
            )
            .with_checkpoint_id(history[0].id()),
        )
        .await
        .expect_err("non-latest checkpoint should conflict");
    assert!(matches!(
        error,
        GraphRunError::ResumeConflict {
            checkpoint_id,
            latest_checkpoint_id: Some(latest),
            step: 1,
            ..
        } if checkpoint_id == history[0].id() && latest == history[2].id()
    ));
    assert_eq!(
        store
            .history(&ThreadId::from("non-latest"))
            .await
            .expect("history should remain intact")
            .len(),
        3
    );
}

#[tokio::test]
async fn latest_advancing_after_validation_causes_first_resume_save_to_conflict() {
    let block_once = Arc::new(AtomicBool::new(true));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let graph = Arc::new(blocking_linear_graph(
        Arc::clone(&block_once),
        Arc::clone(&started),
        Arc::clone(&release),
    ));
    let store = Arc::new(InMemoryCheckpointer::new());
    let base = create_middle_checkpoint(&graph, "cas-race", &store, false).await;
    let node_started = started.notified();

    let resume_task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let store = Arc::clone(&store);
        async move {
            graph
                .resume(ResumeConfig::new(
                    "cas-race",
                    store as Arc<dyn Checkpointer<ResumeSnapshot>>,
                ))
                .await
        }
    });
    node_started.await;

    let advancing = versioned_zero_node_graph()
        .invoke_with_checkpoint(
            ResumeState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("cas-race", &store).with_expected_parent(Some(base.id())),
        )
        .await
        .expect("competing run should advance latest");
    let advanced = store
        .latest(&ThreadId::from("cas-race"))
        .await
        .expect("latest should load")
        .expect("advanced checkpoint should exist");
    assert_eq!(advanced.run_id(), advancing.run_id());
    assert_eq!(advanced.parent_id(), Some(base.id()));

    release.notify_waiters();
    let error = resume_task
        .await
        .expect("resume task should not panic")
        .expect_err("resume save should lose the CAS race");
    assert!(matches!(
        error,
        GraphRunError::CheckpointConflict {
            expected_parent: Some(expected),
            actual_parent: Some(actual),
            superstep: 2,
            step: 2,
            ..
        } if expected == base.id() && actual == advanced.id()
    ));

    let history = store
        .history(&ThreadId::from("cas-race"))
        .await
        .expect("history should load");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id(), base.id());
    assert_eq!(history[1].id(), advanced.id());
    assert_eq!(history[1].parent_id(), Some(base.id()));

    let report = graph
        .invoke(ResumeState::default())
        .await
        .expect("compiled graph should remain reusable after CAS conflict");
    assert_eq!(report.final_state().value, 6);
}

#[tokio::test]
async fn graph_version_mismatch_and_unknown_frontier_are_incompatible() {
    let v1 = linear_graph("version-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    let (checkpoint, version_restore_calls) =
        create_middle_checkpoint_with_counter(&v1, "version", &store, false).await;
    let v2 = linear_graph("version-v2");
    let version_error = v2
        .resume(ResumeConfig::new(
            "version",
            Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
        ))
        .await
        .expect_err("version mismatch should fail");
    assert!(matches!(
        version_error,
        GraphRunError::CheckpointIncompatible {
            reason: CheckpointIncompatibility::GraphVersionMismatch {
                ref checkpoint,
                ref compiled,
            },
            ..
        } if checkpoint == &GraphVersion::from("version-v1")
            && compiled == &GraphVersion::from("version-v2")
    ));
    assert_eq!(
        checkpoint.graph_version(),
        Some(&GraphVersion::from("version-v1"))
    );
    assert_eq!(version_restore_calls.load(Ordering::SeqCst), 0);

    let source = two_node_graph("frontier-v1", "obsolete");
    let frontier_store = Arc::new(InMemoryCheckpointer::new());
    let (_, frontier_restore_calls) =
        create_middle_checkpoint_with_counter(&source, "unknown-frontier", &frontier_store, true)
            .await;
    let target = two_node_graph("frontier-v1", "replacement");
    let frontier_error = target
        .resume(ResumeConfig::new(
            "unknown-frontier",
            frontier_store as Arc<dyn Checkpointer<ResumeSnapshot>>,
        ))
        .await
        .expect_err("unknown frontier node should fail");
    assert!(matches!(
        frontier_error,
        GraphRunError::CheckpointIncompatible {
            reason: CheckpointIncompatibility::UnknownFrontierNode { ref node_id },
            ..
        } if node_id == &NodeId::from("obsolete")
    ));
    assert_eq!(frontier_restore_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unversioned_checkpoint_is_incompatible_with_resume() {
    let graph = unversioned_linear_graph();
    let store = Arc::new(InMemoryCheckpointer::new());
    let (_, restore_calls) =
        create_middle_checkpoint_with_counter(&graph, "unversioned", &store, false).await;
    let error = graph
        .resume(ResumeConfig::new(
            "unversioned",
            store as Arc<dyn Checkpointer<ResumeSnapshot>>,
        ))
        .await
        .expect_err("unversioned checkpoint should not resume");
    assert!(matches!(
        error,
        GraphRunError::CheckpointIncompatible {
            reason: CheckpointIncompatibility::UnversionedCheckpoint,
            ..
        }
    ));
    assert_eq!(restore_calls.load(Ordering::SeqCst), 0);
}

#[derive(Debug)]
struct RestoreRootError;

impl fmt::Display for RestoreRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("restore root error")
    }
}

impl std::error::Error for RestoreRootError {}

#[derive(Debug)]
struct RestoreLayerError {
    source: RestoreRootError,
}

impl fmt::Display for RestoreLayerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("restore layer error")
    }
}

impl std::error::Error for RestoreLayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[tokio::test]
async fn restore_failure_preserves_source_chain_emits_once_and_graph_is_reusable() {
    let graph = linear_graph("restore-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    let (_, restore_calls) =
        create_middle_checkpoint_with_counter(&graph, "restore-failure", &store, true).await;
    let sink = Arc::new(RecordingSink::default());
    let error = graph
        .resume(
            ResumeConfig::new(
                "restore-failure",
                Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
            )
            .with_event_config(
                EventConfig::new(EventRetention::None)
                    .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
            ),
        )
        .await
        .expect_err("restore should fail");
    assert!(matches!(
        error,
        GraphRunError::RestoreFailed {
            superstep: 1,
            step: 1,
            ..
        }
    ));
    let snapshot_error = error
        .source()
        .expect("run error should expose snapshot error");
    assert_eq!(snapshot_error.to_string(), "restore adapter failed");
    let layer = snapshot_error
        .source()
        .expect("snapshot error should expose restore layer");
    assert_eq!(layer.to_string(), "restore layer error");
    assert_eq!(
        layer
            .source()
            .expect("restore layer should expose root source")
            .to_string(),
        "restore root error"
    );
    assert_eq!(
        store
            .history(&ThreadId::from("restore-failure"))
            .await
            .expect("history should load")
            .len(),
        1
    );
    assert_eq!(restore_calls.load(Ordering::SeqCst), 1);
    {
        let events = sink.0.lock().expect("sink lock should not be poisoned");
        assert!(matches!(
            events.first(),
            Some(GraphEvent::RunStarted { .. })
        ));
        assert!(matches!(
            events.last(),
            Some(GraphEvent::RunFailed {
                failure: RunFailure::RestoreFailed {
                    superstep: 1,
                    step: 1,
                    ..
                },
                ..
            })
        ));
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
                .any(|event| matches!(event, GraphEvent::RunResumed { .. }))
        );
    }

    let report = graph
        .invoke(ResumeState::default())
        .await
        .expect("graph should remain reusable");
    assert_eq!(report.final_state().value, 6);
}

#[tokio::test]
async fn missing_checkpoint_and_pre_cancelled_resume_fail_after_run_started() {
    let graph = linear_graph("missing-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    let missing = graph
        .resume(ResumeConfig::new(
            "missing",
            Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
        ))
        .await
        .expect_err("missing checkpoint should fail");
    assert!(matches!(
        missing,
        GraphRunError::CheckpointNotFound {
            checkpoint_id: None,
            ..
        }
    ));

    let sink = Arc::new(RecordingSink::default());
    let token = CancellationToken::new();
    token.cancel();
    let cancelled = graph
        .resume(
            ResumeConfig::new("cancelled", store as Arc<dyn Checkpointer<ResumeSnapshot>>)
                .with_event_config(
                    EventConfig::new(EventRetention::None)
                        .with_sink(Arc::clone(&sink) as Arc<dyn EventSink>),
                )
                .with_control(RunControl::new().with_cancellation_token(token)),
        )
        .await
        .expect_err("pre-cancelled resume should fail");
    assert!(matches!(
        cancelled,
        GraphRunError::Cancelled {
            node_id: None,
            step: 0,
            ..
        }
    ));
    let events = sink.0.lock().expect("sink lock should not be poisoned");
    assert!(matches!(
        events.as_slice(),
        [
            GraphEvent::RunStarted { .. },
            GraphEvent::RunFailed {
                failure: RunFailure::Cancelled {
                    node_id: None,
                    step: 0,
                },
                ..
            }
        ]
    ));
}

struct BlockingLatestCheckpointer {
    inner: Arc<InMemoryCheckpointer<ResumeSnapshot>>,
    started: Notify,
    release: Notify,
}

#[async_trait]
impl Checkpointer<ResumeSnapshot> for BlockingLatestCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<ResumeSnapshot>,
    ) -> Result<Arc<Checkpoint<ResumeSnapshot>>, CheckpointWriteError> {
        self.inner.save(request).await
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<ResumeSnapshot>>>, CheckpointerError> {
        self.started.notify_waiters();
        self.release.notified().await;
        self.inner.latest(thread_id).await
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<ResumeSnapshot>>>, CheckpointerError> {
        self.inner.get(thread_id, checkpoint_id).await
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<ResumeSnapshot>>>, CheckpointerError> {
        self.inner.history(thread_id).await
    }
}

#[tokio::test(start_paused = true)]
async fn run_timeout_remains_active_while_loading_resume_checkpoint() {
    let graph = Arc::new(linear_graph("timeout-v1"));
    let inner = Arc::new(InMemoryCheckpointer::new());
    create_middle_checkpoint(&graph, "load-timeout", &inner, false).await;
    let checkpointer = Arc::new(BlockingLatestCheckpointer {
        inner,
        started: Notify::new(),
        release: Notify::new(),
    });
    let started = checkpointer.started.notified();
    let task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let checkpointer = Arc::clone(&checkpointer);
        async move {
            graph
                .resume(
                    ResumeConfig::new(
                        "load-timeout",
                        checkpointer as Arc<dyn Checkpointer<ResumeSnapshot>>,
                    )
                    .with_control(RunControl::new().with_run_timeout(Duration::from_secs(5))),
                )
                .await
        }
    });
    started.await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = task
        .await
        .expect("resume task should not panic")
        .expect_err("loading should time out");
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
async fn cancellation_during_checkpoint_load_is_observed_before_restore() {
    let graph = Arc::new(linear_graph("load-cancel-v1"));
    let inner = Arc::new(InMemoryCheckpointer::new());
    let (_, restore_calls) =
        create_middle_checkpoint_with_counter(&graph, "load-cancel", &inner, false).await;
    let checkpointer = Arc::new(BlockingLatestCheckpointer {
        inner,
        started: Notify::new(),
        release: Notify::new(),
    });
    let started = checkpointer.started.notified();
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let graph = Arc::clone(&graph);
        let checkpointer = Arc::clone(&checkpointer);
        let token = token.clone();
        async move {
            graph
                .resume(
                    ResumeConfig::new(
                        "load-cancel",
                        checkpointer as Arc<dyn Checkpointer<ResumeSnapshot>>,
                    )
                    .with_control(RunControl::new().with_cancellation_token(token)),
                )
                .await
        }
    });
    started.await;
    token.cancel();
    let error = task
        .await
        .expect("resume task should not panic")
        .expect_err("checkpoint load should be cancelled");
    assert!(matches!(
        error,
        GraphRunError::Cancelled {
            node_id: None,
            step: 0,
            ..
        }
    ));
    assert_eq!(restore_calls.load(Ordering::SeqCst), 0);
}

#[derive(Debug)]
struct LoadRootError;

impl fmt::Display for LoadRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("load root error")
    }
}

impl std::error::Error for LoadRootError {}

#[derive(Debug)]
struct LoadLayerError {
    source: LoadRootError,
}

impl fmt::Display for LoadLayerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("load layer error")
    }
}

impl std::error::Error for LoadLayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

struct FailingLoadCheckpointer;

#[async_trait]
impl Checkpointer<ResumeSnapshot> for FailingLoadCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<ResumeSnapshot>,
    ) -> Result<Arc<Checkpoint<ResumeSnapshot>>, CheckpointWriteError> {
        Ok(Arc::new(request.into_checkpoint()))
    }

    async fn latest(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<ResumeSnapshot>>>, CheckpointerError> {
        Err(CheckpointerError::with_source(
            "checkpoint adapter load failed",
            LoadLayerError {
                source: LoadRootError,
            },
        ))
    }

    async fn history(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<ResumeSnapshot>>>, CheckpointerError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn checkpoint_load_failure_preserves_complete_source_chain() {
    let error = linear_graph("load-source-v1")
        .resume(ResumeConfig::new(
            "load-source",
            Arc::new(FailingLoadCheckpointer) as Arc<dyn Checkpointer<ResumeSnapshot>>,
        ))
        .await
        .expect_err("checkpoint load should fail");
    assert!(matches!(
        error,
        GraphRunError::CheckpointLoadFailed {
            checkpoint_id: None,
            ..
        }
    ));
    let checkpointer_error = error
        .source()
        .expect("run error should expose checkpointer error");
    assert_eq!(
        checkpointer_error.to_string(),
        "checkpoint adapter load failed"
    );
    let layer = checkpointer_error
        .source()
        .expect("checkpointer error should expose load layer");
    assert_eq!(layer.to_string(), "load layer error");
    assert_eq!(
        layer
            .source()
            .expect("load layer should expose root source")
            .to_string(),
        "load root error"
    );
}

struct CancelOnResumeSink {
    events: Mutex<Vec<GraphEvent>>,
    token: CancellationToken,
}

impl EventSink for CancelOnResumeSink {
    fn on_event(&self, event: &GraphEvent) {
        self.events
            .lock()
            .expect("sink lock should not be poisoned")
            .push(event.clone());
        if matches!(event, GraphEvent::RunResumed { .. }) {
            self.token.cancel();
        }
    }
}

#[tokio::test]
async fn completed_resume_control_failures_have_stable_event_order() {
    let graph = linear_graph("completed-control-v1");
    let store = Arc::new(InMemoryCheckpointer::new());
    graph
        .invoke_with_checkpoint(
            ResumeState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config("completed-control", &store),
        )
        .await
        .expect("completed checkpoint setup should succeed");

    let token = CancellationToken::new();
    let cancel_sink = Arc::new(CancelOnResumeSink {
        events: Mutex::new(Vec::new()),
        token: token.clone(),
    });
    let cancelled = graph
        .resume(
            ResumeConfig::new(
                "completed-control",
                Arc::clone(&store) as Arc<dyn Checkpointer<ResumeSnapshot>>,
            )
            .with_event_config(
                EventConfig::new(EventRetention::None)
                    .with_sink(Arc::clone(&cancel_sink) as Arc<dyn EventSink>),
            )
            .with_control(RunControl::new().with_cancellation_token(token)),
        )
        .await
        .expect_err("sink cancellation should stop completed resume");
    assert!(matches!(
        cancelled,
        GraphRunError::Cancelled {
            node_id: None,
            step: 3,
            ..
        }
    ));
    {
        let cancel_events = cancel_sink
            .events
            .lock()
            .expect("sink lock should not be poisoned");
        assert!(matches!(
            cancel_events.as_slice(),
            [
                GraphEvent::RunStarted { .. },
                GraphEvent::RunResumed {
                    step: 3,
                    superstep: 3,
                    ..
                },
                GraphEvent::RunFailed {
                    failure: RunFailure::Cancelled {
                        node_id: None,
                        step: 3,
                    },
                    ..
                }
            ]
        ));
    }

    let timeout_sink = Arc::new(RecordingSink::default());
    let timed_out = graph
        .resume(
            ResumeConfig::new(
                "completed-control",
                store as Arc<dyn Checkpointer<ResumeSnapshot>>,
            )
            .with_event_config(
                EventConfig::new(EventRetention::None)
                    .with_sink(Arc::clone(&timeout_sink) as Arc<dyn EventSink>),
            )
            .with_control(RunControl::new().with_run_timeout(Duration::ZERO)),
        )
        .await
        .expect_err("zero run timeout should stop before loading");
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
            }
        ]
    ));
}
