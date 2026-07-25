use std::error::Error as _;
use std::sync::Arc;

use async_trait::async_trait;
use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy, CheckpointState,
    CheckpointStore, Checkpointer, CodecDescriptor, END, EncodedValue, EventConfig, ForkConfig,
    GraphRunError, GraphState, InterruptPayload, InterruptibleNode, Node, NodeContext, NodeError,
    NodeId, NodeOutcome, NodePath, NodeUpdate, RecordCheckpointer, ReplayConfig, ResumeConfig,
    RunConfig, RunControl, START, SnapshotError, StateError, StateGraph, ThreadId,
};
use tempfile::TempDir;

#[derive(Debug, Default, Eq, PartialEq)]
struct DurableState {
    value: u64,
    observations: Vec<(&'static str, u64)>,
    applied: Vec<&'static str>,
}

enum Update {
    Add(u64),
    Observe(&'static str, u64),
    Join,
}

impl GraphState for DurableState {
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

#[derive(Debug, Eq, PartialEq)]
struct Snapshot {
    value: u64,
}

impl CheckpointState for DurableState {
    type Snapshot = Snapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(Snapshot { value: self.value })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            value: snapshot.value,
            ..Self::default()
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ApprovalPrompt(String);

struct Codec {
    descriptor: CodecDescriptor,
}

impl Codec {
    fn current() -> Self {
        Self {
            descriptor: CodecDescriptor::new(
                "group.tests.sqlite-runtime.snapshot",
                1,
                "raw-u64-le",
            ),
        }
    }
}

impl CheckpointCodec<Snapshot> for Codec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        self.descriptor.clone()
    }

    fn encode_snapshot(&self, snapshot: &Snapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.value.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<Snapshot, CheckpointCodecError> {
        let bytes = <[u8; 8]>::try_from(bytes)
            .map_err(|_| CheckpointCodecError::message("invalid SQLite test snapshot"))?;
        Ok(Snapshot {
            value: u64::from_le_bytes(bytes),
        })
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        let prompt = payload
            .downcast_ref::<ApprovalPrompt>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        Ok(EncodedValue::new(
            CodecDescriptor::new("group.tests.sqlite-runtime.prompt", 1, "raw-u64-le"),
            prompt.0.as_bytes(),
        ))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        let expected = CodecDescriptor::new("group.tests.sqlite-runtime.prompt", 1, "raw-u64-le");
        if value.descriptor() != &expected {
            return Err(CheckpointCodecError::message(
                "unsupported SQLite test prompt descriptor",
            ));
        }
        let message = std::str::from_utf8(value.bytes())
            .map_err(|source| CheckpointCodecError::with_source("invalid prompt text", source))?;
        Ok(InterruptPayload::new(ApprovalPrompt(message.to_owned())))
    }
}

struct Add(u64);

#[async_trait]
impl Node<DurableState> for Add {
    async fn run(
        &self,
        _state: &DurableState,
        _context: &NodeContext,
    ) -> Result<Update, NodeError> {
        Ok(Update::Add(self.0))
    }
}

struct Observe(&'static str);

#[async_trait]
impl Node<DurableState> for Observe {
    async fn run(&self, state: &DurableState, _context: &NodeContext) -> Result<Update, NodeError> {
        Ok(Update::Observe(self.0, state.value))
    }
}

struct Join;

#[async_trait]
impl Node<DurableState> for Join {
    async fn run(
        &self,
        _state: &DurableState,
        _context: &NodeContext,
    ) -> Result<Update, NodeError> {
        Ok(Update::Join)
    }
}

struct Approval;

#[async_trait]
impl InterruptibleNode<DurableState> for Approval {
    async fn run(
        &self,
        _state: &DurableState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<Update>, NodeError> {
        if let Some(value) = context.resume_value::<u64>() {
            Ok(NodeOutcome::update(Update::Add(*value)))
        } else {
            Ok(NodeOutcome::interrupt(ApprovalPrompt(String::from(
                "approve SQLite work",
            ))))
        }
    }
}

fn database() -> (TempDir, String) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("runtime.sqlite3");
    (directory, format!("sqlite://{}", path.to_string_lossy()))
}

async fn raw_store(database_url: &str) -> Arc<SqliteCheckpointStore> {
    let store = Arc::new(
        SqliteCheckpointStore::connect(database_url)
            .await
            .expect("SQLite should connect"),
    );
    store.migrate().await.expect("migration should succeed");
    store
}

fn typed_store(store: Arc<SqliteCheckpointStore>) -> Arc<RecordCheckpointer<Snapshot>> {
    let store: Arc<dyn CheckpointStore> = store;
    Arc::new(RecordCheckpointer::new(store, Arc::new(Codec::current())))
}

fn linear_graph() -> group_agent_core::CompiledGraph<DurableState> {
    let mut graph = StateGraph::new();
    graph.set_version("sqlite-linear-v1");
    graph.add_node("one", Add(1)).expect("one");
    graph.add_node("two", Add(2)).expect("two");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    graph.compile().expect("linear graph")
}

fn nested_graph() -> group_agent_core::CompiledGraph<DurableState> {
    let mut child = StateGraph::new();
    child.add_node("one", Add(1)).expect("one");
    child.add_node("two", Add(2)).expect("two");
    child
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);

    let mut root = StateGraph::new();
    root.set_version("sqlite-subgraph-v1");
    root.add_subgraph("child", child.compile().expect("child graph"))
        .expect("child mount");
    root.add_edge(START, "child").add_edge("child", END);
    root.compile().expect("nested graph")
}

fn fan_out_graph() -> group_agent_core::CompiledGraph<DurableState> {
    let mut graph = StateGraph::new();
    graph.set_version("sqlite-conditional-fan-out-v1");
    graph.add_node("router", Add(1)).expect("router");
    graph.add_node("alpha", Observe("alpha")).expect("alpha");
    graph.add_node("beta", Observe("beta")).expect("beta");
    graph.add_node("join", Join).expect("join");
    graph.add_edge(START, "router");
    graph
        .add_conditional_fan_out("router", ["alpha", "beta", END], |state| {
            assert_eq!(state.value, 1);
            Ok(vec![
                NodeId::from("beta"),
                NodeId::end(),
                NodeId::from("alpha"),
            ])
        })
        .expect("conditional fan-out");
    graph
        .add_edge("alpha", "join")
        .add_edge("beta", "join")
        .add_edge("join", END);
    graph.compile().expect("fan-out graph")
}

fn interrupt_graph() -> group_agent_core::CompiledGraph<DurableState> {
    let mut graph = StateGraph::new();
    graph.set_version("sqlite-interrupt-v1");
    graph
        .add_interruptible_node("approval", Approval)
        .expect("approval");
    graph.add_edge(START, "approval").add_edge("approval", END);
    graph.compile().expect("interrupt graph")
}

#[tokio::test]
async fn ordinary_record_only_restart_resumes_and_extends_lineage() {
    let (_directory, database_url) = database();
    let graph = linear_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-linear",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("setup should stop after one node");
    let first = store_a
        .latest(&ThreadId::from("sqlite-linear"))
        .await
        .expect("latest")
        .expect("record");
    assert_eq!((first.step(), first.superstep()), (1, 1));
    assert!(!first.completed());
    assert_eq!(first.next_frontier(), [NodePath::from("two")]);
    let first_id = first.id();
    drop(first);
    drop(typed_a);
    drop(store_a);

    let store_b = raw_store(&database_url).await;
    let typed_b = typed_store(Arc::clone(&store_b));
    let outcome = graph
        .resume(
            ResumeConfig::new("sqlite-linear", typed_b as Arc<dyn Checkpointer<Snapshot>>)
                .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("fresh pool and adapter should resume");
    assert_eq!(outcome.final_state().value, 3);
    let history = store_b
        .history(&ThreadId::from("sqlite-linear"))
        .await
        .expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!((history[0].step(), history[0].superstep()), (1, 1));
    assert_eq!(history[1].parent_id(), Some(first_id));
    assert!(history[1].completed());
    assert!(history[1].next_frontier().is_empty());
    assert_eq!((history[1].step(), history[1].superstep()), (2, 2));
}

#[tokio::test]
async fn historical_checkpoint_replays_after_file_restart_without_changing_lineage() {
    let (_directory, database_url) = database();
    let graph = linear_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-replay",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("setup should complete");
    let before = store_a
        .history(&ThreadId::from("sqlite-replay"))
        .await
        .expect("history");
    assert_eq!(before.len(), 2);
    let source_id = before[0].id();
    let latest_id = before[1].id();
    let before_shape = before
        .iter()
        .map(|record| {
            (
                record.id(),
                record.parent_id(),
                record.step(),
                record.superstep(),
                record.completed(),
                record.next_frontier().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    drop(before);
    drop(typed_a);
    drop(store_a);

    let store_b = raw_store(&database_url).await;
    let typed_b = typed_store(Arc::clone(&store_b));
    let replay = graph
        .replay(
            ReplayConfig::new(
                "sqlite-replay",
                source_id,
                typed_b as Arc<dyn Checkpointer<Snapshot>>,
            )
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("record-only replay after restart");
    assert_eq!(replay.final_state().value, 3);
    assert_eq!((replay.source_step(), replay.source_superstep()), (1, 1));
    assert_eq!(replay.steps(), 2);

    let after = store_b
        .history(&ThreadId::from("sqlite-replay"))
        .await
        .expect("history after replay");
    assert_eq!(
        after
            .iter()
            .map(|record| {
                (
                    record.id(),
                    record.parent_id(),
                    record.step(),
                    record.superstep(),
                    record.completed(),
                    record.next_frontier().to_vec(),
                )
            })
            .collect::<Vec<_>>(),
        before_shape
    );
    assert_eq!(after.last().expect("latest").id(), latest_id);
}

#[tokio::test]
async fn branch_head_and_history_survive_file_restart_and_leave_default_head_unchanged() {
    let (_directory, database_url) = database();
    let graph = linear_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-fork",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("seed");
    let default_history = store_a
        .history(&ThreadId::from("sqlite-fork"))
        .await
        .expect("history");
    let source_id = default_history[0].id();
    let default_latest = default_history[1].id();
    drop(default_history);
    drop(typed_a);
    drop(store_a);

    let store_b = raw_store(&database_url).await;
    let typed_b = typed_store(Arc::clone(&store_b));
    let config = ForkConfig::new(
        "sqlite-fork",
        source_id,
        typed_b.clone() as Arc<dyn Checkpointer<Snapshot>>,
    )
    .with_run_config(RunConfig::new(0));
    let branch_id = config.branch_id();
    assert!(matches!(
        graph.fork(config).await.expect_err("zero budget"),
        GraphRunError::MaxStepsExceeded { .. }
    ));
    let other_config = ForkConfig::new(
        "sqlite-fork",
        source_id,
        typed_b.clone() as Arc<dyn Checkpointer<Snapshot>>,
    )
    .with_run_config(RunConfig::new(0));
    let other_branch_id = other_config.branch_id();
    assert!(matches!(
        graph
            .fork(other_config)
            .await
            .expect_err("other zero budget"),
        GraphRunError::MaxStepsExceeded { .. }
    ));
    assert_eq!(
        store_b
            .branch_head(&ThreadId::from("sqlite-fork"), branch_id)
            .await
            .expect("branch head")
            .expect("branch")
            .id(),
        source_id
    );
    assert!(
        store_b
            .branch_head(&ThreadId::from("wrong-thread"), branch_id)
            .await
            .expect("wrong-thread head")
            .is_none()
    );
    assert!(
        store_b
            .branch_history(&ThreadId::from("wrong-thread"), branch_id)
            .await
            .expect("wrong-thread history")
            .is_empty()
    );
    drop(typed_b);
    drop(store_b);

    let store_c = raw_store(&database_url).await;
    let typed_c = typed_store(Arc::clone(&store_c));
    let outcome = graph
        .resume(
            ResumeConfig::new(
                "sqlite-fork",
                typed_c.clone() as Arc<dyn Checkpointer<Snapshot>>,
            )
            .with_branch_id(branch_id)
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("branch resume after restart");
    assert_eq!(outcome.final_state().value, 3);
    let other_outcome = graph
        .resume(
            ResumeConfig::new("sqlite-fork", typed_c as Arc<dyn Checkpointer<Snapshot>>)
                .with_branch_id(other_branch_id)
                .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("other branch resume after restart");
    assert_eq!(other_outcome.final_state().value, 3);

    let branch = store_c
        .branch_history(&ThreadId::from("sqlite-fork"), branch_id)
        .await
        .expect("branch history");
    assert_eq!(branch.len(), 2);
    assert_eq!(branch[0].id(), source_id);
    assert_eq!(branch[1].parent_id(), Some(source_id));
    assert!(branch[1].completed());
    let other_branch = store_c
        .branch_history(&ThreadId::from("sqlite-fork"), other_branch_id)
        .await
        .expect("other branch history");
    assert_eq!(other_branch.len(), 2);
    assert_eq!(other_branch[0].id(), source_id);
    assert_eq!(other_branch[1].parent_id(), Some(source_id));
    assert_ne!(branch[1].id(), other_branch[1].id());
    assert!(other_branch[1].completed());
    let default_history = store_c
        .history(&ThreadId::from("sqlite-fork"))
        .await
        .expect("default history");
    assert_eq!(default_history.len(), 2);
    assert_eq!(default_history.last().expect("latest").id(), default_latest);
}

#[tokio::test]
async fn fan_out_subgraph_and_interrupt_replay_after_file_restart_remain_read_only() {
    let scenarios = [
        ("sqlite-replay-fan-out", fan_out_graph(), 3usize),
        ("sqlite-replay-subgraph", nested_graph(), 1usize),
    ];
    for (thread, graph, replay_budget) in scenarios {
        let (_directory, database_url) = database();
        let store_a = raw_store(&database_url).await;
        let typed_a = typed_store(Arc::clone(&store_a));
        graph
            .invoke_with_checkpoint(
                DurableState::default(),
                RunConfig::new(1),
                EventConfig::default(),
                RunControl::default(),
                CheckpointConfig::new(
                    thread,
                    typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                    CheckpointPolicy::EverySuperstep,
                ),
            )
            .await
            .expect_err("seed should stop after one node");
        let source = store_a
            .latest(&ThreadId::from(thread))
            .await
            .expect("latest")
            .expect("source");
        let source_id = source.id();
        let source_parent = source.parent_id();
        drop(source);
        drop(typed_a);
        drop(store_a);

        let store_b = raw_store(&database_url).await;
        let typed_b = typed_store(Arc::clone(&store_b));
        let before = store_b
            .history(&ThreadId::from(thread))
            .await
            .expect("before");
        let replay = graph
            .replay(
                ReplayConfig::new(
                    thread,
                    source_id,
                    typed_b as Arc<dyn Checkpointer<Snapshot>>,
                )
                .with_run_config(RunConfig::new(replay_budget)),
            )
            .await
            .expect("replay");
        if thread.contains("fan-out") {
            assert_eq!(replay.final_state().value, 1);
            assert_eq!(
                replay.final_state().observations,
                [("alpha", 1), ("beta", 1)]
            );
            assert_eq!(replay.final_state().applied, ["alpha", "beta", "join"]);
        } else {
            assert_eq!(replay.final_state().value, 3);
        }
        let after = store_b
            .history(&ThreadId::from(thread))
            .await
            .expect("after");
        assert_eq!(after.len(), before.len());
        assert_eq!(after[0].id(), source_id);
        assert_eq!(after[0].parent_id(), source_parent);
    }

    let (_directory, database_url) = database();
    let graph = interrupt_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-replay-interrupt",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("interrupt");
    let source_id = store_a
        .latest(&ThreadId::from("sqlite-replay-interrupt"))
        .await
        .expect("latest")
        .expect("source")
        .id();
    drop(typed_a);
    drop(store_a);

    let store_b = raw_store(&database_url).await;
    let typed_b = typed_store(Arc::clone(&store_b));
    let replay = graph
        .replay(
            ReplayConfig::new(
                "sqlite-replay-interrupt",
                source_id,
                typed_b as Arc<dyn Checkpointer<Snapshot>>,
            )
            .with_resume_value(7_u64),
        )
        .await
        .expect("durable interrupt replay");
    assert_eq!(replay.final_state().value, 7);
    let after = store_b
        .history(&ThreadId::from("sqlite-replay-interrupt"))
        .await
        .expect("history");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id(), source_id);
    assert!(after[0].interrupt().is_some());
}

#[tokio::test]
async fn conditional_fan_out_record_only_restart_preserves_snapshot_and_fan_in() {
    let (_directory, database_url) = database();
    let graph = fan_out_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-fan-out",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("setup should stop before fan-out frontier");
    let persisted = store_a
        .latest(&ThreadId::from("sqlite-fan-out"))
        .await
        .expect("latest")
        .expect("record");
    assert_eq!(
        persisted.next_frontier(),
        [NodePath::from("alpha"), NodePath::from("beta")]
    );
    assert_eq!((persisted.step(), persisted.superstep()), (1, 1));
    assert!(!persisted.completed());
    let first_id = persisted.id();
    drop(persisted);
    drop(typed_a);
    drop(store_a);

    let store_b = raw_store(&database_url).await;
    let typed_b = typed_store(Arc::clone(&store_b));
    let outcome = graph
        .resume(
            ResumeConfig::new("sqlite-fan-out", typed_b as Arc<dyn Checkpointer<Snapshot>>)
                .with_run_config(RunConfig::new(3)),
        )
        .await
        .expect("record-only fan-out resume");
    assert_eq!(
        outcome.final_state().observations,
        [("alpha", 1), ("beta", 1)]
    );
    assert_eq!(outcome.final_state().applied, ["alpha", "beta", "join"]);
    assert_eq!(
        outcome
            .visited_nodes()
            .iter()
            .filter(|path| path.as_str() == "join")
            .count(),
        1
    );
    let history = store_b
        .history(&ThreadId::from("sqlite-fan-out"))
        .await
        .expect("history");
    assert_eq!(history.first().map(|record| record.id()), Some(first_id));
    for pair in history.windows(2) {
        assert_eq!(pair[1].parent_id(), Some(pair[0].id()));
    }
    assert_eq!(history.len(), 3);
    assert_eq!((history[1].step(), history[1].superstep()), (3, 2));
    assert_eq!(history[1].next_frontier(), [NodePath::from("join")]);
    assert!(!history[1].completed());
    let completed = history.last().expect("completed record");
    assert!(completed.completed());
    assert!(completed.next_frontier().is_empty());
    assert_eq!((completed.step(), completed.superstep()), (4, 3));
}

#[tokio::test]
async fn subgraph_frontier_survives_file_database_restart() {
    let (_directory, database_url) = database();
    let graph = nested_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-subgraph",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("setup should stop in child");
    let record = store_a
        .latest(&ThreadId::from("sqlite-subgraph"))
        .await
        .expect("latest")
        .expect("record");
    assert_eq!(
        record.next_frontier(),
        [NodePath::new(
            &group_agent_core::GraphPath::new(["child"]),
            "two"
        )]
    );
    assert_eq!((record.step(), record.superstep()), (1, 1));
    assert!(!record.completed());
    let first_id = record.id();
    drop(record);
    drop(typed_a);
    drop(store_a);

    let store_b = raw_store(&database_url).await;
    let outcome = graph
        .resume(
            ResumeConfig::new(
                "sqlite-subgraph",
                typed_store(Arc::clone(&store_b)) as Arc<dyn Checkpointer<Snapshot>>,
            )
            .with_run_config(RunConfig::new(1)),
        )
        .await
        .expect("nested record-only resume");
    assert_eq!(outcome.final_state().value, 3);
    let history = store_b
        .history(&ThreadId::from("sqlite-subgraph"))
        .await
        .expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].parent_id(), Some(first_id));
    assert_eq!((history[1].step(), history[1].superstep()), (2, 2));
    assert!(history[1].completed());
    assert!(history[1].next_frontier().is_empty());
}

#[tokio::test]
async fn durable_interrupt_survives_restart_and_accepts_resume_value() {
    let (_directory, database_url) = database();
    let graph = interrupt_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    let interrupted = graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-interrupt",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect("interrupt should save");
    assert!(interrupted.as_interrupted().is_some());
    let interrupted_record = store_a
        .latest(&ThreadId::from("sqlite-interrupt"))
        .await
        .expect("latest")
        .expect("interrupted record");
    assert_eq!(
        (interrupted_record.step(), interrupted_record.superstep()),
        (0, 0)
    );
    assert!(!interrupted_record.completed());
    assert_eq!(
        interrupted_record.next_frontier(),
        [NodePath::from("approval")]
    );
    assert!(interrupted_record.interrupt().is_some());
    let interrupted_id = interrupted_record.id();
    drop(interrupted_record);
    drop(interrupted);
    drop(typed_a);
    drop(store_a);

    let store_b = raw_store(&database_url).await;
    let typed_b = typed_store(Arc::clone(&store_b));
    let decoded = typed_b
        .latest(&ThreadId::from("sqlite-interrupt"))
        .await
        .expect("decode")
        .expect("checkpoint");
    assert_eq!(
        decoded
            .interrupt()
            .expect("interrupt")
            .payload()
            .downcast_ref::<ApprovalPrompt>()
            .expect("prompt"),
        &ApprovalPrompt(String::from("approve SQLite work"))
    );
    drop(decoded);
    let outcome = graph
        .resume(
            ResumeConfig::new(
                "sqlite-interrupt",
                typed_b as Arc<dyn Checkpointer<Snapshot>>,
            )
            .with_resume_value(9_u64),
        )
        .await
        .expect("interrupt resume");
    assert_eq!(outcome.final_state().value, 9);
    let history = store_b
        .history(&ThreadId::from("sqlite-interrupt"))
        .await
        .expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].parent_id(), Some(interrupted_id));
    assert_eq!((history[1].step(), history[1].superstep()), (1, 1));
    assert!(history[1].completed());
    assert!(history[1].next_frontier().is_empty());
    assert!(history[1].interrupt().is_none());
}

#[tokio::test]
async fn descriptor_mismatches_fail_before_snapshot_decode_after_restart() {
    let (_directory, database_url) = database();
    let graph = linear_graph();
    let store_a = raw_store(&database_url).await;
    let typed_a = typed_store(Arc::clone(&store_a));
    graph
        .invoke_with_checkpoint(
            DurableState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "sqlite-descriptor",
                typed_a.clone() as Arc<dyn Checkpointer<Snapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await
        .expect_err("setup");
    drop(typed_a);
    drop(store_a);

    for descriptor in [
        CodecDescriptor::new("group.tests.sqlite-runtime.snapshot", 1, "other-encoding"),
        CodecDescriptor::new("other-schema", 1, "raw-u64-le"),
        CodecDescriptor::new("group.tests.sqlite-runtime.snapshot", 2, "raw-u64-le"),
    ] {
        let store_b = raw_store(&database_url).await;
        let store_port: Arc<dyn CheckpointStore> = store_b;
        let typed = RecordCheckpointer::new(store_port, Arc::new(Codec { descriptor }));
        let error = typed
            .latest(&ThreadId::from("sqlite-descriptor"))
            .await
            .expect_err("descriptor mismatch must precede decode");
        let reconstruction = error
            .source()
            .and_then(|source| {
                source.downcast_ref::<group_agent_core::CheckpointReconstructionError>()
            })
            .expect("reconstruction error should remain in source chain");
        assert!(matches!(
            reconstruction,
            group_agent_core::CheckpointReconstructionError::SnapshotEncoding { .. }
                | group_agent_core::CheckpointReconstructionError::SnapshotSchema { .. }
        ));
    }

    let pool = sqlx::SqlitePool::connect(&database_url)
        .await
        .expect("raw pool for incompatible format");
    sqlx::query("UPDATE group_checkpoint_records SET format_version = 2")
        .execute(&pool)
        .await
        .expect("format mutation");
    pool.close().await;
    let store_c = raw_store(&database_url).await;
    let typed_c = typed_store(store_c);
    let error = typed_c
        .latest(&ThreadId::from("sqlite-descriptor"))
        .await
        .expect_err("format mismatch must be structured");
    assert!(matches!(
        error.source().and_then(
            |source| source.downcast_ref::<group_agent_core::CheckpointReconstructionError>()
        ),
        Some(group_agent_core::CheckpointReconstructionError::FormatVersion { .. })
    ));
}
