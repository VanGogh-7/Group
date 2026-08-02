use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use group_agent_checkpoint_sqlite::SqliteCheckpointStore;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy, CheckpointState,
    CheckpointStore, Checkpointer, CodecDescriptor, END, EventConfig, ForkConfig, GraphState, Node,
    NodeContext, NodeError, RecordCheckpointer, ResumeConfig, RunConfig, RunControl, START,
    SnapshotError, StateError, StateGraph,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[derive(Debug, Default)]
struct State {
    value: u64,
}

impl GraphState for State {
    type Update = u64;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }
}

impl CheckpointState for State {
    type Snapshot = u64;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(self.value)
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self { value: *snapshot })
    }
}

struct Codec;

impl CheckpointCodec<u64> for Codec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.bench.sqlite-branch", 1, "u64-le")
    }

    fn encode_snapshot(&self, snapshot: &u64) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<u64, CheckpointCodecError> {
        let bytes = <[u8; 8]>::try_from(bytes)
            .map_err(|_| CheckpointCodecError::message("invalid benchmark snapshot"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}

struct Add(u64);

#[async_trait]
impl Node<State> for Add {
    async fn run(&self, _state: &State, _context: &NodeContext) -> Result<u64, NodeError> {
        Ok(self.0)
    }
}

fn graph() -> group_agent_core::CompiledGraph<State> {
    let mut graph = StateGraph::new();
    graph.set_version("sqlite-branch-benchmark-v1");
    graph.add_node("one", Add(1)).expect("one");
    graph.add_node("two", Add(2)).expect("two");
    graph
        .add_edge(START, "one")
        .add_edge("one", "two")
        .add_edge("two", END);
    graph.compile().expect("benchmark graph")
}

fn branch_restart_benchmark(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("Tokio benchmark runtime");
    let graph = graph();

    criterion.bench_function(
        "sqlite_restart_branch_resume_one_immediate_node",
        |bencher| {
            bencher.iter_batched_ref(
                || {
                    let directory = tempfile::tempdir().expect("temporary database");
                    let path = directory.path().join("branch.sqlite3");
                    let database_url = format!("sqlite://{}", path.to_string_lossy());
                    let branch_id = runtime.block_on(async {
                        let store = Arc::new(
                            SqliteCheckpointStore::connect(&database_url)
                                .await
                                .expect("connect"),
                        );
                        store.migrate().await.expect("migrate");
                        let checkpointer: Arc<dyn Checkpointer<u64>> =
                            Arc::new(RecordCheckpointer::new(
                                Arc::clone(&store) as Arc<dyn CheckpointStore>,
                                Arc::new(Codec),
                            ));
                        graph
                            .invoke_with_checkpoint(
                                State::default(),
                                RunConfig::new(1),
                                EventConfig::default(),
                                RunControl::default(),
                                CheckpointConfig::new(
                                    "sqlite-branch-benchmark",
                                    Arc::clone(&checkpointer),
                                    CheckpointPolicy::EverySuperstep,
                                ),
                            )
                            .await
                            .expect_err("seed stops before node two");
                        let source = checkpointer
                            .latest(&"sqlite-branch-benchmark".into())
                            .await
                            .expect("latest")
                            .expect("source")
                            .id();
                        let config = ForkConfig::new(
                            "sqlite-branch-benchmark",
                            source,
                            Arc::clone(&checkpointer),
                        )
                        .with_run_config(RunConfig::new(0));
                        let branch_id = config.branch_id();
                        graph
                            .fork(config)
                            .await
                            .expect_err("zero-budget fork creates source head");
                        branch_id
                    });
                    (directory, database_url, branch_id)
                },
                |(_directory, database_url, branch_id): &mut (TempDir, String, _)| {
                    let steps = runtime.block_on(async {
                        let store = Arc::new(
                            SqliteCheckpointStore::connect(database_url)
                                .await
                                .expect("restart connect"),
                        );
                        store.migrate().await.expect("restart migrate");
                        let checkpointer: Arc<dyn Checkpointer<u64>> =
                            Arc::new(RecordCheckpointer::new(
                                Arc::clone(&store) as Arc<dyn CheckpointStore>,
                                Arc::new(Codec),
                            ));
                        let steps = graph
                            .resume(
                                ResumeConfig::new(
                                    "sqlite-branch-benchmark",
                                    Arc::clone(&checkpointer),
                                )
                                .with_branch_id(*branch_id)
                                .with_run_config(RunConfig::new(1)),
                            )
                            .await
                            .expect("restart branch resume")
                            .steps();
                        drop(checkpointer);
                        drop(store);
                        steps
                    });
                    black_box(steps);
                },
                BatchSize::SmallInput,
            );
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(8))
        .noise_threshold(0.05);
    targets = branch_restart_benchmark
}
criterion_main!(benches);
