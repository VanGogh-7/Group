use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use group_agent_core::{
    Checkpoint, CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointId,
    CheckpointPolicy, CheckpointRecord, CheckpointRecordParts, CheckpointRequest, CheckpointState,
    CheckpointStore, CheckpointWriteError, Checkpointer, CheckpointerError, CodecDescriptor,
    CompiledGraph, END, EncodedValue, EventConfig, EventRetention, ForkConfig, GraphState,
    GraphVersion, InMemoryCheckpointStore, InMemoryCheckpointer, InterruptPayload,
    InterruptibleNode, Node, NodeContext, NodeError, NodeId, NodeOutcome, NodePath, NodeUpdate,
    RecordCheckpointer, ReplayConfig, ResumeConfig, RunConfig, RunControl, RunId, START,
    SnapshotError, StateError, StateGraph, ThreadId,
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct BenchState {
    steps: usize,
}

struct StepUpdate;

impl GraphState for BenchState {
    type Update = StepUpdate;

    fn apply(&mut self, StepUpdate: Self::Update) -> Result<(), StateError> {
        self.steps += 1;
        Ok(())
    }

    fn apply_batch(&mut self, updates: Vec<NodeUpdate<Self::Update>>) -> Result<(), StateError> {
        self.steps += updates.len();
        Ok(())
    }
}

impl CheckpointState for BenchState {
    type Snapshot = usize;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(self.steps)
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self { steps: *snapshot })
    }
}

struct BenchCodec;

impl CheckpointCodec<usize> for BenchCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.bench.usize", 1, "group.bench.raw-v1")
    }

    fn encode_snapshot(&self, snapshot: &usize) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<usize, CheckpointCodecError> {
        bytes
            .try_into()
            .map(usize::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid benchmark snapshot"))
    }

    fn encode_interrupt(
        &self,
        payload: &InterruptPayload,
    ) -> Result<EncodedValue, CheckpointCodecError> {
        payload
            .downcast_ref::<()>()
            .ok_or_else(|| CheckpointCodecError::unsupported_interrupt(payload))?;
        Ok(EncodedValue::new(
            CodecDescriptor::new("group.bench.unit", 1, "group.bench.raw-v1"),
            Vec::<u8>::new(),
        ))
    }

    fn decode_interrupt(
        &self,
        value: &EncodedValue,
    ) -> Result<InterruptPayload, CheckpointCodecError> {
        if value.descriptor() != &CodecDescriptor::new("group.bench.unit", 1, "group.bench.raw-v1")
            || !value.bytes().is_empty()
        {
            return Err(CheckpointCodecError::message("invalid benchmark interrupt"));
        }
        Ok(InterruptPayload::new(()))
    }
}

struct ImmediateNode;

#[async_trait]
impl Node<BenchState> for ImmediateNode {
    async fn run(
        &self,
        _state: &BenchState,
        _context: &NodeContext,
    ) -> Result<StepUpdate, NodeError> {
        Ok(StepUpdate)
    }
}

struct DelayedNode;

#[async_trait]
impl Node<BenchState> for DelayedNode {
    async fn run(
        &self,
        _state: &BenchState,
        _context: &NodeContext,
    ) -> Result<StepUpdate, NodeError> {
        tokio::time::sleep(Duration::from_micros(100)).await;
        Ok(StepUpdate)
    }
}

struct InterruptBenchmarkNode;

#[async_trait]
impl InterruptibleNode<BenchState> for InterruptBenchmarkNode {
    async fn run(
        &self,
        _state: &BenchState,
        context: &NodeContext,
    ) -> Result<NodeOutcome<StepUpdate>, NodeError> {
        if context.resume_value::<()>().is_some() {
            Ok(NodeOutcome::update(StepUpdate))
        } else {
            Ok(NodeOutcome::interrupt(()))
        }
    }
}

struct ResumeBenchCheckpointer {
    checkpoint: Arc<Checkpoint<usize>>,
}

#[async_trait]
impl Checkpointer<usize> for ResumeBenchCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<usize>,
    ) -> Result<Arc<Checkpoint<usize>>, CheckpointWriteError> {
        Ok(Arc::new(request.into_checkpoint()))
    }

    async fn latest(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok((self.checkpoint.thread_id() == thread_id).then(|| Arc::clone(&self.checkpoint)))
    }

    async fn get(
        &self,
        thread_id: &ThreadId,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(
            (self.checkpoint.thread_id() == thread_id && self.checkpoint.id() == checkpoint_id)
                .then(|| Arc::clone(&self.checkpoint)),
        )
    }

    async fn history(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(if self.checkpoint.thread_id() == thread_id {
            vec![Arc::clone(&self.checkpoint)]
        } else {
            Vec::new()
        })
    }
}

struct BranchResumeBenchCheckpointer {
    checkpoint: Arc<Checkpoint<usize>>,
    branch_id: group_agent_core::BranchId,
}

#[async_trait]
impl Checkpointer<usize> for BranchResumeBenchCheckpointer {
    async fn save(
        &self,
        request: CheckpointRequest<usize>,
    ) -> Result<Arc<Checkpoint<usize>>, CheckpointWriteError> {
        Ok(Arc::new(request.into_checkpoint()))
    }

    async fn latest(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Option<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(None)
    }

    async fn history(
        &self,
        _thread_id: &ThreadId,
    ) -> Result<Vec<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(Vec::new())
    }

    async fn save_branch(
        &self,
        branch_id: group_agent_core::BranchId,
        request: CheckpointRequest<usize>,
    ) -> Result<Arc<Checkpoint<usize>>, CheckpointWriteError> {
        assert_eq!(branch_id, self.branch_id);
        Ok(Arc::new(request.into_checkpoint()))
    }

    async fn branch_head(
        &self,
        thread_id: &ThreadId,
        branch_id: group_agent_core::BranchId,
    ) -> Result<Option<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(
            (branch_id == self.branch_id && self.checkpoint.thread_id() == thread_id)
                .then(|| Arc::clone(&self.checkpoint)),
        )
    }

    async fn branch_history(
        &self,
        thread_id: &ThreadId,
        branch_id: group_agent_core::BranchId,
    ) -> Result<Vec<Arc<Checkpoint<usize>>>, CheckpointerError> {
        Ok(
            if branch_id == self.branch_id && self.checkpoint.thread_id() == thread_id {
                vec![Arc::clone(&self.checkpoint)]
            } else {
                Vec::new()
            },
        )
    }
}

fn fixed_graph_builder(node_count: usize) -> StateGraph<BenchState> {
    let mut graph = StateGraph::new();
    graph.set_version("benchmark-v1");
    let node_ids = (0..node_count)
        .map(|index| NodeId::from(format!("node_{index}")))
        .collect::<Vec<_>>();

    for node_id in &node_ids {
        graph
            .add_node(node_id.as_str(), ImmediateNode)
            .expect("benchmark node should register");
    }
    graph.add_edge(START, node_ids[0].clone());
    for window in node_ids.windows(2) {
        graph.add_edge(window[0].clone(), window[1].clone());
    }
    graph.add_edge(
        node_ids
            .last()
            .expect("benchmark graph has at least one node")
            .clone(),
        END,
    );

    graph
}

fn fixed_graph(node_count: usize) -> CompiledGraph<BenchState> {
    fixed_graph_builder(node_count)
        .compile()
        .expect("benchmark graph should compile")
}

fn conditional_loop_graph() -> CompiledGraph<BenchState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("loop", ImmediateNode)
        .expect("benchmark node should register");
    graph.add_edge(START, "loop");
    graph
        .add_conditional_edges("loop", ["loop", END], |state: &BenchState| {
            if state.steps >= 1_000 {
                Ok(NodeId::end())
            } else {
                Ok(NodeId::from("loop"))
            }
        })
        .expect("benchmark router should register");
    graph.compile().expect("benchmark graph should compile")
}

fn single_conditional_graph() -> CompiledGraph<BenchState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("router", ImmediateNode)
        .expect("benchmark node should register");
    graph.add_edge(START, "router");
    graph
        .add_conditional_edges("router", [END], |_| Ok(NodeId::end()))
        .expect("benchmark router should register");
    graph.compile().expect("benchmark graph should compile")
}

fn fan_out_graph(
    branch_count: usize,
    delayed: bool,
    conditional: bool,
) -> CompiledGraph<BenchState> {
    let mut graph = StateGraph::new();
    graph.set_version(format!("fan-out-{branch_count}-v1"));
    graph
        .add_node("fork", ImmediateNode)
        .expect("fork node should register");
    let branch_ids = (0..branch_count)
        .map(|index| NodeId::from(format!("branch_{index}")))
        .collect::<Vec<_>>();
    for node_id in &branch_ids {
        if delayed {
            graph
                .add_node(node_id.as_str(), DelayedNode)
                .expect("delayed branch should register");
        } else {
            graph
                .add_node(node_id.as_str(), ImmediateNode)
                .expect("immediate branch should register");
        }
        graph.add_edge(node_id.clone(), END);
    }
    graph.add_edge(START, "fork");
    if conditional {
        let selected = branch_ids.iter().rev().cloned().collect::<Vec<_>>();
        graph
            .add_conditional_fan_out("fork", branch_ids, move |_| Ok(selected.clone()))
            .expect("benchmark conditional fan-out should register");
    } else {
        graph
            .add_fan_out("fork", branch_ids)
            .expect("benchmark static fan-out should register");
    }
    graph.compile().expect("parallel graph should compile")
}

fn parallel_graph(branch_count: usize, delayed: bool) -> CompiledGraph<BenchState> {
    fan_out_graph(branch_count, delayed, false)
}

fn conditional_fan_out_graph(branch_count: usize) -> CompiledGraph<BenchState> {
    fan_out_graph(branch_count, false, true)
}

fn interrupt_graph() -> CompiledGraph<BenchState> {
    let mut graph = StateGraph::new();
    graph.set_version("interrupt-benchmark-v1");
    graph
        .add_interruptible_node("approval", InterruptBenchmarkNode)
        .expect("interrupt benchmark node should register");
    graph.add_edge(START, "approval").add_edge("approval", END);
    graph.compile().expect("interrupt graph should compile")
}

fn subgraph_graph(node_count: usize) -> CompiledGraph<BenchState> {
    let mut parent = StateGraph::new();
    parent.set_version("subgraph-benchmark-v1");
    parent
        .add_subgraph("child", fixed_graph(node_count))
        .expect("benchmark child should mount");
    parent.add_edge(START, "child").add_edge("child", END);
    parent.compile().expect("subgraph benchmark should compile")
}

fn nested_subgraph_graph() -> CompiledGraph<BenchState> {
    let mut middle = StateGraph::new();
    middle
        .add_subgraph("inner", fixed_graph(5))
        .expect("inner benchmark child should mount");
    middle.add_edge(START, "inner").add_edge("inner", END);
    let middle = middle
        .compile()
        .expect("middle benchmark graph should compile");

    let mut root = StateGraph::new();
    root.set_version("nested-subgraph-benchmark-v1");
    root.add_subgraph("outer", middle)
        .expect("outer benchmark child should mount");
    root.add_edge(START, "outer").add_edge("outer", END);
    root.compile()
        .expect("nested benchmark graph should compile")
}

fn subgraph_interrupt_graph() -> CompiledGraph<BenchState> {
    let mut parent = StateGraph::new();
    parent.set_version("subgraph-interrupt-benchmark-v1");
    parent
        .add_subgraph("approval_flow", interrupt_graph())
        .expect("interrupt child should mount");
    parent
        .add_edge(START, "approval_flow")
        .add_edge("approval_flow", END);
    parent
        .compile()
        .expect("subgraph interrupt benchmark should compile")
}

fn runtime_benchmarks(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("Tokio benchmark runtime should start");
    let fixed_10 = fixed_graph(10);
    let fixed_2 = fixed_graph(2);
    let fixed_32 = fixed_graph(32);
    let fixed_100 = fixed_graph(100);
    let parallel_2 = parallel_graph(2, false);
    let parallel_8 = parallel_graph(8, false);
    let parallel_32 = parallel_graph(32, false);
    let parallel_delayed_8 = parallel_graph(8, true);
    let single_conditional = single_conditional_graph();
    let conditional_fan_out_2 = conditional_fan_out_graph(2);
    let conditional_fan_out_8 = conditional_fan_out_graph(8);
    let conditional_fan_out_32 = conditional_fan_out_graph(32);
    let conditional_1_000 = conditional_loop_graph();
    let interrupt_graph = interrupt_graph();
    let subgraph_10 = subgraph_graph(10);
    let nested_subgraph = nested_subgraph_graph();
    let subgraph_resume_graph = subgraph_graph(2);
    let subgraph_interrupt_graph = subgraph_interrupt_graph();
    let compile_100 = fixed_graph_builder(100);
    let compile_1_000 = fixed_graph_builder(1_000);
    let uncancelled_token = CancellationToken::new();
    let middle_store = Arc::new(InMemoryCheckpointer::new(BenchCodec));
    runtime
        .block_on(fixed_2.invoke_with_checkpoint(
            BenchState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "resume-middle-benchmark",
                Arc::clone(&middle_store) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        ))
        .expect_err("one-step setup should stop at the second node");
    let middle_checkpoint = runtime
        .block_on(middle_store.latest(&ThreadId::from("resume-middle-benchmark")))
        .expect("middle checkpoint query should succeed")
        .expect("middle checkpoint should exist");
    let middle_checkpoint_id = middle_checkpoint.id();
    let branch_resume_id = group_agent_core::BranchId::new();
    let branch_resume_store: Arc<dyn Checkpointer<usize>> =
        Arc::new(BranchResumeBenchCheckpointer {
            checkpoint: Arc::clone(&middle_checkpoint),
            branch_id: branch_resume_id,
        });
    let middle_record = runtime
        .block_on(
            middle_store
                .record_store()
                .latest(&ThreadId::from("resume-middle-benchmark")),
        )
        .expect("middle record query should succeed")
        .expect("middle record should exist");
    let middle_resume_store: Arc<dyn Checkpointer<usize>> = Arc::new(ResumeBenchCheckpointer {
        checkpoint: middle_checkpoint,
    });

    let fan_out_store = Arc::new(InMemoryCheckpointer::new(BenchCodec));
    runtime
        .block_on(conditional_fan_out_2.invoke_with_checkpoint(
            BenchState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "conditional-fan-out-resume-benchmark",
                Arc::clone(&fan_out_store) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        ))
        .expect_err("one-step setup should stop before the branch frontier");
    let fan_out_checkpoint = runtime
        .block_on(fan_out_store.latest(&ThreadId::from("conditional-fan-out-resume-benchmark")))
        .expect("fan-out checkpoint query should succeed")
        .expect("fan-out checkpoint should exist");
    let fan_out_checkpoint_id = fan_out_checkpoint.id();
    let fan_out_resume_store: Arc<dyn Checkpointer<usize>> = Arc::new(ResumeBenchCheckpointer {
        checkpoint: fan_out_checkpoint,
    });

    let record_checkpoint_id = CheckpointId::new();
    let record_run_id = RunId::new();
    let record_sample = CheckpointRecord::try_from_parts(CheckpointRecordParts {
        format_version: group_agent_core::CheckpointFormatVersion::CURRENT,
        checkpoint_id: record_checkpoint_id,
        thread_id: ThreadId::from("record-benchmark"),
        run_id: record_run_id,
        parent_id: None,
        graph_version: Some(GraphVersion::from("benchmark-v1")),
        superstep: 1,
        step: 1,
        snapshot: EncodedValue::new(
            BenchCodec.snapshot_descriptor(),
            1_usize.to_le_bytes().to_vec(),
        ),
        next_frontier: vec![NodePath::from("node_1")],
        completed: false,
        interrupt: None,
    })
    .expect("benchmark record should be valid");

    let completed_store = Arc::new(InMemoryCheckpointer::new(BenchCodec));
    runtime
        .block_on(fixed_2.invoke_with_checkpoint(
            BenchState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "resume-completed-benchmark",
                Arc::clone(&completed_store) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        ))
        .expect("completed setup should succeed");
    let completed_checkpoint = runtime
        .block_on(completed_store.latest(&ThreadId::from("resume-completed-benchmark")))
        .expect("completed checkpoint query should succeed")
        .expect("completed checkpoint should exist");
    let completed_checkpoint_id = completed_checkpoint.id();
    let completed_resume_store: Arc<dyn Checkpointer<usize>> = Arc::new(ResumeBenchCheckpointer {
        checkpoint: completed_checkpoint,
    });
    let interrupted_store = Arc::new(InMemoryCheckpointer::new(BenchCodec));
    runtime
        .block_on(interrupt_graph.invoke_with_checkpoint(
            BenchState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "interrupt-resume-benchmark",
                Arc::clone(&interrupted_store) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        ))
        .expect("interrupt setup should succeed");
    let interrupted_checkpoint = runtime
        .block_on(interrupted_store.latest(&ThreadId::from("interrupt-resume-benchmark")))
        .expect("interrupted checkpoint query should succeed")
        .expect("interrupted checkpoint should exist");
    let interrupt_resume_store: Arc<dyn Checkpointer<usize>> = Arc::new(ResumeBenchCheckpointer {
        checkpoint: interrupted_checkpoint,
    });

    let subgraph_middle_store = Arc::new(InMemoryCheckpointer::new(BenchCodec));
    runtime
        .block_on(subgraph_resume_graph.invoke_with_checkpoint(
            BenchState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "subgraph-resume-benchmark",
                Arc::clone(&subgraph_middle_store) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        ))
        .expect_err("one-step setup should stop at the second child node");
    let subgraph_middle_checkpoint = runtime
        .block_on(subgraph_middle_store.latest(&ThreadId::from("subgraph-resume-benchmark")))
        .expect("subgraph checkpoint query should succeed")
        .expect("subgraph checkpoint should exist");
    let subgraph_resume_store: Arc<dyn Checkpointer<usize>> = Arc::new(ResumeBenchCheckpointer {
        checkpoint: subgraph_middle_checkpoint,
    });

    let subgraph_interrupted_store = Arc::new(InMemoryCheckpointer::new(BenchCodec));
    runtime
        .block_on(subgraph_interrupt_graph.invoke_with_checkpoint(
            BenchState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "subgraph-interrupt-resume-benchmark",
                Arc::clone(&subgraph_interrupted_store) as Arc<dyn Checkpointer<usize>>,
                CheckpointPolicy::EverySuperstep,
            ),
        ))
        .expect("subgraph interrupt setup should succeed");
    let subgraph_interrupted_checkpoint = runtime
        .block_on(
            subgraph_interrupted_store
                .latest(&ThreadId::from("subgraph-interrupt-resume-benchmark")),
        )
        .expect("subgraph interrupt checkpoint query should succeed")
        .expect("subgraph interrupt checkpoint should exist");
    let subgraph_interrupt_resume_store: Arc<dyn Checkpointer<usize>> =
        Arc::new(ResumeBenchCheckpointer {
            checkpoint: subgraph_interrupted_checkpoint,
        });

    criterion.bench_function("uuid_v4_id_generation", |bencher| {
        bencher.iter(|| black_box(RunId::new()));
    });

    criterion.bench_function("checkpoint_record_encode", |bencher| {
        bencher.iter(|| {
            let bytes = BenchCodec
                .encode_snapshot(black_box(&1_usize))
                .expect("benchmark snapshot should encode");
            black_box(
                CheckpointRecord::try_from_parts(CheckpointRecordParts {
                    format_version: group_agent_core::CheckpointFormatVersion::CURRENT,
                    checkpoint_id: record_checkpoint_id,
                    thread_id: ThreadId::from("record-benchmark"),
                    run_id: record_run_id,
                    parent_id: None,
                    graph_version: Some(GraphVersion::from("benchmark-v1")),
                    superstep: 1,
                    step: 1,
                    snapshot: EncodedValue::new(BenchCodec.snapshot_descriptor(), bytes),
                    next_frontier: vec![NodePath::from("node_1")],
                    completed: false,
                    interrupt: None,
                })
                .expect("benchmark record should be valid"),
            );
        });
    });

    criterion.bench_function("checkpoint_record_decode", |bencher| {
        bencher.iter(|| {
            black_box(
                Checkpoint::<usize>::from_record(black_box(&record_sample), &BenchCodec)
                    .expect("benchmark record should decode"),
            );
        });
    });

    criterion.bench_function(
        "record_reconstruction_resume_one_node_and_final_save",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || {
                    let store = Arc::new(
                        InMemoryCheckpointStore::try_from_records([middle_record.as_ref().clone()])
                            .expect("persisted setup record should import"),
                    );
                    let adapter: Arc<dyn Checkpointer<usize>> = Arc::new(RecordCheckpointer::new(
                        store as Arc<dyn CheckpointStore>,
                        Arc::new(BenchCodec),
                    ));
                    ResumeConfig::new("resume-middle-benchmark", adapter)
                        .with_run_config(RunConfig::new(1))
                },
                |config| async {
                    let outcome = fixed_2
                        .resume(config)
                        .await
                        .expect("record resume benchmark should succeed");
                    black_box(outcome.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function("compile_fixed_linear_100_nodes", |bencher| {
        bencher.iter(|| {
            black_box(
                compile_100
                    .compile()
                    .expect("benchmark graph should compile"),
            );
        });
    });

    criterion.bench_function("compile_fixed_linear_1000_nodes", |bencher| {
        bencher.iter(|| {
            black_box(
                compile_1_000
                    .compile()
                    .expect("benchmark graph should compile"),
            );
        });
    });

    criterion.bench_function(
        "invoke_default_no_control_checkpoint_disabled_10_nodes",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                BenchState::default,
                |initial_state| async {
                    let report = fixed_10
                        .invoke(initial_state)
                        .await
                        .expect("benchmark invocation should succeed");
                    black_box(report.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function("invoke_normal_node_single_box_path_10_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = fixed_10
                .invoke(BenchState::default())
                .await
                .expect("normal-node benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("invoke_shared_state_subgraph_10_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = subgraph_10
                .invoke(BenchState::default())
                .await
                .expect("subgraph benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("invoke_two_level_nested_subgraph_5_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = nested_subgraph
                .invoke(BenchState::default())
                .await
                .expect("nested subgraph benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function(
        "subgraph_resume_load_restore_one_node_and_final_save",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || {
                    ResumeConfig::new(
                        "subgraph-resume-benchmark",
                        Arc::clone(&subgraph_resume_store),
                    )
                    .with_run_config(RunConfig::new(1))
                },
                |config| async {
                    let outcome = subgraph_resume_graph
                        .resume(config)
                        .await
                        .expect("subgraph resume benchmark should succeed");
                    black_box(outcome.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function(
        "subgraph_interrupt_resume_restore_one_node_and_final_save",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || {
                    ResumeConfig::new(
                        "subgraph-interrupt-resume-benchmark",
                        Arc::clone(&subgraph_interrupt_resume_store),
                    )
                    .with_resume_value(())
                },
                |config| async {
                    let outcome = subgraph_interrupt_graph
                        .resume(config)
                        .await
                        .expect("subgraph interrupt resume benchmark should succeed");
                    black_box(outcome.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function(
        "resume_load_restore_one_immediate_node_and_final_save",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || {
                    ResumeConfig::new("resume-middle-benchmark", Arc::clone(&middle_resume_store))
                        .with_run_config(RunConfig::new(1))
                },
                |config| async {
                    let report = fixed_2
                        .resume(config)
                        .await
                        .expect("middle resume benchmark should succeed");
                    black_box(report.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function("resume_completed_checkpoint_noop", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            || {
                ResumeConfig::new(
                    "resume-completed-benchmark",
                    Arc::clone(&completed_resume_store),
                )
            },
            |config| async {
                let report = fixed_2
                    .resume(config)
                    .await
                    .expect("completed resume benchmark should succeed");
                black_box(report.steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function("replay_middle_checkpoint_one_immediate_node", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            || {
                ReplayConfig::new(
                    "resume-middle-benchmark",
                    middle_checkpoint_id,
                    Arc::clone(&middle_resume_store),
                )
                .with_run_config(RunConfig::new(1))
            },
            |config| async {
                let report = fixed_2
                    .replay(config)
                    .await
                    .expect("middle replay benchmark should succeed");
                black_box(report.steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function("fork_middle_checkpoint_one_immediate_node", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            || {
                ForkConfig::new(
                    "resume-middle-benchmark",
                    middle_checkpoint_id,
                    Arc::clone(&middle_store) as Arc<dyn Checkpointer<usize>>,
                )
                .with_run_config(RunConfig::new(1))
            },
            |config| async {
                let report = fixed_2
                    .fork(config)
                    .await
                    .expect("middle fork benchmark should succeed");
                black_box(report.outcome().steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function("branch_resume_one_immediate_node", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            || {
                ResumeConfig::new("resume-middle-benchmark", Arc::clone(&branch_resume_store))
                    .with_branch_id(branch_resume_id)
                    .with_run_config(RunConfig::new(1))
            },
            |config| async {
                let report = fixed_2
                    .resume(config)
                    .await
                    .expect("branch resume benchmark should succeed");
                black_box(report.steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function("replay_completed_checkpoint_noop", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            || {
                ReplayConfig::new(
                    "resume-completed-benchmark",
                    completed_checkpoint_id,
                    Arc::clone(&completed_resume_store),
                )
            },
            |config| async {
                let report = fixed_2
                    .replay(config)
                    .await
                    .expect("completed replay benchmark should succeed");
                black_box(report.steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function("replay_multi_node_frontier_2_targets", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            || {
                ReplayConfig::new(
                    "conditional-fan-out-resume-benchmark",
                    fan_out_checkpoint_id,
                    Arc::clone(&fan_out_resume_store),
                )
                .with_run_config(RunConfig::new(2))
            },
            |config| async {
                let report = conditional_fan_out_2
                    .replay(config)
                    .await
                    .expect("multi-frontier replay benchmark should succeed");
                black_box(report.steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function("interrupt_save_single_node", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            || {
                let store: Arc<dyn Checkpointer<usize>> =
                    Arc::new(InMemoryCheckpointer::new(BenchCodec));
                (
                    BenchState::default(),
                    CheckpointConfig::new(
                        "interrupt-benchmark",
                        store,
                        CheckpointPolicy::EverySuperstep,
                    ),
                )
            },
            |(initial_state, checkpoint_config)| async {
                let outcome = interrupt_graph
                    .invoke_with_checkpoint(
                        initial_state,
                        RunConfig::default(),
                        EventConfig::default(),
                        RunControl::default(),
                        checkpoint_config,
                    )
                    .await
                    .expect("interrupt benchmark should succeed");
                black_box(outcome.steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function(
        "interrupt_resume_restore_one_node_and_final_save",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || {
                    ResumeConfig::new(
                        "interrupt-resume-benchmark",
                        Arc::clone(&interrupt_resume_store),
                    )
                    .with_resume_value(())
                },
                |config| async {
                    let outcome = interrupt_graph
                        .resume(config)
                        .await
                        .expect("interrupt resume benchmark should succeed");
                    black_box(outcome.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function(
        "invoke_checkpoint_enabled_every_superstep_10_nodes",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || {
                    let checkpointer: Arc<dyn Checkpointer<usize>> =
                        Arc::new(InMemoryCheckpointer::new(BenchCodec));
                    (
                        BenchState::default(),
                        RunConfig::default(),
                        EventConfig::default(),
                        RunControl::default(),
                        CheckpointConfig::new(
                            "benchmark-thread",
                            checkpointer,
                            CheckpointPolicy::EverySuperstep,
                        ),
                    )
                },
                |(initial_state, run_config, event_config, control, checkpoint_config)| async {
                    let report = fixed_10
                        .invoke_with_checkpoint(
                            initial_state,
                            run_config,
                            event_config,
                            control,
                            checkpoint_config,
                        )
                        .await
                        .expect("checkpoint benchmark invocation should succeed");
                    black_box(report.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function("invoke_uncancelled_token_10_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = fixed_10
                .invoke_with_control(
                    BenchState::default(),
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_cancellation_token(uncancelled_token.clone()),
                )
                .await
                .expect("benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("invoke_no_retention_no_sink_10_nodes", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            BenchState::default,
            |initial_state| async {
                let report = fixed_10
                    .invoke_with_events(
                        initial_state,
                        RunConfig::default(),
                        EventConfig::new(EventRetention::None),
                    )
                    .await
                    .expect("benchmark invocation should succeed");
                black_box(report.steps());
            },
            BatchSize::SmallInput,
        );
    });

    criterion.bench_function("invoke_node_timeout_immediate_10_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = fixed_10
                .invoke_with_control(
                    BenchState::default(),
                    RunConfig::default(),
                    EventConfig::default(),
                    RunControl::new().with_node_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("fixed_linear_100_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = fixed_100
                .invoke(BenchState::default())
                .await
                .expect("benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("single_conditional_one_target", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            BenchState::default,
            |initial_state| async {
                let report = single_conditional
                    .invoke(initial_state)
                    .await
                    .expect("single conditional benchmark should succeed");
                black_box(report.steps());
            },
            BatchSize::SmallInput,
        );
    });

    for (name, graph) in [
        ("parallel_immediate_2_nodes", &parallel_2),
        ("parallel_immediate_8_nodes", &parallel_8),
        ("parallel_immediate_32_nodes", &parallel_32),
        ("conditional_fan_out_2_targets", &conditional_fan_out_2),
        ("conditional_fan_out_8_targets", &conditional_fan_out_8),
        ("conditional_fan_out_32_targets", &conditional_fan_out_32),
    ] {
        criterion.bench_function(name, |bencher| {
            bencher.to_async(&runtime).iter_batched(
                BenchState::default,
                |initial_state| async {
                    let report = graph
                        .invoke(initial_state)
                        .await
                        .expect("fan-out benchmark invocation should succeed");
                    black_box(report.steps());
                },
                BatchSize::SmallInput,
            );
        });
    }

    criterion.bench_function(
        "checkpoint_resume_conditional_fan_out_2_target_frontier",
        |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || {
                    ResumeConfig::new(
                        "conditional-fan-out-resume-benchmark",
                        Arc::clone(&fan_out_resume_store),
                    )
                    .with_run_config(RunConfig::new(2))
                },
                |config| async {
                    let outcome = conditional_fan_out_2
                        .resume(config)
                        .await
                        .expect("conditional fan-out resume benchmark should succeed");
                    black_box(outcome.steps());
                },
                BatchSize::SmallInput,
            );
        },
    );

    criterion.bench_function("parallel_short_wait_8_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = parallel_delayed_8
                .invoke(BenchState::default())
                .await
                .expect("delayed parallel benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("scheduler_linear_chain_32_total_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = fixed_32
                .invoke(BenchState::default())
                .await
                .expect("sequential benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("scheduler_fan_out_32_branches_33_total_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = parallel_32
                .invoke(BenchState::default())
                .await
                .expect("parallel benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("conditional_loop_1000_steps", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = conditional_1_000
                .invoke(BenchState::default())
                .await
                .expect("benchmark invocation should succeed");
            black_box(report.steps());
        });
    });

    criterion.bench_function("repeated_invoke_same_compiled_graph", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = fixed_10
                .invoke(BenchState::default())
                .await
                .expect("benchmark invocation should succeed");
            black_box(report.steps());
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(100)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.03);
    targets = runtime_benchmarks
}
criterion_main!(benches);
