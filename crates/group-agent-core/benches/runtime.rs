use std::hint::black_box;

use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use group_agent_core::{
    CompiledGraph, END, GraphState, Node, NodeContext, NodeError, NodeId, START, StateError,
    StateGraph,
};
use tokio::runtime::Runtime;

#[derive(Default)]
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

fn fixed_graph_builder(node_count: usize) -> StateGraph<BenchState> {
    let mut graph = StateGraph::new();
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

fn runtime_benchmarks(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("Tokio benchmark runtime should start");
    let fixed_10 = fixed_graph(10);
    let fixed_100 = fixed_graph(100);
    let conditional_1_000 = conditional_loop_graph();
    let compile_100 = fixed_graph_builder(100);
    let compile_1_000 = fixed_graph_builder(1_000);

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

    criterion.bench_function("fixed_linear_10_nodes", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let report = fixed_10
                .invoke(BenchState::default())
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

criterion_group!(benches, runtime_benchmarks);
criterion_main!(benches);
