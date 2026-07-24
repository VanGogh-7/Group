use std::hint::black_box;

use async_trait::async_trait;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use group_agent_core::{
    END, EventConfig, EventRetention, GraphState, Node, NodeContext, NodeError, RunConfig, START,
    StateError, StateGraph,
};
use group_agent_observability_tokio::EventBroadcast;
use tokio::runtime::Runtime;

#[derive(Default)]
struct BenchState {
    completed: bool,
}

struct Complete;

impl GraphState for BenchState {
    type Update = ();

    fn apply(&mut self, (): Self::Update) -> Result<(), StateError> {
        self.completed = true;
        Ok(())
    }
}

#[async_trait]
impl Node<BenchState> for Complete {
    async fn run(&self, _state: &BenchState, _context: &NodeContext) -> Result<(), NodeError> {
        Ok(())
    }
}

fn graph() -> group_agent_core::CompiledGraph<BenchState> {
    let mut graph = StateGraph::new();
    graph
        .add_node("complete", Complete)
        .expect("benchmark node");
    graph.add_edge(START, "complete").add_edge("complete", END);
    graph.compile().expect("benchmark graph")
}

fn event_broadcast(c: &mut Criterion) {
    let runtime = Runtime::new().expect("Tokio benchmark runtime");
    let graph = graph();
    let no_subscribers = EventBroadcast::new(64).expect("broadcast");
    let one_subscriber = EventBroadcast::new(64).expect("broadcast");
    let _one_stream = one_subscriber.subscribe();
    let four_subscribers = EventBroadcast::new(64).expect("broadcast");
    let _four_streams = (0..4)
        .map(|_| four_subscribers.subscribe())
        .collect::<Vec<_>>();
    let no_retention = EventBroadcast::new(64).expect("broadcast");
    let _no_retention_stream = no_retention.subscribe();

    let cases = [
        ("no_sink", EventConfig::default()),
        (
            "broadcast_no_subscribers",
            EventConfig::default().with_sink(no_subscribers.sink()),
        ),
        (
            "broadcast_one_subscriber",
            EventConfig::default().with_sink(one_subscriber.sink()),
        ),
        (
            "broadcast_four_subscribers",
            EventConfig::default().with_sink(four_subscribers.sink()),
        ),
        (
            "retention_none_one_subscriber",
            EventConfig::new(EventRetention::None).with_sink(no_retention.sink()),
        ),
    ];

    let mut group = c.benchmark_group("tokio_event_broadcast");
    for (name, events) in cases {
        group.bench_function(name, |bencher| {
            bencher.to_async(&runtime).iter_batched(
                || (BenchState::default(), RunConfig::default(), events.clone()),
                |(state, run_config, events)| async {
                    black_box(
                        graph
                            .invoke_with_events(state, run_config, events)
                            .await
                            .expect("benchmark run"),
                    );
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, event_broadcast);
criterion_main!(benches);
