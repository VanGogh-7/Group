use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointConfig, CheckpointPolicy, CheckpointState, Checkpointer, END, EventConfig,
    GraphState, InMemoryCheckpointer, Node, NodeContext, NodeError, RunConfig, RunControl, START,
    SnapshotError, StateError, StateGraph, ThreadId,
};

#[derive(Debug, Default)]
struct CounterState {
    value: usize,
}

#[derive(Debug)]
struct CounterSnapshot {
    value: usize,
}

struct Increment;

impl GraphState for CounterState {
    type Update = usize;

    fn apply(&mut self, update: Self::Update) -> Result<(), StateError> {
        self.value += update;
        Ok(())
    }
}

impl CheckpointState for CounterState {
    type Snapshot = CounterSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, SnapshotError> {
        Ok(CounterSnapshot { value: self.value })
    }

    fn restore(snapshot: &Self::Snapshot) -> Result<Self, SnapshotError> {
        Ok(Self {
            value: snapshot.value,
        })
    }
}

#[async_trait]
impl Node<CounterState> for Increment {
    async fn run(&self, _state: &CounterState, _context: &NodeContext) -> Result<usize, NodeError> {
        Ok(1)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.add_node("first", Increment)?;
    graph.add_node("second", Increment)?;
    graph
        .add_edge(START, "first")
        .add_edge("first", "second")
        .add_edge("second", END);
    let compiled = graph.compile()?;

    let checkpointer = Arc::new(InMemoryCheckpointer::<CounterSnapshot>::new());
    let checkpoint_config = CheckpointConfig::new(
        "example-thread",
        Arc::clone(&checkpointer) as Arc<dyn Checkpointer<CounterSnapshot>>,
        CheckpointPolicy::EverySuperstep,
    );
    let report = compiled
        .invoke_with_checkpoint(
            CounterState::default(),
            RunConfig::default(),
            EventConfig::default(),
            RunControl::default(),
            checkpoint_config,
        )
        .await?;

    let history = checkpointer
        .history(&ThreadId::from("example-thread"))
        .await?;
    assert_eq!(report.final_state().value, 2);
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].snapshot().value, 1);
    assert_eq!(history[1].snapshot().value, 2);
    assert!(history[1].completed());
    assert_eq!(history[1].parent_id(), Some(history[0].id()));

    println!("final value: {}", report.final_state().value);
    println!("checkpoint count: {}", history.len());
    println!("latest checkpoint: {}", history[1].id());
    Ok(())
}
