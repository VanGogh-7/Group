use std::sync::Arc;

use async_trait::async_trait;
use group_agent_core::{
    CheckpointCodec, CheckpointCodecError, CheckpointConfig, CheckpointPolicy, CheckpointState,
    Checkpointer, CodecDescriptor, END, EventConfig, GraphRunError, GraphState,
    InMemoryCheckpointer, Node, NodeContext, NodeError, ResumeConfig, RunConfig, RunControl, START,
    SnapshotError, StateError, StateGraph,
};

#[derive(Debug, Default)]
struct CounterState {
    value: usize,
}

#[derive(Debug)]
struct CounterSnapshot {
    value: usize,
}

struct CounterCodec;

impl CheckpointCodec<CounterSnapshot> for CounterCodec {
    fn snapshot_descriptor(&self) -> CodecDescriptor {
        CodecDescriptor::new("group.example.counter", 1, "group.example.le-u64-v1")
    }

    fn encode_snapshot(&self, snapshot: &CounterSnapshot) -> Result<Vec<u8>, CheckpointCodecError> {
        Ok(snapshot.value.to_le_bytes().to_vec())
    }

    fn decode_snapshot(&self, bytes: &[u8]) -> Result<CounterSnapshot, CheckpointCodecError> {
        let value = bytes
            .try_into()
            .map(usize::from_le_bytes)
            .map_err(|_| CheckpointCodecError::message("invalid counter snapshot"))?;
        Ok(CounterSnapshot { value })
    }
}

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

struct Increment;

#[async_trait]
impl Node<CounterState> for Increment {
    async fn run(&self, _state: &CounterState, _context: &NodeContext) -> Result<usize, NodeError> {
        Ok(1)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = StateGraph::new();
    graph.set_version("counter-v1");
    graph.add_node("first", Increment)?;
    graph.add_node("second", Increment)?;
    graph
        .add_edge(START, "first")
        .add_edge("first", "second")
        .add_edge("second", END);
    let compiled = graph.compile()?;

    let checkpointer = Arc::new(InMemoryCheckpointer::new(CounterCodec));
    let interrupted = compiled
        .invoke_with_checkpoint(
            CounterState::default(),
            RunConfig::new(1),
            EventConfig::default(),
            RunControl::default(),
            CheckpointConfig::new(
                "resume-example",
                Arc::clone(&checkpointer) as Arc<dyn Checkpointer<CounterSnapshot>>,
                CheckpointPolicy::EverySuperstep,
            ),
        )
        .await;
    assert!(matches!(
        interrupted,
        Err(GraphRunError::MaxStepsExceeded { step: 2, .. })
    ));

    let report = compiled
        .resume(
            ResumeConfig::new(
                "resume-example",
                checkpointer as Arc<dyn Checkpointer<CounterSnapshot>>,
            )
            .with_run_config(RunConfig::new(1)),
        )
        .await?;

    assert_eq!(report.final_state().value, 2);
    assert_eq!(report.steps(), 2);
    println!("resumed final value: {}", report.final_state().value);
    println!("cumulative steps: {}", report.steps());
    Ok(())
}
